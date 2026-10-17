use crate::config::Config;
use crate::protocol::{Request, Response, ServiceRole};
use crate::rpc::{RpcServer, wait_for_shutdown};
use crate::{Result, Role, RpcConfig};
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

/// A concrete role owns all storage behavior; the runtime only serves requests.
pub trait Storage: Send + Sync + 'static {
    fn open_config(config: &Config) -> impl Future<Output = Result<Self>> + Send
    where
        Self: Sized;
    fn role(&self) -> Role;
    fn put(&self, key: &[u8], value: &[u8]) -> impl Future<Output = Result<()>> + Send;
    fn put_with_ttl(
        &self,
        key: &[u8],
        value: &[u8],
        ttl_millis: u64,
    ) -> impl Future<Output = Result<()>> + Send;
    fn get(&self, key: &[u8]) -> impl Future<Output = Result<Option<Vec<u8>>>> + Send;
    fn delete(&self, key: &[u8]) -> impl Future<Output = Result<()>> + Send;
    fn table(
        &self,
        request: &crate::protocol::table::TableRequest,
    ) -> impl Future<Output = Result<Response>> + Send;
    fn close(&self) -> impl Future<Output = Result<()>> + Send;
}

/// Storage endpoint. Shared RPC owns connections; each concrete role owns storage behavior.
pub struct Server<S: Storage> {
    rpc: RpcServer,
    store: Arc<S>,
}

#[derive(Debug)]
struct RpcAndCloseError {
    rpc: Box<dyn Error + Send + Sync>,
    close: Box<dyn Error + Send + Sync>,
}

impl fmt::Display for RpcAndCloseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "RPC failed: {}; storage close also failed: {}",
            self.rpc, self.close
        )
    }
}

impl Error for RpcAndCloseError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.rpc.as_ref())
    }
}

impl<S: Storage> Server<S> {
    /// Reserve the RPC address before opening storage and acquiring a writer epoch.
    ///
    /// A bind failure leaves the configured database untouched. If storage cannot
    /// open, the reserved listener is dropped without announcing readiness.
    pub async fn from_config(config: Config) -> Result<Self> {
        config.memory.validate()?;
        let rpc = RpcServer::bind_with_config(&config.address, config.rpc)
            .await?
            .with_role(service_role(config.role));
        let store = S::open_config(&config).await?;
        Ok(Self {
            rpc,
            store: Arc::new(store),
        })
    }

    pub async fn bind(address: &str, store: S) -> Result<Self> {
        Self::bind_with_rpc(address, store, RpcConfig::default()).await
    }

    /// On bind failure, close the supplied store and report any close error too.
    pub async fn bind_with_rpc(address: &str, store: S, config: RpcConfig) -> Result<Self> {
        let role = service_role(store.role());
        let rpc = match RpcServer::bind_with_config(address, config).await {
            Ok(rpc) => rpc.with_role(role),
            Err(rpc) => {
                return Err(match store.close().await {
                    Ok(()) => rpc,
                    Err(close) => Box::new(RpcAndCloseError { rpc, close }),
                });
            }
        };
        Ok(Self {
            rpc,
            store: Arc::new(store),
        })
    }

    pub fn storage(&self) -> &Arc<S> {
        &self.store
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.rpc.local_addr()
    }
    pub async fn run(self) -> Result<()> {
        self.run_with_ready(|| {}).await
    }

    /// Run until an OS shutdown signal, announcing readiness after signal registration.
    ///
    /// The callback should return promptly. It does not wait for reader freshness,
    /// and shutdown still uses the same RPC cancellation and storage close path.
    pub async fn run_with_ready(self, ready: impl FnOnce()) -> Result<()> {
        self.run_until(wait_for_shutdown(ready)).await
    }

    /// Close storage after the RPC loop exits, including an error from `shutdown`.
    /// If both stages fail, the returned diagnostic includes both errors.
    pub async fn run_until(self, shutdown: impl Future<Output = Result<()>>) -> Result<()> {
        let store = Arc::clone(&self.store);
        let result = self
            .rpc
            .run_until(
                move |request| {
                    let store = Arc::clone(&store);
                    async move { handle_request(store.as_ref(), request).await }
                },
                shutdown,
            )
            .await;
        let closed = self.store.close().await;
        match (result, closed) {
            (Err(rpc), Err(close)) => Err(Box::new(RpcAndCloseError { rpc, close })),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Ok(()), Ok(())) => Ok(()),
        }
    }
}

async fn handle_request<S: Storage>(store: &S, request: Request) -> Result<Response> {
    let operation = match &request {
        Request::Put { .. } => "put",
        Request::PutWithTtl { .. } => "put_with_ttl",
        Request::Get { .. } => "get",
        Request::Delete { .. } => "delete",
        Request::Status => "status",
        Request::Table(_) => "table",
    };
    let result = async {
        match request {
            Request::Put { key, value } => {
                store.put(&key, &value).await?;
                Ok(Response::Ok)
            }
            Request::PutWithTtl {
                key,
                value,
                ttl_millis,
            } => {
                store.put_with_ttl(&key, &value, ttl_millis).await?;
                Ok(Response::Ok)
            }
            Request::Get { key } => Ok(Response::Value {
                value: store.get(&key).await?,
            }),
            Request::Delete { key } => {
                store.delete(&key).await?;
                Ok(Response::Ok)
            }
            Request::Table(request) => store.table(&request).await,
            Request::Status => Err("Status must be handled locally by RPC".into()),
        }
    }
    .await;
    if let Err(error) = &result {
        tracing::warn!(operation, role = ?store.role(), error = %error, "storage request failed");
    }
    result
}

fn service_role(role: Role) -> ServiceRole {
    match role {
        Role::Writer => ServiceRole::Writer,
        Role::Reader => ServiceRole::Reader,
    }
}
