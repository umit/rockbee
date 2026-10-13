use crate::config::Config;
use crate::protocol::{Request, Response, ServiceRole};
use crate::rpc::{RpcServer, wait_for_shutdown};
use crate::{Result, RpcConfig, Store};
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

/// Storage endpoint. Shared RPC owns connections; Store owns database behavior.
pub struct Server {
    rpc: RpcServer,
    store: Arc<Store>,
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

impl Server {
    /// Reserve the RPC address before opening storage and acquiring a writer epoch.
    ///
    /// A bind failure leaves the configured database untouched. If storage cannot
    /// open, the reserved listener is dropped without announcing readiness.
    pub async fn from_config(config: Config) -> Result<Self> {
        config.memory.validate()?;
        let rpc = RpcServer::bind_with_config(&config.address, config.rpc)
            .await?
            .with_role(service_role(config.role));
        let objects = config.backend.connect()?;
        let store = Store::open_with_memory_and_reader_config(
            config.role,
            &config.prefix,
            objects,
            config.memory,
            config.reader,
        )
        .await?;
        Ok(Self {
            rpc,
            store: Arc::new(store),
        })
    }

    pub async fn bind(address: &str, store: Store) -> Result<Self> {
        Self::bind_with_rpc(address, store, RpcConfig::default()).await
    }

    /// On bind failure, close the supplied store and report any close error too.
    pub async fn bind_with_rpc(address: &str, store: Store, config: RpcConfig) -> Result<Self> {
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
                    async move { handle_request(&store, request).await }
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

async fn handle_request(store: &Store, request: Request) -> Result<Response> {
    let operation = match &request {
        Request::Put { .. } => "put",
        Request::PutWithTtl { .. } => "put_with_ttl",
        Request::Get { .. } => "get",
        Request::Delete { .. } => "delete",
        Request::Status => "status",
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
            Request::Status => Err("Status must be handled locally by RPC".into()),
        }
    }
    .await;
    if let Err(error) = &result {
        tracing::warn!(operation, role = ?store.role(), error = %error, "storage request failed");
    }
    result
}

fn service_role(role: crate::storage::Role) -> ServiceRole {
    match role {
        crate::storage::Role::Writer => ServiceRole::Writer,
        crate::storage::Role::Reader => ServiceRole::Reader,
    }
}

#[cfg(test)]
mod tests;
