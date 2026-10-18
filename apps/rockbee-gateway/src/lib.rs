//! Stateless client entry point: writes to the writer, eventual reads to readers.
pub mod config;
pub mod router;
mod upstream;

pub use rockbee_protocol::Result;
use rockbee_protocol::ServiceRole;
pub use rockbee_rpc::RpcConfig;
use rockbee_rpc::{RpcServer, wait_for_shutdown};
use router::Router;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

/// Stateless client-facing server that routes mutations to one writer and
/// KV/document reads and SQL queries to independently configured readers.
///
/// Readers can lag durable writes. The gateway keeps no key/value cache; data
/// caching belongs to SlateDB. Mutations are never automatically replayed.
pub struct Gateway {
    rpc: RpcServer,
    router: Arc<Router>,
}

impl Gateway {
    /// Reserves the listening address with default upstream and RPC limits.
    /// Call [`Self::run`] or [`Self::run_until`] to start serving requests.
    /// Binding validates topology but does not establish backend health.
    pub async fn bind(address: &str, writer: String, readers: Vec<String>) -> Result<Self> {
        Self::bind_with_config(
            address,
            writer,
            readers,
            config::UpstreamConfig::default(),
            RpcConfig::default(),
        )
        .await
    }

    /// Reserves the listener with explicit upstream deadlines and admission limits,
    /// while retaining default limits for incoming client RPC connections.
    pub async fn bind_with_upstream(
        address: &str,
        writer: String,
        readers: Vec<String>,
        upstream: config::UpstreamConfig,
    ) -> Result<Self> {
        Self::bind_with_config(address, writer, readers, upstream, RpcConfig::default()).await
    }

    /// Reserves the listener with separate limits for incoming RPC and backend requests.
    ///
    /// # Errors
    /// Returns an error for an invalid topology, invalid limits, or a bind failure.
    pub async fn bind_with_config(
        address: &str,
        writer: String,
        readers: Vec<String>,
        upstream: config::UpstreamConfig,
        rpc: RpcConfig,
    ) -> Result<Self> {
        let router = Arc::new(Router::new_with_upstream(writer, readers, upstream)?);
        Ok(Self {
            rpc: RpcServer::bind_with_config(address, rpc)
                .await?
                .with_role(ServiceRole::Gateway),
            router,
        })
    }

    /// Returns the actual listener address, including an OS-assigned port when bound to `:0`.
    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.rpc.local_addr()
    }
    /// Serves requests until an OS shutdown signal and drains RPC work before exiting.
    pub async fn run(self) -> Result<()> {
        self.run_with_ready(|| {}).await
    }

    /// Run until an OS shutdown signal, announcing readiness after signal registration.
    ///
    /// The callback should return promptly. Readiness covers the local listener
    /// and shutdown handling; it does not check backend health or reader freshness.
    pub async fn run_with_ready(self, ready: impl FnOnce()) -> Result<()> {
        self.run_until(wait_for_shutdown(ready)).await
    }

    /// Serves until the supplied shutdown future completes, then drains RPC work.
    /// This owns the gateway listener, not the separately running storage nodes.
    pub async fn run_until(self, shutdown: impl Future<Output = Result<()>>) -> Result<()> {
        self.rpc
            .run_until(
                move |request| {
                    let router = Arc::clone(&self.router);
                    async move { router.route(request).await }
                },
                shutdown,
            )
            .await
    }
}
