use super::config::RpcConfig;
use super::connection::handle_connection;
use super::stats::RpcStats;
use rockbee_protocol::{Request, Response, Result, ServiceRole};
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinSet;

pub struct RpcServer {
    listener: TcpListener,
    config: RpcConfig,
    role: ServiceRole,
}

impl RpcServer {
    #[cfg(test)]
    pub async fn bind(address: &str) -> Result<Self> {
        Self::bind_with_config(address, RpcConfig::default()).await
    }

    pub async fn bind_with_config(address: &str, config: RpcConfig) -> Result<Self> {
        Ok(Self {
            listener: TcpListener::bind(address).await?,
            config,
            role: ServiceRole::Unspecified,
        })
    }

    #[must_use]
    pub fn with_role(mut self, role: ServiceRole) -> Self {
        self.role = role;
        self
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.listener.local_addr()?)
    }

    pub async fn run_until<F, Fut>(
        self,
        handler: F,
        shutdown: impl Future<Output = Result<()>>,
    ) -> Result<()>
    where
        F: Fn(Request) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Response>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        let stats = Arc::new(RpcStats::new(self.role));
        let mut connections = JoinSet::new();
        let slots = Arc::new(Semaphore::new(self.config.max_connections()));
        let process_admissions = Arc::new(Semaphore::new(self.config.global_max_in_flight()));
        let (stop_connections, _) = watch::channel(false);
        tokio::pin!(shutdown);
        let result = loop {
            // Reap completed owners before deciding whether a new socket can enter.
            while let Some(finished) = connections.try_join_next() {
                report_connection(finished, &stats);
            }
            tokio::select! {
                biased;
                result = &mut shutdown => break result,
                finished = connections.join_next(), if !connections.is_empty() => {
                    if let Some(finished) = finished { report_connection(finished, &stats); }
                }
                accepted = self.listener.accept() => {
                    match accepted {
                        Ok((stream, _)) => {
                            let Ok(permit) = slots.clone().try_acquire_owned() else {
                                // No decoded request or ID exists yet. Close without replying,
                                // queueing a socket or spawning a waiting connection task.
                                drop(stream);
                                stats.rejected_connection();
                                continue;
                            };
                            let handler = Arc::clone(&handler);
                            let stop = stop_connections.subscribe();
                            let config = self.config;
                            let process_admissions = Arc::clone(&process_admissions);
                            let stats = Arc::clone(&stats);
                            connections.spawn(async move {
                                let _permit = permit;
                                let current_stats = Arc::clone(&stats);
                                let _current = current_stats.admitted_connection();
                                handle_connection(stream, handler, stop, config, process_admissions, stats).await
                            });
                        }
                        Err(error) => break Err(error.into()),
                    }
                }
            }
        };
        // Let internal owners abort AND drain their children before storage closes.
        stop_connections.send_replace(true);
        while connections.join_next().await.is_some() {}
        result
    }
}

fn report_connection(
    finished: std::result::Result<Result<()>, tokio::task::JoinError>,
    stats: &RpcStats,
) {
    match finished {
        Ok(Err(error)) => {
            stats.failed_connection();
            eprintln!("Connection failed: {error}");
        }
        Err(error) => {
            stats.failed_connection();
            eprintln!("Connection task failed: {error}");
        }
        _ => {}
    }
}
