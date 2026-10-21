//! Outer ownership for reply-loss startup and shutdown.

use super::{
    backend_proxy::{BackendProxyReceipt, BackendReplyLossProxy},
    proxy::{ProxyReceipt, ReplyLossProxy},
};
use crate::cluster::{NodeError, NodeRuntime};
use rockbee_raft_client::RockBeeRaftClient;
use rockbee_raft_gateway::{GatewayError, GatewayRuntime};
use std::future::Future;
use tokio::task::JoinHandle;

/// Keeps every runtime owner outside the deadline-bounded observation task.
/// Startup handles remain owned until they yield either a runtime or a stored
/// failure; cleanup never aborts a Node/Gateway constructor.
pub(super) struct Resources {
    pub(super) nodes: Vec<NodeRuntime>,
    pub(super) roots: Vec<tempfile::TempDir>,
    pub(super) gateway: Option<GatewayRuntime>,
    pub(super) proxy: Option<ReplyLossProxy>,
    pub(super) backend_proxies: Vec<BackendReplyLossProxy>,
    pub(super) original: Option<RockBeeRaftClient>,
    pub(super) fresh: Option<RockBeeRaftClient>,
    pending_node: Option<JoinHandle<Result<NodeRuntime, NodeError>>>,
    pending_gateway: Option<JoinHandle<Result<GatewayRuntime, GatewayError>>>,
    node_startup_failure: Option<String>,
    gateway_startup_failure: Option<String>,
}

impl Resources {
    pub(super) fn new() -> Self {
        Self {
            nodes: Vec::new(),
            roots: Vec::new(),
            gateway: None,
            proxy: None,
            backend_proxies: Vec::new(),
            original: None,
            fresh: None,
            pending_node: None,
            pending_gateway: None,
            node_startup_failure: None,
            gateway_startup_failure: None,
        }
    }

    pub(super) fn begin_node<F>(&mut self, startup: F)
    where
        F: Future<Output = Result<NodeRuntime, NodeError>> + Send + 'static,
    {
        assert!(
            self.pending_node.is_none(),
            "one owned node startup at a time"
        );
        self.pending_node = Some(tokio::spawn(startup));
    }

    pub(super) async fn finish_node(&mut self) -> Result<(), String> {
        let result = self
            .pending_node
            .as_mut()
            .expect("owned node startup required")
            .await;
        self.pending_node.take();
        match result {
            Ok(Ok(node)) => self.nodes.push(node),
            Ok(Err(error)) => return Err(self.remember_node_failure(error.to_string())),
            Err(error) => {
                return Err(
                    self.remember_node_failure(format!("node startup task failed: {error}"))
                );
            }
        }
        Ok(())
    }

    pub(super) fn begin_gateway<F>(&mut self, startup: F)
    where
        F: Future<Output = Result<GatewayRuntime, GatewayError>> + Send + 'static,
    {
        assert!(
            self.pending_gateway.is_none(),
            "one owned gateway startup at a time"
        );
        self.pending_gateway = Some(tokio::spawn(startup));
    }

    pub(super) async fn finish_gateway(&mut self) -> Result<(), String> {
        let result = self
            .pending_gateway
            .as_mut()
            .expect("owned gateway startup required")
            .await;
        self.pending_gateway.take();
        match result {
            Ok(Ok(gateway)) => self.gateway = Some(gateway),
            Ok(Err(error)) => return Err(self.remember_gateway_failure(error.to_string())),
            Err(error) => {
                return Err(
                    self.remember_gateway_failure(format!("gateway startup task failed: {error}"))
                );
            }
        }
        Ok(())
    }

    fn remember_node_failure(&mut self, failure: String) -> String {
        self.node_startup_failure
            .get_or_insert_with(|| failure.clone());
        failure
    }

    fn remember_gateway_failure(&mut self, failure: String) -> String {
        self.gateway_startup_failure
            .get_or_insert_with(|| failure.clone());
        failure
    }

    /// Finishes constructors, then closes application owners in dependency order.
    pub(super) async fn cleanup(&mut self) -> Cleanup {
        let pending_node_joined = match self.pending_node.is_some() {
            true => self.finish_node().await.is_ok(),
            false => self.node_startup_failure.is_none(),
        };
        let pending_gateway_joined = match self.pending_gateway.is_some() {
            true => self.finish_gateway().await.is_ok(),
            false => self.gateway_startup_failure.is_none(),
        };
        if let Some(client) = self.fresh.take() {
            client.close().await;
        }
        if let Some(client) = self.original.take() {
            client.close().await;
        }
        let proxy = match self.proxy.as_mut() {
            Some(proxy) => proxy.close().await,
            None => Ok(ProxyReceipt::default()),
        };
        self.proxy.take();
        if let Some(mut gateway) = self.gateway.take() {
            gateway.close().await;
        }
        // Backend relays remain available until the gateway has joined its
        // links. Only then can their final counts and EOF receipts be read.
        let mut backend_proxies = Vec::new();
        for proxy in &mut self.backend_proxies {
            backend_proxies.push(proxy.close().await);
        }
        self.backend_proxies.clear();
        for node in &mut self.nodes {
            node.close().await;
        }
        let client_write_calls = self
            .nodes
            .iter()
            .map(|node| node.service().test_client_write_calls())
            .sum();
        Cleanup {
            proxy,
            backend_proxies,
            client_write_calls,
            pending_node_joined,
            pending_gateway_joined,
            node_startup_failure: self.node_startup_failure.take(),
            gateway_startup_failure: self.gateway_startup_failure.take(),
        }
    }
}

pub(super) struct Cleanup {
    pub(super) proxy: Result<ProxyReceipt, String>,
    pub(super) backend_proxies: Vec<Result<BackendProxyReceipt, String>>,
    pub(super) client_write_calls: usize,
    pub(super) pending_node_joined: bool,
    pub(super) pending_gateway_joined: bool,
    pub(super) node_startup_failure: Option<String>,
    pub(super) gateway_startup_failure: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use tokio::sync::{Mutex, Notify};

    #[tokio::test]
    async fn consumed_constructor_panics_remain_visible_to_cleanup() {
        let mut resources = Resources::new();
        resources.begin_node(async {
            panic!("controlled node constructor panic");
        });
        assert!(resources.finish_node().await.is_err());
        resources.begin_gateway(async {
            panic!("controlled gateway constructor panic");
        });
        assert!(resources.finish_gateway().await.is_err());
        let cleanup = resources.cleanup().await;
        assert!(!cleanup.pending_node_joined);
        assert!(
            cleanup
                .node_startup_failure
                .as_deref()
                .is_some_and(|message| message.contains("startup task failed"))
        );
        assert!(!cleanup.pending_gateway_joined);
        assert!(
            cleanup
                .gateway_startup_failure
                .as_deref()
                .is_some_and(|message| message.contains("startup task failed"))
        );
    }

    #[tokio::test]
    async fn pending_constructor_survives_observer_abort_until_owner_cleanup() {
        let release_constructor = Arc::new(Notify::new());
        let observer_started = Arc::new(Notify::new());
        let constructor_started = Arc::new(Notify::new());
        let constructor_completed = Arc::new(AtomicBool::new(false));
        let mut resources = Resources::new();
        let release = Arc::clone(&release_constructor);
        let started = Arc::clone(&constructor_started);
        let completed = Arc::clone(&constructor_completed);
        resources.begin_node(async move {
            started.notify_one();
            release.notified().await;
            completed.store(true, Ordering::Release);
            Err(NodeError::Config(
                "controlled delayed startup failure".into(),
            ))
        });
        let owner = Arc::new(Mutex::new(resources));
        let observer_owner = Arc::clone(&owner);
        let started = Arc::clone(&observer_started);
        let observer = tokio::spawn(async move {
            let mut owner = observer_owner.lock().await;
            started.notify_one();
            owner.finish_node().await
        });
        observer_started.notified().await;
        constructor_started.notified().await;
        tokio::task::yield_now().await;
        observer.abort();
        let _ = observer.await;
        release_constructor.notify_one();
        let mutex = match Arc::try_unwrap(owner) {
            Ok(mutex) => mutex,
            Err(_) => panic!("aborted observer releases owner lock"),
        };
        let mut resources = mutex.into_inner();
        let cleanup = resources.cleanup().await;
        assert!(constructor_completed.load(Ordering::Acquire));
        assert!(!cleanup.pending_node_joined);
        assert!(
            cleanup
                .node_startup_failure
                .as_deref()
                .is_some_and(|message| message.contains("controlled delayed startup failure"))
        );
    }
}
