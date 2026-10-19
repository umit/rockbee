//! One bounded logical initializer publishes the immutable group-zero directory.
use std::time::Duration;

use log::{debug, info, warn};
use rockbee_raft_protocol::{
    FailureReason, GroupId, MutationOutcome, NodeId,
    backend::{BackendOperation, BackendReply, BackendRequest, BackendResponse},
    transport::{Link, LinkConfig},
};
use tokio::time::{Instant, sleep_until};

use super::{NodeError, NodeRuntime};

type BackendLink = Link<BackendRequest, BackendResponse>;

impl NodeRuntime {
    /// Publishes the canonical initial directory through the actual metadata leader.
    ///
    /// All nodes must share the same immutable initial configuration. Every
    /// uncertain attempt is reconciled by a linearizable directory read before
    /// another attempt. An absent read does not prove the old proposal is gone:
    /// repetition is safe only because all bootstrap proposals contain the same
    /// bytes and directory updates are currently disabled. Public writes are
    /// never retried by this method.
    pub async fn bootstrap_directory(&self, deadline: Instant) -> Result<(), NodeError> {
        self.require_initializer()?;
        info!(
            "raft_directory_bootstrap_start node={}",
            self.config.node_id.get()
        );
        while Instant::now() < deadline {
            if let Some(node_id) = self.leader_hint(GroupId::new(0)) {
                let attempt_deadline = deadline.min(Instant::now() + Duration::from_secs(1));
                if self.bootstrap_attempt(node_id, attempt_deadline).await? {
                    info!(
                        "raft_directory_bootstrap_complete node={}",
                        self.config.node_id.get()
                    );
                    return Ok(());
                }
            }
            sleep_until(deadline.min(Instant::now() + Duration::from_millis(25))).await;
        }
        warn!(
            "raft_directory_bootstrap_deadline node={}",
            self.config.node_id.get()
        );
        Err(NodeError::Raft(
            "metadata bootstrap deadline expired; publication may still complete".into(),
        ))
    }

    async fn bootstrap_attempt(
        &self,
        node_id: NodeId,
        deadline: Instant,
    ) -> Result<bool, NodeError> {
        let endpoint = self.config.directory.node(node_id).ok_or_else(|| {
            NodeError::Config("metadata leader is absent from initial directory".into())
        })?;
        let Ok(link) =
            BackendLink::connect(endpoint.data_address, LinkConfig::default(), deadline).await
        else {
            debug!(
                "raft_directory_bootstrap_connect_failed node={} target={}",
                self.config.node_id.get(),
                node_id.get()
            );
            return Ok(false);
        };
        let result = self.reconcile_or_publish(&link, node_id, deadline).await;
        link.close().await;
        result
    }

    async fn reconcile_or_publish(
        &self,
        link: &BackendLink,
        node_id: NodeId,
        deadline: Instant,
    ) -> Result<bool, NodeError> {
        // Read first even after transport loss. Never infer non-commit from a
        // timeout, or from a read that happens before an earlier proposal applies.
        match self
            .bootstrap_request(link, node_id, BackendOperation::Directory, deadline)
            .await
        {
            Some(BackendReply::Directory(Ok(directory))) => {
                return if directory == self.config.directory {
                    Ok(true)
                } else {
                    Err(NodeError::Config(
                        "stored directory differs from immutable bootstrap".into(),
                    ))
                };
            }
            Some(BackendReply::Directory(Err(error)))
                if error.0.reason == FailureReason::Internal =>
            {
                return Err(NodeError::Config(error.0.message));
            }
            Some(BackendReply::Directory(Err(_))) => {}
            _ => return Ok(false),
        }
        match self
            .bootstrap_request(
                link,
                node_id,
                BackendOperation::BootstrapDirectory,
                deadline,
            )
            .await
        {
            Some(BackendReply::Bootstrap(MutationOutcome::Committed)) => Ok(true),
            Some(BackendReply::Bootstrap(MutationOutcome::Rejected(error)))
                if error.reason == FailureReason::Internal =>
            {
                Err(NodeError::Config(error.message))
            }
            // Safe rejection or uncertainty: the next attempt reconciles first.
            _ => Ok(false),
        }
    }

    async fn bootstrap_request(
        &self,
        link: &BackendLink,
        node_id: NodeId,
        operation: BackendOperation,
        deadline: Instant,
    ) -> Option<BackendReply> {
        let pending = link
            .try_submit(
                BackendRequest {
                    cluster_id: self.config.directory.cluster_id,
                    target_node: node_id,
                    operation,
                },
                deadline,
            )
            .ok()?;
        Some(pending.await.ok()?.reply)
    }
}
