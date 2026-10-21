//! One bounded logical initializer publishes the immutable group-zero directory.
use std::{num::NonZeroU64, time::Duration};

use log::{debug, info, warn};
use rockbee_raft_protocol::{
    FailureReason, GroupId, NodeId,
    backend::{BackendOperation, BackendReply, BackendRequest, BackendResponse, BootstrapOutcome},
    routing::ClusterDirectory,
    transport::{Link, LinkConfig},
};
use tokio::time::{Instant, sleep_until};

use super::super::{NodeError, NodeRuntime};

type BackendLink = Link<BackendRequest, BackendResponse>;

impl NodeRuntime {
    /// Publishes the canonical initial directory through the actual metadata leader.
    ///
    /// All nodes must share the same immutable initial configuration. Every
    /// uncertain attempt is reconciled by a linearizable directory read before
    /// another attempt. An absent read does not prove the old proposal is gone:
    /// repetition is safe because conditional initialization compares immutable
    /// origin and preserves later placements. A compatible directory read alone
    /// never completes bootstrap. Public writes are never retried by this method.
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
        let endpoint = self.config.origin()?.node(node_id).ok_or_else(|| {
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
            // The receiver validates the entire coherent metadata view against
            // its configured origin. A current later revision is legitimate;
            // conditional initialization below still establishes/verifies origin.
            Some(BackendReply::Directory(Ok(directory))) => {
                validate_returned_directory(&directory, self.config.origin()?)?;
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
            Some(BackendReply::Bootstrap(BootstrapOutcome::Ready { revision })) => {
                // An absent pre-read cannot bind caller and receiver origins.
                // Only a fresh authority read covering this applied revision
                // can finish initialization, using the same original deadline.
                match self
                    .bootstrap_request(link, node_id, BackendOperation::Directory, deadline)
                    .await
                {
                    Some(BackendReply::Directory(Ok(directory))) => {
                        validate_ready_directory(&directory, self.config.origin()?, revision)?;
                        Ok(true)
                    }
                    Some(BackendReply::Directory(Err(error)))
                        if error.0.reason == FailureReason::Internal =>
                    {
                        Err(NodeError::Config(error.0.message))
                    }
                    _ => Ok(false),
                }
            }
            Some(BackendReply::Bootstrap(BootstrapOutcome::Conflict(error))) => {
                Err(NodeError::Config(error.message))
            }
            Some(BackendReply::Bootstrap(BootstrapOutcome::NotSubmitted(error)))
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
                    cluster_id: self.config.cluster_id,
                    target_node: node_id,
                    operation,
                },
                deadline,
            )
            .ok()?;
        Some(pending.await.ok()?.reply)
    }
}

fn validate_ready_directory(
    current: &ClusterDirectory,
    configured_origin: &ClusterDirectory,
    ready_revision: NonZeroU64,
) -> Result<(), NodeError> {
    validate_returned_directory(current, configured_origin)?;
    if current.revision < ready_revision.get() {
        return Err(NodeError::Raft(
            "directory read does not cover initialized revision".into(),
        ));
    }
    Ok(())
}

fn validate_returned_directory(
    current: &ClusterDirectory,
    configured_origin: &ClusterDirectory,
) -> Result<(), NodeError> {
    // A canonical origin has the same three voters in every group. Its exact
    // endpoints and immutable metadata voters determine that origin even when
    // legitimate completed moves have changed the returned current directory.
    let metadata_voters = current.voters(GroupId::new(0));
    let initial_voters = configured_origin.voters(GroupId::new(0));
    let same_voters = match (metadata_voters, initial_voters) {
        (Some(current), Some(initial)) => {
            current.len() == initial.len() && initial.iter().all(|id| current.contains(id))
        }
        _ => false,
    };
    if current.cluster_id != configured_origin.cluster_id
        || current.shard_config != configured_origin.shard_config
        || !same_voters
        || configured_origin
            .nodes
            .iter()
            .any(|node| current.node(node.node_id) != Some(node))
        || (current.revision == 1 && current != configured_origin)
    {
        return Err(NodeError::Config(
            "returned directory differs from configured immutable origin".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use rockbee_raft_protocol::{ClusterId, ShardConfig, routing::NodeEndpoint};

    fn endpoint(id: u64) -> NodeEndpoint {
        NodeEndpoint {
            node_id: NodeId::new(id).unwrap(),
            peer_address: format!("127.0.0.1:{}", 17000 + id).parse().unwrap(),
            data_address: format!("127.0.0.1:{}", 18000 + id).parse().unwrap(),
        }
    }

    #[test]
    fn initializer_binds_immutable_origin_while_allowing_current_placement() {
        let initial = ClusterDirectory::initial(
            ClusterId::new([71; 16]).unwrap(),
            ShardConfig::new(1, 0).unwrap(),
            (1..=3).map(endpoint).collect(),
        )
        .unwrap();
        validate_returned_directory(&initial, &initial).unwrap();
        validate_ready_directory(&initial, &initial, NonZeroU64::new(1).unwrap()).unwrap();
        assert!(validate_ready_directory(&initial, &initial, NonZeroU64::new(2).unwrap()).is_err());
        let mut later = initial.clone();
        later.revision = 2;
        later.nodes.push(endpoint(4));
        later.placements[1].voters[2] = NodeId::new(4).unwrap();
        later.placements[0].voters.reverse();
        later.validate().unwrap();
        validate_returned_directory(&later, &initial).unwrap();
        validate_ready_directory(&later, &initial, NonZeroU64::new(1).unwrap()).unwrap();
        validate_ready_directory(&later, &initial, NonZeroU64::new(2).unwrap()).unwrap();
        for mismatch in 0..4 {
            let mut wrong = initial.clone();
            match mismatch {
                0 => wrong.cluster_id = ClusterId::new([72; 16]).unwrap(),
                1 => wrong.shard_config = ShardConfig::new(1, 99).unwrap(),
                2 => wrong.nodes[0].peer_address.set_port(19000),
                _ => {
                    wrong.nodes[2] = endpoint(5);
                    for placement in &mut wrong.placements {
                        placement.voters[2] = NodeId::new(5).unwrap();
                    }
                }
            }
            assert!(validate_returned_directory(&later, &wrong).is_err());
        }
        let mut legacy_mismatch = initial.clone();
        legacy_mismatch.nodes.reverse();
        legacy_mismatch.validate().unwrap();
        assert!(validate_returned_directory(&legacy_mismatch, &initial).is_err());
    }

    async fn metadata_leader(nodes: &[NodeRuntime], deadline: Instant) -> usize {
        tokio::time::timeout_at(deadline, async {
            loop {
                for (index, node) in nodes.iter().enumerate() {
                    if node.leader_hint(GroupId::new(0)) == Some(node.config.node_id) {
                        return index;
                    }
                }
                sleep_until(Instant::now() + Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("owned metadata quorum elects a leader")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn actual_initializer_rejects_different_receiver_origin_after_initial_absence() {
        let (_, roots, mut nodes) = start_nodes(74).await;
        let deadline = Instant::now() + Duration::from_secs(30);
        let leader = metadata_leader(&nodes, deadline).await;
        assert!(
            nodes[leader].service.groups[&GroupId::new(0)]
                .reader
                .read_metadata()
                .await
                .unwrap()
                .view
                .directory()
                .is_none()
        );
        let changed_endpoint = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        // Only the initializer's expected origin differs. The existing backend
        // receivers and private consensus addresses remain the real cluster.
        // This targets caller-side origin binding without any public test hook.
        let changed_follower = if leader == 2 { 1 } else { 2 };
        nodes[0].config.bootstrap_origin.as_mut().unwrap().nodes[changed_follower].data_address =
            changed_endpoint.local_addr().unwrap();
        nodes[0].config.origin().unwrap().validate().unwrap();
        let result = nodes[0].bootstrap_directory(deadline).await;
        for node in &mut nodes {
            node.close().await;
        }
        drop(nodes);
        drop(roots);
        assert!(
            matches!(result, Err(NodeError::Config(_))),
            "a Ready reply after an absent pre-read must still bind caller origin; got {result:?}"
        );
    }

    async fn start_nodes(
        cluster_byte: u8,
    ) -> (ClusterDirectory, Vec<tempfile::TempDir>, Vec<NodeRuntime>) {
        use crate::{config::NodeConfig, storage::OpenMode};
        use tokio::net::TcpListener;
        let mut listeners = Vec::new();
        let mut endpoints = Vec::new();
        for id in 1..=3 {
            let peer = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let data = TcpListener::bind("127.0.0.1:0").await.unwrap();
            endpoints.push(NodeEndpoint {
                node_id: NodeId::new(id).unwrap(),
                peer_address: peer.local_addr().unwrap(),
                data_address: data.local_addr().unwrap(),
            });
            listeners.push((peer, data));
        }
        let initial = ClusterDirectory::initial(
            ClusterId::new([cluster_byte; 16]).unwrap(),
            ShardConfig::new(1, 0).unwrap(),
            endpoints,
        )
        .unwrap();
        let mut roots = Vec::new();
        let mut nodes = Vec::new();
        for (index, (peer, data)) in listeners.into_iter().enumerate() {
            let root = tempfile::tempdir().unwrap();
            let config = NodeConfig::new(
                initial.clone(),
                initial.nodes[index].node_id,
                root.path().join("db"),
                OpenMode::CreateNew,
            )
            .unwrap();
            nodes.push(
                NodeRuntime::start_with_listeners(config, peer, data)
                    .await
                    .unwrap(),
            );
            roots.push(root);
        }
        nodes[0].initialize_groups().await.unwrap();
        (initial, roots, nodes)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn actual_initializer_adopts_legacy_and_preserves_fixture_revision_two() {
        use crate::{
            control::{DIRECTORY_KEY, MembershipObservation, MetadataCommand},
            types::{ApplyResult, Command},
        };
        use bytes::Bytes;
        use rockbee_raft_protocol::control::{CommittedLogPosition, MoveJobId, MoveReplica};
        use tokio::net::TcpListener;

        let deadline = Instant::now() + Duration::from_secs(30);
        let (initial, roots, mut nodes) = start_nodes(73).await;
        let leader = metadata_leader(&nodes, deadline).await;
        let group = &nodes[leader].service.groups[&GroupId::new(0)];
        group
            .raft
            .client_write(Command::Put {
                key: Bytes::from_static(DIRECTORY_KEY),
                value: Bytes::from(rockbee_raft_protocol::encode_frame(&initial).unwrap()),
            })
            .await
            .unwrap();
        assert!(
            group
                .reader
                .read_metadata()
                .await
                .unwrap()
                .view
                .origin()
                .is_none()
        );

        // The real initializer first sees a successful legacy directory read.
        // It must still reach the backend conditional producer and adopt origin.
        nodes[0].bootstrap_directory(deadline).await.unwrap();
        let leader = metadata_leader(&nodes, deadline).await;
        let group = &nodes[leader].service.groups[&GroupId::new(0)];
        assert_eq!(
            group.reader.read_metadata().await.unwrap().view.origin(),
            Some(&initial)
        );

        // These actual Raft proposals carry a trusted observation fixture.
        // No learner or membership movement is performed by this test.
        let fourth_peer = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let fourth_data = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let movement = MoveReplica {
            job_id: MoveJobId::new([4; 16]).unwrap(),
            data_group: GroupId::new(1),
            remove_node: NodeId::new(3).unwrap(),
            add_node: NodeEndpoint {
                node_id: NodeId::new(4).unwrap(),
                peer_address: fourth_peer.local_addr().unwrap(),
                data_address: fourth_data.local_addr().unwrap(),
            },
            expected_directory_revision: 1,
        }
        .derive_intent(&initial)
        .unwrap();
        group
            .raft
            .client_write(Command::Metadata(Box::new(MetadataCommand::BeginMove {
                intent: movement.clone(),
            })))
            .await
            .unwrap();
        let complete = group
            .raft
            .client_write(Command::Metadata(Box::new(MetadataCommand::CompleteMove {
                expected_directory_revision: 1,
                data_group: movement.data_group,
                job_id: movement.job_id,
                observation: MembershipObservation {
                    uniform: true,
                    voters: movement.target_voters,
                    replacement_endpoint: movement.add_node.clone(),
                    membership: CommittedLogPosition {
                        term: 7,
                        leader_node_id: NodeId::new(1).unwrap(),
                        index: 99,
                    },
                },
            })))
            .await
            .unwrap();
        assert!(matches!(
            complete.data,
            ApplyResult::Metadata(crate::control::MetadataResult::Completed { revision: 2, .. })
        ));
        let before = group.reader.read_metadata().await.unwrap();
        let before_applied = before.applied;
        let expected = before.view.into_directory().unwrap();
        assert_eq!(expected.revision, 2);
        assert_eq!(
            expected.voters(movement.data_group).unwrap(),
            movement.target_voters
        );

        // Real initializer and backend authority read must accept revision two,
        // then actually submit conditional initialization without resetting it.
        nodes[0].bootstrap_directory(deadline).await.unwrap();
        let leader = metadata_leader(&nodes, deadline).await;
        let after = nodes[leader].service.groups[&GroupId::new(0)]
            .reader
            .read_metadata()
            .await
            .unwrap();
        assert!(after.applied.map(|id| id.index) > before_applied.map(|id| id.index));
        assert_eq!(after.view.origin(), Some(&initial));
        assert_eq!(after.view.into_directory(), Some(expected));
        for node in &mut nodes {
            node.close().await;
        }
        drop(nodes);
        drop(roots);
    }
}
