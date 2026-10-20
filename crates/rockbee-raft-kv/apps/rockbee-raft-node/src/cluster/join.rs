//! Discovery observations never become local group-zero authority.
use super::NodeError;
use crate::config::JoinConfig;
use rockbee_raft_protocol::{
    backend::{BackendOperation, BackendReply, BackendRequest, BackendResponse},
    routing::ClusterDirectory,
    transport::{Link, LinkConfig},
};
use std::time::Duration;
use tokio::time::{Instant, sleep_until};

type BackendLink = Link<BackendRequest, BackendResponse>;

/// Each seed is only a candidate. Only a successful authoritative Directory
/// response with the expected immutable identity can admit local storage open.
/// Configure all ABC backends so an available metadata leader is among them.
pub(super) async fn discover_join_directory(
    config: &JoinConfig,
    deadline: Instant,
) -> Result<ClusterDirectory, NodeError> {
    while Instant::now() < deadline {
        for (index, seed) in config.seeds.iter().enumerate() {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            // A stalled candidate gets at most its fair share of the remaining
            // original budget, including when the total budget is below250ms.
            let candidates = u32::try_from(config.seeds.len() - index)
                .map_err(|_| NodeError::Config("too many join seeds".into()))?;
            let budget = (deadline - now) / candidates;
            let attempt_deadline = deadline.min(now + budget.min(Duration::from_millis(250)));
            let Ok(link) =
                BackendLink::connect(seed.address, LinkConfig::default(), attempt_deadline).await
            else {
                continue;
            };
            let response = match link.try_submit(
                BackendRequest {
                    cluster_id: config.cluster_id,
                    target_node: seed.node_id,
                    operation: BackendOperation::Directory,
                },
                attempt_deadline,
            ) {
                Ok(pending) => pending.await.ok(),
                Err(_) => None,
            };
            // Closing joins the owned workers on every response/error branch.
            link.close().await;
            if let Some(response) = response {
                // Link also verifies identity, kind and complete directory shape.
                if response.cluster_id != config.cluster_id || response.node_id != seed.node_id {
                    continue;
                }
                if let BackendReply::Directory(Ok(directory)) = response.reply {
                    validate_join_directory(config, &directory)?;
                    return Ok(directory);
                }
            }
        }
        sleep_until(deadline.min(Instant::now() + Duration::from_millis(10))).await;
    }
    Err(NodeError::Config(
        "join directory discovery deadline expired".into(),
    ))
}

fn validate_join_directory(
    config: &JoinConfig,
    directory: &ClusterDirectory,
) -> Result<(), NodeError> {
    directory.validate()?;
    if directory.cluster_id != config.cluster_id || directory.shard_config != config.shard_config {
        return Err(NodeError::Config(
            "join directory cluster/hash/shards differ from expected identity".into(),
        ));
    }
    let local = &config.local_endpoint;
    if directory
        .voters(rockbee_raft_protocol::GroupId::new(0))
        .is_some_and(|voters| voters.contains(&local.node_id))
    {
        return Err(NodeError::Config(
            "join identity is an existing metadata voter; use bootstrap startup".into(),
        ));
    }
    for node in &directory.nodes {
        if node.node_id == local.node_id {
            if node != local {
                return Err(NodeError::Config(
                    "published local endpoint differs from join endpoint".into(),
                ));
            }
        } else if [node.peer_address, node.data_address]
            .iter()
            .any(|address| *address == local.peer_address || *address == local.data_address)
        {
            return Err(NodeError::Config(
                "local endpoint collides with a published node".into(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::super::NodeRuntime;
    use super::{discover_join_directory, validate_join_directory};
    use crate::{
        config::{JoinConfig, JoinSeed, NodeConfig},
        rpc::PeerLimits,
        storage::{OpenMode, StorageOptions},
        types::Command,
    };
    use bytes::Bytes;
    use openraft::BasicNode;
    use rockbee_raft_protocol::{
        ClusterId, GroupId, KvRequest, KvResponse, MutationOutcome, NodeId, ShardConfig,
        backend::{
            BackendOperation, BackendReply, BackendRequest, BackendResponse, BootstrapOutcome,
        },
        routing::{ClusterDirectory, NodeEndpoint},
        transport::{
            Link, LinkConfig,
            server::{RequestHandler, Server, ServerConfig},
        },
    };
    use std::{sync::Arc, time::Duration};
    use tokio::{
        net::TcpListener,
        time::{Instant, sleep, timeout_at},
    };

    struct Cluster {
        nodes: Vec<NodeRuntime>,
        directory: ClusterDirectory,
        _roots: Vec<tempfile::TempDir>,
    }

    impl Cluster {
        async fn start() -> Self {
            let mut listeners = Vec::new();
            let mut endpoints = Vec::new();
            for id in 10..=12 {
                let peer = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let data = TcpListener::bind("127.0.0.1:0").await.unwrap();
                endpoints.push(NodeEndpoint {
                    node_id: NodeId::new(id).unwrap(),
                    peer_address: peer.local_addr().unwrap(),
                    data_address: data.local_addr().unwrap(),
                });
                listeners.push((peer, data));
            }
            let directory = ClusterDirectory::initial(
                ClusterId::new([81; 16]).unwrap(),
                ShardConfig::new(2, 0).unwrap(),
                endpoints,
            )
            .unwrap();
            let mut roots = Vec::new();
            let mut nodes = Vec::new();
            for (index, (peer, data)) in listeners.into_iter().enumerate() {
                let root = tempfile::tempdir().unwrap();
                let config = NodeConfig::new(
                    directory.clone(),
                    directory.nodes[index].node_id,
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
            nodes[0]
                .bootstrap_directory(Instant::now() + Duration::from_secs(30))
                .await
                .unwrap();
            Self {
                nodes,
                directory,
                _roots: roots,
            }
        }

        async fn leader(&self, group: GroupId) -> usize {
            timeout_at(Instant::now() + Duration::from_secs(20), async {
                loop {
                    for (index, node) in self.nodes.iter().enumerate() {
                        if node.leader_hint(group) == Some(node.config.node_id) {
                            return index;
                        }
                    }
                    sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap()
        }

        async fn close(&mut self) {
            for node in &mut self.nodes {
                node.close().await;
            }
        }
    }

    async fn join_config(
        directory: &ClusterDirectory,
        path: std::path::PathBuf,
    ) -> (JoinConfig, TcpListener, TcpListener) {
        let peer = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let data = TcpListener::bind("127.0.0.1:0").await.unwrap();
        (
            JoinConfig {
                local_endpoint: NodeEndpoint {
                    node_id: NodeId::new(1).unwrap(),
                    peer_address: peer.local_addr().unwrap(),
                    data_address: data.local_addr().unwrap(),
                },
                cluster_id: directory.cluster_id,
                shard_config: directory.shard_config,
                seeds: directory
                    .nodes
                    .iter()
                    .map(|node| JoinSeed {
                        node_id: node.node_id,
                        address: node.data_address,
                    })
                    .collect(),
                startup_timeout: Duration::from_secs(10),
                data_directory: path,
                open_mode: OpenMode::CreateNew,
                storage: StorageOptions::default(),
                peers: PeerLimits::default(),
                server: ServerConfig::default(),
            },
            peer,
            data,
        )
    }

    async fn request(
        endpoint: &NodeEndpoint,
        cluster_id: ClusterId,
        operation: BackendOperation,
    ) -> BackendReply {
        let deadline = Instant::now() + Duration::from_secs(2);
        let link = Link::<BackendRequest, BackendResponse>::connect(
            endpoint.data_address,
            LinkConfig::default(),
            deadline,
        )
        .await
        .unwrap();
        let result = link
            .try_submit(
                BackendRequest {
                    cluster_id,
                    target_node: endpoint.node_id,
                    operation,
                },
                deadline,
            )
            .unwrap()
            .await
            .unwrap();
        link.close().await;
        result.reply
    }

    async fn assert_no_authority(endpoint: &NodeEndpoint, cluster_id: ClusterId) {
        assert!(matches!(
            request(endpoint, cluster_id, BackendOperation::Directory).await,
            BackendReply::Directory(Err(_))
        ));
        assert!(matches!(
            request(endpoint, cluster_id, BackendOperation::BootstrapDirectory).await,
            BackendReply::Bootstrap(BootstrapOutcome::NotSubmitted(_))
        ));
        assert!(matches!(
            request(
                endpoint,
                cluster_id,
                BackendOperation::Kv {
                    group_id: GroupId::new(1),
                    request: KvRequest::Get {
                        key: Bytes::from_static(b"learner-key")
                    }
                }
            )
            .await,
            BackendReply::Kv {
                response: KvResponse::ReadError(_),
                ..
            }
        ));
        assert!(matches!(
            request(
                endpoint,
                cluster_id,
                BackendOperation::Kv {
                    group_id: GroupId::new(1),
                    request: KvRequest::Put {
                        key: Bytes::from_static(b"learner-key"),
                        value: Bytes::from_static(b"unauthorized")
                    }
                }
            )
            .await,
            BackendReply::Kv {
                response: KvResponse::Mutation(MutationOutcome::Rejected(_)),
                ..
            }
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn join_real_directory_hosts_only_uninitialized_data_groups() {
        let mut cluster = Cluster::start().await;
        let root = tempfile::tempdir().unwrap();
        let (config, peer, data) = join_config(&cluster.directory, root.path().join("db")).await;
        let local = config.local_endpoint.clone();
        let joined = NodeRuntime::start_join_with_listeners(config, peer, data).await;
        if joined.is_err() {
            cluster.close().await;
        }
        let mut joined = joined.expect("explicit join must discover the running ABC directory");
        assert!(!joined.service.groups.contains_key(&GroupId::new(0)));
        assert_eq!(joined.service.groups.len(), 2);
        for group in joined.service.groups.values() {
            assert!(!group.raft.is_initialized().await.unwrap());
            assert!(group.raft.metrics().borrow().last_log_index.is_none());
        }
        assert!(joined.initialize_groups().await.is_err());
        assert!(
            joined
                .bootstrap_directory(Instant::now() + Duration::from_secs(1))
                .await
                .is_err()
        );
        assert_no_authority(&local, cluster.directory.cluster_id).await;
        let leader = cluster.leader(GroupId::new(0)).await;
        assert_eq!(
            request(
                &cluster.directory.nodes[leader],
                cluster.directory.cluster_id,
                BackendOperation::Directory
            )
            .await,
            BackendReply::Directory(Ok(cluster.directory.clone()))
        );
        joined.close().await;
        cluster.close().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn join_real_learner_replicates_and_reopens_without_becoming_voter() {
        let mut cluster = Cluster::start().await;
        let root = tempfile::tempdir().unwrap();
        let (mut config, peer, data) =
            join_config(&cluster.directory, root.path().join("db")).await;
        let local = config.local_endpoint.clone();
        let mut joined = NodeRuntime::start_join_with_listeners(config.clone(), peer, data)
            .await
            .unwrap();
        let id = GroupId::new(1);
        let leader = cluster.leader(id).await;
        let group = &cluster.nodes[leader].service.groups[&id];
        // Real Raft learner membership and replication; no trusted observation
        // or direct D-state fixture is used here. ABC voters remain unchanged.
        let learner = group
            .raft
            .add_learner(
                local.node_id.get(),
                BasicNode::new(local.peer_address.to_string()),
                false,
            )
            .await
            .unwrap();
        let key = Bytes::from_static(b"learner-key");
        assert_eq!(
            cluster.directory.shard_config.group_for_key(&key).unwrap(),
            id
        );
        let value = Bytes::from_static(b"durably-replicated");
        let write = group
            .raft
            .client_write(Command::Put {
                key: key.clone(),
                value: value.clone(),
            })
            .await
            .unwrap();
        timeout_at(Instant::now() + Duration::from_secs(20), async {
            loop {
                let read = joined.service.groups[&id]
                    .reader
                    .read_local(key.clone())
                    .await
                    .unwrap();
                let covers = |required: crate::types::LogId| {
                    read.applied.is_some_and(|applied| {
                        applied == required
                            || (applied.index > required.index
                                && applied.leader_id >= required.leader_id)
                    })
                };
                if read.value == Some(value.clone())
                    && covers(learner.log_id)
                    && covers(write.log_id)
                {
                    break;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("real learner must durably apply replicated bytes");
        assert_eq!(
            group
                .raft
                .metrics()
                .borrow()
                .membership_config
                .membership()
                .voter_ids()
                .collect::<Vec<_>>(),
            vec![10, 11, 12]
        );
        assert_eq!(
            joined.service.groups[&id].raft.metrics().borrow().state,
            openraft::ServerState::Learner
        );
        assert_no_authority(&local, cluster.directory.cluster_id).await;
        let before_close = joined.service.groups[&id]
            .reader
            .read_local(key.clone())
            .await
            .unwrap();
        joined.close().await;
        drop(joined);
        // Inspect actual durable D state while its peer/data listeners are down.
        // Networking reopen must not repair lost bytes before this assertion.
        {
            use crate::storage::{Storage, StorageIdentity};
            use openraft::storage::RaftStateMachine;
            let storage = Storage::open(
                config.data_directory.clone(),
                StorageIdentity {
                    cluster_id: config.cluster_id,
                    node_id: config.local_endpoint.node_id,
                    shard_config: config.shard_config,
                },
                config.storage.clone(),
                OpenMode::OpenExisting,
            )
            .await
            .unwrap();
            let mut stored = storage.open_group(id).unwrap();
            let offline = stored
                .state_machine
                .reader()
                .read_local(key.clone())
                .await
                .unwrap();
            assert_eq!(offline.value, Some(value.clone()));
            assert_eq!(offline.applied, before_close.applied);
            let (applied, membership) = stored.state_machine.applied_state().await.unwrap();
            assert_eq!(applied, before_close.applied);
            assert_eq!(*membership.log_id(), Some(learner.log_id));
            assert_eq!(
                membership.membership().voter_ids().collect::<Vec<_>>(),
                vec![10, 11, 12]
            );
            assert_eq!(
                membership.membership().learner_ids().collect::<Vec<_>>(),
                vec![local.node_id.get()]
            );
            assert_eq!(
                membership.membership().get_node(&local.node_id.get()),
                Some(&BasicNode::new(local.peer_address.to_string()))
            );
        }
        config.open_mode = OpenMode::OpenExisting;
        let mut reopened = NodeRuntime::start_join(config).await.unwrap();
        let read = reopened.service.groups[&id]
            .reader
            .read_local(key)
            .await
            .unwrap();
        assert_eq!(read.value, Some(value));
        assert_eq!(read.applied, before_close.applied);
        assert!(!reopened.service.groups.contains_key(&GroupId::new(0)));
        assert!(reopened.initialize_groups().await.is_err());
        assert_no_authority(&local, cluster.directory.cluster_id).await;
        let leader = cluster.leader(GroupId::new(0)).await;
        assert_eq!(
            request(
                &cluster.directory.nodes[leader],
                cluster.directory.cluster_id,
                BackendOperation::Directory
            )
            .await,
            BackendReply::Directory(Ok(cluster.directory.clone()))
        );
        reopened.close().await;
        cluster.close().await;
    }

    fn fixture_directory() -> ClusterDirectory {
        ClusterDirectory::initial(
            ClusterId::new([82; 16]).unwrap(),
            ShardConfig::new(2, 0).unwrap(),
            (10..=12)
                .map(|id| NodeEndpoint {
                    node_id: NodeId::new(id).unwrap(),
                    peer_address: format!("127.0.0.1:{}", 17000 + id).parse().unwrap(),
                    data_address: format!("127.0.0.1:{}", 18000 + id).parse().unwrap(),
                })
                .collect(),
        )
        .unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn join_learner_receives_streamed_snapshot_after_source_logs_are_purged() {
        use crate::storage::{Storage, StorageIdentity};
        use openraft::storage::RaftStateMachine;

        let mut cluster = Cluster::start().await;
        let id = GroupId::new(1);
        let leader = cluster.leader(id).await;
        let group = &cluster.nodes[leader].service.groups[&id];
        let keys: Vec<Bytes> = (0..100)
            .map(|index| Bytes::from(format!("snapshot-key-{index}")))
            .filter(|key| cluster.directory.shard_config.group_for_key(key).unwrap() == id)
            .take(8)
            .collect();
        assert_eq!(keys.len(), 8);
        // More than one configured 256 KiB snapshot chunk; use distinct keys
        // and values so a missing chunk cannot masquerade as successful replay.
        for (index, key) in keys.iter().enumerate() {
            group
                .raft
                .client_write(Command::Put {
                    key: key.clone(),
                    value: Bytes::from(vec![u8::try_from(index).unwrap(); 64 * 1024]),
                })
                .await
                .unwrap();
        }
        group
            .raft
            .client_write(Command::Delete {
                key: keys[0].clone(),
            })
            .await
            .unwrap();
        let updated = Bytes::from_static(b"replaced-before-snapshot");
        let last = group
            .raft
            .client_write(Command::Put {
                key: keys[1].clone(),
                value: updated.clone(),
            })
            .await
            .unwrap();
        group.raft.trigger().snapshot().await.unwrap();
        timeout_at(Instant::now() + Duration::from_secs(20), async {
            loop {
                if group.raft.metrics().borrow().snapshot == Some(last.log_id) {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("leader must build the actual snapshot before purging");
        group
            .raft
            .trigger()
            .purge_log(last.log_id.index)
            .await
            .unwrap();
        timeout_at(Instant::now() + Duration::from_secs(20), async {
            loop {
                if group.raft.metrics().borrow().purged == Some(last.log_id) {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("source must purge the log prefix before D joins");
        let source_snapshot = group
            .raft
            .get_snapshot()
            .await
            .unwrap()
            .expect("source snapshot must be retained");
        assert!(
            source_snapshot.snapshot.metadata().await.unwrap().len()
                > group.raft.config().snapshot_max_chunk_size
        );
        assert!(
            cluster.nodes[leader]
                ._storage
                .test_log_prefix_is_empty(id, last.log_id.index)
                .await
                .unwrap()
        );
        // D is not a cluster member yet: the deleted source history cannot be
        // recovered by ordinary append replay to this fresh, different node.
        let root = tempfile::tempdir().unwrap();
        let (config, peer, data) = join_config(&cluster.directory, root.path().join("db")).await;
        let local = config.local_endpoint.clone();
        let mut joined = NodeRuntime::start_join_with_listeners(config.clone(), peer, data)
            .await
            .unwrap();
        assert!(
            joined.service.groups[&id]
                .raft
                .metrics()
                .borrow()
                .last_log_index
                .is_none()
        );
        let learner = group
            .raft
            .add_learner(
                local.node_id.get(),
                BasicNode::new(local.peer_address.to_string()),
                false,
            )
            .await
            .unwrap();
        let tail_value = Bytes::from_static(b"written-after-snapshot");
        let tail = group
            .raft
            .client_write(Command::Put {
                key: keys[2].clone(),
                value: tail_value.clone(),
            })
            .await
            .unwrap();
        timeout_at(Instant::now() + Duration::from_secs(20), async {
            loop {
                let metrics = joined.service.groups[&id].raft.metrics().borrow().clone();
                if metrics.snapshot == Some(last.log_id)
                    && metrics.last_applied.is_some_and(|applied| {
                        applied == tail.log_id
                            || (applied.index > tail.log_id.index
                                && applied.leader_id >= tail.log_id.leader_id)
                    })
                {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("new learner must install the streamed snapshot and apply its tail");
        let mut expected = Vec::new();
        for (index, key) in keys.iter().enumerate() {
            let value = match index {
                0 => None,
                1 => Some(updated.clone()),
                2 => Some(tail_value.clone()),
                _ => Some(Bytes::from(vec![u8::try_from(index).unwrap(); 64 * 1024])),
            };
            let read = joined.service.groups[&id]
                .reader
                .read_local(key.clone())
                .await
                .unwrap();
            assert_eq!(read.value, value, "snapshot/tail value for key {index}");
            expected.push((key.clone(), value));
        }
        let before_close = joined.service.groups[&id]
            .reader
            .read_local(keys[2].clone())
            .await
            .unwrap()
            .applied;
        assert_eq!(
            joined.service.groups[&id].raft.metrics().borrow().state,
            openraft::ServerState::Learner
        );
        joined.close().await;
        drop(joined);
        // Reopen only the store: no networking can repair missing snapshot or
        // tail data before the durability assertions below.
        {
            let storage = Storage::open(
                config.data_directory,
                StorageIdentity {
                    cluster_id: config.cluster_id,
                    node_id: local.node_id,
                    shard_config: config.shard_config,
                },
                config.storage,
                OpenMode::OpenExisting,
            )
            .await
            .unwrap();
            let mut stored = storage.open_group(id).unwrap();
            for (key, value) in expected {
                let read = stored.state_machine.reader().read_local(key).await.unwrap();
                assert_eq!(read.value, value);
                assert_eq!(read.applied, before_close);
            }
            let (applied, membership) = stored.state_machine.applied_state().await.unwrap();
            assert_eq!(applied, before_close);
            assert_eq!(*membership.log_id(), Some(learner.log_id));
            assert_eq!(
                membership.membership().voter_ids().collect::<Vec<_>>(),
                vec![10, 11, 12]
            );
            assert_eq!(
                membership.membership().learner_ids().collect::<Vec<_>>(),
                vec![local.node_id.get()]
            );
            let snapshot = stored
                .state_machine
                .get_current_snapshot()
                .await
                .unwrap()
                .expect("received snapshot must survive reopen");
            assert_eq!(snapshot.meta.last_log_id, Some(last.log_id));
        }
        cluster.close().await;
    }

    #[tokio::test]
    async fn join_invalid_settings_fail_before_database_creation() {
        let directory = fixture_directory();
        let root = tempfile::tempdir().unwrap();
        let (base, _peer, _data) = join_config(&directory, root.path().join("db")).await;
        for case in 0..13 {
            let mut config = base.clone();
            match case {
                0 => config.seeds.clear(),
                1 => config.seeds = vec![config.seeds[0]; 17],
                2 => config.seeds[1].node_id = config.seeds[0].node_id,
                3 => config.seeds[1].address = config.seeds[0].address,
                4 => config.startup_timeout = Duration::ZERO,
                5 => config.startup_timeout = Duration::from_secs(61),
                6 => config.local_endpoint.peer_address.set_port(0),
                7 => config.local_endpoint.data_address = config.local_endpoint.peer_address,
                8 => config.seeds[0].address.set_port(0),
                9 => config.storage.io_chunk_entries = 0,
                10 => config.peers.max_frame_bytes = 0,
                11 => config.server.max_connections = 0,
                _ => config.seeds[0].node_id = config.local_endpoint.node_id,
            }
            assert!(NodeRuntime::start_join(config).await.is_err(), "case{case}");
            assert!(
                !base.data_directory.exists(),
                "invalid join must not create storage"
            );
        }
        // Data-only join validates generic resources, not ABC metadata effects.
        let mut data_only = base;
        data_only.storage.max_entry_bytes = 4096;
        data_only.storage.io_chunk_bytes =
            data_only.storage.metadata_minimum_chunk_bytes().unwrap() - 1;
        assert!(data_only.storage.validate().is_ok());
        data_only.validate().unwrap();
    }

    struct FixtureBackend {
        response: BackendResponse,
    }
    impl RequestHandler for FixtureBackend {
        type Request = BackendRequest;
        type Response = BackendResponse;
        async fn handle(&self, _request: BackendRequest, _deadline: Instant) -> BackendResponse {
            self.response.clone()
        }
    }

    async fn fixture_server(directory: &ClusterDirectory) -> (Server, JoinSeed) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed = JoinSeed {
            node_id: directory.nodes[0].node_id,
            address: listener.local_addr().unwrap(),
        };
        let server = Server::start(
            listener,
            ServerConfig::default(),
            Arc::new(FixtureBackend {
                response: BackendResponse {
                    cluster_id: directory.cluster_id,
                    node_id: seed.node_id,
                    reply: BackendReply::Directory(Ok(directory.clone())),
                },
            }),
        )
        .unwrap();
        (server, seed)
    }

    #[tokio::test]
    async fn join_accepts_revision_two_published_data_node_and_rejects_collisions_before_open() {
        // Trusted current-directory fixture; this is not membership movement.
        let mut directory = fixture_directory();
        let root = tempfile::tempdir().unwrap();
        let (config, peer, data) = join_config(&directory, root.path().join("db")).await;
        directory.revision = 2;
        directory.nodes.push(config.local_endpoint.clone());
        directory.placements[1].voters[2] = config.local_endpoint.node_id;
        directory.validate().unwrap();
        let (mut server, seed) = fixture_server(&directory).await;
        let mut valid = config.clone();
        valid.seeds = vec![seed];
        let mut joined = NodeRuntime::start_join_with_listeners(valid, peer, data)
            .await
            .unwrap();
        assert!(!joined.service.groups.contains_key(&GroupId::new(0)));
        joined.close().await;
        drop(joined);
        server.close().await;

        for case in 0..7 {
            let root = tempfile::tempdir().unwrap();
            let (mut candidate, peer, data) = join_config(&directory, root.path().join("db")).await;
            let mut published = directory.clone();
            published.nodes[3] = candidate.local_endpoint.clone();
            match case {
                0 => candidate.shard_config = ShardConfig::new(2, 999).unwrap(),
                1 => published.nodes[3].peer_address.set_port(20001),
                2 => published.nodes[3].data_address.set_port(20002),
                3 => {
                    published.nodes.pop();
                    candidate.local_endpoint.peer_address = published.nodes[0].data_address;
                    published.placements[1].voters[2] = published.nodes[2].node_id;
                }
                4 => {
                    published.nodes.pop();
                    candidate.local_endpoint.data_address = published.nodes[0].peer_address;
                    published.placements[1].voters[2] = published.nodes[2].node_id;
                }
                5 => {
                    published.nodes.pop();
                    published.placements[1].voters[2] = published.nodes[2].node_id;
                    candidate.local_endpoint = published.nodes[1].clone();
                }
                _ => candidate.shard_config = ShardConfig::new(1, 0).unwrap(),
            }
            let (mut server, seed) = fixture_server(&published).await;
            candidate.seeds = vec![seed];
            let path = candidate.data_directory.clone();
            // For catalog collisions the listener addresses intentionally differ;
            // test actual discovery before opening through the named path.
            drop(peer);
            drop(data);
            let result = NodeRuntime::start_join(candidate).await;
            if case == 5 {
                assert!(
                    matches!(result, Err(super::NodeError::Config(ref message)) if message.contains("metadata voter"))
                );
            } else {
                assert!(result.is_err(), "case{case}");
            }
            assert!(!path.exists());
            server.close().await;
        }
    }

    #[tokio::test]
    async fn join_discovery_short_budget_skips_blackhole_and_closes_owned_connection() {
        use tokio::io::AsyncReadExt;
        let directory = fixture_directory();
        let root = tempfile::tempdir().unwrap();
        let (mut config, _peer, _data) = join_config(&directory, root.path().join("db")).await;
        let blackhole = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let blackhole_seed = JoinSeed {
            node_id: NodeId::new(99).unwrap(),
            address: blackhole.local_addr().unwrap(),
        };
        let worker = tokio::spawn(async move {
            let (mut stream, _) = blackhole.accept().await.unwrap();
            let mut bytes = Vec::new();
            stream.read_to_end(&mut bytes).await.unwrap();
        });
        let (mut server, seed) = fixture_server(&directory).await;
        config.seeds = vec![blackhole_seed, seed];
        config.startup_timeout = Duration::from_millis(200);
        let started = Instant::now();
        let found = discover_join_directory(&config, started + config.startup_timeout)
            .await
            .unwrap();
        assert_eq!(found, directory);
        assert!(started.elapsed() < Duration::from_millis(200));
        timeout_at(Instant::now() + Duration::from_secs(1), worker)
            .await
            .unwrap()
            .unwrap();
        server.close().await;
    }

    #[tokio::test]
    async fn join_discovery_rejects_wrong_identity_kind_and_malformed_directory_with_finite_budget()
    {
        let directory = fixture_directory();
        let root = tempfile::tempdir().unwrap();
        let (base, _peer, _data) = join_config(&directory, root.path().join("db")).await;
        for case in 0..5 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let mut response = BackendResponse {
                cluster_id: directory.cluster_id,
                node_id: directory.nodes[0].node_id,
                reply: BackendReply::Directory(Ok(directory.clone())),
            };
            match case {
                0 => response.cluster_id = ClusterId::new([83; 16]).unwrap(),
                1 => response.node_id = NodeId::new(99).unwrap(),
                2 => {
                    response.reply = BackendReply::Leader {
                        group_id: GroupId::new(0),
                        leader: Some(directory.nodes[0].node_id),
                    }
                }
                3 => {
                    let mut malformed = directory.clone();
                    malformed.revision = 0;
                    response.reply = BackendReply::Directory(Ok(malformed));
                }
                _ => {
                    let mut wrong = directory.clone();
                    wrong.cluster_id = ClusterId::new([84; 16]).unwrap();
                    response.reply = BackendReply::Directory(Ok(wrong));
                }
            }
            let mut config = base.clone();
            config.startup_timeout = Duration::from_millis(80);
            config.seeds = vec![JoinSeed {
                node_id: directory.nodes[0].node_id,
                address: listener.local_addr().unwrap(),
            }];
            let mut server = Server::start(
                listener,
                ServerConfig::default(),
                Arc::new(FixtureBackend { response }),
            )
            .unwrap();
            let started = Instant::now();
            assert!(NodeRuntime::start_join(config).await.is_err());
            assert!(started.elapsed() < Duration::from_secs(1));
            assert!(!base.data_directory.exists());
            server.close().await;
        }
        // A non-listening owned endpoint also exhausts only the original budget.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = base;
        config.seeds = vec![JoinSeed {
            node_id: directory.nodes[0].node_id,
            address: listener.local_addr().unwrap(),
        }];
        config.startup_timeout = Duration::from_millis(40);
        drop(listener);
        assert!(NodeRuntime::start_join(config).await.is_err());
    }

    #[tokio::test]
    async fn join_directory_guard_allows_absent_d_but_rejects_metadata_voter_identity() {
        let directory = fixture_directory();
        let root = tempfile::tempdir().unwrap();
        let (mut config, _peer, _data) = join_config(&directory, root.path().join("db")).await;
        validate_join_directory(&config, &directory).unwrap();
        config.local_endpoint = directory.nodes[1].clone();
        assert!(validate_join_directory(&config, &directory).is_err());
        assert!(!config.data_directory.exists());
    }
    #[tokio::test]
    async fn join_reopen_checks_durable_cluster_node_and_hash_identity() {
        use crate::storage::{Storage, StorageIdentity};
        let directory = fixture_directory();
        for case in 0..3 {
            let root = tempfile::tempdir().unwrap();
            let (mut config, peer, data) = join_config(&directory, root.path().join("db")).await;
            let mut identity = StorageIdentity {
                cluster_id: config.cluster_id,
                node_id: config.local_endpoint.node_id,
                shard_config: config.shard_config,
            };
            match case {
                0 => identity.cluster_id = ClusterId::new([99; 16]).unwrap(),
                1 => identity.node_id = NodeId::new(2).unwrap(),
                _ => identity.shard_config = ShardConfig::new(2, 99).unwrap(),
            }
            drop(
                Storage::open(
                    config.data_directory.clone(),
                    identity,
                    StorageOptions::default(),
                    OpenMode::CreateNew,
                )
                .await
                .unwrap(),
            );
            let (mut server, seed) = fixture_server(&directory).await;
            config.seeds = vec![seed];
            config.open_mode = OpenMode::OpenExisting;
            assert!(matches!(
                NodeRuntime::start_join_with_listeners(config, peer, data).await,
                Err(super::NodeError::Storage(_))
            ));
            server.close().await;
        }
    }

    #[tokio::test]
    async fn join_metadata_voter_rejected_after_discovery_for_both_open_intents() {
        let directory = fixture_directory();
        for mode in [OpenMode::CreateNew, OpenMode::OpenExisting] {
            let root = tempfile::tempdir().unwrap();
            let (mut config, peer, data) = join_config(&directory, root.path().join("db")).await;
            config.local_endpoint = directory.nodes[1].clone();
            config.open_mode = mode;
            let path = config.data_directory.clone();
            let (mut server, seed) = fixture_server(&directory).await;
            config.seeds = vec![seed];
            drop(peer);
            drop(data);
            assert!(
                matches!(NodeRuntime::start_join(config).await, Err(super::NodeError::Config(ref message)) if message.contains("metadata voter"))
            );
            assert!(!path.exists());
            server.close().await;
        }
    }
}
