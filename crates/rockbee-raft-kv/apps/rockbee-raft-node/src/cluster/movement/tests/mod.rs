#![allow(clippy::unwrap_used, clippy::panic)]

mod delayed_executor;
mod failure;
pub(in crate::cluster::movement) mod handoff;
mod intermediate_process;
mod joint_partition;
pub(in crate::cluster::movement) mod native_membership_observation;
pub(in crate::cluster::movement) mod stale_source;
mod stale_source_tests;

use super::*;
use crate::{
    cluster::NodeRuntime,
    config::{JoinConfig, JoinSeed, NodeConfig},
    rpc::PeerLimits,
    storage::{OpenMode, StorageOptions},
};
use bytes::Bytes;
use rockbee_raft_client::{ClientConfig, RockBeeRaftClient};
use rockbee_raft_gateway::{GatewayConfig, GatewayRuntime, SeedNode};
use rockbee_raft_protocol::{
    ClusterId, NodeId, ShardConfig,
    control::{MoveJobId, MoveProgress},
    routing::{ClusterDirectory, NodeEndpoint},
    transport::server::ServerConfig,
};
use rockbee_raft_protocol::{KvRequest, KvResponse, MutationOutcome};
use std::time::Duration;
use tokio::{net::TcpListener, time::sleep};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runtime_move_accepts_real_abc_and_explicit_d() {
    let _ = env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("rockbee_raft_node=debug"),
    )
    .is_test(true)
    .try_init();
    let mut listeners = Vec::new();
    let mut endpoints = Vec::new();
    for id in 1..=4 {
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
        ClusterId::new([87; 16]).unwrap(),
        ShardConfig::new(2, 0).unwrap(),
        endpoints[..3].to_vec(),
    )
    .unwrap();
    let mut roots = Vec::new();
    let mut nodes = Vec::new();
    for endpoint in &endpoints[..3] {
        let root = tempfile::tempdir().unwrap();
        let (peer, data) = listeners.remove(0);
        let config = NodeConfig::new(
            directory.clone(),
            endpoint.node_id,
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
    let deadline = Instant::now() + Duration::from_secs(30);
    nodes[0].bootstrap_directory(deadline).await.unwrap();
    let root = tempfile::tempdir().unwrap();
    let (peer, data) = listeners.remove(0);
    let config = JoinConfig {
        local_endpoint: endpoints[3].clone(),
        cluster_id: directory.cluster_id,
        shard_config: directory.shard_config,
        seeds: endpoints[..3]
            .iter()
            .map(|node| JoinSeed {
                node_id: node.node_id,
                address: node.data_address,
            })
            .collect(),
        startup_timeout: Duration::from_secs(10),
        data_directory: root.path().join("db"),
        open_mode: OpenMode::CreateNew,
        storage: StorageOptions::default(),
        peers: PeerLimits::default(),
        server: ServerConfig::default(),
    };
    nodes.push(
        NodeRuntime::start_join_with_listeners(config, peer, data)
            .await
            .unwrap(),
    );
    roots.push(root);
    let leader = loop {
        if let Some(id) = nodes
            .iter()
            .find_map(|node| node.leader_hint(GroupId::new(0)))
        {
            break usize::try_from(id.get() - 1).unwrap();
        }
        assert!(Instant::now() < deadline);
        sleep(Duration::from_millis(10)).await;
    };
    // This gateway and public client remain running across the movement.
    let public_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let public_address = public_listener.local_addr().unwrap();
    let gateway_config = GatewayConfig {
        cluster_id: directory.cluster_id,
        expected_shard_config: directory.shard_config,
        seeds: directory
            .nodes
            .iter()
            .map(|node| SeedNode {
                node_id: node.node_id,
                address: node.data_address,
            })
            .collect(),
        listen_address: public_address,
        backend_link: LinkConfig::default(),
        server: ServerConfig::default(),
        startup_timeout: Duration::from_secs(10),
    };
    let mut gateway = GatewayRuntime::start_with_listener(gateway_config, public_listener)
        .await
        .unwrap();
    let client = RockBeeRaftClient::connect(public_address, ClientConfig::default())
        .await
        .unwrap();
    let keys: Vec<_> = (0..100)
        .map(|index| Bytes::from(format!("movement-data-{index}")))
        .filter(|key| directory.shard_config.group_for_key(key).unwrap() == GroupId::new(1))
        .take(2)
        .collect();
    for key in &keys {
        assert_eq!(
            client
                .put(
                    key.clone(),
                    Bytes::from_static(b"preserved across membership movement")
                )
                .unwrap()
                .await,
            MutationOutcome::Committed
        );
    }
    assert_eq!(
        client.delete(keys[1].clone()).unwrap().await,
        MutationOutcome::Committed
    );
    assert_eq!(
        client.get(keys[0].clone()).await.unwrap(),
        Some(Bytes::from_static(b"preserved across membership movement"))
    );
    let request = MoveReplica {
        job_id: MoveJobId::new([9; 16]).unwrap(),
        data_group: GroupId::new(1),
        remove_node: NodeId::new(3).unwrap(),
        add_node: endpoints[3].clone(),
        expected_directory_revision: 1,
    };
    let result = backend(
        &nodes[leader],
        BackendOperation::MoveReplica(request.clone()),
        deadline,
    )
    .await;
    assert!(
        matches!(
            result,
            BackendReply::Move {
                result: AdminResult::Accepted { .. } | AdminResult::AlreadyAccepted { .. },
                ..
            }
        ),
        "runtime must accept a real move, got {result:?}"
    );
    let job_id = request.job_id;
    let group = request.data_group;
    let controller = nodes
        .iter()
        .position(|node| node.leader_hint(group) == Some(node.config.node_id))
        .unwrap();
    let controller_id = nodes[controller].config.node_id;
    let restart = NodeConfig::new(
        directory.clone(),
        controller_id,
        roots[controller].path().join("db"),
        OpenMode::OpenExisting,
    )
    .unwrap();
    let pause = Arc::new(TestPause::default());
    *nodes[controller]
        .service
        .movement
        .after_catchup
        .lock()
        .unwrap() = Some(Arc::clone(&pause));
    let publication_pause = Arc::new(TestPause::default());
    *nodes[controller]
        .service
        .movement
        .before_publication
        .lock()
        .unwrap() = Some(Arc::clone(&publication_pause));

    // Full runner capacity rejects scheduling immediately without creating work.
    let capacity = Arc::clone(&nodes[controller].service.movement.active)
        .try_acquire_many_owned(4)
        .unwrap();
    assert!(matches!(
        backend(
            &nodes[controller],
            BackendOperation::ReconcileMove {
                data_group: group,
                job_id
            },
            deadline
        )
        .await,
        BackendReply::Reconcile {
            result: Ok(ReconcileStatus::Busy),
            ..
        }
    ));
    drop(capacity);
    assert!(matches!(
        backend(
            &nodes[controller],
            BackendOperation::ReconcileMove {
                data_group: group,
                job_id
            },
            deadline
        )
        .await,
        BackendReply::Reconcile {
            result: Ok(ReconcileStatus::Scheduled),
            ..
        }
    ));
    // backend() has closed the caller's actual TCP link. The runner still owns
    // the catch-up lifecycle and its process permit.
    tokio::time::timeout_at(deadline, pause.reached.notified())
        .await
        .unwrap();
    assert_eq!(
        nodes[controller]
            .service
            .movement
            .active
            .available_permits(),
        3
    );
    assert!(matches!(
        backend(
            &nodes[controller],
            BackendOperation::ReconcileMove {
                data_group: group,
                job_id
            },
            deadline
        )
        .await,
        BackendReply::Reconcile {
            result: Ok(ReconcileStatus::Running),
            ..
        }
    ));
    let wrong_job = backend(
        &nodes[controller],
        BackendOperation::ReconcileMove {
            data_group: group,
            job_id: MoveJobId::new([88; 16]).unwrap(),
        },
        deadline,
    )
    .await;
    assert!(
        matches!(wrong_job,BackendReply::Reconcile{result:Err(ReadFailure(ref error)),..} if error.reason==FailureReason::InvalidRequest),
        "a different job must not be reported as the active runner: {wrong_job:?}"
    );
    let captured_vote = nodes[controller].service.groups[&group]
        .raft
        .metrics()
        .borrow()
        .vote;
    let other = nodes
        .iter()
        .position(|node| node.config.node_id != controller_id && node.config.node_id.get() <= 3)
        .unwrap();
    force_authority(&nodes, other, group, deadline).await;
    force_authority(&nodes, controller, group, deadline).await;
    assert_ne!(
        nodes[controller].service.groups[&group]
            .raft
            .metrics()
            .borrow()
            .vote,
        captured_vote
    );
    pause.release.notify_one();
    loop {
        let finished = nodes[controller]
            .service
            .movement
            .tasks
            .lock()
            .unwrap()
            .get(&group)
            .unwrap()
            .task
            .is_finished();
        if finished {
            break;
        }
        assert!(Instant::now() < deadline);
        sleep(Duration::from_millis(10)).await;
    }
    let source = nodes[controller]
        .service
        .observe_membership(group, deadline)
        .await
        .unwrap();
    assert_eq!(
        source.voters,
        request.derive_intent(&directory).unwrap().old_voters,
        "old catch-up proof must not promote under a new vote"
    );
    assert_eq!(source.joint_voters, None);
    assert!(matches!(
        backend(
            &nodes[controller],
            BackendOperation::ReconcileMove {
                data_group: group,
                job_id
            },
            deadline
        )
        .await,
        BackendReply::Reconcile {
            result: Ok(ReconcileStatus::Scheduled),
            ..
        }
    ));
    tokio::time::timeout_at(deadline, publication_pause.reached.notified())
        .await
        .unwrap();
    let pending = metadata_state(&nodes, group, deadline).await;
    assert_eq!(pending.directory.revision, 1);
    assert!(matches!(
        pending.record.unwrap().progress,
        MoveProgress::Pending
    ));
    let uniform = nodes[controller]
        .service
        .observe_membership(group, deadline)
        .await
        .unwrap();
    assert_eq!(uniform.joint_voters, None);
    assert_eq!(
        uniform.voters,
        request.derive_intent(&directory).unwrap().target_voters
    );

    let d_before_shutdown = nodes
        .iter()
        .find(|node| node.config.node_id == request.add_node.node_id)
        .unwrap();
    loop {
        let (applied, membership) = d_before_shutdown.service.groups[&group]
            .reader
            .read_membership()
            .await
            .unwrap();
        if membership.membership().get_joint_config().len() == 1
            && membership.voter_ids().collect::<Vec<_>>() == vec![1, 2, 4]
            && position(membership.log_id().unwrap()).unwrap() == uniform.membership
            && covers(applied, *membership.log_id())
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "D must apply uniform membership before controlled controller shutdown"
        );
        sleep(Duration::from_millis(10)).await;
    }

    // Stop the controller after uniform ABD but before directory publication.
    // Its tracked runner exits after the hook; a fresh D-leader runner must use
    // retained metadata seeds and skip learner catch-up regardless of self metrics.
    nodes[controller].service.movement.stop();
    publication_pause.release.notify_one();
    nodes[controller].close().await;
    assert!(
        nodes[controller]
            .service
            .movement
            .tasks
            .lock()
            .unwrap()
            .is_empty()
    );
    drop(nodes.remove(controller));
    let d = nodes
        .iter()
        .position(|node| node.config.node_id.get() == 4)
        .unwrap();
    force_authority(&nodes, d, group, deadline).await;
    for node in &nodes {
        node.service.groups[&group]
            .raft
            .runtime_config()
            .elect(true);
        node.service.groups[&group]
            .raft
            .runtime_config()
            .heartbeat(true);
    }
    let d_authority = nodes[d]
        .service
        .observe_membership(group, deadline)
        .await
        .unwrap();
    assert_eq!(d_authority.joint_voters, None);
    assert_eq!(
        d_authority.voters,
        request.derive_intent(&directory).unwrap().target_voters
    );
    assert!(matches!(
        backend(
            &nodes[d],
            BackendOperation::ReconcileMove {
                data_group: group,
                job_id
            },
            deadline
        )
        .await,
        BackendReply::Reconcile {
            result: Ok(ReconcileStatus::Scheduled),
            ..
        }
    ));
    loop {
        if Instant::now() >= deadline {
            for node in &nodes {
                for group in [GroupId::new(0), GroupId::new(1)] {
                    let hosted = &node.service.groups.get(&group);
                    if let Some(hosted) = hosted {
                        eprintln!(
                            "move deadline node={} group={} metrics={:?}",
                            node.config.node_id.get(),
                            group.get(),
                            *hosted.raft.metrics().borrow()
                        );
                        eprintln!(
                            "move deadline local membership={:?}",
                            hosted.reader.read_membership().await.unwrap()
                        );
                    }
                }
            }
            for node in &mut nodes {
                node.close().await;
            }
            panic!("movement must complete before deadline");
        }
        let state = metadata_state(&nodes, group, deadline).await;
        if let Some(record) = state.record
            && let MoveProgress::Completed { membership } = record.progress
        {
            assert_eq!(state.directory.revision, 2);
            assert_eq!(
                state.directory.voters(group).unwrap(),
                [
                    NodeId::new(1).unwrap(),
                    NodeId::new(2).unwrap(),
                    NodeId::new(4).unwrap()
                ]
            );
            assert_eq!(
                state.directory.voters(GroupId::new(0)),
                directory.voters(GroupId::new(0))
            );
            assert_eq!(
                state.directory.voters(GroupId::new(2)),
                directory.voters(GroupId::new(2))
            );
            let data_leader = nodes
                .iter()
                .find(|node| node.leader_hint(group) == Some(node.config.node_id))
                .unwrap();
            assert!(
                matches!(backend(data_leader,BackendOperation::Kv{group_id:group,request:KvRequest::Get{key:keys[0].clone()}},deadline).await,BackendReply::Kv{response:KvResponse::Value(Some(value)),..} if value==Bytes::from_static(b"preserved across membership movement"))
            );
            assert!(matches!(
                backend(
                    data_leader,
                    BackendOperation::Kv {
                        group_id: group,
                        request: KvRequest::Get {
                            key: keys[1].clone()
                        }
                    },
                    deadline
                )
                .await,
                BackendReply::Kv {
                    response: KvResponse::Value(None),
                    ..
                }
            ));
            for unchanged in [GroupId::new(0), GroupId::new(2)] {
                // G1's completed move does not imply that G0/G2 have finished
                // electing after the controller stopped. Wait for real authority
                // before checking their unchanged membership exactly once.
                let leader = wait_for_group_authority(&nodes, unchanged, deadline).await;
                let hosted = &leader.service.groups[&unchanged];
                let (_, membership) = hosted.reader.read_membership().await.unwrap();
                assert_eq!(membership.membership().get_joint_config().len(), 1);
                assert_eq!(membership.voter_ids().collect::<Vec<_>>(), vec![1, 2, 3]);
            }
            let observed = data_leader
                .service
                .observe_membership(group, deadline)
                .await
                .unwrap();
            assert_eq!(observed.joint_voters, None);
            assert_eq!(
                completion_position(observed.membership).unwrap(),
                membership
            );
            assert_eq!(
                observed.peer(NodeId::new(4).unwrap()),
                Some(endpoints[3].peer_address)
            );
            // The same running gateway still cached revision one and the
            // stopped A route. This strong GET must discover D at revision
            // two; later writes use the refreshed route without replay.
            assert_eq!(
                client.get(keys[0].clone()).await.unwrap(),
                Some(Bytes::from_static(b"preserved across membership movement"))
            );
            assert_eq!(client.get(keys[1].clone()).await.unwrap(), None);
            assert_eq!(
                client
                    .put(
                        keys[0].clone(),
                        Bytes::from_static(b"D is the observed leader")
                    )
                    .unwrap()
                    .await,
                MutationOutcome::Committed
            );
            assert_eq!(
                client.get(keys[0].clone()).await.unwrap(),
                Some(Bytes::from_static(b"D is the observed leader"))
            );
            assert_eq!(
                client.delete(keys[0].clone()).unwrap().await,
                MutationOutcome::Committed
            );
            assert_eq!(client.get(keys[0].clone()).await.unwrap(), None);
            let metadata_leader = nodes
                .iter()
                .find(|node| node.leader_hint(GroupId::new(0)) == Some(node.config.node_id))
                .unwrap();
            let same = backend(
                metadata_leader,
                BackendOperation::MoveReplica(request.clone()),
                deadline,
            )
            .await;
            assert!(
                matches!(
                    same,
                    BackendReply::Move {
                        result: AdminResult::Completed { .. },
                        ..
                    }
                ),
                "same original request after publication: {same:?}"
            );
            let mut different = request.clone();
            different.job_id = MoveJobId::new([10; 16]).unwrap();
            assert!(matches!(
                backend(
                    metadata_leader,
                    BackendOperation::MoveReplica(different),
                    deadline
                )
                .await,
                BackendReply::Move {
                    result: AdminResult::Conflict { .. },
                    ..
                }
            ));
            break;
        }
        sleep(Duration::from_millis(25)).await;
    }
    // The original controller reopens its actual durable database and catches
    // up the completed metadata job without replacing its immutable origin.
    nodes.push(NodeRuntime::start(restart).await.unwrap());
    loop {
        let reopened = nodes.last().unwrap();
        let state = reopened.service.groups[&GroupId::new(0)]
            .reader
            .read_metadata()
            .await
            .unwrap();
        if matches!(
            state.view.job(group).map(|record| &record.progress),
            Some(MoveProgress::Completed { .. })
        ) {
            assert_eq!(state.view.directory().unwrap().revision, 2);
            break;
        }
        assert!(Instant::now() < deadline);
        sleep(Duration::from_millis(10)).await;
    }
    client.close().await;
    gateway.close().await;
    for node in &mut nodes {
        node.close().await;
    }
}

async fn wait_for_group_authority(
    nodes: &[NodeRuntime],
    group: GroupId,
    deadline: Instant,
) -> &NodeRuntime {
    loop {
        for node in nodes {
            let Some(hosted) = node.service.groups.get(&group) else {
                continue;
            };
            let attempt = deadline.min(Instant::now() + Duration::from_millis(500));
            if node.leader_hint(group) == Some(node.config.node_id)
                && authority_barrier(group, hosted, attempt).await.is_ok()
            {
                return node;
            }
        }
        assert!(
            Instant::now() < deadline,
            "group {} must regain quorum authority after controller shutdown",
            group.get()
        );
        sleep_until(deadline.min(Instant::now() + Duration::from_millis(10))).await;
    }
}

pub(super) async fn backend(
    node: &NodeRuntime,
    operation: BackendOperation,
    deadline: Instant,
) -> BackendReply {
    let link = BackendLink::connect(
        node.config.endpoint.data_address,
        LinkConfig::default(),
        deadline,
    )
    .await
    .unwrap();
    let response = link
        .try_submit(
            BackendRequest {
                cluster_id: node.config.cluster_id,
                target_node: node.config.node_id,
                operation,
            },
            deadline,
        )
        .unwrap()
        .await
        .unwrap();
    link.close().await;
    response.reply
}
// These native switches are test controls for an observed election schedule;
// production Config and runtime defaults are untouched. A trigger alone cannot
// defeat the healthy leader lease renewed by ordinary idle heartbeats.
pub(super) async fn force_authority(
    nodes: &[NodeRuntime],
    desired: usize,
    group: GroupId,
    deadline: Instant,
) {
    for node in nodes {
        node.service.groups[&group]
            .raft
            .runtime_config()
            .elect(false);
        node.service.groups[&group]
            .raft
            .runtime_config()
            .heartbeat(false);
    }
    let mut required = None;
    for node in nodes {
        let (applied, _) = node.service.groups[&group]
            .reader
            .read_membership()
            .await
            .unwrap();
        if applied.map(|id| id.index) > required.map(|id: LogId| id.index) {
            required = applied;
        }
    }
    loop {
        let (applied, _) = nodes[desired].service.groups[&group]
            .reader
            .read_membership()
            .await
            .unwrap();
        if covers(applied, required) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "election candidate must cover the captured applied history"
        );
        sleep(Duration::from_millis(10)).await;
    }
    let config = nodes[desired].service.groups[&group].raft.config();
    let lease_and_inflight =
        Duration::from_millis(config.election_timeout_max + config.heartbeat_interval);
    sleep_until(deadline.min(Instant::now() + lease_and_inflight)).await;
    nodes[desired].service.groups[&group]
        .raft
        .trigger()
        .elect()
        .await
        .unwrap();
    wait_authority(&nodes[desired], group, deadline).await;
}

pub(super) async fn wait_authority(node: &NodeRuntime, group: GroupId, deadline: Instant) {
    loop {
        let attempt = deadline.min(Instant::now() + Duration::from_millis(500));
        if node.leader_hint(group) == Some(node.config.node_id)
            && node
                .service
                .observe_membership(group, attempt)
                .await
                .is_ok()
        {
            return;
        }
        if Instant::now() >= deadline {
            eprintln!(
                "forced authority failure node={} group={} metrics={:?}",
                node.config.node_id.get(),
                group.get(),
                *node.service.groups[&group].raft.metrics().borrow()
            );
            panic!(
                "expected actual fresh authority on node {}",
                node.config.node_id.get()
            );
        }
        sleep(Duration::from_millis(10)).await;
    }
}
pub(super) async fn metadata_state(
    nodes: &[NodeRuntime],
    group: GroupId,
    deadline: Instant,
) -> MoveState {
    loop {
        for node in nodes {
            if node.leader_hint(GroupId::new(0)) != Some(node.config.node_id) {
                continue;
            }
            let attempt = deadline.min(Instant::now() + Duration::from_millis(500));
            if let BackendReply::MoveState {
                result: Ok(state), ..
            } = backend(
                node,
                BackendOperation::GetMove { data_group: group },
                attempt,
            )
            .await
            {
                return *state;
            }
        }
        assert!(Instant::now() < deadline, "metadata authority must recover");
        sleep(Duration::from_millis(10)).await;
    }
}

#[test]
fn native_genesis_membership_identity_is_representable_without_substitution() {
    let native = LogId::new(openraft::CommittedLeaderId::new(0, 0), 0);
    let observed = position(native)
        .expect("native initialization membership carries the real (term0,node0,index0) identity");
    assert_eq!(
        observed,
        ObservedLogPosition {
            term: 0,
            leader_node_id: 0,
            index: 0
        }
    );
    let bytes = rockbee_raft_protocol::encode_frame(&observed).unwrap();
    assert_eq!(
        rockbee_raft_protocol::decode_frame::<ObservedLogPosition>(&bytes).unwrap(),
        observed
    );
    assert!(
        completion_position(observed).is_err(),
        "genesis cannot prove completed target membership"
    );
    assert!(
        ObservedLogPosition {
            term: 1,
            leader_node_id: 0,
            index: 0
        }
        .validate()
        .is_err()
    );
    assert!(
        ObservedLogPosition {
            term: 0,
            leader_node_id: 0,
            index: 1
        }
        .validate()
        .is_err()
    );
}

#[test]
fn promotion_rejects_a_vote_change_after_catchup() {
    let vote = openraft::Vote::new_committed(3, 1);
    assert!(check_promotion_authority(vote, vote, Some(1), 1).is_ok());
    assert!(
        check_promotion_authority(vote, openraft::Vote::new_committed(4, 1), Some(1), 1).is_err()
    );
    assert!(check_promotion_authority(vote, vote, Some(2), 1).is_err());
    assert!(check_promotion_authority(vote, openraft::Vote::new(3, 1), Some(1), 1).is_err());
}

#[tokio::test]
async fn cancelling_drain_retains_pending_runner_and_its_permit() {
    let runners = Arc::new(MoveRunners::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let wait = Arc::clone(&release);
    let permit = Arc::clone(&runners.active).try_acquire_owned().unwrap();
    runners.tasks.lock().unwrap().insert(
        GroupId::new(1),
        RunningMove {
            job_id: MoveJobId::new([1; 16]).unwrap(),
            task: tokio::spawn(async move {
                let _permit = permit;
                wait.notified().await;
            }),
        },
    );
    runners.stop();
    assert!(
        tokio::time::timeout(Duration::from_millis(10), runners.drain())
            .await
            .is_err()
    );
    let retained = runners.tasks.lock().unwrap().contains_key(&GroupId::new(1));
    assert_eq!(runners.active.available_permits(), 3);
    release.notify_one();
    runners.drain().await;
    assert!(
        retained,
        "cancelling close/drain must keep the actual pending runner tracked"
    );
    assert!(runners.tasks.lock().unwrap().is_empty());
    assert_eq!(runners.active.available_permits(), 4);
}

#[test]
fn catchup_requires_full_identity_and_valid_history_coverage() {
    let required = LogId::new(openraft::CommittedLeaderId::new(4, 2), 10);
    assert!(covers(Some(required), Some(required)));
    assert!(!covers(None, Some(required)));
    assert!(!covers(
        Some(LogId::new(openraft::CommittedLeaderId::new(4, 1), 10)),
        Some(required)
    ));
    assert!(!covers(
        Some(LogId::new(openraft::CommittedLeaderId::new(3, 3), 11)),
        Some(required)
    ));
    assert!(!covers(
        Some(LogId::new(openraft::CommittedLeaderId::new(4, 2), 9)),
        Some(required)
    ));
    assert!(covers(
        Some(LogId::new(openraft::CommittedLeaderId::new(4, 2), 11)),
        Some(required)
    ));
}

fn proof_fixture() -> (MoveIntent, AppliedMembership) {
    let endpoints: Vec<_> = (1..=4)
        .map(|id| NodeEndpoint {
            node_id: NodeId::new(id).unwrap(),
            peer_address: format!("127.0.0.1:{}", 10000 + id).parse().unwrap(),
            data_address: format!("127.0.0.1:{}", 20000 + id).parse().unwrap(),
        })
        .collect();
    let directory = ClusterDirectory::initial(
        ClusterId::new([92; 16]).unwrap(),
        ShardConfig::new(2, 0).unwrap(),
        endpoints[..3].to_vec(),
    )
    .unwrap();
    let intent = MoveReplica {
        job_id: MoveJobId::new([2; 16]).unwrap(),
        data_group: GroupId::new(1),
        remove_node: NodeId::new(3).unwrap(),
        add_node: endpoints[3].clone(),
        expected_directory_revision: 1,
    }
    .derive_intent(&directory)
    .unwrap();
    let mut peers = [None; 16];
    for (index, node) in endpoints.iter().enumerate() {
        peers[index] = Some((node.node_id, node.peer_address));
    }
    let proof = AppliedMembership {
        data_group: intent.data_group,
        applied: ObservedLogPosition {
            term: 4,
            leader_node_id: 1,
            index: 20,
        },
        membership: ObservedLogPosition {
            term: 3,
            leader_node_id: 1,
            index: 19,
        },
        voters: intent.target_voters,
        joint_voters: None,
        peers,
    };
    (intent, proof)
}

#[test]
fn final_proof_rejects_wrong_group_joint_unapplied_and_endpoint_observations() {
    let (intent, proof) = proof_fixture();
    require_target(&proof, &intent).unwrap();
    let frame = rockbee_raft_protocol::encode_frame(&proof).unwrap();
    let roundtrip = rockbee_raft_protocol::decode_frame::<AppliedMembership>(&frame).unwrap();
    assert_eq!(proof, roundtrip);
    let mut wrong = proof.clone();
    wrong.data_group = GroupId::new(2);
    assert!(require_target(&wrong, &intent).is_err());
    let mut wrong = proof.clone();
    wrong.voters = intent.old_voters;
    wrong.joint_voters = Some(intent.target_voters);
    assert!(require_target(&wrong, &intent).is_err());
    let mut wrong = proof.clone();
    wrong.applied.index = 18;
    assert!(require_target(&wrong, &intent).is_err());
    let mut wrong = proof.clone();
    wrong.applied.index = 19;
    wrong.applied.term = 2;
    assert!(require_target(&wrong, &intent).is_err());
    let mut wrong = proof.clone();
    wrong.peers[3].as_mut().unwrap().1.set_port(30004);
    assert!(require_target(&wrong, &intent).is_err());
    let mut wrong = proof.clone();
    wrong.peers[3] = None;
    assert!(require_target(&wrong, &intent).is_err());
    let mut wrong = proof.clone();
    wrong.peers[3] = wrong.peers[0];
    assert!(wrong.validate().is_err());
    let mut wrong = proof;
    wrong.voters[1] = wrong.voters[0];
    assert!(wrong.validate().is_err());
}

#[test]
fn backend_matching_binds_actual_observation_group_and_job_identity() {
    use rockbee_raft_protocol::WireRequest;
    let (_, mut proof) = proof_fixture();
    let request = BackendRequest {
        cluster_id: ClusterId::new([92; 16]).unwrap(),
        target_node: NodeId::new(1).unwrap(),
        operation: BackendOperation::ObserveMembership {
            data_group: GroupId::new(1),
        },
    };
    proof.data_group = GroupId::new(2);
    let response = BackendResponse {
        cluster_id: request.cluster_id,
        node_id: request.target_node,
        reply: BackendReply::Membership {
            data_group: GroupId::new(1),
            result: Ok(Box::new(proof)),
        },
    };
    assert!(BackendRequest::validate_response(request.expected_response(), &response).is_err());
    let request = BackendRequest {
        operation: BackendOperation::ReconcileMove {
            data_group: GroupId::new(1),
            job_id: MoveJobId::new([1; 16]).unwrap(),
        },
        ..request
    };
    let response = BackendResponse {
        reply: BackendReply::Reconcile {
            data_group: GroupId::new(1),
            job_id: MoveJobId::new([2; 16]).unwrap(),
            result: Ok(ReconcileStatus::Running),
        },
        ..response
    };
    assert!(BackendRequest::validate_response(request.expected_response(), &response).is_err());
}
