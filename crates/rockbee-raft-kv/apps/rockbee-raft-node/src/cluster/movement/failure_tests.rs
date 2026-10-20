//! Removed-C authority gate, separate from the happy-path movement schedule.
#![allow(clippy::unwrap_used, clippy::panic)]
use super::tests::{backend, force_authority, metadata_state};
use super::*;
use crate::{
    cluster::{NodeError, NodeRuntime},
    config::{JoinConfig, JoinSeed, NodeConfig},
    rpc::{PeerLimits, test_support::PeerRelays},
    storage::{OpenMode, StorageOptions},
};
use bytes::Bytes;
use rockbee_raft_protocol::{
    ClusterId, KvRequest, KvResponse, MutationOutcome, ShardConfig,
    routing::{ClusterDirectory, NodeEndpoint},
    transport::server::ServerConfig,
};
use tokio::{
    net::TcpListener,
    time::{sleep, timeout_at},
};

pub(super) struct RelayFixture {
    pub(super) directory: ClusterDirectory,
    pub(super) endpoints: Vec<NodeEndpoint>,
    pub(super) nodes: Vec<NodeRuntime>,
    pub(super) relays: PeerRelays,
    roots: Vec<tempfile::TempDir>,
}

impl RelayFixture {
    pub(super) async fn start() -> Result<Self, NodeError> {
        let cluster = ClusterId::new([105; 16])?;
        let mut endpoints = Vec::new();
        let mut listeners = Vec::new();
        let mut relayed = Vec::new();
        for id in 1..=4 {
            let node_id = NodeId::new(id)?;
            let advertised = TcpListener::bind("127.0.0.1:0").await?;
            let private = TcpListener::bind("127.0.0.1:0").await?;
            let data = TcpListener::bind("127.0.0.1:0").await?;
            endpoints.push(NodeEndpoint {
                node_id,
                peer_address: advertised.local_addr()?,
                data_address: data.local_addr()?,
            });
            relayed.push((node_id, advertised, private.local_addr()?));
            listeners.push((private, data));
        }
        let directory =
            ClusterDirectory::initial(cluster, ShardConfig::new(2, 0)?, endpoints[..3].to_vec())?;
        let relays = PeerRelays::start(cluster, relayed, PeerLimits::default());
        let mut fixture = Self {
            directory,
            endpoints,
            nodes: Vec::new(),
            relays,
            roots: Vec::new(),
        };
        if let Err(error) = fixture.start_nodes(listeners).await {
            fixture.close().await;
            return Err(error);
        }
        Ok(fixture)
    }

    async fn start_nodes(
        &mut self,
        mut listeners: Vec<(TcpListener, TcpListener)>,
    ) -> Result<(), NodeError> {
        for endpoint in &self.endpoints[..3] {
            let root = tempfile::tempdir()?;
            let (peer, data) = listeners.remove(0);
            let config = NodeConfig::new(
                self.directory.clone(),
                endpoint.node_id,
                root.path().join("db"),
                OpenMode::CreateNew,
            )?;
            self.nodes
                .push(NodeRuntime::start_with_peer_relay(config, peer, data, &self.relays).await?);
            self.roots.push(root);
        }
        self.nodes[0].initialize_groups().await?;
        self.nodes[0]
            .bootstrap_directory(Instant::now() + Duration::from_secs(30))
            .await?;
        let root = tempfile::tempdir()?;
        let (peer, data) = listeners.remove(0);
        let config = JoinConfig {
            local_endpoint: self.endpoints[3].clone(),
            cluster_id: self.directory.cluster_id,
            shard_config: self.directory.shard_config,
            seeds: self.endpoints[..3]
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
        self.nodes
            .push(NodeRuntime::start_join_with_peer_relay(config, peer, data, &self.relays).await?);
        self.roots.push(root);
        Ok(())
    }

    pub(super) async fn close(&mut self) {
        // Healing precedes server/native draining. An uncertain mutation is
        // allowed to finish after healing; it is never automatically replayed.
        self.relays.heal();
        restore_elections(&self.nodes);
        for node in &mut self.nodes {
            node.close().await;
        }
        self.relays.close().await;
        log::info!(
            "raft_removed_c_cleanup nodes={} relay_tasks_joined=true",
            self.nodes.len()
        );
    }
}

pub(super) fn restore_elections(nodes: &[NodeRuntime]) {
    for node in nodes {
        for group in node.service.groups.values() {
            group.raft.runtime_config().elect(true);
            group.raft.runtime_config().heartbeat(true);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn removed_c_cannot_serve_fresh_put_or_strong_get_after_completed_abd_cut() {
    let _ = env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("rockbee_raft_node=info"),
    )
    .is_test(true)
    .try_init();
    let fixture = Arc::new(RelayFixture::start().await.unwrap());
    let scenario = Arc::clone(&fixture);
    // Keep cleanup ownership outside the assertion task, including on panic.
    // No native Raft future is aborted to implement this boundary.
    let outcome = tokio::spawn(async move { removed_c_scenario(&scenario).await }).await;
    let mut fixture = match Arc::try_unwrap(fixture) {
        Ok(fixture) => fixture,
        Err(_) => panic!("finished assertion task must release its fixture"),
    };
    fixture.close().await;
    if let Err(error) = outcome {
        if error.is_panic() {
            std::panic::resume_unwind(error.into_panic());
        }
        panic!("removed-C assertion task cancelled after owned cleanup: {error}");
    }
}

async fn removed_c_scenario(fixture: &RelayFixture) {
    let nodes = &fixture.nodes;
    let group = GroupId::new(1);
    let sibling = GroupId::new(2);
    let c = NodeId::new(3).unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    let keys: Vec<_> = (0..100)
        .map(|id| Bytes::from(format!("removed-c-{id}")))
        .filter(|key| fixture.directory.shard_config.group_for_key(key).unwrap() == group)
        .take(3)
        .collect();
    let baseline = Bytes::from_static(b"C retains this committed baseline");
    force_authority(nodes, 2, group, deadline).await;
    restore_elections(nodes);
    let former = nodes[2]
        .service
        .observe_membership(group, deadline)
        .await
        .unwrap();
    assert_eq!(former.voters, fixture.directory.voters(group).unwrap());
    assert_eq!(former.joint_voters, None);
    committed_put(
        &nodes[2],
        group,
        keys[0].clone(),
        baseline.clone(),
        deadline,
    )
    .await;
    strong_get(
        &nodes[2],
        group,
        keys[0].clone(),
        baseline.clone(),
        deadline,
    )
    .await;
    log::info!(
        "raft_removed_c_former_authority node=3 group=1 applied={:?} membership={:?} baseline_put=Committed baseline_get=Value",
        former.applied,
        former.membership
    );

    let request = MoveReplica {
        job_id: MoveJobId::new([106; 16]).unwrap(),
        data_group: group,
        remove_node: c,
        add_node: fixture.endpoints[3].clone(),
        expected_directory_revision: 1,
    };
    let metadata = healthy_leader(nodes, GroupId::new(0), &[0, 1, 2], deadline).await;
    let accepted = backend(
        &nodes[metadata],
        BackendOperation::MoveReplica(request.clone()),
        deadline,
    )
    .await;
    assert!(
        matches!(
            accepted,
            BackendReply::Move {
                result: AdminResult::Accepted { .. } | AdminResult::AlreadyAccepted { .. },
                ..
            }
        ),
        "real immutable move must be accepted: {accepted:?}"
    );
    let scheduled = backend(
        &nodes[2],
        BackendOperation::ReconcileMove {
            data_group: group,
            job_id: request.job_id,
        },
        deadline,
    )
    .await;
    assert!(
        matches!(
            scheduled,
            BackendReply::Reconcile {
                result: Ok(ReconcileStatus::Scheduled | ReconcileStatus::Running),
                ..
            }
        ),
        "formerly authoritative C must start native movement: {scheduled:?}"
    );
    let completed = loop {
        let state = metadata_state(nodes, group, deadline).await;
        if matches!(
            state.record.as_ref().map(|record| &record.progress),
            Some(MoveProgress::Completed { .. })
        ) {
            break state;
        }
        // Removing the current leader may stop that runner. Resume only this
        // original immutable job on a surviving, freshly observed authority.
        for index in [0, 1, 3] {
            let attempt = deadline.min(Instant::now() + Duration::from_millis(500));
            if nodes[index].leader_hint(group) == Some(nodes[index].config.node_id)
                && nodes[index]
                    .service
                    .observe_membership(group, attempt)
                    .await
                    .is_ok()
            {
                let resumed = backend(
                    &nodes[index],
                    BackendOperation::ReconcileMove {
                        data_group: group,
                        job_id: request.job_id,
                    },
                    deadline,
                )
                .await;
                log::info!(
                    "raft_removed_c_same_job_resume node={} job={:?} result={resumed:?}",
                    nodes[index].config.node_id.get(),
                    request.job_id.get()
                );
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "same immutable job must reach completed publication"
        );
        sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(completed.directory.revision, 2);
    assert_eq!(
        completed.record.as_ref().unwrap().intent,
        request.derive_intent(&fixture.directory).unwrap()
    );
    assert_eq!(completed.directory.node(c), Some(&fixture.endpoints[2]));
    for unchanged in [GroupId::new(0), sibling] {
        assert_eq!(
            completed.directory.voters(unchanged),
            fixture.directory.voters(unchanged)
        );
    }
    let abd = healthy_leader(nodes, group, &[0, 1, 3], deadline).await;
    let uniform = nodes[abd]
        .service
        .observe_membership(group, deadline)
        .await
        .unwrap();
    require_target(
        &uniform,
        &request.derive_intent(&fixture.directory).unwrap(),
    )
    .unwrap();
    let MoveProgress::Completed { membership } = completed.record.unwrap().progress else {
        unreachable!()
    };
    assert_eq!(completion_position(uniform.membership).unwrap(), membership);
    log::info!(
        "raft_removed_c_completed job={:?} revision=2 uniform_voters={:?} applied={:?} membership={:?}",
        request.job_id.get(),
        uniform.voters,
        uniform.applied,
        uniform.membership
    );

    restore_elections(nodes);
    let ack = timeout_at(deadline, fixture.relays.cut(c)).await.unwrap();
    let acknowledged_at = Instant::now();
    assert_eq!(ack.surviving_connections, 0);
    assert_eq!(ack.earlier_dispatches, 0);
    assert_eq!(ack.blocked_edges.len(), 6);
    let admitted_at_ack = fixture.relays.dispatch_tracker().admitted_for(c);
    log::info!(
        "raft_removed_c_cut_ack generation={} blocked_node={} edges={:?} surviving_forwarders={} earlier_dispatches={} admitted={}",
        ack.generation,
        ack.blocked_node.get(),
        ack.blocked_edges,
        ack.surviving_connections,
        ack.earlier_dispatches,
        admitted_at_ack
    );
    let liveness_started = Instant::now();
    assert!(liveness_started > acknowledged_at);
    let liveness = backend(
        &nodes[2],
        BackendOperation::Leader { group_id: group },
        liveness_started + Duration::from_secs(2),
    )
    .await;
    assert!(
        matches!(liveness, BackendReply::Leader { group_id, .. } if group_id == group),
        "C backend must remain running under peer cut: {liveness:?}"
    );
    log::info!(
        "raft_removed_c_backend_liveness after_ack_us={} reply={liveness:?} authority_claim=false",
        liveness_started.duration_since(acknowledged_at).as_micros()
    );
    let probe_deadline = Instant::now() + Duration::from_secs(2);
    let (put, get) = tokio::join!(
        direct_probe(
            &nodes[2],
            1,
            group,
            KvRequest::Put {
                key: keys[1].clone(),
                value: Bytes::from_static(b"unique uncertain C probe")
            },
            acknowledged_at,
            probe_deadline
        ),
        direct_probe(
            &nodes[2],
            2,
            group,
            KvRequest::Get {
                key: keys[0].clone()
            },
            acknowledged_at,
            probe_deadline
        ),
    );
    assert!(
        !matches!(
            put,
            Ok(BackendReply::Kv {
                response: KvResponse::Mutation(MutationOutcome::Committed),
                ..
            })
        ),
        "cut removed C cannot commit a fresh PUT: {put:?}"
    );
    assert!(
        !matches!(
            get,
            Ok(BackendReply::Kv {
                response: KvResponse::Value(_),
                ..
            })
        ),
        "cut removed C cannot return any Value for strong GET: {get:?}"
    );

    let abd = healthy_leader(nodes, group, &[0, 1, 3], deadline).await;
    let healthy_value = Bytes::from_static(b"healthy ABD under confirmed C cut");
    committed_put(
        &nodes[abd],
        group,
        keys[2].clone(),
        healthy_value.clone(),
        deadline,
    )
    .await;
    strong_get(&nodes[abd], group, keys[2].clone(), healthy_value, deadline).await;
    let meta = healthy_leader(nodes, GroupId::new(0), &[0, 1], deadline).await;
    let status = backend(
        &nodes[meta],
        BackendOperation::GetMove { data_group: group },
        deadline,
    )
    .await;
    assert!(
        matches!(status, BackendReply::MoveState { result: Ok(ref state), .. } if state.directory.revision == 2 && matches!(state.record.as_ref().map(|record| &record.progress), Some(MoveProgress::Completed { .. }))),
        "healthy metadata/status must remain available: {status:?}"
    );
    let sibling_leader = healthy_leader(nodes, sibling, &[0, 1], deadline).await;
    let sibling_key = (0..100)
        .map(|id| Bytes::from(format!("removed-c-sibling-{id}")))
        .find(|key| fixture.directory.shard_config.group_for_key(key).unwrap() == sibling)
        .unwrap();
    let sibling_value = Bytes::from_static(b"ABC sibling remains live with AB majority");
    committed_put(
        &nodes[sibling_leader],
        sibling,
        sibling_key.clone(),
        sibling_value.clone(),
        deadline,
    )
    .await;
    strong_get(
        &nodes[sibling_leader],
        sibling,
        sibling_key,
        sibling_value,
        deadline,
    )
    .await;
    for (unchanged, leader) in [(GroupId::new(0), meta), (sibling, sibling_leader)] {
        let hosted = &nodes[leader].service.groups[&unchanged];
        let required = authority_barrier(unchanged, hosted, deadline)
            .await
            .unwrap();
        let (applied, membership) = hosted.reader.read_membership().await.unwrap();
        assert!(covers(applied, required) && covers(applied, *membership.log_id()));
        assert_eq!(membership.membership().get_joint_config().len(), 1);
        assert_eq!(membership.voter_ids().collect::<Vec<_>>(), vec![1, 2, 3]);
    }
    let local = nodes[2].service.groups[&group]
        .reader
        .read_local(keys[0].clone())
        .await
        .unwrap();
    assert_eq!(local.value, Some(baseline));
    assert_eq!(
        fixture.relays.dispatch_tracker().admitted_for(c),
        admitted_at_ack,
        "blocked reconnects cannot reach dispatch after ACK"
    );
    log::info!(
        "raft_removed_c_positive_controls abd_node={} metadata_node={} sibling_node={} c_local_generation={} c_local_applied={:?} baseline_retained=true blocked_dispatch_count_unchanged=true",
        nodes[abd].config.node_id.get(),
        nodes[meta].config.node_id.get(),
        nodes[sibling_leader].config.node_id.get(),
        local.generation,
        local.applied
    );
}

pub(super) async fn healthy_leader(
    nodes: &[NodeRuntime],
    group: GroupId,
    candidates: &[usize],
    deadline: Instant,
) -> usize {
    loop {
        for &index in candidates {
            let node = &nodes[index];
            let Some(hosted) = node.service.groups.get(&group) else {
                continue;
            };
            let attempt = deadline.min(Instant::now() + Duration::from_millis(500));
            if node.leader_hint(group) == Some(node.config.node_id)
                && authority_barrier(group, hosted, attempt).await.is_ok()
            {
                return index;
            }
        }
        assert!(
            Instant::now() < deadline,
            "fresh actual healthy authority required for group {}",
            group.get()
        );
        sleep(Duration::from_millis(10)).await;
    }
}

pub(super) async fn committed_put(
    node: &NodeRuntime,
    group: GroupId,
    key: Bytes,
    value: Bytes,
    deadline: Instant,
) {
    let reply = backend(
        node,
        BackendOperation::Kv {
            group_id: group,
            request: KvRequest::Put { key, value },
        },
        deadline,
    )
    .await;
    assert!(
        matches!(
            reply,
            BackendReply::Kv {
                response: KvResponse::Mutation(MutationOutcome::Committed),
                ..
            }
        ),
        "fresh direct PUT must commit: {reply:?}"
    );
}

pub(super) async fn strong_get(
    node: &NodeRuntime,
    group: GroupId,
    key: Bytes,
    expected: Bytes,
    deadline: Instant,
) {
    let reply = backend(
        node,
        BackendOperation::Kv {
            group_id: group,
            request: KvRequest::Get { key },
        },
        deadline,
    )
    .await;
    assert!(
        matches!(reply, BackendReply::Kv { response: KvResponse::Value(Some(ref value)), .. } if *value == expected),
        "fresh direct strong GET must return expected value: {reply:?}"
    );
}

pub(super) async fn direct_probe(
    node: &NodeRuntime,
    operation_id: u64,
    group: GroupId,
    request: KvRequest,
    cut_ack: Instant,
    deadline: Instant,
) -> Result<BackendReply, String> {
    let started = Instant::now();
    assert!(
        started > cut_ack && deadline > started,
        "fresh probe must start strictly after cut ACK with its original deadline"
    );
    let reply = match BackendLink::connect(
        node.config.endpoint.data_address,
        LinkConfig::default(),
        deadline,
    )
    .await
    {
        Err(error) => Err(format!("connect before submission: {error:?}")),
        Ok(link) => {
            let reply = match link.try_submit(
                BackendRequest {
                    cluster_id: node.config.cluster_id,
                    target_node: node.config.node_id,
                    operation: BackendOperation::Kv {
                        group_id: group,
                        request,
                    },
                },
                deadline,
            ) {
                Ok(pending) => pending
                    .await
                    .map(|response| response.reply)
                    .map_err(|error| format!("transport delivery certainty: {error:?}")),
                Err(error) => Err(format!("not submitted: {error:?}")),
            };
            link.close().await;
            reply
        }
    };
    log::info!(
        "raft_membership_probe operation_id={} node={} group={} after_ack_us={} original_budget_us={} elapsed_us={} result={reply:?}",
        operation_id,
        node.config.node_id.get(),
        group.get(),
        started.duration_since(cut_ack).as_micros(),
        deadline.duration_since(started).as_micros(),
        started.elapsed().as_micros()
    );
    reply
}

#[tokio::test]
async fn relay_startup_rejects_wrong_mapping_and_public_constructor_stays_strict() {
    for ordinary_constructor in [false, true] {
        let mut endpoints = Vec::new();
        let mut listeners = Vec::new();
        let mut relayed = Vec::new();
        for id in 1..=3 {
            let node_id = NodeId::new(id).unwrap();
            let advertised = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let private = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let data = TcpListener::bind("127.0.0.1:0").await.unwrap();
            endpoints.push(NodeEndpoint {
                node_id,
                peer_address: advertised.local_addr().unwrap(),
                data_address: data.local_addr().unwrap(),
            });
            relayed.push((node_id, advertised, private.local_addr().unwrap()));
            listeners.push((private, data));
        }
        let cluster = ClusterId::new([107; 16]).unwrap();
        let directory =
            ClusterDirectory::initial(cluster, ShardConfig::new(2, 0).unwrap(), endpoints).unwrap();
        let mut relays = PeerRelays::start(cluster, relayed, PeerLimits::default());
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("must-not-open");
        let config = NodeConfig::new(
            directory,
            NodeId::new(1).unwrap(),
            path.clone(),
            OpenMode::CreateNew,
        )
        .unwrap();
        let (mapped_private, data) = listeners.remove(0);
        let rejected = if ordinary_constructor {
            NodeRuntime::start_with_listeners(config, mapped_private, data).await
        } else {
            let wrong_private = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let result =
                NodeRuntime::start_with_peer_relay(config, wrong_private, data, &relays).await;
            drop(mapped_private);
            result
        };
        let unopened = !path.exists();
        let rejected_before_startup = match rejected {
            Err(NodeError::Config(_)) => true,
            Err(_) => false,
            Ok(mut node) => {
                node.close().await;
                false
            }
        };
        relays.close().await;
        assert!(
            rejected_before_startup,
            "wrong listener mapping must be rejected before native startup"
        );
        assert!(unopened, "listener validation must precede opening storage");
    }
}
