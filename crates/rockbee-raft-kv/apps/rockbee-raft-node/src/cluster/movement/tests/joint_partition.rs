//! Applied joint ABC/ABD under a real BC | AD peer partition.
#![allow(clippy::unwrap_used, clippy::panic)]
use super::super::*;
use super::failure::{
    RelayFixture, committed_put, direct_probe, healthy_leader, restore_elections, strong_get,
};
use super::{backend, force_authority, metadata_state};
use crate::storage::JointApplyPause;
use bytes::Bytes;
use rockbee_raft_protocol::{KvRequest, KvResponse, MutationOutcome};
use tokio::time::{sleep, timeout_at};

struct JointFixture {
    cluster: RelayFixture,
    pause: Mutex<Option<Arc<JointApplyPause>>>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn joint_bc_ad_partition_denies_fresh_puts_and_same_job_recovers() {
    let _ = env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("rockbee_raft_node=info"),
    )
    .is_test(true)
    .try_init();
    let fixture = Arc::new(JointFixture {
        cluster: RelayFixture::start().await.unwrap(),
        pause: Mutex::new(None),
    });
    let scenario = Arc::clone(&fixture);
    // Runtime ownership, temporary roots, relay handles, and the release handle
    // remain outside the assertion task. A panic never abandons native work.
    let outcome = tokio::spawn(async move {
        joint_partition_scenario(&scenario).await;
    })
    .await;
    if let Some(pause) = fixture.pause.lock().unwrap().as_ref() {
        pause.release();
    }
    fixture.cluster.relays.heal();
    let mut fixture = match Arc::try_unwrap(fixture) {
        Ok(fixture) => fixture,
        Err(_) => panic!("finished assertion task must release its fixture"),
    };
    fixture.cluster.close().await;
    log::info!(
        "raft_joint_cleanup pause_released=true peers_healed=true native_and_relay_owners_joined=true"
    );
    if let Err(error) = outcome {
        if error.is_panic() {
            std::panic::resume_unwind(error.into_panic());
        }
        panic!("joint partition assertion task cancelled after owned cleanup: {error}");
    }
}

fn joint_configs() -> Vec<BTreeSet<u64>> {
    vec![BTreeSet::from([1, 2, 3]), BTreeSet::from([1, 2, 4])]
}

fn partition_edges() -> BTreeSet<(NodeId, NodeId)> {
    [2, 3]
        .into_iter()
        .flat_map(|left| {
            [1, 4].into_iter().flat_map(move |right| {
                let left = NodeId::new(left).unwrap();
                let right = NodeId::new(right).unwrap();
                [(left, right), (right, left)]
            })
        })
        .collect()
}

async fn joint_partition_scenario(owner: &JointFixture) {
    let fixture = &owner.cluster;
    let nodes = &fixture.nodes;
    let group = GroupId::new(1);
    let sibling = GroupId::new(2);
    let deadline = Instant::now() + Duration::from_secs(60);
    let keys: Vec<_> = (0..100)
        .map(|id| Bytes::from(format!("joint-partition-{id}")))
        .filter(|key| fixture.directory.shard_config.group_for_key(key).unwrap() == group)
        .take(6)
        .collect();
    force_authority(nodes, 1, group, deadline).await;
    restore_elections(nodes);
    committed_put(
        &nodes[1],
        group,
        keys[0].clone(),
        Bytes::from_static(b"baseline before joint"),
        deadline,
    )
    .await;
    strong_get(
        &nodes[1],
        group,
        keys[0].clone(),
        Bytes::from_static(b"baseline before joint"),
        deadline,
    )
    .await;
    let (applied, source) = nodes[1].service.groups[&group]
        .reader
        .read_membership()
        .await
        .unwrap();
    assert!(covers(applied, *source.log_id()));
    assert_eq!(
        source.membership().get_joint_config(),
        &vec![BTreeSet::from([1, 2, 3])],
        "B must have applied uniform ABC before arming the sole promotion pause"
    );
    let pause = nodes[1].service.groups[&group]
        .reader
        .pause_next_joint_apply_return(joint_configs());
    *owner.pause.lock().unwrap() = Some(Arc::clone(&pause));
    log::info!(
        "raft_joint_pause_armed node=2 group=1 source_applied={applied:?} source_membership={source:?} exact_joint={:?}",
        joint_configs()
    );
    let request = MoveReplica {
        job_id: MoveJobId::new([109; 16]).unwrap(),
        data_group: group,
        remove_node: NodeId::new(3).unwrap(),
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
        &nodes[1],
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
                result: Ok(ReconcileStatus::Scheduled),
                ..
            }
        ),
        "one B controller must own original native membership call: {scheduled:?}"
    );
    let reached = timeout_at(deadline, pause.wait_reached()).await.unwrap();
    assert_eq!(
        reached.membership.membership().get_joint_config(),
        &joint_configs()
    );
    let joint_id = reached.membership.log_id().unwrap();
    assert!(covers(reached.applied, Some(joint_id)));
    // This positive propagation gate is required before any partition. Failure
    // stops the scenario; no synthetic membership or weaker metrics substitute.
    loop {
        let mut observed = Vec::new();
        for node in nodes {
            let (applied, membership) = node.service.groups[&group]
                .reader
                .read_membership()
                .await
                .unwrap();
            observed.push((node.config.node_id, applied, membership));
        }
        if observed.iter().all(|(_, applied, membership)| {
            *membership == reached.membership && covers(*applied, Some(joint_id))
        }) {
            for (node, applied, membership) in observed {
                log::info!(
                    "raft_joint_all_four node={} group=1 applied={applied:?} membership={membership:?} b_apply_return_held=true",
                    node.get()
                );
            }
            break;
        }
        assert!(
            Instant::now() < deadline,
            "all four must durably apply the same full joint while B return is held: {observed:?}"
        );
        sleep(Duration::from_millis(10)).await;
    }
    let edges = partition_edges();
    assert_eq!(edges.len(), 8);
    let installed = fixture.relays.install_edge_cut(edges.clone());
    let installed_at = Instant::now();
    assert_eq!(installed.blocked_edges, edges);
    log::info!(
        "raft_joint_cut_installed generation={} edges={:?} drained=false b_apply_return_held=true",
        installed.generation,
        installed.blocked_edges
    );
    pause.release();
    timeout_at(deadline, pause.wait_exited()).await.unwrap();
    let exited_at = Instant::now();
    log::info!(
        "raft_joint_pause_exit after_install_us={} apply_return_released=true",
        exited_at.duration_since(installed_at).as_micros()
    );
    // An admitted RPC can await the paused apply response. Drain only after
    // release and positive exit, while the atomic receiver seals stay installed.
    let ack = timeout_at(deadline, fixture.relays.drain_edge_cut(&installed))
        .await
        .unwrap();
    let acknowledged_at = Instant::now();
    assert_eq!(ack.generation, installed.generation);
    assert_eq!(ack.blocked_edges, edges);
    assert_eq!(ack.surviving_connections, 0);
    assert_eq!(ack.earlier_dispatches, 0);
    let admitted_at_ack = fixture.relays.dispatch_tracker().admitted_for_edges(&edges);
    log::info!(
        "raft_joint_cut_ack generation={} edges={:?} surviving_forwarders={} earlier_dispatches={} cross_admissions={} after_pause_exit_us={}",
        ack.generation,
        ack.blocked_edges,
        ack.surviving_connections,
        ack.earlier_dispatches,
        admitted_at_ack,
        acknowledged_at.duration_since(exited_at).as_micros()
    );
    require_applied_joint(fixture, &reached.membership).await;
    for node in nodes {
        let started = Instant::now();
        assert!(started > acknowledged_at);
        let live = backend(
            node,
            BackendOperation::Leader { group_id: group },
            started + Duration::from_secs(2),
        )
        .await;
        assert!(
            matches!(live, BackendReply::Leader { group_id, .. } if group_id == group),
            "typed backend must remain live: {live:?}"
        );
        log::info!(
            "raft_joint_backend_live node={} after_ack_us={} hint_only={live:?}",
            node.config.node_id.get(),
            started.duration_since(acknowledged_at).as_micros()
        );
    }
    let probe_deadline = Instant::now() + Duration::from_secs(2);
    let (a, b, c, d) = tokio::join!(
        direct_probe(
            &nodes[0],
            11,
            group,
            unique_put(keys[1].clone(), 1),
            acknowledged_at,
            probe_deadline
        ),
        direct_probe(
            &nodes[1],
            12,
            group,
            unique_put(keys[2].clone(), 2),
            acknowledged_at,
            probe_deadline
        ),
        direct_probe(
            &nodes[2],
            13,
            group,
            unique_put(keys[3].clone(), 3),
            acknowledged_at,
            probe_deadline
        ),
        direct_probe(
            &nodes[3],
            14,
            group,
            unique_put(keys[4].clone(), 4),
            acknowledged_at,
            probe_deadline
        ),
    );
    let probes = [a, b, c, d];
    for (index, probe) in probes.iter().enumerate() {
        assert!(
            !matches!(
                probe,
                Ok(BackendReply::Kv {
                    response: KvResponse::Mutation(MutationOutcome::Committed),
                    ..
                })
            ),
            "node {} cannot commit under BC|AD joint split: {probe:?}",
            index + 1
        );
        log::info!(
            "raft_joint_denied node={} result={probe:?} replayed=false",
            index + 1
        );
    }
    require_applied_joint(fixture, &reached.membership).await;
    for node in nodes {
        for key in &keys[1..5] {
            let local = node.service.groups[&group]
                .reader
                .read_local(key.clone())
                .await
                .unwrap();
            assert!(covers(local.applied, Some(joint_id)));
            assert!(
                local.value.is_none(),
                "fresh probe key cannot appear in coherent applied state under confirmed cut: node={} applied={:?}",
                node.config.node_id.get(),
                local.applied
            );
        }
        log::info!(
            "raft_joint_coherent_absence node={} probe_keys=4 only_during_cut=true",
            node.config.node_id.get()
        );
    }
    // BC has ordinary ABC quorum for the unaffected sibling. Its successful
    // native traffic proves application work is not globally held by a hook.
    let sibling_leader = healthy_leader(nodes, sibling, &[1, 2], deadline).await;
    let sibling_key = (0..100)
        .map(|id| Bytes::from(format!("joint-sibling-{id}")))
        .find(|key| fixture.directory.shard_config.group_for_key(key).unwrap() == sibling)
        .unwrap();
    let sibling_value = Bytes::from_static(b"BC sibling remains live under joint split");
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
    require_applied_joint(fixture, &reached.membership).await;
    assert_eq!(
        fixture.relays.dispatch_tracker().admitted_for_edges(&edges),
        admitted_at_ack,
        "cross reconnects cannot enter native dispatch through the retained seal"
    );
    let retained = fixture.relays.drain_edge_cut(&installed).await;
    assert_eq!(retained.generation, ack.generation);
    log::info!(
        "raft_joint_positive_control sibling_node={} sibling_put=Committed sibling_get=Value committed_membership_still_joint=true cross_admissions_unchanged=true",
        nodes[sibling_leader].config.node_id.get()
    );
    for node in nodes {
        let metrics = node.service.groups[&group].raft.metrics();
        log::info!(
            "raft_joint_effective_only node={} membership={:?} not_applied_authority=true",
            node.config.node_id.get(),
            metrics.borrow().membership_config
        );
    }
    fixture.relays.heal();
    timeout_at(deadline, nodes[1].service.movement.drain())
        .await
        .unwrap();
    assert_eq!(nodes[1].service.movement.active.available_permits(), 4);
    log::info!(
        "raft_joint_healed original_b_membership_runner_joined=true original_job={:?} uncertain_probes_replayed=false",
        request.job_id.get()
    );
    let completed = loop {
        let state = metadata_state(nodes, group, deadline).await;
        if matches!(
            state.record.as_ref().map(|record| &record.progress),
            Some(MoveProgress::Completed { .. })
        ) {
            break state;
        }
        let leader = healthy_leader(nodes, group, &[0, 1, 3], deadline).await;
        let resumed = backend(
            &nodes[leader],
            BackendOperation::ReconcileMove {
                data_group: group,
                job_id: request.job_id,
            },
            deadline,
        )
        .await;
        log::info!(
            "raft_joint_same_job_resume node={} job={:?} result={resumed:?}",
            nodes[leader].config.node_id.get(),
            request.job_id.get()
        );
        assert!(
            Instant::now() < deadline,
            "original immutable job must complete after healing"
        );
        sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(completed.directory.revision, 2);
    assert_eq!(
        completed.record.as_ref().unwrap().intent,
        request.derive_intent(&fixture.directory).unwrap()
    );
    for unchanged in [GroupId::new(0), sibling] {
        assert_eq!(
            completed.directory.voters(unchanged),
            fixture.directory.voters(unchanged)
        );
    }
    let leader = healthy_leader(nodes, group, &[0, 1, 3], deadline).await;
    let uniform = nodes[leader]
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
    let healthy = Bytes::from_static(b"fresh ABD write after joint split healing");
    committed_put(
        &nodes[leader],
        group,
        keys[5].clone(),
        healthy.clone(),
        deadline,
    )
    .await;
    strong_get(&nodes[leader], group, keys[5].clone(), healthy, deadline).await;
    log::info!(
        "raft_joint_recovered leader={} revision=2 voters={:?} applied={:?} membership={:?} fresh_put=Committed fresh_get=Value",
        nodes[leader].config.node_id.get(),
        uniform.voters,
        uniform.applied,
        uniform.membership
    );
    // Unknown is not post-heal absence. Observe, without replaying or rejecting
    // a legitimate late application of any of the original unique operations.
    for (index, key) in keys[1..5].iter().enumerate() {
        let local = nodes[leader].service.groups[&group]
            .reader
            .read_local(key.clone())
            .await
            .unwrap();
        log::info!(
            "raft_joint_probe_after_heal operation_id={} original_result={:?} locally_present={} applied={:?} absence_claim=false replayed=false",
            index + 11,
            probes[index],
            local.value.is_some(),
            local.applied
        );
    }
}

fn unique_put(key: Bytes, node: u64) -> KvRequest {
    KvRequest::Put {
        key,
        value: Bytes::from(format!("unique direct joint probe from {node}")),
    }
}

async fn require_applied_joint(fixture: &RelayFixture, expected: &crate::types::StoredMembership) {
    for node in &fixture.nodes {
        let (applied, membership) = node.service.groups[&GroupId::new(1)]
            .reader
            .read_membership()
            .await
            .unwrap();
        assert_eq!(
            &membership,
            expected,
            "node {} applied membership must remain the exact joint during cut",
            node.config.node_id.get()
        );
        assert!(covers(applied, *membership.log_id()));
    }
}
