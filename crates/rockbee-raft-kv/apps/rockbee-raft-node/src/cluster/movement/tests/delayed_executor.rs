//! A completes the lifetime job while B still owns its first native await.
#![allow(clippy::unwrap_used, clippy::panic)]
use super::super::handoff::HandoffIdentity;
use super::super::*;
use super::failure::{RelayFixture, healthy_leader, restore_elections};
use super::force_authority;
use super::native_membership_observation::{
    NativeMembershipCall, NativeMembershipIdentity, NativeMembershipObservation,
};
use crate::{
    cluster::NodeRuntime,
    storage::{JointApplyObservation, JointApplyPause},
};
use bytes::Bytes;
use rockbee_raft_protocol::{KvRequest, KvResponse, MutationOutcome};
use tokio::{
    task::Id,
    time::{sleep, timeout_at},
};

struct DelayedFixture {
    cluster: RelayFixture,
    pause: Mutex<Option<Arc<JointApplyPause>>>,
    seal_link: tokio::sync::Mutex<Option<BackendLink>>,
}

pub(super) struct Scenario {
    pub(super) group: GroupId,
    pub(super) sibling: GroupId,
    pub(super) keys: Vec<Bytes>,
    pub(super) sibling_key: Bytes,
    pub(super) baseline: Bytes,
    pub(super) request: MoveReplica,
    pub(super) intent: MoveIntent,
    pub(super) deadline: Instant,
}

pub(super) struct BackendReceipt {
    pub(super) reply: BackendReply,
    pub(super) received_at: Instant,
    pub(super) closed_at: Instant,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delayed_b_native_membership_returns_only_after_a_completes_same_job() {
    run_delayed_executor(SealMode::None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_move_seal_waits_for_the_whole_native_membership_call() {
    run_delayed_executor(SealMode::Local).await;
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum SealMode {
    None,
    Local,
    Remote,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handoff_seal_retains_actual_native_owner_after_caller_close() {
    run_delayed_executor(SealMode::Remote).await;
}

async fn run_delayed_executor(mode: SealMode) {
    let _ = env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("rockbee_raft_node=info"),
    )
    .is_test(true)
    .try_init();
    let owner = Arc::new(DelayedFixture {
        cluster: RelayFixture::start().await.unwrap(),
        pause: Mutex::new(None),
        seal_link: tokio::sync::Mutex::new(None),
    });
    let scenario_owner = Arc::clone(&owner);
    // The assertion task never owns the only release/runtime handles. A failed
    // native-pending premise still releases first and joins the original work.
    let outcome =
        tokio::spawn(async move { delayed_executor_scenario(&scenario_owner, mode).await }).await;
    if let Some(pause) = owner.pause.lock().unwrap().as_ref() {
        pause.release();
    }
    if let Some(link) = owner.seal_link.lock().await.as_ref() {
        link.close().await;
    }
    restore_elections(&owner.cluster.nodes);
    owner.cluster.relays.heal();
    for node in &owner.cluster.nodes {
        node.service.movement.drain().await;
    }
    let mut owner = match Arc::try_unwrap(owner) {
        Ok(owner) => owner,
        Err(_) => panic!("finished assertion task must release its fixture"),
    };
    owner.cluster.close().await;
    log::info!(
        "raft_delayed_cleanup pause_released=true elections_restored=true original_native_and_runners_drained=true runtimes_and_links_joined=true"
    );
    if let Err(error) = outcome {
        if error.is_panic() {
            std::panic::resume_unwind(error.into_panic());
        }
        panic!("delayed executor assertion task cancelled after owned cleanup: {error}");
    }
}

async fn delayed_executor_scenario(owner: &DelayedFixture, mode: SealMode) {
    let seal_before_release = mode == SealMode::Local;
    let scenario = establish_baseline(&owner.cluster).await;
    let nodes = &owner.cluster.nodes;
    let (pause, joint, observer, original) = start_b_native_joint(owner, &scenario).await;
    let runner_id =
        require_original_pending(nodes, &scenario, &pause, &joint, &observer, &original);
    force_authority(nodes, 0, scenario.group, scenario.deadline).await;
    restore_elections(nodes);
    let completed = complete_same_job_on_a(&owner.cluster, &scenario).await;
    let a_completed_at = Instant::now();
    // This is a live prerequisite, not an inference from a still-alive runner.
    let still_same_runner =
        require_original_pending(nodes, &scenario, &pause, &joint, &observer, &original);
    assert_eq!(still_same_runner, runner_id);
    log::info!(
        "raft_delayed_a_completed node=1 original_node=2 invocation={} original_runner={runner_id:?} job={:?} revision=2 native_result=None pause_held=true",
        original.invocation,
        scenario.request.job_id.get()
    );
    let control = nodes[1].service.groups[&scenario.group]
        .reader
        .membership_control();
    let identity = crate::storage::MovementIdentity {
        revision: scenario.intent.expected_directory_revision,
        job_id: scenario.intent.job_id,
    };
    let mut seal = std::pin::pin!(control.seal(identity));
    if seal_before_release {
        assert!(
            !control.native_permit_available_for_test(),
            "the exact pending native invocation must retain movement admission"
        );
        let pending = std::future::poll_fn(|cx| {
            std::task::Poll::Ready(Future::poll(seal.as_mut(), cx).is_pending())
        })
        .await;
        assert!(pending, "seal cannot acknowledge a still-owned native call");
        require_original_pending(nodes, &scenario, &pause, &joint, &observer, &original);
        log::info!(
            "raft_move_seal_waiting node=2 invocation={} native_result=None seal_ack=false",
            original.invocation
        );
    }
    let remote = if mode == SealMode::Remote {
        let observation = start_held_receiver(owner, &scenario, &completed).await;
        require_original_pending(nodes, &scenario, &pause, &joint, &observer, &original);
        assert!(observation.snapshot().unwrap().result.is_none());
        Some(observation)
    } else {
        None
    };
    let release_requested_at = Instant::now();
    pause.release();
    timeout_at(scenario.deadline, pause.wait_exited())
        .await
        .unwrap();
    assert!(pause.held_observation().is_none());
    let returned = wait_original_result(&observer, &original, scenario.deadline).await;
    if seal_before_release {
        // Keep the actual seal future owned until it resolves. The fixture's
        // outer owner releases the native pause even if an assertion fails.
        seal.await.unwrap();
        assert!(control.begin(identity).await.is_err());
        nodes[0].service.movement.drain().await;
        assert!(
            nodes[0].service.groups[&scenario.group]
                .reader
                .membership_control()
                .begin(identity)
                .await
                .is_err(),
            "the completing A runner must seal its own native admission"
        );
        log::info!(
            "raft_move_seal_complete node=2 invocation={} native_result={:?} seal_ack=true later_native_admission=false",
            original.invocation,
            returned.result
        );
    }
    if let Some(observation) = remote {
        let sealed = observation.wait(true, scenario.deadline).await;
        assert_eq!(sealed.result, Some(Ok(())));
        assert!(sealed.entered_at <= release_requested_at);
        assert!(sealed.returned_at.unwrap() >= returned.returned_at.unwrap());
        assert!(control.begin(identity).await.is_err());
        log::info!(
            "raft_handoff_owned_seal node=2 group={} predecessor={:?} successor={:?} caller_closed_before_release=true original_native_return_before_seal=true actual_seal_ok=true stale_native_rejected=true",
            scenario.group.get(),
            sealed.identity.predecessor_job_id.get(),
            sealed.identity.successor_job_id.get()
        );
    }
    assert!(a_completed_at <= release_requested_at);
    assert!(
        release_requested_at <= returned.returned_at.unwrap(),
        "native result must be recorded after the pause-release request boundary"
    );
    timeout_at(scenario.deadline, nodes[1].service.movement.drain())
        .await
        .unwrap();
    assert!(nodes[1].service.movement.tasks.lock().unwrap().is_empty());
    assert_eq!(nodes[1].service.movement.active.available_permits(), 4);
    if seal_before_release {
        // A completed retry uses the same identity and performs no new native
        // membership work; the local seal operation is idempotent.
        nodes[1]
            .service
            .reconcile_move(scenario.group, scenario.request.job_id, scenario.deadline)
            .await
            .unwrap();
        assert!(control.begin(identity).await.is_err());
    }
    log::info!(
        "raft_delayed_original_return node=2 invocation={} job={:?} result={:?} a_completed_before_release_request=true release_request_before_native_return=true original_runner_joined=true permit_returned=true",
        returned.invocation,
        returned.identity.job.get(),
        returned.result
    );
    require_no_regression(&owner.cluster, &scenario, &completed).await;
}

async fn establish_baseline(fixture: &RelayFixture) -> Scenario {
    establish_baseline_at(fixture, 1, MoveJobId::new([111; 16]).unwrap()).await
}

/// Submit through TCP while the exact B membership await still owns its gate.
/// The outer fixture retains both the pause release and the caller Link.
async fn start_held_receiver(
    owner: &DelayedFixture,
    scenario: &Scenario,
    completed: &MoveState,
) -> Arc<super::handoff::SealObservation> {
    let fixture = &owner.cluster;
    let successor = MoveReplica {
        job_id: MoveJobId::new([113; 16]).unwrap(),
        data_group: scenario.group,
        remove_node: NodeId::new(2).unwrap(),
        add_node: fixture.endpoints[2].clone(),
        expected_directory_revision: completed.directory.revision,
    }
    .derive_intent(&completed.directory)
    .unwrap();
    let metadata = healthy_leader(
        &fixture.nodes,
        GroupId::new(0),
        &[0, 1, 2],
        scenario.deadline,
    )
    .await;
    let result = fixture.nodes[metadata].service.groups[&GroupId::new(0)]
        .raft
        .client_write(Command::Metadata(Box::new(
            MetadataCommand::PrepareSuccessor {
                predecessor_job_id: scenario.intent.job_id,
                intent: successor.clone(),
            },
        )))
        .await
        .unwrap();
    assert!(matches!(
        result.data,
        ApplyResult::Metadata(MetadataResult::Prepared { .. })
    ));
    let identity = HandoffIdentity {
        data_group: scenario.group,
        predecessor_revision: scenario.intent.expected_directory_revision,
        predecessor_job_id: scenario.intent.job_id,
        successor_job_id: successor.job_id,
    };
    let observation = Arc::new(super::handoff::SealObservation::new(identity));
    let b = &fixture.nodes[1];
    *b.service.movement.handoff_seal.lock().unwrap() = Some(Arc::clone(&observation));
    let link = BackendLink::connect(
        b.config.endpoint.data_address,
        LinkConfig::default(),
        scenario.deadline,
    )
    .await
    .unwrap();
    *owner.seal_link.lock().await = Some(link);
    let pending = owner
        .seal_link
        .lock()
        .await
        .as_ref()
        .unwrap()
        .try_submit(
            BackendRequest {
                cluster_id: b.config.cluster_id,
                target_node: b.config.node_id,
                operation: BackendOperation::SealPredecessor {
                    data_group: identity.data_group,
                    predecessor_revision: identity.predecessor_revision,
                    predecessor_job_id: identity.predecessor_job_id,
                    successor_job_id: identity.successor_job_id,
                },
            },
            scenario.deadline,
        )
        .unwrap();
    let entered = observation.wait(false, scenario.deadline).await;
    assert_eq!(entered.identity, identity);
    assert!(entered.result.is_none());
    assert!(
        !b.service.groups[&scenario.group]
            .reader
            .membership_control()
            .native_permit_available_for_test()
    );
    owner.seal_link.lock().await.as_ref().unwrap().close().await;
    let caller_result = pending.await;
    assert!(
        caller_result.is_err(),
        "closed caller cannot observe a seal ACK: {caller_result:?}"
    );
    assert!(observation.snapshot().unwrap().result.is_none());
    log::info!(
        "raft_handoff_caller_closed node=2 group={} predecessor={:?} successor={:?} validated_handler_entered=true original_native_gate_held=true seal_result=None",
        identity.data_group.get(),
        identity.predecessor_job_id.get(),
        identity.successor_job_id.get()
    );
    observation
}

pub(super) async fn establish_baseline_at(
    fixture: &RelayFixture,
    initial_authority: usize,
    job_id: MoveJobId,
) -> Scenario {
    let group = GroupId::new(1);
    let sibling = GroupId::new(2);
    let deadline = Instant::now() + Duration::from_secs(60);
    let keys: Vec<_> = (0..100)
        .map(|id| Bytes::from(format!("delayed-native-{id}")))
        .filter(|key| fixture.directory.shard_config.group_for_key(key).unwrap() == group)
        .take(3)
        .collect();
    let sibling_key = (0..100)
        .map(|id| Bytes::from(format!("delayed-sibling-{id}")))
        .find(|key| fixture.directory.shard_config.group_for_key(key).unwrap() == sibling)
        .unwrap();
    let baseline = Bytes::from_static(b"baseline survives delayed native continuation");
    let request = MoveReplica {
        job_id,
        data_group: group,
        remove_node: NodeId::new(3).unwrap(),
        add_node: fixture.endpoints[3].clone(),
        expected_directory_revision: 1,
    };
    let intent = request.derive_intent(&fixture.directory).unwrap();
    let scenario = Scenario {
        group,
        sibling,
        keys,
        sibling_key,
        baseline,
        request,
        intent,
        deadline,
    };
    force_authority(&fixture.nodes, initial_authority, group, deadline).await;
    restore_elections(&fixture.nodes);
    let b = &fixture.nodes[initial_authority];
    put(
        b,
        group,
        scenario.keys[0].clone(),
        scenario.baseline.clone(),
        deadline,
    )
    .await;
    put(
        b,
        group,
        scenario.keys[1].clone(),
        Bytes::from_static(b"must remain deleted"),
        deadline,
    )
    .await;
    delete(b, group, scenario.keys[1].clone(), deadline).await;
    get(
        b,
        group,
        scenario.keys[0].clone(),
        Some(scenario.baseline.clone()),
        deadline,
    )
    .await;
    get(b, group, scenario.keys[1].clone(), None, deadline).await;
    let sibling_leader = healthy_leader(&fixture.nodes, sibling, &[0, 1, 2], deadline).await;
    put(
        &fixture.nodes[sibling_leader],
        sibling,
        scenario.sibling_key.clone(),
        scenario.baseline.clone(),
        deadline,
    )
    .await;
    let metadata = healthy_leader(&fixture.nodes, GroupId::new(0), &[0, 1, 2], deadline).await;
    let accepted = backend_receipt(
        &fixture.nodes[metadata],
        BackendOperation::MoveReplica(scenario.request.clone()),
        deadline,
    )
    .await;
    assert!(
        matches!(&accepted.reply, BackendReply::Move { data_group, job_id, result: AdminResult::Accepted { intent } } if *data_group == group && *job_id == scenario.request.job_id && *intent == scenario.intent),
        "exact immutable job must be newly accepted: {:?}",
        accepted.reply
    );
    scenario
}

async fn start_b_native_joint(
    owner: &DelayedFixture,
    scenario: &Scenario,
) -> (
    Arc<JointApplyPause>,
    JointApplyObservation,
    Arc<NativeMembershipObservation>,
    NativeMembershipCall,
) {
    let b = &owner.cluster.nodes[1];
    let source = b
        .service
        .observe_membership(scenario.group, scenario.deadline)
        .await
        .unwrap();
    assert_eq!(source.voters, scenario.intent.old_voters);
    assert_eq!(source.joint_voters, None);
    let expected_joint = vec![BTreeSet::from([1, 2, 3]), BTreeSet::from([1, 2, 4])];
    let pause = b.service.groups[&scenario.group]
        .reader
        .pause_next_joint_apply_return(expected_joint.clone());
    *owner.pause.lock().unwrap() = Some(Arc::clone(&pause));
    let observer = Arc::new(NativeMembershipObservation::new(NativeMembershipIdentity {
        node: b.config.node_id,
        group: scenario.group,
        job: scenario.request.job_id,
        target: BTreeSet::from([1, 2, 4]),
    }));
    *b.service.movement.native_membership.lock().unwrap() = Some(Arc::clone(&observer));
    let scheduled = backend_receipt(
        b,
        BackendOperation::ReconcileMove {
            data_group: scenario.group,
            job_id: scenario.request.job_id,
        },
        scenario.deadline,
    )
    .await;
    assert!(
        matches!(scheduled.reply, BackendReply::Reconcile { data_group, job_id, result: Ok(ReconcileStatus::Scheduled) } if data_group == scenario.group && job_id == scenario.request.job_id)
    );
    log::info!(
        "raft_delayed_schedule node=2 group=1 job={:?} response_received_at={:?} caller_link_closed_at={:?} reply_loss_claim=false",
        scenario.request.job_id.get(),
        scheduled.received_at,
        scheduled.closed_at
    );
    let joint = timeout_at(scenario.deadline, pause.wait_reached())
        .await
        .unwrap();
    let joint_reached_at = Instant::now();
    assert_eq!(
        joint.membership.membership().get_joint_config(),
        &expected_joint
    );
    require_all_four_joint(&owner.cluster, scenario, &joint).await;
    let original = observer.snapshot().expect("missing exact original native-call entry receipt; storage pause and runner liveness are insufficient");
    assert!(original.entered_at <= joint_reached_at);
    assert!(scheduled.received_at <= scheduled.closed_at);
    assert!(
        original.result.is_none(),
        "actual original native call must still be pending: {original:?}"
    );
    (pause, joint, observer, original)
}

async fn require_all_four_joint(
    fixture: &RelayFixture,
    scenario: &Scenario,
    joint: &JointApplyObservation,
) {
    let joint_id = joint.membership.log_id().unwrap();
    assert!(covers(joint.applied, Some(joint_id)));
    loop {
        let mut observed = Vec::new();
        for node in &fixture.nodes {
            let (applied, membership) = node.service.groups[&scenario.group]
                .reader
                .read_membership()
                .await
                .unwrap();
            observed.push((node.config.node_id, applied, membership));
        }
        if observed.iter().all(|(_, applied, membership)| {
            *membership == joint.membership && covers(*applied, Some(joint_id))
        }) {
            for (node, applied, membership) in observed {
                log::info!(
                    "raft_delayed_all_four_joint node={} group=1 applied={applied:?} membership={membership:?} b_apply_return_held=true",
                    node.get()
                );
            }
            return;
        }
        assert!(
            Instant::now() < scenario.deadline,
            "all four must apply the same full joint identity: {observed:?}"
        );
        sleep(Duration::from_millis(10)).await;
    }
}

fn require_original_pending(
    nodes: &[NodeRuntime],
    scenario: &Scenario,
    pause: &JointApplyPause,
    joint: &JointApplyObservation,
    observer: &NativeMembershipObservation,
    original: &NativeMembershipCall,
) -> Id {
    let held = pause
        .held_observation()
        .expect("original apply return must still be held");
    assert_eq!(held.applied, joint.applied);
    assert_eq!(held.membership, joint.membership);
    let current = observer
        .snapshot()
        .expect("exact native entry receipt must remain present");
    assert_eq!(current.identity, original.identity);
    assert_eq!(current.identity.node, NodeId::new(2).unwrap());
    assert_eq!(current.identity.group, scenario.group);
    assert_eq!(current.identity.job, scenario.request.job_id);
    assert_eq!(current.identity.target, BTreeSet::from([1, 2, 4]));
    assert_ne!(current.invocation, 0);
    assert_eq!(current.invocation, original.invocation);
    assert_eq!(current.entered_at, original.entered_at);
    assert!(
        current.result.is_none() && current.returned_at.is_none(),
        "original native call already returned; delayed-native premise failed: {current:?}"
    );
    let movement = &nodes[1].service.movement;
    let tasks = movement.tasks.lock().unwrap();
    assert_eq!(tasks.len(), 1);
    let running = tasks.get(&scenario.group).unwrap();
    assert_eq!(running.job_id, scenario.request.job_id);
    assert!(!running.task.is_finished());
    assert_eq!(movement.active.available_permits(), 3);
    assert!(!movement.stopping.load(Ordering::Acquire));
    running.task.id()
}

async fn complete_same_job_on_a(fixture: &RelayFixture, scenario: &Scenario) -> MoveState {
    let a = &fixture.nodes[0];
    let authority = a
        .service
        .observe_membership(scenario.group, scenario.deadline)
        .await
        .unwrap();
    assert_eq!(authority.voters, scenario.intent.old_voters);
    assert_eq!(authority.joint_voters, Some(scenario.intent.target_voters));
    let scheduled = backend_receipt(
        a,
        BackendOperation::ReconcileMove {
            data_group: scenario.group,
            job_id: scenario.request.job_id,
        },
        scenario.deadline,
    )
    .await;
    assert!(matches!(
        scheduled.reply,
        BackendReply::Reconcile {
            result: Ok(ReconcileStatus::Scheduled),
            ..
        }
    ));
    let completed = loop {
        let state = read_metadata(fixture, scenario).await;
        if matches!(
            state.record.as_ref().map(|record| &record.progress),
            Some(MoveProgress::Completed { .. })
        ) {
            break state;
        }
        assert!(
            Instant::now() < scenario.deadline,
            "A must complete while B's original native call remains held"
        );
        sleep(Duration::from_millis(10)).await;
    };
    require_completed_placement(fixture, scenario, &completed);
    let uniform = a
        .service
        .observe_membership(scenario.group, scenario.deadline)
        .await
        .unwrap();
    require_target(&uniform, &scenario.intent).unwrap();
    let MoveProgress::Completed { membership } = completed.record.as_ref().unwrap().progress else {
        unreachable!()
    };
    assert_eq!(completion_position(uniform.membership).unwrap(), membership);
    get(
        a,
        scenario.group,
        scenario.keys[0].clone(),
        Some(scenario.baseline.clone()),
        scenario.deadline,
    )
    .await;
    get(
        a,
        scenario.group,
        scenario.keys[1].clone(),
        None,
        scenario.deadline,
    )
    .await;
    let sibling_leader = healthy_leader(
        &fixture.nodes,
        scenario.sibling,
        &[0, 1, 2],
        scenario.deadline,
    )
    .await;
    let sibling = &fixture.nodes[sibling_leader];
    get(
        sibling,
        scenario.sibling,
        scenario.sibling_key.clone(),
        Some(scenario.baseline.clone()),
        scenario.deadline,
    )
    .await;
    let control = Bytes::from_static(b"sibling live while B native await is held");
    put(
        sibling,
        scenario.sibling,
        scenario.sibling_key.clone(),
        control.clone(),
        scenario.deadline,
    )
    .await;
    get(
        sibling,
        scenario.sibling,
        scenario.sibling_key.clone(),
        Some(control),
        scenario.deadline,
    )
    .await;
    log::info!(
        "raft_delayed_replacement node=1 group=1 same_job={:?} revision=2 membership={:?} baseline_value=true tombstone=true sibling_put_get=true",
        scenario.request.job_id.get(),
        uniform.membership
    );
    completed
}

async fn wait_original_result(
    observer: &NativeMembershipObservation,
    original: &NativeMembershipCall,
    deadline: Instant,
) -> NativeMembershipCall {
    loop {
        let current = observer.snapshot().unwrap();
        assert_eq!(current.identity, original.identity);
        assert_eq!(current.invocation, original.invocation);
        if current.result.is_some() {
            assert!(current.returned_at.is_some());
            return current;
        }
        assert!(
            Instant::now() < deadline,
            "original owned native future must resolve after release"
        );
        sleep(Duration::from_millis(10)).await;
    }
}

pub(super) fn require_completed_placement(
    fixture: &RelayFixture,
    scenario: &Scenario,
    state: &MoveState,
) {
    assert_eq!(state.directory.revision, 2);
    assert_eq!(
        state.directory.voters(scenario.group).unwrap(),
        scenario.intent.target_voters
    );
    assert_eq!(state.record.as_ref().unwrap().intent, scenario.intent);
    assert!(matches!(
        state.record.as_ref().unwrap().progress,
        MoveProgress::Completed { .. }
    ));
    for endpoint in &fixture.endpoints {
        assert_eq!(
            state
                .directory
                .nodes
                .iter()
                .find(|node| node.node_id == endpoint.node_id),
            Some(endpoint)
        );
    }
    for unchanged in [GroupId::new(0), scenario.sibling] {
        assert_eq!(
            state.directory.voters(unchanged),
            fixture.directory.voters(unchanged)
        );
    }
}

pub(super) async fn require_no_regression(
    fixture: &RelayFixture,
    scenario: &Scenario,
    completed: &MoveState,
) {
    let state = read_metadata(fixture, scenario).await;
    assert_eq!(
        &state, completed,
        "stale executor must not republish, replace job, or regress directory"
    );
    require_completed_placement(fixture, scenario, &state);
    let leader = healthy_leader(
        &fixture.nodes,
        scenario.group,
        &[0, 1, 3],
        scenario.deadline,
    )
    .await;
    let node = &fixture.nodes[leader];
    let uniform = node
        .service
        .observe_membership(scenario.group, scenario.deadline)
        .await
        .unwrap();
    require_target(&uniform, &scenario.intent).unwrap();
    let MoveProgress::Completed { membership } = completed.record.as_ref().unwrap().progress else {
        unreachable!()
    };
    let current_membership = LogId::new(
        openraft::CommittedLeaderId::new(
            uniform.membership.term,
            uniform.membership.leader_node_id,
        ),
        uniform.membership.index,
    );
    let published_membership = LogId::new(
        openraft::CommittedLeaderId::new(membership.term, membership.leader_node_id.get()),
        membership.index,
    );
    // A legal same-target entry can advance membership after publication. The
    // authoritative history must cover the retained identity, while placement,
    // endpoints and the completed metadata record remain exact below/above.
    assert!(covers(Some(current_membership), Some(published_membership)));
    for endpoint in &fixture.endpoints {
        let expected =
            (endpoint.node_id != scenario.request.remove_node).then_some(endpoint.peer_address);
        assert_eq!(uniform.peer(endpoint.node_id), expected);
    }
    get(
        node,
        scenario.group,
        scenario.keys[0].clone(),
        Some(scenario.baseline.clone()),
        scenario.deadline,
    )
    .await;
    get(
        node,
        scenario.group,
        scenario.keys[1].clone(),
        None,
        scenario.deadline,
    )
    .await;
    let fresh = Bytes::from_static(b"fresh after original native result");
    put(
        node,
        scenario.group,
        scenario.keys[2].clone(),
        fresh.clone(),
        scenario.deadline,
    )
    .await;
    get(
        node,
        scenario.group,
        scenario.keys[2].clone(),
        Some(fresh),
        scenario.deadline,
    )
    .await;
    delete(
        node,
        scenario.group,
        scenario.keys[2].clone(),
        scenario.deadline,
    )
    .await;
    get(
        node,
        scenario.group,
        scenario.keys[2].clone(),
        None,
        scenario.deadline,
    )
    .await;
    let conflicting = MoveReplica {
        job_id: MoveJobId::new([112; 16]).unwrap(),
        data_group: scenario.group,
        remove_node: NodeId::new(4).unwrap(),
        add_node: fixture.endpoints[2].clone(),
        expected_directory_revision: 2,
    };
    assert!(
        conflicting.derive_intent(&completed.directory).is_ok(),
        "later ABD -> ABC candidate must be valid before testing the permanent lifetime-job conflict"
    );
    let metadata = healthy_leader(
        &fixture.nodes,
        GroupId::new(0),
        &[0, 1, 2],
        scenario.deadline,
    )
    .await;
    let rejected = backend_receipt(
        &fixture.nodes[metadata],
        BackendOperation::MoveReplica(conflicting),
        scenario.deadline,
    )
    .await;
    assert!(matches!(
        rejected.reply,
        BackendReply::Move {
            result: AdminResult::Conflict { .. },
            ..
        }
    ));
    assert_eq!(read_metadata(fixture, scenario).await, *completed);
    log::info!(
        "raft_delayed_no_regression revision=2 same_completed_job=true exact_endpoints=true uniform_ABD=true C_voter=false baseline_value=true tombstone=true fresh_put_get_delete=true conflicting_job_rejected=true uncertain_mutations_replayed=false"
    );
}

pub(super) async fn read_metadata(fixture: &RelayFixture, scenario: &Scenario) -> MoveState {
    let metadata = healthy_leader(
        &fixture.nodes,
        GroupId::new(0),
        &[0, 1, 2],
        scenario.deadline,
    )
    .await;
    let reply = backend_receipt(
        &fixture.nodes[metadata],
        BackendOperation::GetMove {
            data_group: scenario.group,
        },
        scenario.deadline,
    )
    .await;
    let BackendReply::MoveState {
        data_group,
        result: Ok(state),
    } = reply.reply
    else {
        panic!("fresh metadata authority required: {:?}", reply.reply)
    };
    assert_eq!(data_group, scenario.group);
    *state
}

pub(super) async fn backend_receipt(
    node: &NodeRuntime,
    operation: BackendOperation,
    deadline: Instant,
) -> BackendReceipt {
    let link = BackendLink::connect(
        node.config.endpoint.data_address,
        LinkConfig::default(),
        deadline,
    )
    .await
    .unwrap();
    // Link's expected-response token validates cluster/node/group/job/type.
    // Close and join this caller's transport before asserting either outcome.
    let result = match link.try_submit(
        BackendRequest {
            cluster_id: node.config.cluster_id,
            target_node: node.config.node_id,
            operation,
        },
        deadline,
    ) {
        Ok(pending) => pending.await.map_err(|error| format!("{error:?}")),
        Err(error) => Err(format!("{error:?}")),
    };
    let received_at = Instant::now();
    link.close().await;
    let closed_at = Instant::now();
    let response =
        result.expect("typed backend request must finish with its original deadline; no replay");
    assert_eq!(response.cluster_id, node.config.cluster_id);
    assert_eq!(response.node_id, node.config.node_id);
    BackendReceipt {
        reply: response.reply,
        received_at,
        closed_at,
    }
}

pub(super) async fn put(
    node: &NodeRuntime,
    group: GroupId,
    key: Bytes,
    value: Bytes,
    deadline: Instant,
) {
    let receipt = backend_receipt(
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
            receipt.reply,
            BackendReply::Kv {
                response: KvResponse::Mutation(MutationOutcome::Committed),
                ..
            }
        ),
        "original PUT must commit without replay: {:?}",
        receipt.reply
    );
}

pub(super) async fn delete(node: &NodeRuntime, group: GroupId, key: Bytes, deadline: Instant) {
    let receipt = backend_receipt(
        node,
        BackendOperation::Kv {
            group_id: group,
            request: KvRequest::Delete { key },
        },
        deadline,
    )
    .await;
    assert!(
        matches!(
            receipt.reply,
            BackendReply::Kv {
                response: KvResponse::Mutation(MutationOutcome::Committed),
                ..
            }
        ),
        "original DELETE must commit without replay: {:?}",
        receipt.reply
    );
}

pub(super) async fn get(
    node: &NodeRuntime,
    group: GroupId,
    key: Bytes,
    expected: Option<Bytes>,
    deadline: Instant,
) {
    let receipt = backend_receipt(
        node,
        BackendOperation::Kv {
            group_id: group,
            request: KvRequest::Get { key },
        },
        deadline,
    )
    .await;
    assert!(
        matches!(&receipt.reply, BackendReply::Kv { response: KvResponse::Value(value), .. } if *value == expected),
        "fresh strong GET must match value/tombstone: {:?}",
        receipt.reply
    );
}
