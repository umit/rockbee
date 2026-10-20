//! Original A retains Source ABC while B completes; A then adds existing voter D.
#![allow(clippy::unwrap_used, clippy::panic)]
use super::delayed_executor_tests::{
    Scenario, backend_receipt, establish_baseline_at, get, put, read_metadata,
    require_completed_placement, require_no_regression,
};
use super::failure_tests::{RelayFixture, healthy_leader, restore_elections};
use super::stale_source::{NativeLearnerObservation, SourceIdentity, SourcePause, SourceReceipt};
use super::tests::force_authority;
use super::*;
use bytes::Bytes;
use tokio::{task::Id, time::timeout_at};

struct StaleSourceFixture {
    cluster: RelayFixture,
    pause: Mutex<Option<Arc<SourcePause>>>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_source_a_adds_existing_d_only_after_b_completes_same_job() {
    run_stale_source(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_move_seal_rejects_a_runner_with_an_old_source_observation() {
    run_stale_source(true).await;
}

async fn run_stale_source(seal_before_release: bool) {
    let _ = env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("rockbee_raft_node=info"),
    )
    .is_test(true)
    .try_init();
    let owner = Arc::new(StaleSourceFixture {
        cluster: RelayFixture::start().await.unwrap(),
        pause: Mutex::new(None),
    });
    let scenario_owner = Arc::clone(&owner);
    // Retain all release/runtime owners outside the assertion task, including
    // when a native-success premise fails. Never cancel the native future.
    let outcome =
        tokio::spawn(
            async move { stale_source_scenario(&scenario_owner, seal_before_release).await },
        )
        .await;
    if let Some(pause) = owner.pause.lock().unwrap().as_ref() {
        pause.release();
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
        "raft_stale_source_cleanup pause_released=true elections_restored=true original_native_and_runners_drained=true runtimes_and_links_joined=true"
    );
    if let Err(error) = outcome {
        if error.is_panic() {
            std::panic::resume_unwind(error.into_panic());
        }
        panic!("stale Source assertion task cancelled after owned cleanup: {error}");
    }
}

async fn stale_source_scenario(owner: &StaleSourceFixture, seal_before_release: bool) {
    let fixture = &owner.cluster;
    let scenario = establish_baseline_at(fixture, 0, MoveJobId::new([113; 16]).unwrap()).await;
    let a = &fixture.nodes[0];
    let identity = SourceIdentity {
        node: a.config.node_id,
        group: scenario.group,
        job: scenario.request.job_id,
        learner: scenario.intent.add_node.node_id,
        peer: scenario.intent.add_node.peer_address,
    };
    let pause = Arc::new(SourcePause::new(identity.clone()));
    *owner.pause.lock().unwrap() = Some(Arc::clone(&pause));
    *a.service.movement.before_source_learner.lock().unwrap() = Some(Arc::clone(&pause));
    let observer = Arc::new(NativeLearnerObservation::new(identity.clone()));
    *a.service.movement.native_learner.lock().unwrap() = Some(Arc::clone(&observer));
    let scheduled = backend_receipt(
        a,
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
    assert!(scheduled.received_at <= scheduled.closed_at);
    let captured = timeout_at(scenario.deadline, pause.wait_reached())
        .await
        .unwrap();
    assert_eq!(captured.identity, identity);
    captured.observed.validate().unwrap();
    assert_eq!(captured.observed.data_group, scenario.group);
    assert_eq!(captured.observed.voters, scenario.intent.old_voters);
    assert_eq!(captured.observed.joint_voters, None);
    assert_eq!(captured.observed.peer(identity.learner), None);
    let runner = require_original_held(fixture, &scenario, &pause, &captured, &observer);
    log::info!(
        "raft_stale_source_captured node=1 source={captured:?} original_runner={runner:?} permit_held=true scheduled_received_at={:?} caller_link_closed_at={:?} original_native_entry=None original_native_result=None",
        scheduled.received_at,
        scheduled.closed_at
    );

    force_authority(&fixture.nodes, 1, scenario.group, scenario.deadline).await;
    restore_elections(&fixture.nodes);
    let completed = complete_on_b(fixture, &scenario).await;
    assert_eq!(
        require_original_held(fixture, &scenario, &pause, &captured, &observer),
        runner
    );
    force_authority(&fixture.nodes, 0, scenario.group, scenario.deadline).await;
    let pre_release = a
        .service
        .observe_membership(scenario.group, scenario.deadline)
        .await
        .unwrap();
    require_exact_target(fixture, &scenario, &pre_release);
    restore_elections(&fixture.nodes);
    assert_eq!(
        require_original_held(fixture, &scenario, &pause, &captured, &observer),
        runner
    );
    assert_eq!(read_metadata(fixture, &scenario).await, completed);
    log::info!(
        "raft_stale_source_a_fresh_authority node=1 pre_release={pre_release:?} original_runner={runner:?} same_captured_source=true revision=2 completed={completed:?} original_native_entry=None original_native_result=None"
    );

    if seal_before_release {
        a.service.groups[&scenario.group]
            .reader
            .membership_control()
            .seal(crate::storage::MovementIdentity {
                revision: scenario.intent.expected_directory_revision,
                job_id: scenario.intent.job_id,
            })
            .await
            .unwrap();
        require_original_held(fixture, &scenario, &pause, &captured, &observer);
    }

    let release_requested_at = Instant::now();
    pause.release();
    timeout_at(scenario.deadline, pause.wait_exited())
        .await
        .unwrap();
    assert!(pause.held_receipt().is_none());
    timeout_at(scenario.deadline, a.service.movement.drain())
        .await
        .unwrap();
    assert!(a.service.movement.tasks.lock().unwrap().is_empty());
    assert_eq!(a.service.movement.active.available_permits(), 4);
    if seal_before_release {
        assert!(
            observer.snapshot().is_none(),
            "a captured Source observation cannot bypass the durable local seal"
        );
        let final_membership = a
            .service
            .observe_membership(scenario.group, scenario.deadline)
            .await
            .unwrap();
        require_exact_target(fixture, &scenario, &final_membership);
        assert_eq!(final_membership.membership, pre_release.membership);
        log::info!(
            "raft_move_seal_stale_source node=1 original_runner={runner:?} seal_ack=true original_native_entry=None runner_joined=true unchanged_membership=true"
        );
    } else {
        let returned = observer.snapshot().expect(
        "missing exact original stale Source add_learner entry/result receipt after original runner joined",
    );
        assert_eq!(returned.identity, identity);
        assert!(!returned.blocking);
        assert!(release_requested_at <= returned.entered_at);
        assert!(returned.entered_at <= returned.returned_at.unwrap());
        let actual = returned
            .result
            .as_ref()
            .expect("exact native invocation must resolve")
            .as_ref()
            .expect("stale Source add_learner of existing D must return actual Ok");
        let membership = actual
            .membership
            .as_ref()
            .expect("native membership response required");
        assert_eq!(
            membership.get_joint_config(),
            &vec![BTreeSet::from([1, 2, 4])]
        );
        for endpoint in &fixture.endpoints {
            let expected = (endpoint.node_id != scenario.request.remove_node)
                .then_some(BasicNode::new(endpoint.peer_address.to_string()));
            assert_eq!(
                membership.get_node(&endpoint.node_id.get()),
                expected.as_ref()
            );
        }
        let pre_release_id = native_log_id(pre_release.membership);
        assert!(actual.log_id.index > pre_release_id.index);
        assert!(covers(Some(actual.log_id), Some(pre_release_id)));
        let final_membership = a
            .service
            .observe_membership(scenario.group, scenario.deadline)
            .await
            .unwrap();
        require_exact_target(fixture, &scenario, &final_membership);
        assert!(covers(
            Some(native_log_id(final_membership.membership)),
            Some(actual.log_id)
        ));
        let MoveProgress::Completed {
            membership: published,
        } = completed.record.as_ref().unwrap().progress
        else {
            unreachable!()
        };
        let published_id = LogId::new(
            openraft::CommittedLeaderId::new(published.term, published.leader_node_id.get()),
            published.index,
        );
        assert!(covers(
            Some(native_log_id(final_membership.membership)),
            Some(published_id)
        ));
        log::info!(
            "raft_stale_source_native_success original_runner={runner:?} actual={returned:?} pre_release={pre_release_id:?} final={final_membership:?} post_release_entry=true later_membership_index=true original_runner_joined=true permit_returned=true"
        );
    }
    require_no_regression(fixture, &scenario, &completed).await;
    let sibling = healthy_leader(
        &fixture.nodes,
        scenario.sibling,
        &[0, 1, 2],
        scenario.deadline,
    )
    .await;
    let sibling = &fixture.nodes[sibling];
    let live = Bytes::from_static(b"sibling live while original A Source remains held");
    get(
        sibling,
        scenario.sibling,
        scenario.sibling_key.clone(),
        Some(live),
        scenario.deadline,
    )
    .await;
    let final_control = Bytes::from_static(b"sibling live after original A runner joined");
    put(
        sibling,
        scenario.sibling,
        scenario.sibling_key.clone(),
        final_control.clone(),
        scenario.deadline,
    )
    .await;
    get(
        sibling,
        scenario.sibling,
        scenario.sibling_key.clone(),
        Some(final_control),
        scenario.deadline,
    )
    .await;
    assert_eq!(read_metadata(fixture, &scenario).await, completed);
}

fn require_original_held(
    fixture: &RelayFixture,
    scenario: &Scenario,
    pause: &SourcePause,
    captured: &SourceReceipt,
    observer: &NativeLearnerObservation,
) -> Id {
    assert_eq!(pause.held_receipt().as_ref(), Some(captured));
    assert!(
        observer.snapshot().is_none(),
        "original native entry/result must remain absent before release"
    );
    let movement = &fixture.nodes[0].service.movement;
    let tasks = movement.tasks.lock().unwrap();
    assert_eq!(tasks.len(), 1);
    let running = tasks.get(&scenario.group).unwrap();
    assert_eq!(running.job_id, scenario.request.job_id);
    assert!(!running.task.is_finished());
    assert_eq!(movement.active.available_permits(), 3);
    assert!(!movement.stopping.load(Ordering::Acquire));
    running.task.id()
}

async fn complete_on_b(fixture: &RelayFixture, scenario: &Scenario) -> MoveState {
    let b = &fixture.nodes[1];
    let source = b
        .service
        .observe_membership(scenario.group, scenario.deadline)
        .await
        .unwrap();
    assert_eq!(source.voters, scenario.intent.old_voters);
    assert_eq!(source.joint_voters, None);
    assert_eq!(source.peer(scenario.intent.add_node.node_id), None);
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
            "B must complete while original A Source remains held"
        );
        sleep_until(
            scenario
                .deadline
                .min(Instant::now() + Duration::from_millis(10)),
        )
        .await;
    };
    require_completed_placement(fixture, scenario, &completed);
    let uniform = b
        .service
        .observe_membership(scenario.group, scenario.deadline)
        .await
        .unwrap();
    require_exact_target(fixture, scenario, &uniform);
    let MoveProgress::Completed { membership } = completed.record.as_ref().unwrap().progress else {
        unreachable!()
    };
    assert_eq!(completion_position(uniform.membership).unwrap(), membership);
    get(
        b,
        scenario.group,
        scenario.keys[0].clone(),
        Some(scenario.baseline.clone()),
        scenario.deadline,
    )
    .await;
    get(
        b,
        scenario.group,
        scenario.keys[1].clone(),
        None,
        scenario.deadline,
    )
    .await;
    let sibling = healthy_leader(
        &fixture.nodes,
        scenario.sibling,
        &[0, 1, 2],
        scenario.deadline,
    )
    .await;
    let sibling = &fixture.nodes[sibling];
    get(
        sibling,
        scenario.sibling,
        scenario.sibling_key.clone(),
        Some(scenario.baseline.clone()),
        scenario.deadline,
    )
    .await;
    let live = Bytes::from_static(b"sibling live while original A Source remains held");
    put(
        sibling,
        scenario.sibling,
        scenario.sibling_key.clone(),
        live.clone(),
        scenario.deadline,
    )
    .await;
    get(
        sibling,
        scenario.sibling,
        scenario.sibling_key.clone(),
        Some(live),
        scenario.deadline,
    )
    .await;
    log::info!(
        "raft_stale_source_b_completed node=2 same_job={:?} revision=2 membership={:?} baseline_value=true tombstone=true sibling_put_get=true original_a_source_held=true",
        scenario.request.job_id.get(),
        uniform.membership
    );
    completed
}

fn require_exact_target(fixture: &RelayFixture, scenario: &Scenario, observed: &AppliedMembership) {
    observed.validate().unwrap();
    require_target(observed, &scenario.intent).unwrap();
    for endpoint in &fixture.endpoints {
        let expected =
            (endpoint.node_id != scenario.request.remove_node).then_some(endpoint.peer_address);
        assert_eq!(observed.peer(endpoint.node_id), expected);
    }
}

fn native_log_id(position: ObservedLogPosition) -> LogId {
    LogId::new(
        openraft::CommittedLeaderId::new(position.term, position.leader_node_id),
        position.index,
    )
}
