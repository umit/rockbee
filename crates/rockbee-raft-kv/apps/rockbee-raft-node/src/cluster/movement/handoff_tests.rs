//! Receiver contracts; synthetic model fixtures are separate from the TCP test.
#![allow(clippy::unwrap_used, clippy::panic)]
use super::handoff::{HandoffIdentity, prepared_executor, prepared_from_view};
use super::*;
use crate::control::MetadataView;
use rockbee_raft_protocol::{
    ClusterId, ShardConfig,
    backend::PreparedMoveState,
    routing::{ClusterDirectory, NodeEndpoint},
};

#[derive(Clone, Debug)]
pub(super) struct SealCall {
    pub(super) identity: HandoffIdentity,
    pub(super) entered_at: Instant,
    pub(super) returned_at: Option<Instant>,
    pub(super) result: Option<Result<(), String>>,
}

/// One fixture-owned observation of the actual receiver await; no pause or gate.
pub(super) struct SealObservation {
    expected: HandoffIdentity,
    call: tokio::sync::watch::Sender<Option<SealCall>>,
}

impl SealObservation {
    pub(super) fn new(expected: HandoffIdentity) -> Self {
        Self {
            expected,
            call: tokio::sync::watch::channel(None).0,
        }
    }

    pub(super) fn enter(self: &Arc<Self>, identity: HandoffIdentity) -> Option<Arc<Self>> {
        if identity != self.expected {
            return None;
        }
        let mut entered = false;
        self.call.send_if_modified(|slot| {
            if slot.is_some() {
                return false;
            }
            *slot = Some(SealCall {
                identity,
                entered_at: Instant::now(),
                returned_at: None,
                result: None,
            });
            entered = true;
            true
        });
        entered.then(|| Arc::clone(self))
    }

    pub(super) fn finish(&self, result: &Result<(), crate::types::StoreError>) {
        self.call.send_modify(|slot| {
            let call = slot.as_mut().expect("seal result follows actual entry");
            assert!(call.result.is_none());
            call.returned_at = Some(Instant::now());
            call.result = Some(result.as_ref().copied().map_err(ToString::to_string));
        });
    }

    pub(super) fn snapshot(&self) -> Option<SealCall> {
        self.call.borrow().clone()
    }

    pub(super) async fn wait(&self, completed: bool, deadline: Instant) -> SealCall {
        let mut receiver = self.call.subscribe();
        tokio::time::timeout_at(deadline, async {
            loop {
                if let Some(call) = receiver.borrow_and_update().clone()
                    && (!completed || call.result.is_some())
                {
                    return call;
                }
                receiver.changed().await.unwrap();
            }
        })
        .await
        .expect("bounded seal observation")
    }
}

fn endpoint(id: u64) -> NodeEndpoint {
    NodeEndpoint {
        node_id: NodeId::new(id).unwrap(),
        peer_address: format!("127.0.0.1:{}", 11000 + id * 2).parse().unwrap(),
        data_address: format!("127.0.0.1:{}", 11001 + id * 2).parse().unwrap(),
    }
}

fn prepared_model() -> (MetadataView, ClusterDirectory, HandoffIdentity) {
    let origin = ClusterDirectory::initial(
        ClusterId::new([41; 16]).unwrap(),
        ShardConfig::new(2, 0).unwrap(),
        (1..=3).map(endpoint).collect(),
    )
    .unwrap();
    let mut view =
        MetadataView::from_parts(Some(origin.clone()), Some(origin.clone()), BTreeMap::new())
            .unwrap();
    let first = MoveReplica {
        job_id: MoveJobId::new([42; 16]).unwrap(),
        data_group: GroupId::new(1),
        remove_node: NodeId::new(3).unwrap(),
        add_node: endpoint(4),
        expected_directory_revision: 1,
    }
    .derive_intent(&origin)
    .unwrap();
    view.apply(MetadataCommand::BeginMove {
        intent: first.clone(),
    })
    .unwrap();
    view.apply(MetadataCommand::CompleteMove {
        expected_directory_revision: 1,
        data_group: first.data_group,
        job_id: first.job_id,
        observation: MembershipObservation {
            uniform: true,
            voters: first.target_voters,
            replacement_endpoint: endpoint(4),
            membership: CommittedLogPosition {
                term: 7,
                leader_node_id: NodeId::new(1).unwrap(),
                index: 19,
            },
        },
    })
    .unwrap();
    let successor = MoveReplica {
        job_id: MoveJobId::new([43; 16]).unwrap(),
        data_group: first.data_group,
        remove_node: NodeId::new(2).unwrap(),
        add_node: endpoint(3),
        expected_directory_revision: 2,
    }
    .derive_intent(view.directory().unwrap())
    .unwrap();
    view.apply(MetadataCommand::PrepareSuccessor {
        predecessor_job_id: first.job_id,
        intent: successor.clone(),
    })
    .unwrap();
    (
        view,
        origin,
        HandoffIdentity {
            data_group: first.data_group,
            predecessor_revision: 1,
            predecessor_job_id: first.job_id,
            successor_job_id: successor.job_id,
        },
    )
}

#[test]
fn handoff_projection_preserves_frozen_preparing_and_absent_group() {
    let (view, origin, identity) = prepared_model();
    let state = prepared_from_view(view.clone(), &origin, identity.data_group)
        .unwrap()
        .unwrap();
    assert_eq!(state.directory.revision, 2);
    assert_eq!(state.first.intent.job_id, identity.predecessor_job_id);
    assert_eq!(state.preparing.intent.job_id, identity.successor_job_id);
    assert_eq!(
        state.preparing.executors,
        [endpoint(1), endpoint(2), endpoint(3), endpoint(4)]
    );
    assert_eq!(view.job(identity.data_group), Some(&state.first));
    assert!(
        prepared_from_view(view, &origin, GroupId::new(2))
            .unwrap()
            .is_none()
    );
}

#[test]
fn handoff_projection_does_not_reopen_an_accepted_successor() {
    use crate::control::{HandoffFenceReceipt, HandoffProof, SealReceipt};
    let (mut view, origin, identity) = prepared_model();
    let first = view.first_job(identity.data_group).unwrap();
    let preparing = view.prepared_successor(identity.data_group).unwrap();
    let position = |index| ObservedLogPosition {
        term: 7,
        leader_node_id: 1,
        index,
    };
    let mut peers = [None; 16];
    for (slot, id) in first.intent.target_voters.iter().enumerate() {
        peers[slot] = Some((*id, endpoint(id.get()).peer_address));
    }
    // Synthetic model proof only: the runtime test separately establishes the
    // native receipts. This checks the read projection's non-runnable boundary.
    let proof = HandoffProof {
        seals: preparing.executors.clone().map(|endpoint| SealReceipt {
            endpoint,
            data_group: identity.data_group,
            predecessor_revision: identity.predecessor_revision,
            predecessor_job_id: identity.predecessor_job_id,
        }),
        fence: HandoffFenceReceipt {
            data_group: identity.data_group,
            predecessor_revision: identity.predecessor_revision,
            predecessor_job_id: identity.predecessor_job_id,
            successor_job_id: identity.successor_job_id,
            committed: CommittedLogPosition {
                term: 7,
                leader_node_id: NodeId::new(1).unwrap(),
                index: 25,
            },
        },
        observed: AppliedMembership {
            data_group: identity.data_group,
            applied: position(25),
            membership: position(19),
            voters: first.intent.target_voters,
            joint_voters: None,
            peers,
        },
    };
    let transition = view
        .apply(MetadataCommand::ActivateSuccessor {
            expected_directory_revision: 2,
            data_group: identity.data_group,
            predecessor_job_id: identity.predecessor_job_id,
            successor_job_id: identity.successor_job_id,
            proof: Box::new(proof),
        })
        .unwrap();
    assert!(matches!(
        transition.result,
        MetadataResult::Activated { .. }
    ));
    assert_eq!(
        view.job(identity.data_group).unwrap().intent.job_id,
        identity.successor_job_id
    );
    assert!(
        prepared_from_view(view, &origin, identity.data_group)
            .unwrap()
            .is_none()
    );
}

#[test]
fn handoff_executor_requires_exact_group_jobs_revision_and_frozen_endpoint() {
    let (view, _, identity) = prepared_model();
    let state = PreparedMoveState {
        directory: view.directory().unwrap().clone(),
        first: view.first_job(identity.data_group).unwrap().clone(),
        preparing: view
            .prepared_successor(identity.data_group)
            .unwrap()
            .clone(),
    };
    for id in 1..=4 {
        assert_eq!(
            prepared_executor(&state, identity, NodeId::new(id).unwrap()).unwrap(),
            endpoint(id)
        );
    }
    let invalid = [
        HandoffIdentity {
            data_group: GroupId::new(2),
            ..identity
        },
        HandoffIdentity {
            predecessor_revision: 2,
            ..identity
        },
        HandoffIdentity {
            predecessor_job_id: identity.successor_job_id,
            ..identity
        },
        HandoffIdentity {
            successor_job_id: MoveJobId::new([44; 16]).unwrap(),
            ..identity
        },
    ];
    for identity in invalid {
        assert!(prepared_executor(&state, identity, NodeId::new(1).unwrap()).is_err());
    }
    assert!(prepared_executor(&state, identity, NodeId::new(5).unwrap()).is_err());
    let mut wrong = state.clone();
    wrong.preparing.executors[0].data_address = endpoint(5).data_address;
    assert!(prepared_executor(&wrong, identity, NodeId::new(1).unwrap()).is_err());
    let mut pending = state;
    pending.first.progress = MoveProgress::Pending;
    assert!(prepared_executor(&pending, identity, NodeId::new(1).unwrap()).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handoff_receivers_read_seal_and_fence_without_kv_submission() {
    let _ = env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("rockbee_raft_node=info"),
    )
    .is_test(true)
    .try_init();
    let fixture = Arc::new(failure_tests::RelayFixture::start().await.unwrap());
    let owner = Arc::clone(&fixture);
    let outcome = tokio::spawn(async move { receiver_scenario(&owner).await }).await;
    let mut fixture = match Arc::try_unwrap(fixture) {
        Ok(fixture) => fixture,
        Err(_) => panic!("completed observer retained fixture"),
    };
    fixture.close().await;
    if let Err(error) = outcome {
        if error.is_panic() {
            std::panic::resume_unwind(error.into_panic());
        }
        panic!("handoff receiver observer failed after cleanup: {error}");
    }
}

async fn receiver_scenario(fixture: &failure_tests::RelayFixture) {
    use super::delayed_executor_tests::{establish_baseline_at, get};
    use super::tests::{backend, force_authority, metadata_state};
    let scenario = establish_baseline_at(fixture, 0, MoveJobId::new([51; 16]).unwrap()).await;
    let group = scenario.group;
    let deadline = scenario.deadline;
    let meta =
        failure_tests::healthy_leader(&fixture.nodes, GroupId::new(0), &[0, 1, 2], deadline).await;
    let absent = backend(
        &fixture.nodes[meta],
        BackendOperation::GetPreparedMove { data_group: group },
        deadline,
    )
    .await;
    assert!(
        matches!(
            absent,
            BackendReply::PreparedMoveState {
                result: Ok(None),
                ..
            }
        ),
        "{absent:?}"
    );
    let scheduled = backend(
        &fixture.nodes[0],
        BackendOperation::ReconcileMove {
            data_group: group,
            job_id: scenario.intent.job_id,
        },
        deadline,
    )
    .await;
    assert!(matches!(
        scheduled,
        BackendReply::Reconcile { result: Ok(_), .. }
    ));
    let completed = loop {
        let state = metadata_state(&fixture.nodes, group, deadline).await;
        if matches!(
            state.record.as_ref().map(|record| &record.progress),
            Some(MoveProgress::Completed { .. })
        ) {
            break state;
        }
        assert!(Instant::now() < deadline, "first move must complete");
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let successor = MoveReplica {
        job_id: MoveJobId::new([52; 16]).unwrap(),
        data_group: group,
        remove_node: NodeId::new(2).unwrap(),
        add_node: fixture.endpoints[2].clone(),
        expected_directory_revision: 2,
    }
    .derive_intent(&completed.directory)
    .unwrap();
    let meta =
        failure_tests::healthy_leader(&fixture.nodes, GroupId::new(0), &[0, 1, 2], deadline).await;
    let result = fixture.nodes[meta].service.groups[&GroupId::new(0)]
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
    let state = backend(
        &fixture.nodes[meta],
        BackendOperation::GetPreparedMove { data_group: group },
        deadline,
    )
    .await;
    let BackendReply::PreparedMoveState {
        result: Ok(Some(state)),
        ..
    } = state
    else {
        panic!("missing preparation: {state:?}")
    };
    assert_eq!(state.first, completed.record.unwrap());
    assert_eq!(state.preparing.intent, successor);
    let executors: [NodeEndpoint; 4] = fixture.endpoints.clone().try_into().unwrap();
    assert_eq!(state.preparing.executors, executors);
    let before: Vec<_> = fixture
        .nodes
        .iter()
        .map(|node| node.service.test_client_write_calls())
        .collect();
    // Every node validates the frozen preparation independently. This fixture
    // does not activate the successor or claim a coordinator implementation.
    for node in &fixture.nodes {
        let reply = backend(
            node,
            BackendOperation::SealPredecessor {
                data_group: group,
                predecessor_revision: 1,
                predecessor_job_id: scenario.intent.job_id,
                successor_job_id: successor.job_id,
            },
            deadline,
        )
        .await;
        let BackendReply::PredecessorSealed {
            result: Ok(receipt),
            ..
        } = reply
        else {
            panic!("seal failed: {reply:?}")
        };
        assert_eq!(
            receipt.endpoint,
            fixture.endpoints[usize::try_from(node.config.node_id.get() - 1).unwrap()]
        );
        assert!(
            node.service.groups[&group]
                .reader
                .membership_control()
                .begin(crate::storage::MovementIdentity {
                    revision: 1,
                    job_id: scenario.intent.job_id,
                })
                .await
                .is_err()
        );
    }
    force_authority(&fixture.nodes, 0, group, deadline).await;
    reject_invalid_receivers(fixture, &scenario, successor.job_id).await;
    let sibling =
        failure_tests::healthy_leader(&fixture.nodes, scenario.sibling, &[0, 1, 2], deadline).await;
    get(
        &fixture.nodes[sibling],
        scenario.sibling,
        scenario.sibling_key.clone(),
        Some(scenario.baseline.clone()),
        deadline,
    )
    .await;
    let sibling_reader = &fixture.nodes[sibling].service.groups[&scenario.sibling].reader;
    let sibling_before = sibling_reader
        .read_local(scenario.sibling_key.clone())
        .await
        .unwrap();
    assert_eq!(sibling_before.value, Some(scenario.baseline.clone()));
    let fence_operation = BackendOperation::CommitHandoffFence {
        data_group: group,
        predecessor_revision: 1,
        predecessor_job_id: scenario.intent.job_id,
        successor_job_id: successor.job_id,
    };
    let native_admission = &fixture.nodes[0].service.groups[&group].mutation_admission;
    let quota = u32::try_from(native_admission.available_permits()).unwrap();
    assert!(quota > 0);
    let held_quota = Arc::clone(native_admission)
        .try_acquire_many_owned(quota)
        .unwrap();
    let overloaded = backend(&fixture.nodes[0], fence_operation.clone(), deadline).await;
    drop(held_quota);
    assert!(
        matches!(overloaded, BackendReply::HandoffFenced { result: Err(ReadFailure(ref error)), .. }
        if error.reason == FailureReason::Overloaded),
        "{overloaded:?}"
    );
    let before_membership = fixture.nodes[0]
        .service
        .observe_membership(group, deadline)
        .await
        .unwrap();
    let reply = backend(&fixture.nodes[0], fence_operation.clone(), deadline).await;
    let BackendReply::HandoffFenced {
        result: Ok(receipt),
        ..
    } = reply
    else {
        panic!("fence failed: {reply:?}")
    };
    assert!(receipt.committed.index > before_membership.applied.index);
    let after = fixture.nodes[0]
        .service
        .observe_membership(group, deadline)
        .await
        .unwrap();
    assert_eq!(after.membership, before_membership.membership);
    check_applied_covers(
        Some(LogId::new(
            openraft::CommittedLeaderId::new(after.applied.term, after.applied.leader_node_id),
            after.applied.index,
        )),
        Some(LogId::new(
            openraft::CommittedLeaderId::new(
                receipt.committed.term,
                receipt.committed.leader_node_id.get(),
            ),
            receipt.committed.index,
        )),
    )
    .unwrap();
    get(
        &fixture.nodes[0],
        group,
        scenario.keys[0].clone(),
        Some(scenario.baseline.clone()),
        deadline,
    )
    .await;
    get(
        &fixture.nodes[0],
        group,
        scenario.keys[1].clone(),
        None,
        deadline,
    )
    .await;
    assert_eq!(
        fixture
            .nodes
            .iter()
            .map(|node| node.service.test_client_write_calls())
            .collect::<Vec<_>>(),
        before
    );
    assert_eq!(
        metadata_state(&fixture.nodes, group, deadline)
            .await
            .record
            .unwrap(),
        state.first
    );
    let sibling_after = sibling_reader
        .read_local(scenario.sibling_key.clone())
        .await
        .unwrap();
    assert_eq!(sibling_after.value, Some(scenario.baseline.clone()));
    assert_eq!(sibling_after.applied, sibling_before.applied);
    let raft = &fixture.nodes[0].service.groups[&group].raft;
    assert!(!native_admission.is_closed());
    raft.shutdown().await.unwrap();
    assert!(matches!(
        &raft.metrics().borrow().running_state,
        Err(openraft::error::Fatal::Stopped)
    ));
    assert_eq!(
        fixture.nodes[0].service.leader_hint(group),
        Some(NodeId::new(1).unwrap()),
        "this real shutdown schedule must naturally retain the leader hint to reach client_write"
    );
    let stopped = backend(&fixture.nodes[0], fence_operation, deadline).await;
    assert!(
        matches!(stopped, BackendReply::HandoffFenced { result: Err(ReadFailure(ref error)), .. }
        if error.reason == FailureReason::Unavailable && error.message.contains("raft stopped")),
        "{stopped:?}"
    );
    assert!(
        native_admission.is_closed(),
        "actual Fatal closes admission before the handler returns"
    );
    assert_eq!(
        fixture
            .nodes
            .iter()
            .map(|node| node.service.test_client_write_calls())
            .collect::<Vec<_>>(),
        before
    );
    log::info!(
        "raft_handoff_receivers group=1 seals=4 fence={:?} kv_calls_unchanged=true sibling_value_applied_unchanged=true overloaded_before_native=true real_stopped_fatal_gate_closed=true successor_activated=false",
        receipt.committed
    );
}

/// Real backend rejection paths after the predecessor is uniform and Preparing
/// is durable. No native writes are scheduled during these position checks.
async fn reject_invalid_receivers(
    fixture: &failure_tests::RelayFixture,
    scenario: &delayed_executor_tests::Scenario,
    successor_job_id: MoveJobId,
) {
    use super::tests::backend;
    let group = scenario.group;
    let deadline = scenario.deadline;
    let join_reply = backend(
        &fixture.nodes[3],
        BackendOperation::GetPreparedMove { data_group: group },
        deadline,
    )
    .await;
    assert!(
        matches!(join_reply, BackendReply::PreparedMoveState { result: Err(ReadFailure(ref error)), .. }
        if error.reason == FailureReason::NotLeader),
        "{join_reply:?}"
    );

    let reader = &fixture.nodes[0].service.groups[&group].reader;
    let baseline = reader.read_membership().await.unwrap();
    let unrelated = MoveJobId::new([53; 16]).unwrap();
    for (predecessor_job_id, successor_job_id) in [
        (unrelated, successor_job_id),
        (scenario.intent.job_id, unrelated),
    ] {
        let seal = backend(
            &fixture.nodes[0],
            BackendOperation::SealPredecessor {
                data_group: group,
                predecessor_revision: 1,
                predecessor_job_id,
                successor_job_id,
            },
            deadline,
        )
        .await;
        assert!(
            matches!(seal, BackendReply::PredecessorSealed { result: Err(ReadFailure(ref error)), .. }
            if error.reason == FailureReason::InvalidRequest),
            "{seal:?}"
        );
        assert_eq!(reader.read_membership().await.unwrap(), baseline);
        let fence = backend(
            &fixture.nodes[0],
            BackendOperation::CommitHandoffFence {
                data_group: group,
                predecessor_revision: 1,
                predecessor_job_id,
                successor_job_id,
            },
            deadline,
        )
        .await;
        assert!(
            matches!(fence, BackendReply::HandoffFenced { result: Err(ReadFailure(ref error)), .. }
            if error.reason == FailureReason::InvalidRequest),
            "{fence:?}"
        );
        assert_eq!(reader.read_membership().await.unwrap(), baseline);
    }

    let follower = &fixture.nodes[1];
    assert_ne!(
        follower.service.leader_hint(group),
        Some(follower.config.node_id)
    );
    let follower_reader = &follower.service.groups[&group].reader;
    // Observe actual convergence before taking the follower's baseline; a
    // delayed application of the leader's election entry is not a fence effect.
    let follower_before = loop {
        let state = follower_reader.read_membership().await.unwrap();
        if covers(state.0, baseline.0) {
            break state;
        }
        assert!(
            Instant::now() < deadline,
            "follower must cover the stable data history"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let nonleader = backend(
        follower,
        BackendOperation::CommitHandoffFence {
            data_group: group,
            predecessor_revision: 1,
            predecessor_job_id: scenario.intent.job_id,
            successor_job_id,
        },
        deadline,
    )
    .await;
    assert!(
        matches!(nonleader, BackendReply::HandoffFenced { result: Err(ReadFailure(ref error)), .. }
        if error.reason == FailureReason::NotLeader),
        "{nonleader:?}"
    );
    assert_eq!(
        follower_reader.read_membership().await.unwrap(),
        follower_before
    );
    assert_eq!(reader.read_membership().await.unwrap(), baseline);
    log::info!(
        "raft_handoff_negative_receivers group=1 join_read_not_leader=true wrong_jobs_rejected=4 nonleader_fence_rejected=true applied_membership_unchanged=true"
    );
}
