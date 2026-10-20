//! Synthetic receipts exercise model consistency, never real handoff causality.
use super::*;
use rockbee_raft_protocol::backend::{AppliedMembership, ObservedLogPosition};
use serde::Serialize;

fn position(index: u64) -> CommittedLogPosition {
    CommittedLogPosition {
        term: 7,
        leader_node_id: NodeId::new(1).unwrap(),
        index,
    }
}

fn observed(index: u64) -> ObservedLogPosition {
    ObservedLogPosition {
        term: 7,
        leader_node_id: 1,
        index,
    }
}

fn prepared() -> (MetadataView, MoveRecord, PreparingSuccessor, HandoffProof) {
    let (mut view, first) = successor::completed();
    let next = successor::candidate(&view, 3);
    view.apply(MetadataCommand::PrepareSuccessor {
        predecessor_job_id: first.intent.job_id,
        intent: next,
    })
    .unwrap();
    let preparing = view.prepared_successor(GroupId::new(1)).unwrap().clone();
    let seals = preparing.executors.clone().map(|endpoint| SealReceipt {
        endpoint,
        data_group: first.intent.data_group,
        predecessor_revision: first.intent.expected_directory_revision,
        predecessor_job_id: first.intent.job_id,
    });
    let mut peers = [None; 16];
    for (slot, id) in first.intent.target_voters.iter().enumerate() {
        peers[slot] = Some((
            *id,
            view.directory().unwrap().node(*id).unwrap().peer_address,
        ));
    }
    let proof = HandoffProof {
        seals,
        fence: HandoffFenceReceipt {
            data_group: first.intent.data_group,
            predecessor_revision: first.intent.expected_directory_revision,
            predecessor_job_id: first.intent.job_id,
            successor_job_id: preparing.intent.job_id,
            committed: position(25),
        },
        observed: AppliedMembership {
            data_group: first.intent.data_group,
            applied: observed(25),
            membership: observed(19),
            voters: first.intent.target_voters,
            joint_voters: None,
            peers,
        },
    };
    (view, first, preparing, proof)
}

fn activation(
    first: &MoveRecord,
    preparing: &PreparingSuccessor,
    proof: HandoffProof,
    revision: u64,
) -> MetadataCommand {
    MetadataCommand::ActivateSuccessor {
        expected_directory_revision: revision,
        data_group: first.intent.data_group,
        predecessor_job_id: first.intent.job_id,
        successor_job_id: preparing.intent.job_id,
        proof: Box::new(proof),
    }
}

fn accepted(
    first: &MoveRecord,
    preparing: &PreparingSuccessor,
    proof: HandoffProof,
) -> StoredMoves {
    StoredMoves {
        first: first.clone(),
        successor: Some(SuccessorState::Accepted(Box::new(AcceptedSuccessor {
            record: MoveRecord {
                intent: preparing.intent.clone(),
                progress: MoveProgress::Pending,
            },
            handoff: proof,
        }))),
    }
}

fn load(view: &MetadataView, moves: StoredMoves) -> Result<MetadataView, MetadataError> {
    MetadataView::from_stored_parts(
        view.directory().cloned(),
        view.origin().cloned(),
        BTreeMap::from([(GroupId::new(1), moves)]),
    )
}

#[derive(Serialize)]
struct V3Fixture<'a> {
    marker: [u8; 16],
    version: u8,
    first: &'a MoveRecord,
    accepted: &'a AcceptedSuccessor,
}

fn v3_frame(moves: &StoredMoves, version: u8) -> Vec<u8> {
    let accepted = moves
        .successor
        .as_ref()
        .and_then(|successor| match successor {
            SuccessorState::Accepted(accepted) => Some(accepted),
            SuccessorState::Preparing(_) => None,
        })
        .unwrap();
    rockbee_raft_protocol::encode_frame(&V3Fixture {
        marker: [0; 16],
        version,
        first: &moves.first,
        accepted,
    })
    .unwrap()
}

#[test]
fn activation_switches_only_runnable_job_and_keeps_first_and_directory() {
    let (mut view, first, preparing, proof) = prepared();
    let before = view.clone();
    let command = activation(&first, &preparing, proof.clone(), 2);
    command.validate().unwrap();
    let transition = view.apply(command.clone()).unwrap();
    assert_eq!(
        transition.result,
        MetadataResult::Activated {
            data_group: GroupId::new(1),
            job_id: preparing.intent.job_id
        }
    );
    assert_eq!(transition.effects.len(), 1);
    assert_eq!(transition.effects[0].key, job_key(GroupId::new(1)));
    assert_eq!(view.directory(), before.directory());
    assert_eq!(view.origin(), before.origin());
    assert_eq!(view.first_job(GroupId::new(1)), Some(&first));
    assert_eq!(view.job(GroupId::new(1)).unwrap().intent, preparing.intent);
    assert!(view.prepared_successor(GroupId::new(1)).is_none());
    assert_eq!(
        view.accepted_successor(GroupId::new(1)).unwrap().handoff,
        proof
    );
    let decoded = decode_stored_moves(&transition.effects[0].value).unwrap();
    assert_eq!(decoded, accepted(&first, &preparing, proof));
    assert_eq!(load(&before, decoded).unwrap(), view);
    let stable = view.clone();
    assert_conflict(
        &view
            .apply(MetadataCommand::PrepareSuccessor {
                predecessor_job_id: first.intent.job_id,
                intent: preparing.intent.clone(),
            })
            .unwrap(),
        ConflictReason::ExistingMove,
    );
    assert_eq!(
        view, stable,
        "preparation retry must not downgrade Accepted"
    );
    let retry = view.apply(command).unwrap();
    assert_eq!(
        retry.result,
        MetadataResult::AlreadyActivated {
            data_group: GroupId::new(1),
            job_id: preparing.intent.job_id
        }
    );
    assert!(retry.effects.is_empty());
    assert_eq!(view, stable);
    let late = view.apply(complete(&first.intent, 1)).unwrap();
    assert_eq!(
        late.result,
        MetadataResult::AlreadyCompleted {
            data_group: GroupId::new(1),
            job_id: first.intent.job_id,
            revision: 2
        }
    );
    assert!(late.effects.is_empty());
    assert_eq!(view, stable);
}

#[test]
fn activation_v3_codec_preserves_v2_and_rejects_unknown_or_trailing_payloads() {
    let (view, first, preparing, proof) = prepared();
    let v2 = StoredMoves {
        first: first.clone(),
        successor: Some(SuccessorState::Preparing(Box::new(preparing.clone()))),
    };
    assert_eq!(
        encode_stored_moves(&v2).unwrap(),
        successor::v2_frame(&v2, 2)
    );
    assert_eq!(
        decode_stored_moves(&successor::v2_frame(&v2, 2)).unwrap(),
        v2
    );
    let mut moves = accepted(&first, &preparing, proof);
    for completed in [false, true] {
        if let Some(SuccessorState::Accepted(next)) = &mut moves.successor {
            next.record.progress = if completed {
                MoveProgress::Completed {
                    membership: position(40),
                }
            } else {
                MoveProgress::Pending
            };
        }
        let frame = encode_stored_moves(&moves).unwrap();
        assert_eq!(frame, v3_frame(&moves, 3));
        assert_eq!(&frame[4..20], &[0; 16]);
        assert!(rockbee_raft_protocol::decode_frame::<MoveRecord>(&frame).is_err());
        assert_eq!(decode_stored_moves(&frame).unwrap(), moves);
        assert!(frame.len() <= MAX_STORED_MOVES_BYTES);
        assert!(decode_stored_moves(&v3_frame(&moves, 4)).is_err());
        let mut trailing = frame.clone();
        trailing.push(0);
        let size = u32::try_from(trailing.len() - 4).unwrap();
        trailing[..4].copy_from_slice(&size.to_be_bytes());
        assert!(decode_stored_moves(&trailing).is_err());
        assert!(decode_stored_moves(&frame[..frame.len() - 1]).is_err());
    }
    assert!(
        load(&view, moves).is_err(),
        "completed successor must advance directory too"
    );
}

#[test]
fn activation_view_rejects_cross_group_receipts_and_false_history_coverage() {
    let (view, first, preparing, proof) = prepared();
    load(&view, accepted(&first, &preparing, proof.clone())).unwrap();
    for variant in 0..21 {
        let mut invalid = proof.clone();
        match variant {
            0 => invalid.seals[0].data_group = GroupId::new(2),
            1 => invalid.seals[0].predecessor_revision = 2,
            2 => invalid.seals[0].predecessor_job_id = preparing.intent.job_id,
            3 => invalid.seals[0].endpoint = endpoint(5),
            4 => invalid.seals[0] = invalid.seals[1].clone(),
            5 => invalid.seals.swap(0, 1),
            6 => invalid.fence.data_group = GroupId::new(2),
            7 => invalid.fence.predecessor_revision = 2,
            8 => invalid.fence.predecessor_job_id = preparing.intent.job_id,
            9 => invalid.fence.successor_job_id = first.intent.job_id,
            10 => invalid.fence.committed = position(19),
            11 => invalid.fence.committed.term = 6,
            12 => invalid.observed.data_group = GroupId::new(2),
            13 => invalid.observed.applied = observed(24),
            14 => invalid.observed.joint_voters = Some(first.intent.old_voters),
            15 => invalid.observed.voters = first.intent.old_voters,
            16 => {
                invalid.observed.peers[0] =
                    Some((NodeId::new(1).unwrap(), endpoint(5).peer_address))
            }
            17 => invalid.observed.membership = observed(18),
            18 => {
                invalid.observed.applied = observed(25);
                invalid.observed.applied.leader_node_id = 2;
            }
            19 => {
                invalid.observed.applied = observed(26);
                invalid.observed.applied.term = 6;
            }
            _ => invalid.seals[0].endpoint.data_address = endpoint(5).data_address,
        }
        assert!(
            load(&view, accepted(&first, &preparing, invalid.clone())).is_err(),
            "accepted bad evidence variant {variant}"
        );
        let mut candidate = view.clone();
        assert_conflict(
            &candidate
                .apply(activation(&first, &preparing, invalid, 2))
                .unwrap(),
            ConflictReason::MembershipMismatch,
        );
        assert_eq!(candidate, view, "bad proof mutated candidate {variant}");
    }
}

#[test]
fn activation_requires_current_revision_but_retry_preserves_original_handoff() {
    let (mut view, first, preparing, proof) = prepared();
    let mut sibling = intent(2, 20, 5);
    sibling.expected_directory_revision = 2;
    view.apply(MetadataCommand::BeginMove {
        intent: sibling.clone(),
    })
    .unwrap();
    view.apply(complete(&sibling, 2)).unwrap();
    let before = view.clone();
    assert_conflict(
        &view
            .apply(activation(&first, &preparing, proof.clone(), 2))
            .unwrap(),
        ConflictReason::RevisionMismatch,
    );
    assert_eq!(view, before);
    let result = view
        .apply(activation(&first, &preparing, proof.clone(), 3))
        .unwrap();
    assert!(matches!(result.result, MetadataResult::Activated { .. }));
    assert_eq!(
        view.job(GroupId::new(1))
            .unwrap()
            .intent
            .expected_directory_revision,
        2
    );
    let mut alternative = proof.clone();
    alternative.fence.committed = position(26);
    alternative.observed.applied = observed(26);
    let stable = view.clone();
    let retry = view
        .apply(activation(&first, &preparing, alternative, 2))
        .unwrap();
    assert!(matches!(
        retry.result,
        MetadataResult::AlreadyActivated { .. }
    ));
    assert!(retry.effects.is_empty());
    assert_eq!(view, stable);
    assert_eq!(
        view.accepted_successor(GroupId::new(1)).unwrap().handoff,
        proof
    );
}

#[test]
fn activation_successor_completion_counts_both_jobs_and_rejects_third_move() {
    let (mut view, first, preparing, proof) = prepared();
    let result = view
        .apply(activation(&first, &preparing, proof.clone(), 2))
        .unwrap();
    assert!(matches!(result.result, MetadataResult::Activated { .. }));
    let mut command = complete(&preparing.intent, 2);
    if let MetadataCommand::CompleteMove { observation, .. } = &mut command {
        observation.membership = position(40);
    }
    let finished = view.apply(command.clone()).unwrap();
    assert_eq!(
        finished.result,
        MetadataResult::Completed {
            data_group: GroupId::new(1),
            job_id: preparing.intent.job_id,
            revision: 3
        }
    );
    assert_eq!(finished.effects.len(), 2);
    let encoded = finished
        .effects
        .iter()
        .find(|effect| effect.key == job_key(GroupId::new(1)))
        .unwrap();
    let persisted = decode_stored_moves(&encoded.value).unwrap();
    assert_eq!(persisted.first, first);
    assert_eq!(view.first_job(GroupId::new(1)), Some(&first));
    assert_eq!(
        view.accepted_successor(GroupId::new(1)).unwrap().handoff,
        proof
    );
    assert_eq!(
        view.directory().unwrap().voters(GroupId::new(1)),
        Some(preparing.intent.target_voters.as_slice())
    );
    assert_eq!(load(&view, persisted).unwrap(), view);
    let stable = view.clone();
    assert!(view.apply(command).unwrap().effects.is_empty());
    assert!(
        view.apply(complete(&first.intent, 1))
            .unwrap()
            .effects
            .is_empty()
    );
    let third = MoveReplica {
        job_id: MoveJobId::new([99; 16]).unwrap(),
        data_group: GroupId::new(1),
        remove_node: NodeId::new(3).unwrap(),
        add_node: endpoint(2),
        expected_directory_revision: 3,
    }
    .derive_intent(view.directory().unwrap())
    .unwrap();
    assert_conflict(
        &view
            .apply(MetadataCommand::BeginMove {
                intent: third.clone(),
            })
            .unwrap(),
        ConflictReason::ExistingMove,
    );
    assert_conflict(
        &view
            .apply(MetadataCommand::PrepareSuccessor {
                predecessor_job_id: preparing.intent.job_id,
                intent: third,
            })
            .unwrap(),
        ConflictReason::ExistingMove,
    );
    assert_eq!(view, stable);
}

#[test]
fn activation_successor_completion_must_follow_fence_before_persistence() {
    let (mut view, first, preparing, proof) = prepared();
    let activated = view
        .apply(activation(&first, &preparing, proof.clone(), 2))
        .unwrap();
    assert!(matches!(activated.result, MetadataResult::Activated { .. }));
    let mut before_fence = position(40);
    before_fence.term = 6;
    let mut same_index_other_identity = position(25);
    same_index_other_identity.term = 8;
    let invalid = [
        position(19),
        position(25),
        before_fence,
        same_index_other_identity,
    ];
    for membership in invalid {
        let mut command = complete(&preparing.intent, 2);
        if let MetadataCommand::CompleteMove { observation, .. } = &mut command {
            observation.membership = membership;
        }
        let before = view.clone();
        assert_conflict(
            &view.apply(command).unwrap(),
            ConflictReason::MembershipMismatch,
        );
        assert_eq!(view, before);
    }
    let mut command = complete(&preparing.intent, 2);
    if let MetadataCommand::CompleteMove { observation, .. } = &mut command {
        observation.membership = position(40);
    }
    assert!(matches!(
        view.apply(command).unwrap().result,
        MetadataResult::Completed { .. }
    ));
    for membership in invalid {
        let malformed = StoredMoves {
            first: first.clone(),
            successor: Some(SuccessorState::Accepted(Box::new(AcceptedSuccessor {
                record: MoveRecord {
                    intent: preparing.intent.clone(),
                    progress: MoveProgress::Completed { membership },
                },
                handoff: proof.clone(),
            }))),
        };
        assert!(
            load(&view, malformed).is_err(),
            "loaded completion not after fence {membership:?}"
        );
    }
}

#[test]
fn activation_condition_identity_and_accepted_lineage_are_not_interchangeable() {
    let (view, first, preparing, proof) = prepared();
    for predecessor in [true, false] {
        let mut command = activation(&first, &preparing, proof.clone(), 2);
        if let MetadataCommand::ActivateSuccessor {
            predecessor_job_id,
            successor_job_id,
            ..
        } = &mut command
        {
            if predecessor {
                *predecessor_job_id = MoveJobId::new([98; 16]).unwrap();
            } else {
                *successor_job_id = MoveJobId::new([99; 16]).unwrap();
            }
        }
        let mut candidate = view.clone();
        assert_conflict(
            &candidate.apply(command).unwrap(),
            ConflictReason::ExistingMove,
        );
        assert_eq!(candidate, view);
    }
    for variant in 0..5 {
        let mut next = preparing.clone();
        match variant {
            0 => next.intent.job_id = first.intent.job_id,
            1 => next.intent.data_group = GroupId::new(2),
            2 => next.intent.expected_directory_revision = 1,
            3 => next.intent.expected_directory_revision = 3,
            _ => {
                next.intent = intent(1, 12, 5);
                next.intent.expected_directory_revision = 2;
            }
        }
        assert!(
            load(&view, accepted(&first, &next, proof.clone())).is_err(),
            "accepted malformed lineage {variant}"
        );
    }
}
