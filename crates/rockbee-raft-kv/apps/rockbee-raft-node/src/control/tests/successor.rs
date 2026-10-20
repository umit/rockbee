//! Bounded preparation is durable metadata, not permission to run a second move.
use super::*;
use serde::Serialize;

pub(super) fn completed() -> (MetadataView, MoveRecord) {
    let first = intent(1, 11, 4);
    let mut view = pending(&first);
    view.apply(complete(&first, 1)).unwrap();
    let record = view.job(first.data_group).unwrap().clone();
    (view, record)
}

pub(super) fn candidate(view: &MetadataView, add: u64) -> MoveIntent {
    MoveReplica {
        job_id: MoveJobId::new([12; 16]).unwrap(),
        data_group: GroupId::new(1),
        remove_node: NodeId::new(2).unwrap(),
        add_node: endpoint(add),
        expected_directory_revision: view.directory().unwrap().revision,
    }
    .derive_intent(view.directory().unwrap())
    .unwrap()
}

fn stored(view: &MetadataView, first: MoveRecord) -> StoredMoves {
    StoredMoves {
        first,
        successor: Some(SuccessorState::Preparing(Box::new(PreparingSuccessor {
            intent: candidate(view, 3),
            executors: [endpoint(1), endpoint(2), endpoint(3), endpoint(4)],
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
struct V2Fixture<'a> {
    marker: [u8; 16],
    version: u8,
    first: &'a MoveRecord,
    preparing: &'a PreparingSuccessor,
}

pub(super) fn v2_frame(moves: &StoredMoves, version: u8) -> Vec<u8> {
    rockbee_raft_protocol::encode_frame(&V2Fixture {
        marker: [0; 16],
        version,
        first: &moves.first,
        preparing: moves.preparing().unwrap(),
    })
    .unwrap()
}

#[test]
fn successor_prepare_preserves_first_directory_and_public_rejection() {
    let (mut view, first) = completed();
    let before = view.clone();
    let successor = candidate(&view, 3);
    let command = MetadataCommand::PrepareSuccessor {
        predecessor_job_id: first.intent.job_id,
        intent: successor.clone(),
    };
    command.validate().unwrap();
    let transition = view.apply(command.clone()).unwrap();
    assert_eq!(
        transition.result,
        MetadataResult::Prepared {
            data_group: GroupId::new(1),
            job_id: successor.job_id,
        }
    );
    assert_eq!(transition.effects.len(), 1);
    assert_eq!(transition.effects[0].key, job_key(GroupId::new(1)));
    assert_eq!(view.directory(), before.directory());
    assert_eq!(view.origin(), before.origin());
    assert_eq!(view.job(GroupId::new(1)), Some(&first));
    assert_eq!(
        view.prepared_successor(GroupId::new(1)),
        stored(&before, first.clone()).preparing()
    );
    let reloaded = decode_stored_moves(&transition.effects[0].value).unwrap();
    assert_eq!(load(&before, reloaded).unwrap(), view);
    let retry = view.apply(command).unwrap();
    assert_eq!(
        retry.result,
        MetadataResult::AlreadyPrepared {
            data_group: GroupId::new(1),
            job_id: successor.job_id,
        }
    );
    assert!(retry.effects.is_empty());
    let prepared = view.clone();
    assert_conflict(
        &view
            .apply(MetadataCommand::BeginMove {
                intent: successor.clone(),
            })
            .unwrap(),
        ConflictReason::ExistingMove,
    );
    let mut other = successor;
    other.job_id = MoveJobId::new([13; 16]).unwrap();
    assert_conflict(
        &view
            .apply(MetadataCommand::PrepareSuccessor {
                predecessor_job_id: first.intent.job_id,
                intent: other,
            })
            .unwrap(),
        ConflictReason::ExistingMove,
    );
    assert_eq!(view, prepared);
}

#[test]
fn successor_census_is_group_specific_and_completed_count_is_global() {
    let (mut view, first) = completed();
    let mut sibling = intent(2, 20, 5);
    sibling.expected_directory_revision = 2;
    view.apply(MetadataCommand::BeginMove {
        intent: sibling.clone(),
    })
    .unwrap();
    view.apply(complete(&sibling, 2)).unwrap();
    assert_eq!(view.directory().unwrap().revision, 3);
    let successor = candidate(&view, 3);
    let result = view
        .apply(MetadataCommand::PrepareSuccessor {
            predecessor_job_id: first.intent.job_id,
            intent: successor,
        })
        .unwrap();
    assert!(matches!(result.result, MetadataResult::Prepared { .. }));
    let preparing = view.prepared_successor(GroupId::new(1)).unwrap();
    assert_eq!(
        preparing.executors,
        [endpoint(1), endpoint(2), endpoint(3), endpoint(4)]
    );
    assert_eq!(view.directory().unwrap().revision, 3);
    let records = BTreeMap::from([
        (
            GroupId::new(1),
            StoredMoves {
                first,
                successor: Some(SuccessorState::Preparing(Box::new(preparing.clone()))),
            },
        ),
        (
            GroupId::new(2),
            StoredMoves {
                first: view.job(GroupId::new(2)).unwrap().clone(),
                successor: None,
            },
        ),
    ]);
    assert_eq!(
        MetadataView::from_stored_parts(view.directory().cloned(), view.origin().cloned(), records)
            .unwrap(),
        view
    );
}

#[test]
fn successor_legacy_bytes_remain_exact_including_zero_leading_job_id() {
    let (_, mut first) = completed();
    let mut id = [0; 16];
    id[15] = 1;
    first.intent.job_id = MoveJobId::new(id).unwrap();
    let moves = StoredMoves {
        first: first.clone(),
        successor: None,
    };
    let legacy = rockbee_raft_protocol::encode_frame(&first).unwrap();
    assert_eq!(encode_stored_moves(&moves).unwrap(), legacy);
    assert_eq!(decode_stored_moves(&legacy).unwrap(), moves);
}

#[test]
fn successor_v2_same_key_roundtrip_and_actual_legacy_decoder_rejection() {
    let (view, first) = completed();
    let moves = stored(&view, first);
    let frame = encode_stored_moves(&moves).unwrap();
    assert_eq!(
        &frame[4..20],
        &[0; 16],
        "v2 must begin with legacy-invalid job ID"
    );
    assert_eq!(frame, v2_frame(&moves, 2));
    assert!(rockbee_raft_protocol::decode_frame::<MoveRecord>(&frame).is_err());
    assert_eq!(decode_stored_moves(&frame).unwrap(), moves);
    assert_eq!(
        load(&view, moves.clone())
            .unwrap()
            .prepared_successor(GroupId::new(1)),
        moves.preparing()
    );
}

#[test]
fn successor_decoder_accepts_marked_v2_but_never_falls_back_on_corruption() {
    let (view, first) = completed();
    let moves = stored(&view, first);
    let valid = v2_frame(&moves, 2);
    assert_eq!(decode_stored_moves(&valid).unwrap(), moves);
    assert!(decode_stored_moves(&v2_frame(&moves, 3)).is_err());
    for length in [0, 3, 4, 19, 20, valid.len() - 1] {
        assert!(decode_stored_moves(&valid[..length]).is_err());
    }
    let mut trailing = valid.clone();
    trailing.push(0);
    assert!(decode_stored_moves(&trailing).is_err());
    let length = u32::try_from(trailing.len() - 4).unwrap();
    trailing[..4].copy_from_slice(&length.to_be_bytes());
    assert!(
        decode_stored_moves(&trailing).is_err(),
        "inner postcard trailing byte"
    );
    for marker_index in 4..20 {
        let mut corrupt = valid.clone();
        corrupt[marker_index] = 1;
        assert!(
            decode_stored_moves(&corrupt).is_err(),
            "corrupt marker {marker_index}"
        );
    }
    let mut partial_zero = valid.clone();
    partial_zero[4..12].fill(255);
    assert!(decode_stored_moves(&partial_zero).is_err());
    let mut bad_length = valid;
    bad_length[0] ^= 1;
    assert!(decode_stored_moves(&bad_length).is_err());
    assert!(matches!(
        decode_stored_moves(&vec![0; MAX_STORED_MOVES_BYTES + 1]),
        Err(MetadataError::EncodedLimit)
    ));
}

#[test]
fn successor_cross_record_validation_rejects_forged_preparations() {
    let (view, first) = completed();
    let good = stored(&view, first);
    load(&view, good.clone()).unwrap();
    for variant in 0..7 {
        let mut malformed = good.clone();
        let preparing = malformed
            .successor
            .as_mut()
            .and_then(|successor| match successor {
                SuccessorState::Preparing(preparing) => Some(preparing),
                SuccessorState::Accepted(_) => None,
            })
            .unwrap();
        match variant {
            0 => preparing.executors.swap(0, 1),
            1 => preparing.executors[2] = endpoint(5),
            2 => preparing.executors[0].data_address = endpoint(8).data_address,
            3 => preparing.intent.job_id = malformed.first.intent.job_id,
            4 => preparing.intent.expected_directory_revision = 1,
            5 => preparing.intent = intent(1, 12, 5),
            _ => malformed.first.progress = MoveProgress::Pending,
        }
        assert!(
            load(&view, malformed).is_err(),
            "accepted malformed preparation {variant}"
        );
    }
}

#[test]
fn successor_pending_endpoint_union_includes_preparing_candidates() {
    let (mut view, first) = completed();
    let mut successor = candidate(&view, 5);
    let mut sibling = intent(2, 20, 6);
    sibling.expected_directory_revision = 2;
    view.apply(MetadataCommand::BeginMove {
        intent: sibling.clone(),
    })
    .unwrap();
    successor.add_node.peer_address = sibling.add_node.data_address;
    let records = BTreeMap::from([
        (
            GroupId::new(1),
            StoredMoves {
                first,
                successor: Some(SuccessorState::Preparing(Box::new(PreparingSuccessor {
                    intent: successor,
                    executors: [endpoint(1), endpoint(2), endpoint(3), endpoint(4)],
                }))),
            },
        ),
        (
            GroupId::new(2),
            StoredMoves {
                first: view.job(GroupId::new(2)).unwrap().clone(),
                successor: None,
            },
        ),
    ]);
    assert!(
        MetadataView::from_stored_parts(view.directory().cloned(), view.origin().cloned(), records)
            .is_err()
    );
}

#[test]
fn successor_identical_retry_after_sibling_completion_keeps_frozen_identity() {
    let (mut view, first) = completed();
    let command = MetadataCommand::PrepareSuccessor {
        predecessor_job_id: first.intent.job_id,
        intent: candidate(&view, 3),
    };
    let prepared = view.apply(command.clone()).unwrap();
    assert!(matches!(prepared.result, MetadataResult::Prepared { .. }));
    let frozen = view.prepared_successor(GroupId::new(1)).unwrap().clone();
    let mut sibling = intent(2, 20, 5);
    sibling.expected_directory_revision = 2;
    view.apply(MetadataCommand::BeginMove {
        intent: sibling.clone(),
    })
    .unwrap();
    view.apply(complete(&sibling, 2)).unwrap();
    let before = view.clone();
    assert_eq!(before.directory().unwrap().revision, 3);
    let retry = view.apply(command).unwrap();
    assert_eq!(
        retry.result,
        MetadataResult::AlreadyPrepared {
            data_group: GroupId::new(1),
            job_id: frozen.intent.job_id,
        }
    );
    assert!(retry.effects.is_empty());
    assert_eq!(view, before);
    assert_eq!(view.prepared_successor(GroupId::new(1)), Some(&frozen));
    assert_eq!(frozen.intent.expected_directory_revision, 2);
}

#[test]
fn successor_command_conflicts_leave_all_state_unchanged() {
    let (ready, first) = completed();
    for variant in 0..6 {
        let mut view = ready.clone();
        let mut successor = candidate(&view, 3);
        let mut predecessor_job_id = first.intent.job_id;
        let expected = match variant {
            0 => {
                view = pending(&first.intent);
                ConflictReason::ExistingMove
            }
            1 => {
                predecessor_job_id = MoveJobId::new([99; 16]).unwrap();
                ConflictReason::ExistingMove
            }
            2 => {
                successor.job_id = first.intent.job_id;
                ConflictReason::ExistingMove
            }
            3 => {
                successor.expected_directory_revision = 1;
                ConflictReason::RevisionMismatch
            }
            4 => {
                successor = intent(1, 12, 5);
                successor.expected_directory_revision = 2;
                ConflictReason::PlacementMismatch
            }
            _ => {
                successor.add_node.data_address = endpoint(1).peer_address;
                ConflictReason::EndpointUnion
            }
        };
        let before = view.clone();
        let transition = view
            .apply(MetadataCommand::PrepareSuccessor {
                predecessor_job_id,
                intent: successor,
            })
            .unwrap();
        assert_conflict(&transition, expected);
        assert_eq!(view, before, "mutated conflict variant {variant}");
    }
}

#[test]
fn successor_endpoint_reservation_protects_future_and_existing_sibling_moves() {
    for prepare_first in [true, false] {
        for same_id in [true, false] {
            let (mut view, first) = completed();
            let successor = candidate(&view, 5);
            let mut sibling = intent(2, 20, if same_id { 5 } else { 6 });
            sibling.expected_directory_revision = 2;
            if same_id {
                sibling.add_node.data_address = endpoint(7).data_address;
            } else {
                sibling.add_node.peer_address = successor.add_node.data_address;
            }
            let prepare = MetadataCommand::PrepareSuccessor {
                predecessor_job_id: first.intent.job_id,
                intent: successor,
            };
            let begin = MetadataCommand::BeginMove { intent: sibling };
            let (first_command, conflicting) = if prepare_first {
                (prepare, begin)
            } else {
                (begin, prepare)
            };
            let result = view.apply(first_command).unwrap();
            assert!(matches!(
                result.result,
                MetadataResult::Prepared { .. } | MetadataResult::Accepted { .. }
            ));
            let before = view.clone();
            assert_conflict(
                &view.apply(conflicting).unwrap(),
                ConflictReason::EndpointUnion,
            );
            assert_eq!(view, before);
        }
    }
}

#[test]
fn successor_largest_current_shape_fits_counted_envelope_and_effect_reservation() {
    let endpoints: [NodeEndpoint; 4] = std::array::from_fn(|index| {
        let offset = u64::try_from(index).unwrap();
        NodeEndpoint {
            node_id: NodeId::new(u64::MAX - 3 + offset).unwrap(),
            peer_address: format!(
                "[feff:ffff:ffff:ffff:ffff:ffff:ffff:ffff]:{}",
                65528 + index * 2
            )
            .parse()
            .unwrap(),
            data_address: format!(
                "[feff:ffff:ffff:ffff:ffff:ffff:ffff:ffff]:{}",
                65529 + index * 2
            )
            .parse()
            .unwrap(),
        }
    });
    let origin = ClusterDirectory::initial(
        ClusterId::new([255; 16]).unwrap(),
        ShardConfig::new(256, u64::MAX).unwrap(),
        endpoints[..3].to_vec(),
    )
    .unwrap();
    let mut current = origin.clone();
    current.revision = 257;
    current.nodes.push(endpoints[3].clone());
    let target = [
        endpoints[0].node_id,
        endpoints[1].node_id,
        endpoints[3].node_id,
    ];
    for placement in &mut current.placements {
        if !placement.group_id.is_metadata() {
            placement.voters = target.to_vec();
        }
    }
    let mut jobs = BTreeMap::new();
    for group in 1..=256 {
        let mut movement = MoveReplica {
            job_id: MoveJobId::new([255; 16]).unwrap(),
            data_group: GroupId::new(group),
            remove_node: endpoints[2].node_id,
            add_node: endpoints[3].clone(),
            expected_directory_revision: 1,
        }
        .derive_intent(&origin)
        .unwrap();
        movement.expected_directory_revision = group;
        jobs.insert(
            GroupId::new(group),
            MoveRecord {
                intent: movement,
                progress: MoveProgress::Completed {
                    membership: CommittedLogPosition {
                        term: u64::MAX,
                        leader_node_id: endpoints[3].node_id,
                        index: u64::MAX,
                    },
                },
            },
        );
    }
    let mut view = MetadataView::from_parts(Some(current.clone()), Some(origin), jobs).unwrap();
    let successor = MoveReplica {
        job_id: MoveJobId::new([254; 16]).unwrap(),
        data_group: GroupId::new(256),
        remove_node: endpoints[1].node_id,
        add_node: endpoints[2].clone(),
        expected_directory_revision: 257,
    }
    .derive_intent(&current)
    .unwrap();
    let transition = view
        .apply(MetadataCommand::PrepareSuccessor {
            predecessor_job_id: MoveJobId::new([255; 16]).unwrap(),
            intent: successor,
        })
        .unwrap();
    assert!(matches!(transition.result, MetadataResult::Prepared { .. }));
    assert_eq!(transition.effects.len(), 1);
    let effect = &transition.effects[0];
    let moves = decode_stored_moves(&effect.value).unwrap();
    assert_eq!(moves.preparing().unwrap().executors, endpoints);
    assert_eq!(effect.value.len(), v2_frame(&moves, 2).len());
    assert!(effect.value.len() <= MAX_STORED_MOVES_BYTES);
    assert!(effect.key.len() + effect.value.len() + 64 <= METADATA_EFFECT_RESERVATION);
    assert_eq!(view.directory(), Some(&current));
}
