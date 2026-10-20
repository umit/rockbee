use super::*;
use rockbee_raft_protocol::{
    ClusterId, GroupId, NodeId, ShardConfig,
    control::{CommittedLogPosition, MoveIntent, MoveJobId, MoveProgress, MoveRecord, MoveReplica},
    routing::{ClusterDirectory, NodeEndpoint},
};
use std::collections::BTreeMap;

fn endpoint(id: u64) -> NodeEndpoint {
    NodeEndpoint {
        node_id: NodeId::new(id).unwrap(),
        peer_address: format!("127.0.0.1:{}", 10000 + id * 2).parse().unwrap(),
        data_address: format!("127.0.0.1:{}", 10001 + id * 2).parse().unwrap(),
    }
}

fn initial() -> ClusterDirectory {
    ClusterDirectory::initial(
        ClusterId::new([1; 16]).unwrap(),
        ShardConfig::new(2, 0).unwrap(),
        (1..=3).map(endpoint).collect(),
    )
    .unwrap()
}

fn intent(group: u64, job: u8, add: u64) -> MoveIntent {
    MoveReplica {
        job_id: MoveJobId::new([job; 16]).unwrap(),
        data_group: GroupId::new(group),
        remove_node: NodeId::new(3).unwrap(),
        add_node: endpoint(add),
        expected_directory_revision: 1,
    }
    .derive_intent(&initial())
    .unwrap()
}

fn initialized() -> MetadataView {
    MetadataView::from_parts(Some(initial()), Some(initial()), BTreeMap::new()).unwrap()
}

fn pending(move_intent: &MoveIntent) -> MetadataView {
    MetadataView::from_parts(
        Some(initial()),
        Some(initial()),
        BTreeMap::from([(
            move_intent.data_group,
            MoveRecord {
                intent: move_intent.clone(),
                progress: MoveProgress::Pending,
            },
        )]),
    )
    .unwrap()
}

fn complete(move_intent: &MoveIntent, revision: u64) -> MetadataCommand {
    MetadataCommand::CompleteMove {
        expected_directory_revision: revision,
        data_group: move_intent.data_group,
        job_id: move_intent.job_id,
        observation: MembershipObservation {
            uniform: true,
            voters: move_intent.target_voters,
            replacement_endpoint: move_intent.add_node.clone(),
            membership: CommittedLogPosition {
                term: 7,
                leader_node_id: NodeId::new(1).unwrap(),
                index: 19,
            },
        },
    }
}

fn assert_conflict(transition: &MetadataTransition, reason: ConflictReason) {
    assert_eq!(transition.result, MetadataResult::Conflict { reason });
    assert!(transition.effects.is_empty());
}

#[test]
fn initialization_creates_directory_and_origin_once() {
    let mut view = MetadataView::default();
    let command = MetadataCommand::InitializeDirectoryIfAbsent {
        initial_directory: initial(),
    };
    let result = view.apply(command.clone()).unwrap();
    assert_eq!(result.result, MetadataResult::Initialized { revision: 1 });
    assert_eq!(result.effects.len(), 2);
    assert_eq!(view.directory(), Some(&initial()));
    assert_eq!(view.origin(), Some(&initial()));
    let retry = view.apply(command).unwrap();
    assert_eq!(
        retry.result,
        MetadataResult::AlreadyInitialized { revision: 1 }
    );
    assert!(retry.effects.is_empty());
}

#[test]
fn identical_job_is_accepted_again_but_different_job_conflicts_forever() {
    let mut view = initialized();
    let movement = intent(1, 1, 4);
    let accepted = view
        .apply(MetadataCommand::BeginMove {
            intent: movement.clone(),
        })
        .unwrap();
    assert_eq!(
        accepted.result,
        MetadataResult::Accepted {
            data_group: movement.data_group,
            job_id: movement.job_id
        }
    );
    assert_eq!(view.directory().unwrap().revision, 1);
    let retry = view
        .apply(MetadataCommand::BeginMove {
            intent: movement.clone(),
        })
        .unwrap();
    assert_eq!(
        retry.result,
        MetadataResult::AlreadyAccepted {
            data_group: movement.data_group,
            job_id: movement.job_id
        }
    );
    assert!(retry.effects.is_empty());
    assert_conflict(
        &view
            .apply(MetadataCommand::BeginMove {
                intent: intent(1, 2, 4),
            })
            .unwrap(),
        ConflictReason::ExistingMove,
    );
}

#[test]
fn different_groups_can_accept_same_revision_and_share_identical_replacement() {
    let mut view = initialized();
    for movement in [intent(1, 1, 4), intent(2, 2, 4)] {
        let result = view
            .apply(MetadataCommand::BeginMove {
                intent: movement.clone(),
            })
            .unwrap();
        assert_eq!(
            result.result,
            MetadataResult::Accepted {
                data_group: movement.data_group,
                job_id: movement.job_id
            }
        );
    }
    assert_eq!(view.directory().unwrap().revision, 1);
}

#[test]
fn endpoint_union_rejects_cross_role_alias_without_partial_effects() {
    let first = intent(1, 1, 4);
    let mut view = pending(&first);
    let mut second = intent(2, 2, 5);
    second.add_node.peer_address = first.add_node.data_address;
    let before = view.clone();
    assert_conflict(
        &view
            .apply(MetadataCommand::BeginMove { intent: second })
            .unwrap(),
        ConflictReason::EndpointUnion,
    );
    assert_eq!(view, before);
}

#[test]
fn stale_completion_preserves_pending_record_and_directory() {
    let movement = intent(1, 1, 4);
    let mut view = pending(&movement);
    let before = view.clone();
    assert_conflict(
        &view.apply(complete(&movement, 2)).unwrap(),
        ConflictReason::RevisionMismatch,
    );
    assert_eq!(view, before);
}

#[test]
fn completion_requires_uniform_exact_voters_and_endpoint() {
    let movement = intent(1, 1, 4);
    for invalid in 0..3 {
        let mut view = pending(&movement);
        let before = view.clone();
        let mut command = complete(&movement, 1);
        if let MetadataCommand::CompleteMove { observation, .. } = &mut command {
            match invalid {
                0 => observation.uniform = false,
                1 => observation.voters = movement.old_voters,
                _ => observation.replacement_endpoint = endpoint(5),
            }
        }
        assert_conflict(
            &view.apply(command).unwrap(),
            ConflictReason::MembershipMismatch,
        );
        assert_eq!(view, before);
    }
}

#[test]
fn completion_changes_only_target_placement_and_is_idempotent_before_revision() {
    let movement = intent(1, 1, 4);
    let mut view = pending(&movement);
    let done = view.apply(complete(&movement, 1)).unwrap();
    assert_eq!(
        done.result,
        MetadataResult::Completed {
            data_group: movement.data_group,
            job_id: movement.job_id,
            revision: 2
        }
    );
    assert_eq!(done.effects.len(), 2);
    assert_eq!(
        view.directory().unwrap().voters(GroupId::new(0)),
        initial().voters(GroupId::new(0))
    );
    assert_eq!(
        view.directory().unwrap().voters(GroupId::new(2)),
        initial().voters(GroupId::new(2))
    );
    assert_eq!(
        view.directory().unwrap().voters(GroupId::new(1)),
        Some(movement.target_voters.as_slice())
    );
    let before = view.clone();
    let retry = view.apply(complete(&movement, 1)).unwrap();
    assert_eq!(
        retry.result,
        MetadataResult::AlreadyCompleted {
            data_group: movement.data_group,
            job_id: movement.job_id,
            revision: 2
        }
    );
    assert!(retry.effects.is_empty());
    assert_eq!(view, before);
    let begin_retry = view
        .apply(MetadataCommand::BeginMove {
            intent: movement.clone(),
        })
        .unwrap();
    assert_eq!(
        begin_retry.result,
        MetadataResult::AlreadyAccepted {
            data_group: movement.data_group,
            job_id: movement.job_id
        }
    );
}

#[test]
fn revision_two_bootstrap_retry_preserves_new_placement() {
    let movement = intent(1, 1, 4);
    let mut directory = initial();
    directory.revision = 2;
    directory.nodes.push(endpoint(4));
    directory.placements[1].voters = movement.target_voters.to_vec();
    let jobs = BTreeMap::from([(
        movement.data_group,
        MoveRecord {
            intent: movement,
            progress: MoveProgress::Completed {
                membership: CommittedLogPosition {
                    term: 7,
                    leader_node_id: NodeId::new(1).unwrap(),
                    index: 19,
                },
            },
        },
    )]);
    let mut view = MetadataView::from_parts(Some(directory), Some(initial()), jobs).unwrap();
    let before = view.clone();
    let retry = view
        .apply(MetadataCommand::InitializeDirectoryIfAbsent {
            initial_directory: initial(),
        })
        .unwrap();
    assert_eq!(
        retry.result,
        MetadataResult::AlreadyInitialized { revision: 2 }
    );
    assert!(retry.effects.is_empty());
    assert_eq!(view, before);
}

#[test]
fn begin_retry_after_sibling_completion_ignores_current_revision() {
    let mut view = initialized();
    let first = intent(1, 1, 4);
    let sibling = intent(2, 2, 4);
    for movement in [&first, &sibling] {
        view.apply(MetadataCommand::BeginMove {
            intent: movement.clone(),
        })
        .unwrap();
    }
    view.apply(complete(&sibling, 1)).unwrap();
    let before = view.clone();
    let retry = view
        .apply(MetadataCommand::BeginMove {
            intent: first.clone(),
        })
        .unwrap();
    assert_eq!(
        retry.result,
        MetadataResult::AlreadyAccepted {
            data_group: first.data_group,
            job_id: first.job_id
        }
    );
    assert!(retry.effects.is_empty());
    assert_eq!(view, before);
    let done = view.apply(complete(&first, 2)).unwrap();
    assert_eq!(
        done.result,
        MetadataResult::Completed {
            data_group: first.data_group,
            job_id: first.job_id,
            revision: 3
        }
    );
    MetadataView::from_parts(view.current, view.origin, view.jobs).unwrap();
}

#[test]
fn completed_retry_retains_first_membership_identity_and_conflicts_with_new_job() {
    let movement = intent(1, 1, 4);
    let mut view = pending(&movement);
    view.apply(complete(&movement, 1)).unwrap();
    let before = view.clone();
    let mut retry = complete(&movement, u64::MAX);
    if let MetadataCommand::CompleteMove { observation, .. } = &mut retry {
        observation.uniform = false;
        observation.membership = CommittedLogPosition {
            term: 99,
            leader_node_id: NodeId::new(2).unwrap(),
            index: 999,
        };
    }
    assert!(matches!(
        view.apply(retry).unwrap().result,
        MetadataResult::AlreadyCompleted { .. }
    ));
    assert_eq!(view, before);
    assert_conflict(
        &view
            .apply(MetadataCommand::BeginMove {
                intent: intent(1, 2, 5),
            })
            .unwrap(),
        ConflictReason::ExistingMove,
    );
    let mut wrong_job = complete(&movement, 2);
    if let MetadataCommand::CompleteMove { job_id, .. } = &mut wrong_job {
        *job_id = MoveJobId::new([2; 16]).unwrap();
    }
    assert_conflict(
        &view.apply(wrong_job).unwrap(),
        ConflictReason::ExistingMove,
    );
    assert_eq!(view, before);
}

#[test]
fn legacy_revision_one_adopts_exact_origin_before_accepting_moves() {
    let mut view = MetadataView::from_parts(Some(initial()), None, BTreeMap::new()).unwrap();
    let before = view.clone();
    assert_conflict(
        &view
            .apply(MetadataCommand::BeginMove {
                intent: intent(1, 1, 4),
            })
            .unwrap(),
        ConflictReason::BootstrapMismatch,
    );
    assert_eq!(view, before);
    let mut wrong = initial();
    wrong.cluster_id = ClusterId::new([2; 16]).unwrap();
    assert_conflict(
        &view
            .apply(MetadataCommand::InitializeDirectoryIfAbsent {
                initial_directory: wrong,
            })
            .unwrap(),
        ConflictReason::BootstrapMismatch,
    );
    assert_eq!(view, before);
    let adoption = view
        .apply(MetadataCommand::InitializeDirectoryIfAbsent {
            initial_directory: initial(),
        })
        .unwrap();
    assert_eq!(
        adoption.result,
        MetadataResult::AlreadyInitialized { revision: 1 }
    );
    assert_eq!(adoption.effects.len(), 1);
    assert_eq!(adoption.effects[0].key, ORIGIN_KEY);
    assert_eq!(view.origin(), Some(&initial()));
}

fn malformed_origin(shape: u8) -> ClusterDirectory {
    let mut directory = initial();
    match shape {
        0 => {
            directory.nodes.truncate(1);
            for placement in &mut directory.placements {
                placement.voters.truncate(1);
            }
        }
        1 => {
            directory.nodes.push(endpoint(4));
            for placement in &mut directory.placements {
                placement.voters.push(NodeId::new(4).unwrap());
            }
        }
        _ => {
            directory.placements[1].voters.pop();
        }
    }
    directory.validate().unwrap();
    directory
}

#[test]
fn initialization_requires_canonical_three_node_all_group_origin() {
    for shape in 0..3 {
        let mut view = MetadataView::default();
        let before = view.clone();
        assert_conflict(
            &view
                .apply(MetadataCommand::InitializeDirectoryIfAbsent {
                    initial_directory: malformed_origin(shape),
                })
                .unwrap(),
            ConflictReason::BootstrapMismatch,
        );
        assert_eq!(view, before);
    }
}

#[test]
fn persisted_origin_and_legacy_directory_require_canonical_bootstrap_shape() {
    for shape in 0..3 {
        let malformed = malformed_origin(shape);
        assert!(
            MetadataView::from_parts(
                Some(malformed.clone()),
                Some(malformed.clone()),
                BTreeMap::new()
            )
            .is_err()
        );
        assert!(MetadataView::from_parts(Some(malformed), None, BTreeMap::new()).is_err());
    }
}

#[test]
fn missing_or_incoherent_persisted_records_are_errors_not_conflicts() {
    let movement = intent(1, 1, 4);
    let record = MoveRecord {
        intent: movement.clone(),
        progress: MoveProgress::Pending,
    };
    let mut later = initial();
    later.revision = 2;
    assert!(MetadataView::from_parts(Some(later.clone()), None, BTreeMap::new()).is_err());
    assert!(MetadataView::from_parts(Some(later), Some(initial()), BTreeMap::new()).is_err());
    assert!(MetadataView::from_parts(None, Some(initial()), BTreeMap::new()).is_err());
    assert!(
        MetadataView::from_parts(
            Some(initial()),
            None,
            BTreeMap::from([(movement.data_group, record.clone())])
        )
        .is_err()
    );
    assert!(
        MetadataView::from_parts(
            Some(initial()),
            Some(initial()),
            BTreeMap::from([(GroupId::new(2), record)])
        )
        .is_err()
    );

    let mut view = pending(&movement);
    view.current.as_mut().unwrap().placements[1].voters = movement.target_voters.to_vec();
    view.current.as_mut().unwrap().nodes.push(endpoint(4));
    let before = view.clone();
    assert!(
        view.apply(MetadataCommand::BeginMove { intent: movement })
            .is_err()
    );
    assert_eq!(view, before);
}

#[test]
fn persisted_cluster_endpoints_metadata_and_sibling_placements_are_immutable() {
    for corruption in 0..5 {
        let mut directory = initial();
        match corruption {
            0 => directory.cluster_id = ClusterId::new([2; 16]).unwrap(),
            1 => directory.shard_config = ShardConfig::new(2, 99).unwrap(),
            2 => directory.nodes[0].peer_address = "127.0.0.1:19999".parse().unwrap(),
            3 => directory.placements[0].voters.pop().map(|_| ()).unwrap(),
            _ => directory.placements[2].voters.pop().map(|_| ()).unwrap(),
        }
        assert!(
            MetadataView::from_parts(Some(directory), Some(initial()), BTreeMap::new()).is_err()
        );
    }
}

#[test]
fn same_pending_node_id_requires_identical_endpoints() {
    let first = intent(1, 1, 4);
    let mut view = pending(&first);
    let mut changed = intent(2, 2, 4);
    changed.add_node.peer_address = "127.0.0.1:19999".parse().unwrap();
    let before = view.clone();
    assert_conflict(
        &view
            .apply(MetadataCommand::BeginMove { intent: changed })
            .unwrap(),
        ConflictReason::EndpointUnion,
    );
    assert_eq!(view, before);
}

#[test]
fn union_counts_all_pending_nodes_and_rejects_seventeenth_identity() {
    let mut directory = initial();
    directory.shard_config = ShardConfig::new(14, 0).unwrap();
    directory.placements = (0..=14)
        .map(|group| rockbee_raft_protocol::routing::GroupPlacement {
            group_id: GroupId::new(group),
            voters: (1..=3).map(|id| NodeId::new(id).unwrap()).collect(),
        })
        .collect();
    let mut view = MetadataView::from_parts(
        Some(directory.clone()),
        Some(directory.clone()),
        BTreeMap::new(),
    )
    .unwrap();
    for group in 1..=14 {
        let movement = MoveReplica {
            job_id: MoveJobId::new([u8::try_from(group).unwrap(); 16]).unwrap(),
            data_group: GroupId::new(group),
            remove_node: NodeId::new(3).unwrap(),
            add_node: endpoint(group + 3),
            expected_directory_revision: 1,
        }
        .derive_intent(&directory)
        .unwrap();
        let before = view.clone();
        let result = view
            .apply(MetadataCommand::BeginMove { intent: movement })
            .unwrap();
        if group == 14 {
            assert_conflict(&result, ConflictReason::EndpointUnion);
            assert_eq!(view, before);
        } else {
            assert!(matches!(result.result, MetadataResult::Accepted { .. }));
        }
    }
    assert_eq!(view.jobs.len(), 13);
    assert_eq!(view.directory().unwrap().nodes.len(), 3);
}

#[test]
fn stale_source_invalid_intent_and_unknown_group_have_no_effects() {
    let mut view = initialized();
    let before = view.clone();
    let mut stale = intent(1, 1, 4);
    stale.expected_directory_revision = 2;
    assert_conflict(
        &view
            .apply(MetadataCommand::BeginMove { intent: stale })
            .unwrap(),
        ConflictReason::RevisionMismatch,
    );
    let mut invalid = intent(1, 1, 4);
    invalid.target_voters = invalid.old_voters;
    assert_conflict(
        &view
            .apply(MetadataCommand::BeginMove { intent: invalid })
            .unwrap(),
        ConflictReason::InvalidIntent,
    );
    let mut wrong_source = intent(1, 1, 4);
    wrong_source.old_voters[0] = NodeId::new(5).unwrap();
    wrong_source.old_voters.sort_unstable();
    wrong_source.target_voters = [
        NodeId::new(2).unwrap(),
        NodeId::new(4).unwrap(),
        NodeId::new(5).unwrap(),
    ];
    assert_conflict(
        &view
            .apply(MetadataCommand::BeginMove {
                intent: wrong_source,
            })
            .unwrap(),
        ConflictReason::PlacementMismatch,
    );
    let mut unknown = intent(1, 1, 4);
    unknown.data_group = GroupId::new(3);
    assert_conflict(
        &view
            .apply(MetadataCommand::BeginMove { intent: unknown })
            .unwrap(),
        ConflictReason::PlacementMismatch,
    );
    assert_conflict(
        &view.apply(complete(&intent(1, 1, 4), 1)).unwrap(),
        ConflictReason::UnknownMove,
    );
    assert_eq!(view, before);
}

#[test]
fn emitted_values_round_trip_through_legacy_framing_and_bounded_loader() {
    let mut view = MetadataView::default();
    let initialization = view
        .apply(MetadataCommand::InitializeDirectoryIfAbsent {
            initial_directory: initial(),
        })
        .unwrap();
    assert_eq!(initialization.effects[0].key, DIRECTORY_KEY);
    assert_eq!(
        initialization.effects[0].value,
        rockbee_raft_protocol::encode_frame(&initial()).unwrap()
    );
    let movement = intent(1, 1, 4);
    let acceptance = view
        .apply(MetadataCommand::BeginMove {
            intent: movement.clone(),
        })
        .unwrap();
    assert_eq!(acceptance.effects[0].key, job_key(movement.data_group));
    let loaded = MetadataView::from_encoded(
        Some(&initialization.effects[0].value),
        Some(&initialization.effects[1].value),
        [(movement.data_group, acceptance.effects[0].value.as_slice())],
    )
    .unwrap();
    assert_eq!(loaded, view);
    let completion = view.apply(complete(&movement, 1)).unwrap();
    let loaded = MetadataView::from_encoded(
        Some(&completion.effects[0].value),
        Some(&initialization.effects[1].value),
        [(movement.data_group, completion.effects[1].value.as_slice())],
    )
    .unwrap();
    assert_eq!(loaded, view);
}

#[test]
fn loader_rejects_oversize_trailing_duplicate_and_invalid_collection_values() {
    let directory = rockbee_raft_protocol::encode_frame(&initial()).unwrap();
    let record = rockbee_raft_protocol::encode_frame(&MoveRecord {
        intent: intent(1, 1, 4),
        progress: MoveProgress::Pending,
    })
    .unwrap();
    assert!(MetadataView::from_encoded(Some(&vec![0; MAX_DIRECTORY_BYTES + 1]), None, []).is_err());
    assert!(
        MetadataView::from_encoded(
            Some(&directory),
            Some(&directory),
            [(GroupId::new(1), vec![0; MAX_JOB_BYTES + 1].as_slice())]
        )
        .is_err()
    );
    assert!(
        MetadataView::from_encoded(
            Some(&directory),
            Some(&directory),
            [
                (GroupId::new(1), record.as_slice()),
                (GroupId::new(1), record.as_slice())
            ]
        )
        .is_err()
    );
    assert!(
        MetadataView::from_encoded(
            Some(&directory),
            Some(&directory),
            [(GroupId::new(0), record.as_slice())]
        )
        .is_err()
    );
    let mut trailing = directory.clone();
    trailing.push(0);
    assert!(MetadataView::from_encoded(Some(&trailing), None, []).is_err());
    let mut too_many = initial();
    too_many.nodes = (1..=17).map(endpoint).collect();
    let invalid = rockbee_raft_protocol::encode_frame(&too_many).unwrap();
    assert!(MetadataView::from_encoded(Some(&invalid), None, []).is_err());
}

#[test]
fn command_and_result_serialization_preserve_complete_membership_identity() {
    let movement = intent(1, 1, 4);
    let command = complete(&movement, 1);
    let bytes = rockbee_raft_protocol::encode_frame(&command).unwrap();
    assert_eq!(
        rockbee_raft_protocol::decode_frame::<MetadataCommand>(&bytes).unwrap(),
        command
    );
    let mut view = pending(&movement);
    let result = view.apply(command).unwrap().result;
    let bytes = rockbee_raft_protocol::encode_frame(&result).unwrap();
    assert_eq!(
        rockbee_raft_protocol::decode_frame::<MetadataResult>(&bytes).unwrap(),
        result
    );
    assert!(
        matches!(view.job(movement.data_group).unwrap().progress, MoveProgress::Completed {
        membership: CommittedLogPosition { term: 7, leader_node_id, index: 19 }
    } if leader_node_id == NodeId::new(1).unwrap())
    );
}

fn maximum_directory() -> ClusterDirectory {
    let nodes: Vec<_> = (0u16..16)
        .map(|offset| NodeEndpoint {
            node_id: NodeId::new(u64::MAX - u64::from(offset)).unwrap(),
            peer_address: std::net::SocketAddr::new(
                std::net::Ipv6Addr::from(u128::MAX - (1u128 << 120) - u128::from(offset)).into(),
                u16::MAX - offset * 2,
            ),
            data_address: std::net::SocketAddr::new(
                std::net::Ipv6Addr::from(u128::MAX - (1u128 << 120) - u128::from(offset)).into(),
                u16::MAX - offset * 2 - 1,
            ),
        })
        .collect();
    let voters = nodes.iter().map(|node| node.node_id).collect::<Vec<_>>();
    ClusterDirectory {
        cluster_id: ClusterId::new([u8::MAX; 16]).unwrap(),
        revision: u64::MAX,
        shard_config: ShardConfig::new(256, u64::MAX).unwrap(),
        nodes,
        placements: (0..=256)
            .map(|group| rockbee_raft_protocol::routing::GroupPlacement {
                group_id: GroupId::new(group),
                voters: voters.clone(),
            })
            .collect(),
    }
}

#[test]
fn maximum_directory_and_job_shapes_fit_framed_and_effect_reservations() {
    // This is an encoding fixture, not a restricted three-voter move origin.
    let directory = maximum_directory();
    directory.validate().unwrap();
    let encoded_size = encoding::check_size(&directory, MAX_DIRECTORY_BYTES).unwrap();
    let frame = rockbee_raft_protocol::encode_frame(&directory).unwrap();
    assert_eq!(frame.len(), encoded_size);
    assert_eq!(
        rockbee_raft_protocol::decode_frame::<ClusterDirectory>(&frame).unwrap(),
        directory
    );
    let mut movement = intent(1, 1, 4);
    movement.job_id = MoveJobId::new([u8::MAX; 16]).unwrap();
    movement.data_group = GroupId::new(256);
    movement.expected_directory_revision = u64::MAX;
    movement.old_voters =
        [u64::MAX - 3, u64::MAX - 2, u64::MAX - 1].map(|id| NodeId::new(id).unwrap());
    movement.remove_node = movement.old_voters[2];
    movement.add_node = directory.nodes[0].clone();
    movement.target_voters = [
        movement.old_voters[0],
        movement.old_voters[1],
        movement.add_node.node_id,
    ];
    let record = MoveRecord {
        intent: movement,
        progress: MoveProgress::Completed {
            membership: CommittedLogPosition {
                term: u64::MAX,
                leader_node_id: NodeId::new(u64::MAX).unwrap(),
                index: u64::MAX,
            },
        },
    };
    record.validate().unwrap();
    let job_bytes = encoding::check_size(&record, MAX_JOB_BYTES).unwrap();
    let effects =
        encoding::completion_effects(&directory, record.intent.data_group, &record).unwrap();
    assert_eq!(effects[0].value.len(), encoded_size);
    assert_eq!(effects[1].value.len(), job_bytes);
    assert!(
        effects
            .iter()
            .map(|effect| effect.key.len() + effect.value.len() + 64)
            .sum::<usize>()
            < METADATA_EFFECT_RESERVATION
    );
    assert_eq!(encoding::initialize_effects(&directory).unwrap().len(), 2);
}
