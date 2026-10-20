//! Synthetic receipts bound metadata encoding; they prove no live handoff.
use super::*;
use rockbee_raft_protocol::backend::{AppliedMembership, ObservedLogPosition};

fn wide_endpoints() -> [NodeEndpoint; 16] {
    std::array::from_fn(|index| NodeEndpoint {
        node_id: NodeId::new(u64::MAX - 15 + u64::try_from(index).unwrap()).unwrap(),
        peer_address: format!(
            "[feff:ffff:ffff:ffff:ffff:ffff:ffff:ffff]:{}",
            65504 + index * 2
        )
        .parse()
        .unwrap(),
        data_address: format!(
            "[feff:ffff:ffff:ffff:ffff:ffff:ffff:ffff]:{}",
            65505 + index * 2
        )
        .parse()
        .unwrap(),
    })
}

fn wide_position(offset: u64, leader: NodeId) -> CommittedLogPosition {
    CommittedLogPosition {
        term: u64::MAX - offset,
        leader_node_id: leader,
        index: u64::MAX - offset,
    }
}

fn observation(position: CommittedLogPosition) -> ObservedLogPosition {
    ObservedLogPosition {
        term: position.term,
        leader_node_id: position.leader_node_id.get(),
        index: position.index,
    }
}

fn wide_view(endpoints: &[NodeEndpoint; 16]) -> MetadataView {
    let origin = ClusterDirectory::initial(
        ClusterId::new([255; 16]).unwrap(),
        ShardConfig::new(256, u64::MAX).unwrap(),
        endpoints[..3].to_vec(),
    )
    .unwrap();
    let mut current = origin.clone();
    current.revision = 257;
    current.nodes = endpoints.to_vec();
    let mut first_jobs = BTreeMap::new();
    for group in 1..=256 {
        // Every extra catalog endpoint has actual retained completion provenance.
        let replacement = 3 + usize::try_from((group - 1) % 13).unwrap();
        let mut intent = MoveReplica {
            job_id: MoveJobId::new([255; 16]).unwrap(),
            data_group: GroupId::new(group),
            remove_node: endpoints[2].node_id,
            add_node: endpoints[replacement].clone(),
            expected_directory_revision: 1,
        }
        .derive_intent(&origin)
        .unwrap();
        intent.expected_directory_revision = group;
        current.placements[usize::try_from(group).unwrap()].voters = intent.target_voters.to_vec();
        first_jobs.insert(
            GroupId::new(group),
            MoveRecord {
                intent,
                progress: MoveProgress::Completed {
                    membership: wide_position(2, endpoints[0].node_id),
                },
            },
        );
    }
    MetadataView::from_parts(Some(current), Some(origin), first_jobs).unwrap()
}

fn prepare_wide(
    view: &mut MetadataView,
    group: GroupId,
    endpoints: &[NodeEndpoint; 16],
) -> (MoveRecord, MoveIntent, HandoffProof) {
    let first = view.first_job(group).unwrap().clone();
    let next = MoveReplica {
        job_id: MoveJobId::new([254; 16]).unwrap(),
        data_group: group,
        remove_node: endpoints[1].node_id,
        add_node: endpoints[2].clone(),
        expected_directory_revision: view.directory().unwrap().revision,
    }
    .derive_intent(view.directory().unwrap())
    .unwrap();
    let prepared = view
        .apply(MetadataCommand::PrepareSuccessor {
            predecessor_job_id: first.intent.job_id,
            intent: next.clone(),
        })
        .unwrap();
    assert_eq!(
        prepared.result,
        MetadataResult::Prepared {
            data_group: group,
            job_id: next.job_id
        }
    );
    let preparing = view.prepared_successor(group).unwrap();
    let mut peers = [None; 16];
    for (slot, voter) in first.intent.target_voters.iter().enumerate() {
        peers[slot] = Some((
            *voter,
            view.directory().unwrap().node(*voter).unwrap().peer_address,
        ));
    }
    let proof = HandoffProof {
        seals: preparing.executors.clone().map(|endpoint| SealReceipt {
            endpoint,
            data_group: group,
            predecessor_revision: first.intent.expected_directory_revision,
            predecessor_job_id: first.intent.job_id,
        }),
        fence: HandoffFenceReceipt {
            data_group: group,
            predecessor_revision: first.intent.expected_directory_revision,
            predecessor_job_id: first.intent.job_id,
            successor_job_id: next.job_id,
            committed: wide_position(1, endpoints[0].node_id),
        },
        observed: AppliedMembership {
            data_group: group,
            applied: observation(wide_position(1, endpoints[0].node_id)),
            membership: observation(wide_position(2, endpoints[0].node_id)),
            voters: first.intent.target_voters,
            joint_voters: None,
            peers,
        },
    };
    (first, next, proof)
}

fn activate(first: &MoveRecord, next: &MoveIntent, proof: HandoffProof) -> MetadataCommand {
    MetadataCommand::ActivateSuccessor {
        expected_directory_revision: 257,
        data_group: next.data_group,
        predecessor_job_id: first.intent.job_id,
        successor_job_id: next.job_id,
        proof: Box::new(proof),
    }
}

fn assert_effect_budget(transition: &MetadataTransition) {
    let bytes: usize = transition
        .effects
        .iter()
        .map(|effect| effect.key.len() + effect.value.len() + 64)
        .sum();
    assert!(bytes <= METADATA_EFFECT_RESERVATION);
}

#[test]
fn activation_all_256_groups_fit_v3_and_count_both_retained_completions() {
    let endpoints = wide_endpoints();
    let mut view = wide_view(&endpoints);
    let origin = view.origin().unwrap().clone();
    let before = view.directory().unwrap().clone();
    let mut persisted = BTreeMap::new();
    for number in 1..=256 {
        let group = GroupId::new(number);
        let (first, next, proof) = prepare_wide(&mut view, group, &endpoints);
        let transition = view.apply(activate(&first, &next, proof.clone())).unwrap();
        assert_eq!(
            transition.result,
            MetadataResult::Activated {
                data_group: group,
                job_id: next.job_id
            }
        );
        assert_eq!(transition.effects.len(), 1);
        assert_effect_budget(&transition);
        let effect = &transition.effects[0];
        assert_eq!(effect.key, job_key(group));
        assert!(effect.value.len() <= MAX_STORED_MOVES_BYTES);
        assert_eq!(&effect.value[4..20], &[0; 16]);
        assert_eq!(effect.value[20], 3);
        let stored = decode_stored_moves(&effect.value).unwrap();
        assert_eq!(stored.first, first);
        assert_eq!(
            stored.successor,
            Some(SuccessorState::Accepted(Box::new(AcceptedSuccessor {
                record: MoveRecord {
                    intent: next,
                    progress: MoveProgress::Pending
                },
                handoff: proof,
            })))
        );
        persisted.insert(group, stored);
    }
    assert_eq!(view.directory(), Some(&before));
    assert_eq!(view.origin(), Some(&origin));
    assert_eq!(
        MetadataView::from_stored_parts(
            Some(before.clone()),
            Some(origin.clone()),
            persisted.clone()
        )
        .unwrap(),
        view
    );

    let mut expected = before;
    for number in 1..=256 {
        let group = GroupId::new(number);
        let next = view.job(group).unwrap().intent.clone();
        let handoff = view.accepted_successor(group).unwrap().handoff.clone();
        let membership = wide_position(0, endpoints[0].node_id);
        let transition = view
            .apply(MetadataCommand::CompleteMove {
                expected_directory_revision: expected.revision,
                data_group: group,
                job_id: next.job_id,
                observation: MembershipObservation {
                    uniform: true,
                    voters: next.target_voters,
                    replacement_endpoint: next.add_node.clone(),
                    membership,
                },
            })
            .unwrap();
        expected.revision += 1;
        expected.placements[usize::try_from(number).unwrap()].voters = next.target_voters.to_vec();
        assert_eq!(
            transition.result,
            MetadataResult::Completed {
                data_group: group,
                job_id: next.job_id,
                revision: expected.revision
            }
        );
        assert_eq!(transition.effects.len(), 2);
        assert_effect_budget(&transition);
        assert_eq!(view.directory(), Some(&expected));
        let directory_effect = transition
            .effects
            .iter()
            .find(|effect| effect.key == DIRECTORY_KEY)
            .unwrap();
        assert!(directory_effect.value.len() <= MAX_DIRECTORY_BYTES);
        assert_eq!(
            rockbee_raft_protocol::decode_frame::<ClusterDirectory>(&directory_effect.value)
                .unwrap(),
            expected
        );
        let effect = transition
            .effects
            .iter()
            .find(|effect| effect.key == job_key(group))
            .unwrap();
        assert!(effect.value.len() <= MAX_STORED_MOVES_BYTES);
        assert_eq!(effect.value[20], 3);
        let stored = decode_stored_moves(&effect.value).unwrap();
        assert_eq!(&stored.first, view.first_job(group).unwrap());
        assert_eq!(
            stored.successor,
            Some(SuccessorState::Accepted(Box::new(AcceptedSuccessor {
                record: MoveRecord {
                    intent: next,
                    progress: MoveProgress::Completed { membership }
                },
                handoff,
            })))
        );
        persisted.insert(group, stored);
    }
    assert_eq!(expected.revision, 513);
    assert_eq!(expected.nodes, endpoints);
    assert_eq!(view.origin(), Some(&origin));
    assert_eq!(
        MetadataView::from_stored_parts(
            Some(expected.clone()),
            Some(origin.clone()),
            persisted.clone()
        )
        .unwrap(),
        view
    );
    let mut wrong_count = expected.clone();
    wrong_count.revision -= 1;
    assert!(
        MetadataView::from_stored_parts(Some(wrong_count), Some(origin.clone()), persisted.clone())
            .is_err()
    );
    let mut wrong_catalog = expected;
    wrong_catalog.nodes[15].data_address = "[::1]:12345".parse().unwrap();
    assert!(MetadataView::from_stored_parts(Some(wrong_catalog), Some(origin), persisted).is_err());
}

#[test]
fn activation_codec_bounds_all_16_peer_slots_but_rejects_them_as_handoff() {
    let endpoints = wide_endpoints();
    let mut view = wide_view(&endpoints);
    let (first, next, mut proof) = prepare_wide(&mut view, GroupId::new(256), &endpoints);
    proof.observed.peers = endpoints
        .clone()
        .map(|endpoint| Some((endpoint.node_id, endpoint.peer_address)));
    proof.observed.validate().unwrap();
    // The fixed array's largest encoded shape is not a legal first-target proof.
    for progress in [
        MoveProgress::Pending,
        MoveProgress::Completed {
            membership: wide_position(0, endpoints[0].node_id),
        },
    ] {
        let stored = StoredMoves {
            first: first.clone(),
            successor: Some(SuccessorState::Accepted(Box::new(AcceptedSuccessor {
                record: MoveRecord {
                    intent: next.clone(),
                    progress,
                },
                handoff: proof.clone(),
            }))),
        };
        let encoded = encode_stored_moves(&stored).unwrap();
        assert!(encoded.len() <= MAX_STORED_MOVES_BYTES);
        assert_eq!(decode_stored_moves(&encoded).unwrap(), stored);
    }
    let before = view.clone();
    let rejected = view.apply(activate(&first, &next, proof)).unwrap();
    assert_conflict(&rejected, ConflictReason::MembershipMismatch);
    assert_eq!(view, before);
}
