use super::*;
use crate::{
    ClusterId, Failure, FailureReason, MAX_FRAME_PAYLOAD, NodeId, ShardConfig, WireRequest,
    control::{CommittedLogPosition, MoveJobId, MoveProgress, MoveRecord, MoveReplica},
    decode_frame, encode_frame,
    routing::{ClusterDirectory, NodeEndpoint},
};

const GROUP: GroupId = GroupId::new(1);

fn cluster() -> ClusterId {
    ClusterId::new([1; 16]).expect("test cluster")
}

fn node() -> NodeId {
    NodeId::new(1).expect("test node")
}

fn job(byte: u8) -> MoveJobId {
    MoveJobId::new([byte; 16]).expect("test job")
}

fn unavailable() -> ReadFailure {
    ReadFailure::new(Failure::new(FailureReason::Unavailable, "unavailable").expect("failure"))
}

fn endpoint(id: u64) -> NodeEndpoint {
    NodeEndpoint {
        node_id: NodeId::new(id).expect("test node"),
        peer_address: format!("127.0.0.1:{}", 7000 + id)
            .parse()
            .expect("test peer address"),
        data_address: format!("127.0.0.1:{}", 8000 + id)
            .parse()
            .expect("test data address"),
    }
}

fn ipv6_endpoint(id: u64) -> NodeEndpoint {
    NodeEndpoint {
        node_id: NodeId::new(id).expect("test node"),
        peer_address: format!("[2001:db8::{id}]:{}", 7000 + id)
            .parse()
            .expect("test peer address"),
        data_address: format!("[2001:db8::{id}]:{}", 8000 + id)
            .parse()
            .expect("test data address"),
    }
}

fn prepared_state() -> PreparedMoveState {
    let mut directory = ClusterDirectory::initial(
        cluster(),
        ShardConfig::new(1, 0).expect("test shards"),
        vec![endpoint(1), endpoint(2), endpoint(3)],
    )
    .expect("initial directory");
    let first_intent = MoveReplica {
        job_id: job(2),
        data_group: GROUP,
        remove_node: NodeId::new(1).expect("removed node"),
        add_node: endpoint(4),
        expected_directory_revision: 1,
    }
    .derive_intent(&directory)
    .expect("first intent");
    directory.revision = 2;
    directory.nodes.push(endpoint(4));
    directory
        .placements
        .iter_mut()
        .find(|placement| placement.group_id == GROUP)
        .expect("data placement")
        .voters = first_intent.target_voters.to_vec();
    let first = MoveRecord {
        intent: first_intent,
        progress: MoveProgress::Completed {
            membership: CommittedLogPosition {
                term: 1,
                leader_node_id: node(),
                index: 1,
            },
        },
    };
    let preparing = PreparingSuccessor {
        intent: MoveReplica {
            job_id: job(3),
            data_group: GROUP,
            remove_node: NodeId::new(2).expect("removed node"),
            add_node: endpoint(5),
            expected_directory_revision: 2,
        }
        .derive_intent(&directory)
        .expect("preparing intent"),
        executors: [endpoint(1), endpoint(2), endpoint(3), endpoint(4)],
    };
    PreparedMoveState {
        directory,
        first,
        preparing,
    }
}

fn maximum_prepared_state() -> PreparedMoveState {
    let mut directory = ClusterDirectory::initial(
        cluster(),
        ShardConfig::new(256, 0).expect("maximum shards"),
        vec![ipv6_endpoint(1), ipv6_endpoint(2), ipv6_endpoint(3)],
    )
    .expect("initial directory");
    let first_intent = MoveReplica {
        job_id: job(2),
        data_group: GROUP,
        remove_node: NodeId::new(1).expect("removed node"),
        add_node: ipv6_endpoint(4),
        expected_directory_revision: 1,
    }
    .derive_intent(&directory)
    .expect("first intent");
    directory.revision = 2;
    directory.nodes.extend((4..=16).map(ipv6_endpoint));
    directory
        .placements
        .iter_mut()
        .find(|placement| placement.group_id == GROUP)
        .expect("data placement")
        .voters = first_intent.target_voters.to_vec();
    let first = MoveRecord {
        intent: first_intent,
        progress: MoveProgress::Completed {
            membership: CommittedLogPosition {
                term: 1,
                leader_node_id: node(),
                index: 1,
            },
        },
    };
    let preparing = PreparingSuccessor {
        intent: MoveReplica {
            job_id: job(3),
            data_group: GROUP,
            remove_node: NodeId::new(2).expect("removed node"),
            add_node: ipv6_endpoint(5),
            expected_directory_revision: 2,
        }
        .derive_intent(&directory)
        .expect("preparing intent"),
        executors: [
            ipv6_endpoint(1),
            ipv6_endpoint(2),
            ipv6_endpoint(3),
            ipv6_endpoint(4),
        ],
    };
    PreparedMoveState {
        directory,
        first,
        preparing,
    }
}

fn seal_request(revision: u64) -> BackendRequest {
    BackendRequest {
        cluster_id: cluster(),
        target_node: node(),
        operation: BackendOperation::SealPredecessor {
            data_group: GROUP,
            predecessor_revision: revision,
            predecessor_job_id: job(2),
            successor_job_id: job(3),
        },
    }
}

fn sealed_reply(
    data_group: GroupId,
    predecessor_job_id: MoveJobId,
    successor_job_id: MoveJobId,
) -> BackendResponse {
    BackendResponse {
        cluster_id: cluster(),
        node_id: node(),
        reply: BackendReply::PredecessorSealed {
            data_group,
            predecessor_job_id,
            successor_job_id,
            result: Err(unavailable()),
        },
    }
}

fn successful_sealed_reply(
    data_group: GroupId,
    predecessor_revision: u64,
    predecessor_job_id: MoveJobId,
    successor_job_id: MoveJobId,
    receipt_node: NodeId,
) -> BackendResponse {
    BackendResponse {
        cluster_id: cluster(),
        node_id: node(),
        reply: BackendReply::PredecessorSealed {
            data_group,
            predecessor_job_id,
            successor_job_id,
            result: Ok(SealReceipt {
                endpoint: endpoint(receipt_node.get()),
                data_group,
                predecessor_revision,
                predecessor_job_id,
            }),
        },
    }
}

fn fence_request() -> BackendRequest {
    BackendRequest {
        cluster_id: cluster(),
        target_node: node(),
        operation: BackendOperation::CommitHandoffFence {
            data_group: GROUP,
            predecessor_revision: 7,
            predecessor_job_id: job(2),
            successor_job_id: job(3),
        },
    }
}

fn fenced_reply(
    data_group: GroupId,
    predecessor_revision: u64,
    predecessor_job_id: MoveJobId,
    successor_job_id: MoveJobId,
) -> BackendResponse {
    BackendResponse {
        cluster_id: cluster(),
        node_id: node(),
        reply: BackendReply::HandoffFenced {
            data_group,
            predecessor_job_id,
            successor_job_id,
            result: Ok(HandoffFenceReceipt {
                data_group,
                predecessor_revision,
                predecessor_job_id,
                successor_job_id,
                committed: CommittedLogPosition {
                    term: 1,
                    leader_node_id: node(),
                    index: 2,
                },
            }),
        },
    }
}

#[test]
fn legacy_backend_operation_tags_are_stable_when_handoff_variants_append() {
    assert_eq!(
        postcard::to_stdvec(&BackendOperation::Directory).unwrap(),
        vec![0]
    );
    assert_eq!(
        postcard::to_stdvec(&BackendOperation::GetMove { data_group: GROUP }).unwrap(),
        vec![5, 1]
    );
    assert_eq!(
        postcard::to_stdvec(&BackendOperation::PublishMove {
            data_group: GROUP,
            job_id: job(2),
        })
        .unwrap()[0],
        8
    );
}

#[test]
fn seal_reply_with_matching_typed_unavailable_result_is_accepted() {
    let request = seal_request(7);
    let expected = request.expected_response();
    assert!(
        BackendRequest::validate_response(expected, &sealed_reply(GROUP, job(2), job(3)),).is_ok()
    );
}

#[test]
fn sealed_receipt_requires_exact_node_group_revision_and_jobs() {
    let request = seal_request(7);
    let exact = successful_sealed_reply(GROUP, 7, job(2), job(3), node());
    assert!(BackendRequest::validate_response(request.expected_response(), &exact,).is_ok());
    let frame = encode_frame(&exact).expect("seal response frame");
    assert_eq!(
        decode_frame::<BackendResponse>(&frame).expect("seal response decode"),
        exact
    );
    assert!(
        BackendRequest::validate_response(
            request.expected_response(),
            &successful_sealed_reply(GROUP, 8, job(2), job(3), node()),
        )
        .is_err()
    );
    assert!(
        BackendRequest::validate_response(
            request.expected_response(),
            &successful_sealed_reply(
                GROUP,
                7,
                job(2),
                job(3),
                NodeId::new(4).expect("other node")
            ),
        )
        .is_err()
    );
}

#[test]
fn handoff_operation_frames_round_trip_without_changing_legacy_tags() {
    let operations = [
        BackendOperation::GetPreparedMove { data_group: GROUP },
        BackendOperation::SealPredecessor {
            data_group: GROUP,
            predecessor_revision: 7,
            predecessor_job_id: job(2),
            successor_job_id: job(3),
        },
        BackendOperation::CommitHandoffFence {
            data_group: GROUP,
            predecessor_revision: 7,
            predecessor_job_id: job(2),
            successor_job_id: job(3),
        },
    ];
    for operation in operations {
        let request = BackendRequest {
            cluster_id: cluster(),
            target_node: node(),
            operation,
        };
        let frame = encode_frame(&request).expect("request frame");
        assert_eq!(
            decode_frame::<BackendRequest>(&frame).expect("request decode"),
            request
        );
    }
}

#[test]
fn prepared_move_none_and_error_replies_are_valid_and_frame_round_trip() {
    let request = BackendRequest {
        cluster_id: cluster(),
        target_node: node(),
        operation: BackendOperation::GetPreparedMove { data_group: GROUP },
    };
    for result in [Ok(None), Err(unavailable())] {
        let response = BackendResponse {
            cluster_id: cluster(),
            node_id: node(),
            reply: BackendReply::PreparedMoveState {
                data_group: GROUP,
                result,
            },
        };
        assert!(BackendRequest::validate_response(request.expected_response(), &response).is_ok());
        let frame = encode_frame(&response).expect("response frame");
        assert_eq!(
            decode_frame::<BackendResponse>(&frame).expect("response decode"),
            response
        );
    }
}

#[test]
fn handoff_operations_reject_metadata_group_and_zero_revision() {
    let metadata = BackendRequest {
        operation: BackendOperation::GetPreparedMove {
            data_group: GroupId::new(0),
        },
        ..seal_request(7)
    };
    assert!(metadata.validate().is_err());
    assert!(seal_request(0).validate().is_err());
}

#[test]
fn maximum_prepared_state_stays_within_the_existing_backend_frame_bound() {
    let request = BackendRequest {
        cluster_id: cluster(),
        target_node: node(),
        operation: BackendOperation::GetPreparedMove { data_group: GROUP },
    };
    assert!(request.retained_bytes().expect("request retained bytes") <= MAX_FRAME_PAYLOAD);
    let response = BackendResponse {
        cluster_id: cluster(),
        node_id: node(),
        reply: BackendReply::PreparedMoveState {
            data_group: GROUP,
            result: Ok(Some(Box::new(maximum_prepared_state()))),
        },
    };
    let frame = encode_frame(&response).expect("maximum prepared response frame");
    assert!(frame.len() <= MAX_FRAME_PAYLOAD + 4);
    assert_eq!(
        decode_frame::<BackendResponse>(&frame).expect("maximum response decode"),
        response
    );
}

#[test]
fn prepared_move_rejects_a_new_seventeenth_catalog_endpoint() {
    let mut state = maximum_prepared_state();
    state.preparing.intent = MoveReplica {
        job_id: job(3),
        data_group: GROUP,
        remove_node: NodeId::new(2).expect("removed node"),
        add_node: ipv6_endpoint(17),
        expected_directory_revision: 2,
    }
    .derive_intent(&state.directory)
    .expect("seventeenth intent");
    assert!(state.validate().is_err());
}

#[test]
fn handoff_operations_reject_the_same_predecessor_and_successor_job() {
    let seal = BackendRequest {
        cluster_id: cluster(),
        target_node: node(),
        operation: BackendOperation::SealPredecessor {
            data_group: GROUP,
            predecessor_revision: 7,
            predecessor_job_id: job(2),
            successor_job_id: job(2),
        },
    };
    assert!(seal.validate().is_err());
    let fence = BackendRequest {
        operation: BackendOperation::CommitHandoffFence {
            data_group: GROUP,
            predecessor_revision: 7,
            predecessor_job_id: job(2),
            successor_job_id: job(2),
        },
        ..seal
    };
    assert!(fence.validate().is_err());
}

#[test]
fn seal_reply_rejects_response_identity_group_and_job_mismatches() {
    let request = seal_request(7);
    let expected = request.expected_response();
    let mut wrong_node = sealed_reply(GROUP, job(2), job(3));
    wrong_node.node_id = NodeId::new(4).expect("different node");
    assert!(BackendRequest::validate_response(expected, &wrong_node).is_err());

    let expected = request.expected_response();
    assert!(
        BackendRequest::validate_response(expected, &sealed_reply(GroupId::new(2), job(2), job(3)))
            .is_err()
    );

    let expected = request.expected_response();
    assert!(
        BackendRequest::validate_response(expected, &sealed_reply(GROUP, job(4), job(3))).is_err()
    );
}

#[test]
fn fence_reply_accepts_exact_receipt_and_rejects_revision_or_job_mismatches() {
    let request = fence_request();
    let exact = fenced_reply(GROUP, 7, job(2), job(3));
    assert!(BackendRequest::validate_response(request.expected_response(), &exact,).is_ok());
    let frame = encode_frame(&exact).expect("fence response frame");
    assert_eq!(
        decode_frame::<BackendResponse>(&frame).expect("fence response decode"),
        exact
    );
    assert!(
        BackendRequest::validate_response(
            request.expected_response(),
            &fenced_reply(GROUP, 8, job(2), job(3)),
        )
        .is_err()
    );
    assert!(
        BackendRequest::validate_response(
            request.expected_response(),
            &fenced_reply(GROUP, 7, job(2), job(4)),
        )
        .is_err()
    );
}

#[test]
fn prepared_move_reply_validates_the_fixed_census_and_lineage_shape() {
    let request = BackendRequest {
        cluster_id: cluster(),
        target_node: node(),
        operation: BackendOperation::GetPreparedMove { data_group: GROUP },
    };
    let response = |state| BackendResponse {
        cluster_id: cluster(),
        node_id: node(),
        reply: BackendReply::PreparedMoveState {
            data_group: GROUP,
            result: Ok(Some(Box::new(state))),
        },
    };
    assert!(
        BackendRequest::validate_response(request.expected_response(), &response(prepared_state()))
            .is_ok()
    );

    let mut bad_census = prepared_state();
    bad_census.preparing.executors[0] = endpoint(2);
    assert!(
        BackendRequest::validate_response(request.expected_response(), &response(bad_census))
            .is_err()
    );

    let mut bad_first = prepared_state();
    bad_first.first.progress = MoveProgress::Pending;
    assert!(
        BackendRequest::validate_response(request.expected_response(), &response(bad_first))
            .is_err()
    );

    let mut bad_preparing = prepared_state();
    bad_preparing.preparing.intent.expected_directory_revision = 1;
    assert!(
        BackendRequest::validate_response(request.expected_response(), &response(bad_preparing),)
            .is_err()
    );
}

#[test]
fn prepared_move_reply_rejects_forged_predecessor_and_successor_endpoints() {
    let request = BackendRequest {
        cluster_id: cluster(),
        target_node: node(),
        operation: BackendOperation::GetPreparedMove { data_group: GROUP },
    };
    let response = |state| BackendResponse {
        cluster_id: cluster(),
        node_id: node(),
        reply: BackendReply::PreparedMoveState {
            data_group: GROUP,
            result: Ok(Some(Box::new(state))),
        },
    };
    let mut forged_first = prepared_state();
    forged_first.first.intent.add_node.data_address =
        "127.0.0.1:9004".parse().expect("forged address");
    assert!(
        BackendRequest::validate_response(request.expected_response(), &response(forged_first),)
            .is_err()
    );

    let mut conflicting_successor = prepared_state();
    conflicting_successor.preparing.intent.add_node.node_id = NodeId::new(1).expect("catalog node");
    conflicting_successor.preparing.intent.target_voters = [
        NodeId::new(1).expect("catalog node"),
        NodeId::new(3).expect("voter"),
        NodeId::new(4).expect("voter"),
    ];
    conflicting_successor.preparing.intent.add_node.peer_address =
        "127.0.0.1:9001".parse().expect("conflicting peer");
    conflicting_successor.preparing.intent.add_node.data_address =
        "127.0.0.1:9002".parse().expect("conflicting data");
    assert!(
        BackendRequest::validate_response(
            request.expected_response(),
            &response(conflicting_successor),
        )
        .is_err()
    );

    let mut aliased_successor = prepared_state();
    aliased_successor.preparing.intent.add_node.peer_address = endpoint(1).peer_address;
    assert!(
        BackendRequest::validate_response(
            request.expected_response(),
            &response(aliased_successor),
        )
        .is_err()
    );
}
