use std::net::SocketAddr;

use rockbee_raft_protocol::{
    ClusterId, GroupId, MAX_MESSAGE_BYTES, NodeId, ShardConfig,
    control::{
        AdminOperation, AdminResult, CommittedLogPosition, MoveJobId, MoveProgress, MoveRecord,
        MoveReplica,
    },
    decode_frame, encode_frame,
    routing::{ClusterDirectory, NodeEndpoint},
};

fn node(id: u64, peer_port: u16, data_port: u16) -> NodeEndpoint {
    NodeEndpoint {
        node_id: NodeId::new(id).expect("test node"),
        peer_address: SocketAddr::from(([127, 0, 0, 1], peer_port)),
        data_address: SocketAddr::from(([127, 0, 0, 1], data_port)),
    }
}

fn directory() -> ClusterDirectory {
    ClusterDirectory::initial(
        ClusterId::new([1; 16]).expect("test cluster"),
        ShardConfig::new(2, 9).expect("test sharding"),
        vec![
            node(1, 11001, 12001),
            node(2, 11002, 12002),
            node(3, 11003, 12003),
        ],
    )
    .expect("valid initial directory")
}

fn move_request(group: u64) -> MoveReplica {
    MoveReplica {
        job_id: MoveJobId::new([7; 16]).expect("test job"),
        data_group: GroupId::new(group),
        remove_node: NodeId::new(3).expect("test node"),
        add_node: node(4, 11004, 12004),
        expected_directory_revision: 1,
    }
}

#[test]
fn all_zero_job_id_is_rejected_during_decode() {
    let bytes = encode_frame(&[0_u8; 16]).expect("encode raw job bytes");
    assert!(decode_frame::<MoveJobId>(&bytes).is_err());
}

#[test]
fn metadata_group_and_existing_target_are_rejected() {
    assert!(move_request(0).derive_intent(&directory()).is_err());
    let invalid_frame = encode_frame(&move_request(0)).expect("encode invalid raw operator input");
    assert!(decode_frame::<MoveReplica>(&invalid_frame).is_err());
    let mut request = move_request(1);
    request.add_node = node(2, 11002, 12002);
    assert!(request.derive_intent(&directory()).is_err());
}

#[test]
fn derives_exact_three_voter_abc_to_abd_mapping() {
    let intent = move_request(1)
        .derive_intent(&directory())
        .expect("valid move");
    assert_eq!(intent.old_voters.map(NodeId::get), [1, 2, 3]);
    assert_eq!(intent.target_voters.map(NodeId::get), [1, 2, 4]);
}

#[test]
fn decoded_intent_requires_canonical_exact_replacement() {
    let mut intent = move_request(1)
        .derive_intent(&directory())
        .expect("valid move");
    intent.target_voters = [
        NodeId::new(1).expect("test node"),
        NodeId::new(3).expect("test node"),
        NodeId::new(4).expect("test node"),
    ];

    let invalid_frame = encode_frame(&intent).expect("encode raw invalid intent");
    assert!(decode_frame::<rockbee_raft_protocol::control::MoveIntent>(&invalid_frame).is_err());
}

#[test]
fn decoded_admin_groups_and_diagnostics_are_bounded() {
    let invalid_group = AdminOperation::GetMove {
        data_group: GroupId::new(0),
    };
    let invalid_group_frame = encode_frame(&invalid_group).expect("encode raw invalid group");
    assert!(decode_frame::<AdminOperation>(&invalid_group_frame).is_err());

    let oversized_diagnostic = AdminResult::Conflict {
        message: "x".repeat(MAX_MESSAGE_BYTES + 1),
    };
    let diagnostic_frame =
        encode_frame(&oversized_diagnostic).expect("encode raw oversized diagnostic");
    assert!(decode_frame::<AdminResult>(&diagnostic_frame).is_err());
}

#[test]
fn completed_result_requires_completed_record_and_valid_payload() {
    let valid_intent = move_request(1)
        .derive_intent(&directory())
        .expect("valid move");
    let contradictory = AdminResult::Completed {
        record: MoveRecord {
            intent: valid_intent.clone(),
            progress: MoveProgress::Pending,
        },
    };
    let contradictory_frame = encode_frame(&contradictory).expect("encode contradictory result");
    assert!(decode_frame::<AdminResult>(&contradictory_frame).is_err());
    assert!(contradictory.validate().is_err());

    let mut malformed_intent = valid_intent;
    malformed_intent.target_voters.swap(0, 1);
    let direct_result = AdminResult::Accepted {
        intent: malformed_intent,
    };
    assert!(direct_result.validate().is_err());
}

#[test]
fn decoded_request_rejects_same_removed_and_replacement_node() {
    let mut request = move_request(1);
    request.add_node.node_id = request.remove_node;

    let invalid_frame = encode_frame(&request).expect("encode invalid raw request");
    assert!(decode_frame::<MoveReplica>(&invalid_frame).is_err());
}

#[test]
fn committed_log_position_round_trips_full_identity() {
    let position = CommittedLogPosition {
        term: 42,
        leader_node_id: NodeId::new(9).expect("test leader"),
        index: 99,
    };
    let decoded: CommittedLogPosition =
        decode_frame(&encode_frame(&position).expect("encode position")).expect("decode position");
    assert_eq!(decoded, position);
}
