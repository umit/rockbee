//! Gateway/node contracts are a separate message family from public KV.
#![allow(clippy::unwrap_used)]
use bytes::Bytes;
use rockbee_raft_protocol::{
    ClusterId, GroupId, KvRequest, KvResponse, MutationOutcome, NodeId, ShardConfig, WireRequest,
    backend::{BackendOperation, BackendReply, BackendRequest, BackendResponse},
    decode_frame, encode_frame,
    routing::{ClusterDirectory, NodeEndpoint},
};

fn cluster() -> ClusterId {
    ClusterId::new([1; 16]).unwrap()
}

fn nodes() -> Vec<NodeEndpoint> {
    (1..=3)
        .map(|id| NodeEndpoint {
            node_id: NodeId::new(id).unwrap(),
            peer_address: format!("127.0.0.1:{}", 7100 + id).parse().unwrap(),
            data_address: format!("127.0.0.1:{}", 8100 + id).parse().unwrap(),
        })
        .collect()
}

#[test]
fn directory_routes_all_groups_and_validates_endpoints_and_placement() {
    let directory =
        ClusterDirectory::initial(cluster(), ShardConfig::new(4, 7).unwrap(), nodes()).unwrap();
    directory.validate().unwrap();
    assert_eq!(directory.revision, 1);
    assert_eq!(directory.placements.len(), 5);
    for group in 0..=4 {
        assert_eq!(directory.voters(GroupId::new(group)).unwrap().len(), 3);
    }
    assert!(directory.voters(GroupId::new(5)).is_none());
    let decoded: ClusterDirectory = decode_frame(&encode_frame(&directory).unwrap()).unwrap();
    assert_eq!(decoded, directory);

    let mut invalid = directory.clone();
    invalid.nodes[1].node_id = invalid.nodes[0].node_id;
    assert!(invalid.validate().is_err());
    let mut invalid = directory.clone();
    invalid.nodes[1].peer_address = invalid.nodes[0].data_address;
    assert!(invalid.validate().is_err());
    let mut invalid = directory.clone();
    invalid.nodes[0].data_address.set_port(0);
    assert!(invalid.validate().is_err());
    let mut invalid = directory.clone();
    invalid.placements[1].voters[0] = NodeId::new(99).unwrap();
    assert!(invalid.validate().is_err());
    let mut invalid = directory.clone();
    invalid.placements[1].voters[1] = invalid.placements[1].voters[0];
    assert!(invalid.validate().is_err());
    let mut invalid = directory.clone();
    invalid.placements.remove(1);
    assert!(invalid.validate().is_err());
    let mut invalid = directory.clone();
    invalid.placements[1].group_id = GroupId::new(0);
    assert!(invalid.validate().is_err());
    let mut invalid = directory;
    invalid.revision = 0;
    assert!(invalid.validate().is_err());
}

fn request(operation: BackendOperation) -> BackendRequest {
    BackendRequest {
        cluster_id: cluster(),
        target_node: NodeId::new(2).unwrap(),
        operation,
    }
}

fn response(reply: BackendReply) -> BackendResponse {
    BackendResponse {
        cluster_id: cluster(),
        node_id: NodeId::new(2).unwrap(),
        reply,
    }
}

#[test]
fn backend_validates_destination_family_group_and_mutation_certainty() {
    let request = request(BackendOperation::Kv {
        group_id: GroupId::new(1),
        request: KvRequest::Put {
            key: Bytes::from_static(b"product/42"),
            value: Bytes::new(),
        },
    });
    request.validate().unwrap();
    let expected = request.expected_response();
    let success = response(BackendReply::Kv {
        response: KvResponse::Mutation(MutationOutcome::Committed),
        leader_hint: None,
    });
    BackendRequest::validate_response(expected, &success).unwrap();
    let mut wrong_node = success.clone();
    wrong_node.node_id = NodeId::new(3).unwrap();
    assert!(BackendRequest::validate_response(expected, &wrong_node).is_err());
    let mut wrong_cluster = success.clone();
    wrong_cluster.cluster_id = ClusterId::new([2; 16]).unwrap();
    assert!(BackendRequest::validate_response(expected, &wrong_cluster).is_err());
    assert!(
        BackendRequest::validate_response(
            expected,
            &response(BackendReply::Leader {
                group_id: GroupId::new(1),
                leader: None
            }),
        )
        .is_err()
    );
    let read = response(BackendReply::Kv {
        response: KvResponse::Value(None),
        leader_hint: None,
    });
    assert!(BackendRequest::validate_response(expected, &read).is_err());
    let mut metadata_write = request;
    if let BackendOperation::Kv { group_id, .. } = &mut metadata_write.operation {
        *group_id = GroupId::new(0);
    }
    assert!(metadata_write.validate().is_err());
}

#[test]
fn directory_response_must_match_request_cluster_and_validate_its_contents() {
    let expected = request(BackendOperation::Directory).expected_response();
    let mut directory =
        ClusterDirectory::initial(cluster(), ShardConfig::new(4, 7).unwrap(), nodes()).unwrap();
    BackendRequest::validate_response(
        expected,
        &response(BackendReply::Directory(Ok(directory.clone()))),
    )
    .unwrap();
    directory.cluster_id = ClusterId::new([2; 16]).unwrap();
    assert!(
        BackendRequest::validate_response(
            expected,
            &response(BackendReply::Directory(Ok(directory)))
        )
        .is_err()
    );
}

#[test]
fn oversized_directory_collections_are_rejected_while_decoding() {
    let mut directory =
        ClusterDirectory::initial(cluster(), ShardConfig::new(4, 7).unwrap(), nodes()).unwrap();
    directory.nodes = vec![directory.nodes[0].clone(); 17];
    let frame = encode_frame(&directory).unwrap();
    assert!(decode_frame::<ClusterDirectory>(&frame).is_err());
    let mut directory =
        ClusterDirectory::initial(cluster(), ShardConfig::new(4, 7).unwrap(), nodes()).unwrap();
    directory.placements[0].voters = vec![NodeId::new(1).unwrap(); 17];
    assert!(decode_frame::<ClusterDirectory>(&encode_frame(&directory).unwrap()).is_err());
    let mut directory =
        ClusterDirectory::initial(cluster(), ShardConfig::new(4, 7).unwrap(), nodes()).unwrap();
    directory.placements = vec![directory.placements[0].clone(); 258];
    assert!(decode_frame::<ClusterDirectory>(&encode_frame(&directory).unwrap()).is_err());
}

#[test]
fn bootstrap_has_no_caller_payload_and_requires_a_remote_mutation_result() {
    let request = request(BackendOperation::BootstrapDirectory);
    request.validate().unwrap();
    let expected = request.expected_response();
    let decoded: BackendRequest = decode_frame(&encode_frame(&request).unwrap()).unwrap();
    assert_eq!(decoded, request);
    BackendRequest::validate_response(
        expected,
        &response(BackendReply::Bootstrap(MutationOutcome::Committed)),
    )
    .unwrap();
    let error = rockbee_raft_protocol::Failure {
        reason: rockbee_raft_protocol::FailureReason::Unavailable,
        message: "not sent is local-only".into(),
    };
    assert!(
        BackendRequest::validate_response(
            expected,
            &response(BackendReply::Bootstrap(MutationOutcome::NotSent(error))),
        )
        .is_err()
    );
    assert!(
        BackendRequest::validate_response(
            expected,
            &response(BackendReply::Directory(Err(
                rockbee_raft_protocol::ReadFailure(rockbee_raft_protocol::Failure {
                    reason: rockbee_raft_protocol::FailureReason::Unavailable,
                    message: "different family".into(),
                },)
            ))),
        )
        .is_err()
    );
}
