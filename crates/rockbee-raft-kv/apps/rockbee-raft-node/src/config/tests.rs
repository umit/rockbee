#![allow(clippy::unwrap_used)]
use super::*;
use crate::{
    control::MetadataCommand,
    storage::{OpenMode, StorageOptions},
};
use rockbee_raft_protocol::{ClusterId, ShardConfig};
use rockbee_raft_protocol::{
    NodeId,
    routing::{ClusterDirectory, NodeEndpoint},
};
use std::path::PathBuf;

fn config() -> NodeConfig {
    let nodes = (1..=3)
        .map(|id| NodeEndpoint {
            node_id: NodeId::new(id).unwrap(),
            peer_address: format!("127.0.0.1:{}", 7000 + id).parse().unwrap(),
            data_address: format!("127.0.0.1:{}", 8000 + id).parse().unwrap(),
        })
        .collect();
    let directory = ClusterDirectory::initial(
        ClusterId::new([19; 16]).unwrap(),
        ShardConfig::new(1, 0).unwrap(),
        nodes,
    )
    .unwrap();
    NodeConfig::new(
        directory,
        NodeId::new(1).unwrap(),
        PathBuf::from("unused-budget-test-db"),
        OpenMode::CreateNew,
    )
    .unwrap()
}

#[test]
fn node_rejects_insufficient_metadata_output_budget() {
    let mut config = config();
    config.storage.max_entry_bytes = 4096;
    // The accepted control reservation is entry + record + batch + entry
    // overhead + expanded effects. This remains a valid raw-KV budget.
    config.storage.io_chunk_bytes = config.storage.metadata_minimum_chunk_bytes().unwrap() - 1;
    assert!(config.storage.validate().is_ok());
    assert!(
        config.validate().is_err(),
        "a production node must reserve the complete metadata output budget"
    );
}

#[test]
fn node_metadata_output_full_entry_and_frame_boundaries() {
    let mut config = config();
    let command = MetadataCommand::InitializeDirectoryIfAbsent {
        initial_directory: config.directory.clone(),
    };
    config.storage.max_entry_bytes = 4096;
    config.storage.io_chunk_bytes = config.storage.metadata_minimum_chunk_bytes().unwrap();
    config.validate().unwrap();
    config.storage.io_chunk_bytes -= 1;
    assert!(config.validate().is_err());
    config.storage.io_chunk_bytes = StorageOptions::default().io_chunk_bytes;

    let required = crate::storage::metadata_entry_bytes(&command).unwrap();
    let body_only = postcard::experimental::serialized_size(&command).unwrap();
    assert!(
        required > body_only,
        "assigned LogId and durable envelope must be counted"
    );
    config.storage.max_entry_bytes = required;
    config.validate().unwrap();
    config.storage.max_entry_bytes = required - 1;
    assert!(config.validate().is_err());
    config.storage.max_entry_bytes = body_only;
    assert!(config.validate().is_err());
    config.storage.max_entry_bytes = required;

    let frame = crate::rpc::metadata_append_bytes(&command).unwrap();
    config.peers.max_snapshot_chunk_bytes = 1;
    config.peers.max_frame_bytes = frame;
    config.validate().unwrap();
    config.peers.max_frame_bytes = frame - 1;
    assert!(config.validate().is_err());
}

#[test]
fn node_requires_canonical_revision_one_origin_and_checked_expansion() {
    for shape in 0..4 {
        let mut config = config();
        match shape {
            0 => config.directory.revision = 2,
            1 => config.directory.nodes.swap(0, 1),
            2 => config.directory.placements.swap(0, 1),
            _ => {
                config.directory.nodes.truncate(1);
                for placement in &mut config.directory.placements {
                    placement.voters.truncate(1);
                }
            }
        }
        config.directory.validate().unwrap();
        assert!(config.validate().is_err());
    }
    let storage = StorageOptions {
        max_entry_bytes: usize::MAX,
        ..StorageOptions::default()
    };
    assert!(storage.metadata_minimum_chunk_bytes().is_err());
}

#[test]
fn reusable_candidate_guard_covers_later_control_operations() {
    use crate::control::MembershipObservation;
    use rockbee_raft_protocol::{
        GroupId,
        control::{CommittedLogPosition, MoveJobId, MoveReplica},
    };
    let mut config = config();
    let movement = MoveReplica {
        job_id: MoveJobId::new([1; 16]).unwrap(),
        data_group: GroupId::new(1),
        remove_node: NodeId::new(3).unwrap(),
        add_node: NodeEndpoint {
            node_id: NodeId::new(4).unwrap(),
            peer_address: "127.0.0.1:7004".parse().unwrap(),
            data_address: "127.0.0.1:8004".parse().unwrap(),
        },
        expected_directory_revision: 1,
    }
    .derive_intent(&config.directory)
    .unwrap();
    let commands = [
        MetadataCommand::BeginMove {
            intent: movement.clone(),
        },
        MetadataCommand::CompleteMove {
            expected_directory_revision: u64::MAX,
            data_group: movement.data_group,
            job_id: movement.job_id,
            observation: MembershipObservation {
                uniform: true,
                voters: movement.target_voters,
                replacement_endpoint: movement.add_node,
                membership: CommittedLogPosition {
                    term: u64::MAX,
                    leader_node_id: NodeId::new(u64::MAX).unwrap(),
                    index: u64::MAX,
                },
            },
        },
    ];
    for command in commands {
        let entry = crate::storage::metadata_entry_bytes(&command).unwrap();
        let frame = crate::rpc::metadata_append_bytes(&command).unwrap();
        config.storage.max_entry_bytes = entry;
        config.peers.max_frame_bytes = frame;
        validate_metadata_candidate(&command, &config.storage, config.peers).unwrap();
        config.storage.max_entry_bytes -= 1;
        assert!(validate_metadata_candidate(&command, &config.storage, config.peers).is_err());
        config.storage.max_entry_bytes = entry;
        config.peers.max_frame_bytes -= 1;
        assert!(validate_metadata_candidate(&command, &config.storage, config.peers).is_err());
    }
}
