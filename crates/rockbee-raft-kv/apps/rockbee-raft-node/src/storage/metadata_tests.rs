#![allow(clippy::unwrap_used)]

use super::*;
use crate::{
    control::{self, ConflictReason, MembershipObservation, MetadataCommand, MetadataResult},
    types::{ApplyResult, Command, Entry, LogId},
};
use bytes::Bytes;
use openraft::{
    EntryPayload,
    storage::{RaftSnapshotBuilder, RaftStateMachine},
};
use rockbee_raft_protocol::{
    control::{CommittedLogPosition, MoveIntent, MoveJobId, MoveProgress, MoveRecord, MoveReplica},
    routing::{ClusterDirectory, NodeEndpoint},
};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

mod activation;
mod successor;

fn identity() -> StorageIdentity {
    StorageIdentity {
        cluster_id: ClusterId::new([2; 16]).unwrap(),
        node_id: NodeId::new(1).unwrap(),
        shard_config: ShardConfig::new(4, 42).unwrap(),
    }
}

fn endpoint(id: u64) -> NodeEndpoint {
    NodeEndpoint {
        node_id: NodeId::new(id).unwrap(),
        peer_address: format!("127.0.0.1:{}", 20000 + id * 2).parse().unwrap(),
        data_address: format!("127.0.0.1:{}", 20001 + id * 2).parse().unwrap(),
    }
}

fn directory() -> ClusterDirectory {
    ClusterDirectory::initial(
        identity().cluster_id,
        identity().shard_config,
        (1..=3).map(endpoint).collect(),
    )
    .unwrap()
}

fn initialization() -> MetadataCommand {
    MetadataCommand::InitializeDirectoryIfAbsent {
        initial_directory: directory(),
    }
}

fn movement(group: u64, job: u8, add: u64) -> MoveIntent {
    MoveReplica {
        job_id: MoveJobId::new([job; 16]).unwrap(),
        data_group: GroupId::new(group),
        remove_node: NodeId::new(3).unwrap(),
        add_node: endpoint(add),
        expected_directory_revision: 1,
    }
    .derive_intent(&directory())
    .unwrap()
}

fn entry(index: u64, command: Command) -> Entry {
    Entry {
        log_id: LogId::new(openraft::CommittedLeaderId::new(3, 1), index),
        payload: EntryPayload::Normal(command),
    }
}

fn metadata(index: u64, command: MetadataCommand) -> Entry {
    entry(index, Command::Metadata(Box::new(command)))
}

async fn open(path: PathBuf, mode: OpenMode, options: StorageOptions) -> Storage {
    Storage::open(path, identity(), options, mode)
        .await
        .unwrap()
}

async fn read(stores: &GroupStores, key: &'static [u8]) -> Option<Bytes> {
    stores
        .state_machine
        .reader()
        .read_local(Bytes::from_static(key))
        .await
        .unwrap()
        .value
}

#[test]
fn metadata_full_entry_guard_matches_worst_durable_envelope() {
    let command = initialization();
    let required = metadata_entry_bytes(&command).unwrap();
    let entry = Entry {
        log_id: LogId::new(
            openraft::CommittedLeaderId::new(u64::MAX, u64::MAX),
            u64::MAX,
        ),
        payload: EntryPayload::Normal(Command::Metadata(Box::new(command.clone()))),
    };
    let bytes = postcard::to_stdvec(&(STORAGE_FORMAT, &entry)).unwrap();
    assert_eq!(required, bytes.len());
    assert!(required > postcard::experimental::serialized_size(&command).unwrap());
    let mut options = StorageOptions {
        max_entry_bytes: required,
        ..StorageOptions::default()
    };
    assert_eq!(
        prepare_entry(entry.clone(), &options).unwrap().encoded,
        bytes
    );
    options.max_entry_bytes -= 1;
    assert!(prepare_entry(entry, &options).is_err());
}

#[tokio::test]
async fn metadata_failed_sync_preserves_prior_ordinary_chunk_and_pending_move() {
    let dir = tempfile::tempdir().unwrap();
    let storage = open(
        dir.path().join("db"),
        OpenMode::CreateNew,
        StorageOptions::default(),
    )
    .await;
    let mut stores = storage.open_group(GroupId::new(0)).unwrap();
    let movement = movement(1, 1, 4);
    stores
        .state_machine
        .apply([
            metadata(0, initialization()),
            metadata(
                1,
                MetadataCommand::BeginMove {
                    intent: movement.clone(),
                },
            ),
        ])
        .await
        .unwrap();
    stores
        .state_machine
        .group
        .test_writes
        .lock()
        .unwrap()
        .extend([TestWriteAction::Pass, TestWriteAction::Fail]);
    let result = stores
        .state_machine
        .apply([
            entry(
                2,
                Command::Put {
                    key: Bytes::from_static(b"ordinary"),
                    value: Bytes::from_static(b"durable"),
                },
            ),
            metadata(
                3,
                MetadataCommand::CompleteMove {
                    expected_directory_revision: 1,
                    data_group: movement.data_group,
                    job_id: movement.job_id,
                    observation: MembershipObservation {
                        uniform: true,
                        voters: movement.target_voters,
                        replacement_endpoint: movement.add_node.clone(),
                        membership: CommittedLogPosition {
                            term: 9,
                            leader_node_id: NodeId::new(2).unwrap(),
                            index: 99,
                        },
                    },
                },
            ),
        ])
        .await;
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("injected durable write failure")
    );
    assert_eq!(
        read(&stores, b"ordinary").await.unwrap().as_ref(),
        b"durable"
    );
    let current = read(&stores, control::DIRECTORY_KEY).await.unwrap();
    assert_eq!(
        rockbee_raft_protocol::decode_frame::<ClusterDirectory>(&current).unwrap(),
        directory()
    );
    let record = stores
        .state_machine
        .reader()
        .read_local(Bytes::from(control::job_key(movement.data_group)))
        .await
        .unwrap();
    assert_eq!(
        rockbee_raft_protocol::decode_frame::<MoveRecord>(&record.value.unwrap())
            .unwrap()
            .progress,
        MoveProgress::Pending
    );
    let expected_applied = Some(LogId::new(openraft::CommittedLeaderId::new(3, 1), 2));
    assert_eq!(record.applied, expected_applied);
    assert!(
        stores
            .state_machine
            .apply([metadata(3, initialization())])
            .await
            .is_err(),
        "failed write retires group admission"
    );
    drop(stores);
    drop(storage);
    let reopened = open(
        dir.path().join("db"),
        OpenMode::OpenExisting,
        StorageOptions::default(),
    )
    .await;
    let mut stores = reopened.open_group(GroupId::new(0)).unwrap();
    assert_eq!(
        read(&stores, b"ordinary").await.unwrap().as_ref(),
        b"durable"
    );
    for key in [control::DIRECTORY_KEY, control::ORIGIN_KEY] {
        let value = read(&stores, key).await.unwrap();
        assert_eq!(
            rockbee_raft_protocol::decode_frame::<ClusterDirectory>(&value).unwrap(),
            directory()
        );
    }
    let record = stores
        .state_machine
        .reader()
        .read_local(Bytes::from(control::job_key(movement.data_group)))
        .await
        .unwrap();
    assert_eq!(
        rockbee_raft_protocol::decode_frame::<MoveRecord>(&record.value.unwrap()).unwrap(),
        MoveRecord {
            intent: movement,
            progress: MoveProgress::Pending
        }
    );
    assert_eq!(record.applied, expected_applied);
    assert_eq!(
        stores.state_machine.applied_state().await.unwrap().0,
        expected_applied
    );
}

#[tokio::test]
async fn metadata_failed_initialization_has_no_partial_directory_origin_or_applied_identity() {
    let dir = tempfile::tempdir().unwrap();
    let storage = open(
        dir.path().join("db"),
        OpenMode::CreateNew,
        StorageOptions::default(),
    )
    .await;
    let mut stores = storage.open_group(GroupId::new(0)).unwrap();
    stores
        .state_machine
        .group
        .test_writes
        .lock()
        .unwrap()
        .push_back(TestWriteAction::Fail);
    assert!(
        stores
            .state_machine
            .apply([metadata(0, initialization())])
            .await
            .is_err()
    );
    assert!(read(&stores, control::DIRECTORY_KEY).await.is_none());
    assert!(read(&stores, control::ORIGIN_KEY).await.is_none());
    assert!(
        stores
            .state_machine
            .applied_state()
            .await
            .unwrap()
            .0
            .is_none()
    );
    drop(stores);
    drop(storage);
    let reopened = open(
        dir.path().join("db"),
        OpenMode::OpenExisting,
        StorageOptions::default(),
    )
    .await;
    let mut stores = reopened.open_group(GroupId::new(0)).unwrap();
    assert!(read(&stores, control::DIRECTORY_KEY).await.is_none());
    assert!(read(&stores, control::ORIGIN_KEY).await.is_none());
    let record = stores
        .state_machine
        .reader()
        .read_local(Bytes::from(control::job_key(GroupId::new(1))))
        .await
        .unwrap();
    assert!(record.value.is_none());
    assert_eq!(record.applied, None);
    assert_eq!(stores.state_machine.applied_state().await.unwrap().0, None);
}

#[tokio::test]
async fn metadata_raw_loader_rejects_malformed_job_keys_oversized_and_trailing_values() {
    for corruption in 0..5 {
        let dir = tempfile::tempdir().unwrap();
        let storage = open(
            dir.path().join("db"),
            OpenMode::CreateNew,
            StorageOptions::default(),
        )
        .await;
        let mut stores = storage.open_group(GroupId::new(0)).unwrap();
        stores
            .state_machine
            .apply([metadata(0, initialization())])
            .await
            .unwrap();
        let record = MoveRecord {
            intent: movement(1, 1, 4),
            progress: MoveProgress::Pending,
        };
        let mut key = control::job_key(GroupId::new(1));
        let mut value = rockbee_raft_protocol::encode_frame(&record).unwrap();
        match corruption {
            0 => {
                key.pop();
            }
            1 => key.push(0),
            2 => value = vec![0; control::MAX_JOB_BYTES + 1],
            3 => value.push(0),
            _ => {
                key = control::ORIGIN_KEY.to_vec();
                value = vec![0; control::MAX_DIRECTORY_BYTES + 1];
            }
        }
        stores
            .state_machine
            .apply([entry(
                1,
                Command::Put {
                    key: Bytes::from(key),
                    value: Bytes::from(value),
                },
            )])
            .await
            .unwrap();
        assert!(
            stores
                .state_machine
                .apply([metadata(
                    2,
                    MetadataCommand::BeginMove {
                        intent: movement(1, 1, 4)
                    }
                )])
                .await
                .is_err()
        );
        assert_eq!(
            stores
                .state_machine
                .applied_state()
                .await
                .unwrap()
                .0
                .unwrap()
                .index,
            1
        );
        let current = read(&stores, control::DIRECTORY_KEY).await.unwrap();
        assert_eq!(
            rockbee_raft_protocol::decode_frame::<ClusterDirectory>(&current).unwrap(),
            directory()
        );
    }
}

#[tokio::test]
async fn metadata_raw_loader_rejects_more_jobs_than_configured_before_decode() {
    let dir = tempfile::tempdir().unwrap();
    let storage = open(
        dir.path().join("db"),
        OpenMode::CreateNew,
        StorageOptions::default(),
    )
    .await;
    let mut stores = storage.open_group(GroupId::new(0)).unwrap();
    stores
        .state_machine
        .apply([metadata(0, initialization())])
        .await
        .unwrap();
    let writes = (1..=5).map(|group| {
        let value = if group == 5 {
            vec![0]
        } else {
            let record = MoveRecord {
                intent: movement(group, u8::try_from(group).unwrap(), 4),
                progress: MoveProgress::Pending,
            };
            rockbee_raft_protocol::encode_frame(&record).unwrap()
        };
        entry(
            group,
            Command::Put {
                key: Bytes::from(control::job_key(GroupId::new(group))),
                value: Bytes::from(value),
            },
        )
    });
    stores.state_machine.apply(writes).await.unwrap();
    let failure = stores
        .state_machine
        .apply([metadata(6, initialization())])
        .await
        .unwrap_err();
    assert!(
        failure
            .to_string()
            .contains("stored move count exceeds configured groups"),
        "count limit must win over malformed fifth value"
    );
    assert_eq!(
        stores
            .state_machine
            .applied_state()
            .await
            .unwrap()
            .0
            .unwrap()
            .index,
        5
    );
}

#[tokio::test]
async fn metadata_same_iterable_is_conditional_and_conflict_advances_applied() {
    let dir = tempfile::tempdir().unwrap();
    let storage = open(
        dir.path().join("db"),
        OpenMode::CreateNew,
        StorageOptions::default(),
    )
    .await;
    let mut stores = storage.open_group(GroupId::new(0)).unwrap();
    let first = movement(1, 1, 4);
    let results = stores
        .state_machine
        .apply([
            metadata(0, initialization()),
            metadata(
                1,
                MetadataCommand::BeginMove {
                    intent: first.clone(),
                },
            ),
            metadata(
                2,
                MetadataCommand::BeginMove {
                    intent: movement(1, 2, 5),
                },
            ),
            metadata(
                3,
                MetadataCommand::BeginMove {
                    intent: movement(2, 3, 4),
                },
            ),
        ])
        .await
        .unwrap();
    assert_eq!(
        results,
        vec![
            ApplyResult::Metadata(MetadataResult::Initialized { revision: 1 }),
            ApplyResult::Metadata(MetadataResult::Accepted {
                data_group: first.data_group,
                job_id: first.job_id
            }),
            ApplyResult::Metadata(MetadataResult::Conflict {
                reason: ConflictReason::ExistingMove
            }),
            ApplyResult::Metadata(MetadataResult::Accepted {
                data_group: GroupId::new(2),
                job_id: movement(2, 3, 4).job_id
            }),
        ]
    );
    let (applied, _) = stores.state_machine.applied_state().await.unwrap();
    assert_eq!(
        applied,
        Some(LogId::new(openraft::CommittedLeaderId::new(3, 1), 3))
    );
    let bytes = read(&stores, control::DIRECTORY_KEY).await.unwrap();
    assert_eq!(
        rockbee_raft_protocol::decode_frame::<ClusterDirectory>(&bytes)
            .unwrap()
            .revision,
        1
    );
    let key = control::job_key(first.data_group);
    let job = stores
        .state_machine
        .reader()
        .read_local(Bytes::from(key))
        .await
        .unwrap()
        .value
        .unwrap();
    assert_eq!(
        rockbee_raft_protocol::decode_frame::<MoveRecord>(&job)
            .unwrap()
            .intent,
        first
    );
}

#[tokio::test]
async fn metadata_sees_historical_put_and_returns_ordered_results() {
    let dir = tempfile::tempdir().unwrap();
    let storage = open(
        dir.path().join("db"),
        OpenMode::CreateNew,
        StorageOptions::default(),
    )
    .await;
    let mut stores = storage.open_group(GroupId::new(0)).unwrap();
    let framed = rockbee_raft_protocol::encode_frame(&directory()).unwrap();
    let results = stores
        .state_machine
        .apply([
            entry(
                0,
                Command::Put {
                    key: Bytes::from_static(control::DIRECTORY_KEY),
                    value: Bytes::from(framed.clone()),
                },
            ),
            metadata(1, initialization()),
            entry(
                2,
                Command::Put {
                    key: Bytes::from_static(b"after"),
                    value: Bytes::from_static(b"value"),
                },
            ),
            metadata(3, initialization()),
        ])
        .await
        .unwrap();
    assert_eq!(
        results,
        vec![
            ApplyResult::Applied,
            ApplyResult::Metadata(MetadataResult::AlreadyInitialized { revision: 1 }),
            ApplyResult::Applied,
            ApplyResult::Metadata(MetadataResult::AlreadyInitialized { revision: 1 })
        ]
    );
    assert_eq!(
        read(&stores, control::ORIGIN_KEY).await.unwrap().as_ref(),
        framed.as_slice()
    );
    assert_eq!(read(&stores, b"after").await.unwrap().as_ref(), b"value");
}

#[tokio::test]
async fn metadata_is_rejected_on_data_groups_without_writes() {
    let dir = tempfile::tempdir().unwrap();
    let storage = open(
        dir.path().join("db"),
        OpenMode::CreateNew,
        StorageOptions::default(),
    )
    .await;
    let mut stores = storage.open_group(GroupId::new(1)).unwrap();
    let failure = stores
        .state_machine
        .apply([metadata(0, initialization())])
        .await
        .unwrap_err();
    assert!(
        failure
            .to_string()
            .contains("metadata commands require group zero")
    );
    assert!(read(&stores, control::DIRECTORY_KEY).await.is_none());
    assert!(
        stores
            .state_machine
            .applied_state()
            .await
            .unwrap()
            .0
            .is_none()
    );
}

#[tokio::test]
async fn metadata_tiny_input_cannot_bypass_expanded_effect_budget() {
    let dir = tempfile::tempdir().unwrap();
    let options = StorageOptions {
        max_entry_bytes: 512,
        io_chunk_bytes: 2048,
        ..StorageOptions::default()
    };
    let storage = open(dir.path().join("db"), OpenMode::CreateNew, options).await;
    let mut stores = storage.open_group(GroupId::new(0)).unwrap();
    let failure = stores
        .state_machine
        .apply([metadata(0, initialization())])
        .await
        .unwrap_err();
    assert!(failure.to_string().contains("metadata effect reservation"));
    assert!(read(&stores, control::DIRECTORY_KEY).await.is_none());
    assert!(
        stores
            .state_machine
            .applied_state()
            .await
            .unwrap()
            .0
            .is_none()
    );
}

#[tokio::test]
async fn metadata_rejects_foreign_database_identity_before_effects() {
    for hash_mismatch in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let storage = open(
            dir.path().join("db"),
            OpenMode::CreateNew,
            StorageOptions::default(),
        )
        .await;
        let mut stores = storage.open_group(GroupId::new(0)).unwrap();
        let mut foreign = directory();
        if hash_mismatch {
            foreign.shard_config = ShardConfig::new(4, 99).unwrap();
        } else {
            foreign.cluster_id = ClusterId::new([9; 16]).unwrap();
        }
        let failure = stores
            .state_machine
            .apply([metadata(
                0,
                MetadataCommand::InitializeDirectoryIfAbsent {
                    initial_directory: foreign,
                },
            )])
            .await
            .unwrap_err();
        assert!(failure.to_string().contains("storage identity"));
        assert!(read(&stores, control::DIRECTORY_KEY).await.is_none());
        assert!(
            stores
                .state_machine
                .applied_state()
                .await
                .unwrap()
                .0
                .is_none()
        );
    }
}

#[tokio::test]
async fn metadata_rejects_coherent_foreign_persisted_directory() {
    let dir = tempfile::tempdir().unwrap();
    let storage = open(
        dir.path().join("db"),
        OpenMode::CreateNew,
        StorageOptions::default(),
    )
    .await;
    let mut stores = storage.open_group(GroupId::new(0)).unwrap();
    let mut foreign = directory();
    foreign.cluster_id = ClusterId::new([9; 16]).unwrap();
    let value = rockbee_raft_protocol::encode_frame(&foreign).unwrap();
    stores
        .state_machine
        .apply([entry(
            0,
            Command::Put {
                key: Bytes::from_static(control::DIRECTORY_KEY),
                value: Bytes::from(value),
            },
        )])
        .await
        .unwrap();
    let failure = stores
        .state_machine
        .apply([metadata(1, initialization())])
        .await
        .unwrap_err();
    assert!(failure.to_string().contains("storage identity"));
    assert!(read(&stores, control::ORIGIN_KEY).await.is_none());
    assert_eq!(
        stores
            .state_machine
            .applied_state()
            .await
            .unwrap()
            .0
            .unwrap()
            .index,
        0
    );
}

#[tokio::test]
async fn metadata_snapshot_install_and_reopen_retain_completed_and_pending_jobs() {
    let source_dir = tempfile::tempdir().unwrap();
    let source = open(
        source_dir.path().join("db"),
        OpenMode::CreateNew,
        StorageOptions::default(),
    )
    .await;
    let mut stores = source.open_group(GroupId::new(0)).unwrap();
    let completed = movement(1, 1, 4);
    let pending = movement(2, 2, 5);
    let proof = CommittedLogPosition {
        term: 5,
        leader_node_id: NodeId::new(2).unwrap(),
        index: 27,
    };
    stores
        .state_machine
        .apply([
            metadata(0, initialization()),
            metadata(
                1,
                MetadataCommand::BeginMove {
                    intent: completed.clone(),
                },
            ),
            metadata(
                2,
                MetadataCommand::BeginMove {
                    intent: pending.clone(),
                },
            ),
            metadata(
                3,
                MetadataCommand::CompleteMove {
                    expected_directory_revision: 1,
                    data_group: completed.data_group,
                    job_id: completed.job_id,
                    observation: MembershipObservation {
                        uniform: true,
                        voters: completed.target_voters,
                        replacement_endpoint: completed.add_node.clone(),
                        membership: proof,
                    },
                },
            ),
        ])
        .await
        .unwrap();
    let mut image = stores
        .state_machine
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap();
    let target_dir = tempfile::tempdir().unwrap();
    let target_path = target_dir.path().join("db");
    let target = open(
        target_path.clone(),
        OpenMode::CreateNew,
        StorageOptions::default(),
    )
    .await;
    let mut target_group = target.open_group(GroupId::new(0)).unwrap();
    let mut receive = target_group
        .state_machine
        .begin_receiving_snapshot()
        .await
        .unwrap();
    image.snapshot.rewind().await.unwrap();
    tokio::io::copy(&mut image.snapshot, &mut receive)
        .await
        .unwrap();
    receive.flush().await.unwrap();
    target_group
        .state_machine
        .install_snapshot(&image.meta, receive)
        .await
        .unwrap();
    drop(target_group);
    drop(target);
    let reopened = open(
        target_path,
        OpenMode::OpenExisting,
        StorageOptions::default(),
    )
    .await;
    let mut target_group = reopened.open_group(GroupId::new(0)).unwrap();
    for (movement, progress) in [
        (&completed, MoveProgress::Completed { membership: proof }),
        (&pending, MoveProgress::Pending),
    ] {
        let record = target_group
            .state_machine
            .reader()
            .read_local(Bytes::from(control::job_key(movement.data_group)))
            .await
            .unwrap()
            .value
            .unwrap();
        assert_eq!(
            rockbee_raft_protocol::decode_frame::<MoveRecord>(&record).unwrap(),
            MoveRecord {
                intent: movement.clone(),
                progress
            }
        );
    }
    let results = target_group
        .state_machine
        .apply([
            metadata(
                4,
                MetadataCommand::BeginMove {
                    intent: completed.clone(),
                },
            ),
            metadata(
                5,
                MetadataCommand::BeginMove {
                    intent: movement(1, 9, 5),
                },
            ),
            metadata(6, initialization()),
        ])
        .await
        .unwrap();
    assert_eq!(
        results,
        vec![
            ApplyResult::Metadata(MetadataResult::AlreadyAccepted {
                data_group: completed.data_group,
                job_id: completed.job_id
            }),
            ApplyResult::Metadata(MetadataResult::Conflict {
                reason: ConflictReason::ExistingMove
            }),
            ApplyResult::Metadata(MetadataResult::AlreadyInitialized { revision: 2 }),
        ]
    );
}
