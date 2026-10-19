#![allow(clippy::unwrap_used)]
mod support;

#[tokio::test]
async fn snapshot_stream_round_trip_preserves_binary_values_and_reopens() {
    use tokio::io::{AsyncSeekExt, AsyncWriteExt};
    let source_dir = tempfile::tempdir().unwrap();
    let target_dir = tempfile::tempdir().unwrap();
    let source = Storage::open(
        source_dir.path().join("db"),
        identity(),
        StorageOptions::default(),
        OpenMode::CreateNew,
    )
    .await
    .unwrap();
    let target_path = target_dir.path().join("db");
    let mut target_identity = identity();
    target_identity.node_id = NodeId::new(2).unwrap();
    let target = Storage::open(
        target_path.clone(),
        target_identity.clone(),
        StorageOptions::default(),
        OpenMode::CreateNew,
    )
    .await
    .unwrap();
    let mut from = source.open_group(GroupId::new(1)).unwrap();
    let mut to = target.open_group(GroupId::new(1)).unwrap();
    let key = Bytes::from_static(b"\0\xffbinary");
    from.state_machine
        .apply([
            Entry {
                log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), 0),
                payload: EntryPayload::Membership(openraft::Membership::new(
                    vec![std::collections::BTreeSet::from([0, 2])],
                    std::collections::BTreeMap::from([
                        (0, openraft::BasicNode::new("source:1234")),
                        (2, openraft::BasicNode::new("target:5678")),
                    ]),
                )),
            },
            entry(
                1,
                Command::Put {
                    key: key.clone(),
                    value: Bytes::new(),
                },
            ),
            entry(
                2,
                Command::Put {
                    key: Bytes::from_static(b"deleted"),
                    value: Bytes::from_static(b"old"),
                },
            ),
            entry(
                3,
                Command::Delete {
                    key: Bytes::from_static(b"deleted"),
                },
            ),
        ])
        .await
        .unwrap();
    let mut image = from
        .state_machine
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap();
    assert_eq!(image.snapshot.stream_position().await.unwrap(), 0);
    let mut received = to.state_machine.begin_receiving_snapshot().await.unwrap();
    tokio::io::copy(&mut image.snapshot, &mut received)
        .await
        .unwrap();
    received.flush().await.unwrap();
    to.state_machine
        .install_snapshot(&image.meta, received)
        .await
        .unwrap();
    assert_eq!(
        to.state_machine
            .reader()
            .read_local(key.clone())
            .await
            .unwrap()
            .value,
        Some(Bytes::new())
    );
    assert_eq!(
        to.state_machine
            .reader()
            .read_local(Bytes::from_static(b"deleted"))
            .await
            .unwrap()
            .value,
        None
    );
    assert_eq!(
        to.state_machine.applied_state().await.unwrap(),
        (image.meta.last_log_id, image.meta.last_membership.clone())
    );
    drop(to);
    drop(target);
    let target = Storage::open(
        target_path,
        target_identity,
        StorageOptions::default(),
        OpenMode::OpenExisting,
    )
    .await
    .unwrap();
    let mut to = target.open_group(GroupId::new(1)).unwrap();
    let current = to
        .state_machine
        .get_current_snapshot()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.meta, image.meta);
    assert_eq!(
        to.state_machine
            .reader()
            .read_local(key)
            .await
            .unwrap()
            .value,
        Some(Bytes::new())
    );
}

use bytes::Bytes;
use openraft::{
    EntryPayload, RaftSnapshotBuilder,
    storage::{RaftLogStorage, RaftStateMachine},
};
use rockbee_raft_node::{
    storage::{OpenMode, Storage, StorageIdentity, StorageOptions},
    types::{Command, Entry, LogId},
};
use rockbee_raft_protocol::{ClusterId, GroupId, NodeId, ShardConfig};

#[test]
fn complete_upstream_storage_suite() {
    openraft::testing::Suite::<
        rockbee_raft_node::types::TypeConfig,
        rockbee_raft_node::storage::LogStore,
        rockbee_raft_node::storage::StateMachine,
        support::Builder,
        tempfile::TempDir,
    >::test_all(support::Builder)
    .unwrap();
}

macro_rules! upstream_case {
    ($name:ident) => {
        #[tokio::test]
        async fn $name() {
            use openraft::testing::StoreBuilder;
            type Suite = openraft::testing::Suite<
                rockbee_raft_node::types::TypeConfig,
                rockbee_raft_node::storage::LogStore,
                rockbee_raft_node::storage::StateMachine,
                support::Builder,
                tempfile::TempDir,
            >;
            let (_guard, log, state) = support::Builder.build().await.unwrap();
            Suite::$name(log, state).await.unwrap();
        }
    };
}
upstream_case!(save_vote);
upstream_case!(initial_logs);
upstream_case!(get_initial_state_membership_from_log_and_sm);
upstream_case!(get_log_entries);
upstream_case!(limited_get_log_entries);
upstream_case!(get_log_state);
upstream_case!(last_applied_state);
upstream_case!(purge_logs_upto_0);
upstream_case!(purge_logs_upto_5);
upstream_case!(purge_logs_upto_20);
upstream_case!(delete_logs_since_0);
upstream_case!(delete_logs_since_11);
upstream_case!(append_to_log);
upstream_case!(apply_single);
upstream_case!(apply_multiple);
#[tokio::test]
async fn get_initial_state_re_apply_committed() {
    use openraft::testing::StoreBuilder;
    type Suite = openraft::testing::Suite<
        rockbee_raft_node::types::TypeConfig,
        rockbee_raft_node::storage::LogStore,
        rockbee_raft_node::storage::StateMachine,
        support::Builder,
        tempfile::TempDir,
    >;
    let (_guard, log, state) = support::Builder.build().await.unwrap();
    Suite::get_initial_state_re_apply_committed(log, state)
        .await
        .unwrap();
}

#[tokio::test]
async fn committed_unapplied_logs_recover_through_real_storage_helper() {
    use openraft::storage::{RaftLogStorageExt, StorageHelper};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let storage = Storage::open(
        path.clone(),
        identity(),
        StorageOptions::default(),
        OpenMode::CreateNew,
    )
    .await
    .unwrap();
    let mut group = storage.open_group(GroupId::new(1)).unwrap();
    group
        .log_store
        .save_vote(&openraft::Vote::new(1, 1))
        .await
        .unwrap();
    group
        .log_store
        .blocking_append((0..4).map(|i| {
            entry(
                i,
                Command::Put {
                    key: Bytes::from_static(b"recover"),
                    value: Bytes::from(i.to_string()),
                },
            )
        }))
        .await
        .unwrap();
    group
        .state_machine
        .apply([entry(
            0,
            Command::Put {
                key: Bytes::from_static(b"recover"),
                value: Bytes::from_static(b"0"),
            },
        )])
        .await
        .unwrap();
    group
        .log_store
        .save_committed(Some(LogId::new(openraft::CommittedLeaderId::new(1, 1), 2)))
        .await
        .unwrap();
    drop(group);
    drop(storage);
    let storage = Storage::open(
        path,
        identity(),
        StorageOptions::default(),
        OpenMode::OpenExisting,
    )
    .await
    .unwrap();
    let mut group = storage.open_group(GroupId::new(1)).unwrap();
    StorageHelper::new(&mut group.log_store, &mut group.state_machine)
        .get_initial_state()
        .await
        .unwrap();
    let read = group
        .state_machine
        .reader()
        .read_local(Bytes::from_static(b"recover"))
        .await
        .unwrap();
    assert_eq!(read.applied.unwrap().index, 2);
    assert_eq!(read.value, Some(Bytes::from_static(b"2")));
}

#[tokio::test]
async fn bounded_append_apply_and_max_index_ranges_are_group_isolated() {
    use openraft::{RaftLogReader, storage::RaftLogStorageExt};
    let dir = tempfile::tempdir().unwrap();
    let options = StorageOptions {
        io_chunk_entries: 2,
        ..StorageOptions::default()
    };
    let storage = Storage::open(
        dir.path().join("db"),
        identity(),
        options,
        OpenMode::CreateNew,
    )
    .await
    .unwrap();
    let mut group = storage.open_group(GroupId::new(u64::MAX)).unwrap();
    let mut other = storage.open_group(GroupId::new(2)).unwrap();
    let first = u64::MAX - 5;
    group
        .log_store
        .blocking_append((first..=u64::MAX).map(|i| Entry {
            log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), i),
            payload: EntryPayload::Blank,
        }))
        .await
        .unwrap();
    assert_eq!(
        group.log_store.try_get_log_entries(..).await.unwrap().len(),
        6
    );
    assert_eq!(
        group
            .log_store
            .try_get_log_entries(u64::MAX..=u64::MAX)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        group
            .log_store
            .try_get_log_entries((
                std::ops::Bound::Excluded(u64::MAX),
                std::ops::Bound::Unbounded
            ))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        group
            .log_store
            .limited_get_log_entries(first, u64::MAX)
            .await
            .unwrap()
            .len(),
        2
    );
    assert!(
        other
            .log_store
            .try_get_log_entries(..)
            .await
            .unwrap()
            .is_empty()
    );
    group
        .log_store
        .purge(LogId::new(openraft::CommittedLeaderId::new(1, 1), u64::MAX))
        .await
        .unwrap();
    assert_eq!(
        group
            .log_store
            .get_log_state()
            .await
            .unwrap()
            .last_log_id
            .unwrap()
            .index,
        u64::MAX
    );
    assert!(
        group
            .log_store
            .try_get_log_entries(..)
            .await
            .unwrap()
            .is_empty()
    );
    let responses = other
        .state_machine
        .apply((0..301).map(|i| {
            entry(
                i,
                Command::Put {
                    key: Bytes::from_static(b"long"),
                    value: Bytes::from(i.to_string()),
                },
            )
        }))
        .await
        .unwrap();
    assert_eq!(responses.len(), 301);
    let read = other
        .state_machine
        .reader()
        .read_local(Bytes::from_static(b"long"))
        .await
        .unwrap();
    assert_eq!(read.applied.unwrap().index, 300);
    assert_eq!(read.value, Some(Bytes::from_static(b"300")));
}

fn identity() -> StorageIdentity {
    StorageIdentity {
        cluster_id: ClusterId::new([1; 16]).unwrap(),
        node_id: NodeId::new(1).unwrap(),
        shard_config: ShardConfig::new(4, 42).unwrap(),
    }
}

fn entry(index: u64, command: Command) -> Entry {
    Entry {
        log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), index),
        payload: EntryPayload::Normal(command),
    }
}

#[tokio::test]
async fn sync_apply_survives_reopen_with_empty_values_and_deleted_keys() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let storage = Storage::open(
        path.clone(),
        identity(),
        StorageOptions::default(),
        OpenMode::CreateNew,
    )
    .await
    .unwrap();
    let mut group = storage.open_group(GroupId::new(1)).unwrap();
    let entries = vec![
        entry(
            0,
            Command::Put {
                key: Bytes::from_static(b"k"),
                value: Bytes::from_static(b"old"),
            },
        ),
        entry(
            1,
            Command::Put {
                key: Bytes::from_static(b"k"),
                value: Bytes::from_static(b"new"),
            },
        ),
        entry(
            2,
            Command::Put {
                key: Bytes::from_static(b"empty"),
                value: Bytes::new(),
            },
        ),
        entry(
            3,
            Command::Delete {
                key: Bytes::from_static(b"k"),
            },
        ),
    ];
    assert_eq!(group.state_machine.apply(entries).await.unwrap().len(), 4);
    drop(group);
    drop(storage);
    let storage = Storage::open(
        path,
        identity(),
        StorageOptions::default(),
        OpenMode::OpenExisting,
    )
    .await
    .unwrap();
    let group = storage.open_group(GroupId::new(1)).unwrap();
    let reader = group.state_machine.reader();
    let empty = reader
        .read_local(Bytes::from_static(b"empty"))
        .await
        .unwrap();
    assert_eq!(empty.value, Some(Bytes::new()));
    assert_eq!(empty.applied.unwrap().index, 3);
    assert_eq!(
        reader
            .read_local(Bytes::from_static(b"k"))
            .await
            .unwrap()
            .value,
        None
    );
}

#[tokio::test]
async fn open_checks_identity_and_ownership() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    assert!(
        Storage::open(
            path.clone(),
            identity(),
            StorageOptions::default(),
            OpenMode::OpenExisting
        )
        .await
        .is_err()
    );
    let storage = Storage::open(
        path.clone(),
        identity(),
        StorageOptions::default(),
        OpenMode::CreateNew,
    )
    .await
    .unwrap();
    assert!(
        Storage::open(
            path.clone(),
            identity(),
            StorageOptions::default(),
            OpenMode::CreateNew
        )
        .await
        .is_err()
    );
    assert!(
        Storage::open(
            path.clone(),
            identity(),
            StorageOptions::default(),
            OpenMode::OpenExisting
        )
        .await
        .is_err()
    );
    let group = storage.open_group(GroupId::new(1)).unwrap();
    let reader = group.state_machine.reader();
    assert!(storage.open_group(GroupId::new(1)).is_err());
    drop(group);
    assert!(storage.open_group(GroupId::new(1)).is_err());
    drop(reader);
    drop(storage.open_group(GroupId::new(1)).unwrap());
    drop(storage);
    let mut changed = identity();
    changed.node_id = NodeId::new(2).unwrap();
    assert!(
        Storage::open(
            path,
            changed,
            StorageOptions::default(),
            OpenMode::OpenExisting
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn invalid_options_do_not_create_a_database() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    for options in [
        StorageOptions {
            snapshot_io_slots: 0,
            ..StorageOptions::default()
        },
        StorageOptions {
            log_io_slots: 0,
            ..StorageOptions::default()
        },
        StorageOptions {
            state_io_slots: usize::MAX,
            ..StorageOptions::default()
        },
        StorageOptions {
            io_chunk_entries: 0,
            ..StorageOptions::default()
        },
        StorageOptions {
            io_chunk_bytes: 1,
            ..StorageOptions::default()
        },
        StorageOptions {
            max_entry_bytes: usize::MAX,
            ..StorageOptions::default()
        },
    ] {
        assert!(
            Storage::open(path.clone(), identity(), options, OpenMode::CreateNew)
                .await
                .is_err()
        );
        assert!(!path.exists());
    }
}

#[tokio::test]
async fn opening_wrong_cf_layout_or_nonempty_directory_fails() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    std::fs::create_dir(&path).unwrap();
    std::fs::write(path.join("evidence"), b"keep").unwrap();
    assert!(
        Storage::open(
            path.clone(),
            identity(),
            StorageOptions::default(),
            OpenMode::CreateNew
        )
        .await
        .is_err()
    );
    assert_eq!(std::fs::read(path.join("evidence")).unwrap(), b"keep");
    let wrong = dir.path().join("wrong-cf");
    drop(rocksdb::DB::open_default(&wrong).unwrap());
    assert!(
        Storage::open(
            wrong,
            identity(),
            StorageOptions::default(),
            OpenMode::OpenExisting
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn invalid_later_chunk_preserves_a_durable_prefix_and_retires_writes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let options = StorageOptions {
        io_chunk_entries: 2,
        ..StorageOptions::default()
    };
    let storage = Storage::open(
        path.clone(),
        identity(),
        options.clone(),
        OpenMode::CreateNew,
    )
    .await
    .unwrap();
    let mut group = storage.open_group(GroupId::new(1)).unwrap();
    let entries = [
        entry(
            0,
            Command::Put {
                key: Bytes::from_static(b"k"),
                value: Bytes::from_static(b"first"),
            },
        ),
        entry(
            1,
            Command::Put {
                key: Bytes::from_static(b"k"),
                value: Bytes::from_static(b"second"),
            },
        ),
        entry(
            2,
            Command::Put {
                key: Bytes::from_static(b"uncommitted"),
                value: Bytes::from_static(b"no"),
            },
        ),
        entry(
            3,
            Command::Put {
                key: Bytes::new(),
                value: Bytes::from_static(b"invalid"),
            },
        ),
    ];
    assert!(group.state_machine.apply(entries).await.is_err());
    assert!(
        group
            .log_store
            .save_vote(&openraft::Vote::new(2, 1))
            .await
            .is_err()
    );
    let read = group
        .state_machine
        .reader()
        .read_local(Bytes::from_static(b"k"))
        .await
        .unwrap();
    assert_eq!(read.applied.unwrap().index, 1);
    assert_eq!(read.value, Some(Bytes::from_static(b"second")));
    assert!(
        group
            .state_machine
            .reader()
            .read_local(Bytes::from_static(b"uncommitted"))
            .await
            .unwrap()
            .value
            .is_none()
    );
    drop(group);
    drop(storage);
    let storage = Storage::open(path, identity(), options, OpenMode::OpenExisting)
        .await
        .unwrap();
    let group = storage.open_group(GroupId::new(1)).unwrap();
    let read = group
        .state_machine
        .reader()
        .read_local(Bytes::from_static(b"k"))
        .await
        .unwrap();
    assert_eq!(read.applied.unwrap().index, 1);
    assert_eq!(read.value, Some(Bytes::from_static(b"second")));
}

#[tokio::test]
async fn vote_committed_and_empty_snapshot_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let storage = Storage::open(
        path.clone(),
        identity(),
        StorageOptions::default(),
        OpenMode::CreateNew,
    )
    .await
    .unwrap();
    let mut group = storage.open_group(GroupId::new(1)).unwrap();
    let vote = openraft::Vote::new(2, 1);
    let committed = LogId::new(openraft::CommittedLeaderId::new(2, 1), 5);
    group.log_store.save_vote(&vote).await.unwrap();
    group
        .log_store
        .save_committed(Some(committed))
        .await
        .unwrap();
    let snapshot = group
        .state_machine
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap();
    assert_eq!(snapshot.meta.last_log_id, None);
    let received = group
        .state_machine
        .begin_receiving_snapshot()
        .await
        .unwrap();
    drop(received);
    let current = group
        .state_machine
        .get_current_snapshot()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.meta, snapshot.meta);
    drop(group);
    drop(storage);
    let storage = Storage::open(
        path,
        identity(),
        StorageOptions::default(),
        OpenMode::OpenExisting,
    )
    .await
    .unwrap();
    let mut group = storage.open_group(GroupId::new(1)).unwrap();
    assert_eq!(group.log_store.read_vote().await.unwrap(), Some(vote));
    assert_eq!(
        group.log_store.read_committed().await.unwrap(),
        Some(committed)
    );
}

#[tokio::test]
async fn membership_and_blank_progress_recover_separately() {
    use std::collections::{BTreeMap, BTreeSet};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let storage = Storage::open(
        path.clone(),
        identity(),
        StorageOptions::default(),
        OpenMode::CreateNew,
    )
    .await
    .unwrap();
    let mut group = storage.open_group(GroupId::new(1)).unwrap();
    let membership = openraft::Membership::new(
        vec![BTreeSet::from([1, 2, 3])],
        BTreeMap::from([
            (1, openraft::BasicNode::new("127.0.0.1:9001")),
            (2, openraft::BasicNode::new("127.0.0.1:9002")),
            (3, openraft::BasicNode::new("127.0.0.1:9003")),
        ]),
    );
    let membership_id = LogId::new(openraft::CommittedLeaderId::new(1, 1), 0);
    let blank_id = LogId::new(openraft::CommittedLeaderId::new(1, 1), 1);
    assert_eq!(
        group
            .state_machine
            .apply([
                Entry {
                    log_id: membership_id,
                    payload: EntryPayload::Membership(membership.clone())
                },
                Entry {
                    log_id: blank_id,
                    payload: EntryPayload::Blank
                },
            ])
            .await
            .unwrap()
            .len(),
        2
    );
    drop(group);
    drop(storage);
    let storage = Storage::open(
        path,
        identity(),
        StorageOptions::default(),
        OpenMode::OpenExisting,
    )
    .await
    .unwrap();
    let mut group = storage.open_group(GroupId::new(1)).unwrap();
    let (applied, stored) = group.state_machine.applied_state().await.unwrap();
    assert_eq!(applied, Some(blank_id));
    assert_eq!(stored.log_id(), &Some(membership_id));
    assert_eq!(stored.membership(), &membership);
    assert!(
        group
            .log_store
            .get_log_state()
            .await
            .unwrap()
            .last_log_id
            .is_none()
    );
}

#[tokio::test]
async fn corrupt_identity_or_orphan_state_is_never_reported_as_fresh() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    drop(
        Storage::open(
            path.clone(),
            identity(),
            StorageOptions::default(),
            OpenMode::CreateNew,
        )
        .await
        .unwrap(),
    );
    {
        let db = rocksdb::DB::open_cf(
            &rocksdb::Options::default(),
            &path,
            ["default", "raft_log", "raft_meta", "state", "snapshot_meta"],
        )
        .unwrap();
        let mut key = 1u64.to_be_bytes().to_vec();
        key.push(1);
        key.extend_from_slice(&0u64.to_be_bytes());
        key.extend_from_slice(b"orphan");
        db.put_cf(db.cf_handle("state").unwrap(), key, b"value")
            .unwrap();
    }
    let storage = Storage::open(
        path.clone(),
        identity(),
        StorageOptions::default(),
        OpenMode::OpenExisting,
    )
    .await
    .unwrap();
    let group = storage.open_group(GroupId::new(1)).unwrap();
    assert!(
        group
            .state_machine
            .reader()
            .read_local(Bytes::from_static(b"orphan"))
            .await
            .is_err()
    );
    drop(group);
    drop(storage);
    {
        let db = rocksdb::DB::open_cf(
            &rocksdb::Options::default(),
            &path,
            ["default", "raft_log", "raft_meta", "state", "snapshot_meta"],
        )
        .unwrap();
        let mut key = vec![0u8; 8];
        key.push(0x80);
        let cf = db.cf_handle("raft_meta").unwrap();
        let mut value = db.get_cf(cf, &key).unwrap().unwrap();
        value[0] = 99;
        db.put_cf(cf, key, value).unwrap();
    }
    assert!(
        Storage::open(
            path,
            identity(),
            StorageOptions::default(),
            OpenMode::OpenExisting
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn unknown_snapshot_metadata_is_an_error_not_absence() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    drop(
        Storage::open(
            path.clone(),
            identity(),
            StorageOptions::default(),
            OpenMode::CreateNew,
        )
        .await
        .unwrap(),
    );
    {
        let db = rocksdb::DB::open_cf(
            &rocksdb::Options::default(),
            &path,
            ["default", "raft_log", "raft_meta", "state", "snapshot_meta"],
        )
        .unwrap();
        let mut key = 1u64.to_be_bytes().to_vec();
        key.push(0);
        db.put_cf(
            db.cf_handle("snapshot_meta").unwrap(),
            key,
            b"future-format",
        )
        .unwrap();
    }
    assert!(
        Storage::open(
            path,
            identity(),
            StorageOptions::default(),
            OpenMode::OpenExisting
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn append_truncate_and_purge_recover_full_log_identity() {
    use openraft::{RaftLogReader, storage::RaftLogStorageExt};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let storage = Storage::open(
        path.clone(),
        identity(),
        StorageOptions::default(),
        OpenMode::CreateNew,
    )
    .await
    .unwrap();
    let mut group = storage.open_group(GroupId::new(1)).unwrap();
    group
        .log_store
        .blocking_append((0..20).map(|i| {
            entry(
                i,
                Command::Delete {
                    key: Bytes::from_static(b"key"),
                },
            )
        }))
        .await
        .unwrap();
    let purged = LogId::new(openraft::CommittedLeaderId::new(1, 1), 7);
    group.log_store.purge(purged).await.unwrap();
    group
        .log_store
        .truncate(LogId::new(openraft::CommittedLeaderId::new(1, 1), 18))
        .await
        .unwrap();
    drop(group);
    drop(storage);
    let storage = Storage::open(
        path,
        identity(),
        StorageOptions::default(),
        OpenMode::OpenExisting,
    )
    .await
    .unwrap();
    let mut group = storage.open_group(GroupId::new(1)).unwrap();
    let state = group.log_store.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(purged));
    assert_eq!(
        state.last_log_id,
        Some(LogId::new(openraft::CommittedLeaderId::new(1, 1), 17))
    );
    let entries = group.log_store.try_get_log_entries(..).await.unwrap();
    assert_eq!(entries.len(), 10);
    assert_eq!(entries.first().unwrap().log_id.index, 8);
    assert_eq!(entries.last().unwrap().log_id.index, 17);
}

#[tokio::test]
async fn corrupt_log_bytes_are_errors_for_both_tail_and_range_reads() {
    use openraft::RaftLogReader;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    drop(
        Storage::open(
            path.clone(),
            identity(),
            StorageOptions::default(),
            OpenMode::CreateNew,
        )
        .await
        .unwrap(),
    );
    {
        let db = rocksdb::DB::open_cf(
            &rocksdb::Options::default(),
            &path,
            ["default", "raft_log", "raft_meta", "state", "snapshot_meta"],
        )
        .unwrap();
        let mut key = 1u64.to_be_bytes().to_vec();
        key.extend_from_slice(&0u64.to_be_bytes());
        db.put_cf(db.cf_handle("raft_log").unwrap(), key, b"corrupt-entry")
            .unwrap();
    }
    let storage = Storage::open(
        path,
        identity(),
        StorageOptions::default(),
        OpenMode::OpenExisting,
    )
    .await
    .unwrap();
    let mut group = storage.open_group(GroupId::new(1)).unwrap();
    assert!(group.log_store.get_log_state().await.is_err());
    assert!(group.log_store.try_get_log_entries(..).await.is_err());
}

#[tokio::test]
async fn chunk_budget_without_room_for_metadata_is_rejected_before_open() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let options = StorageOptions {
        max_entry_bytes: 1024,
        io_chunk_bytes: 1024,
        ..StorageOptions::default()
    };
    assert!(
        Storage::open(path.clone(), identity(), options, OpenMode::CreateNew)
            .await
            .is_err()
    );
    assert!(!path.exists());
}

#[tokio::test]
async fn replacing_large_membership_does_not_charge_the_old_record_as_a_write() {
    use std::collections::{BTreeMap, BTreeSet};
    let dir = tempfile::tempdir().unwrap();
    let options = StorageOptions::default();
    let node_address = "x".repeat(options.max_entry_bytes - 64);
    let storage = Storage::open(
        dir.path().join("db"),
        identity(),
        options,
        OpenMode::CreateNew,
    )
    .await
    .unwrap();
    let mut group = storage.open_group(GroupId::new(1)).unwrap();
    for index in 0..2 {
        let membership = openraft::Membership::new(
            vec![BTreeSet::from([1])],
            BTreeMap::from([(1, openraft::BasicNode::new(&node_address))]),
        );
        let entry = Entry {
            log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), index),
            payload: EntryPayload::Membership(membership),
        };
        assert_eq!(group.state_machine.apply([entry]).await.unwrap().len(), 1);
    }
    assert_eq!(
        group
            .state_machine
            .applied_state()
            .await
            .unwrap()
            .0
            .unwrap()
            .index,
        1
    );
}

#[tokio::test]
async fn stale_append_cannot_resurrect_purged_prefix_and_live_holes_still_fail() {
    use openraft::{RaftLogReader, storage::RaftLogStorageExt};
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(
        dir.path().join("db"),
        identity(),
        StorageOptions {
            io_chunk_entries: 1,
            ..StorageOptions::default()
        },
        OpenMode::CreateNew,
    )
    .await
    .unwrap();
    let mut group = storage.open_group(GroupId::new(1)).unwrap();
    let item = |i| {
        entry(
            i,
            Command::Put {
                key: Bytes::from_static(b"key"),
                value: Bytes::new(),
            },
        )
    };
    group
        .log_store
        .blocking_append((0..=2).map(item))
        .await
        .unwrap();
    group.log_store.purge(item(2).log_id).await.unwrap();
    group.log_store.blocking_append([item(1)]).await.unwrap();
    assert!(
        group
            .log_store
            .try_get_log_entries(..)
            .await
            .unwrap()
            .is_empty()
    );
    group
        .log_store
        .blocking_append([item(2), item(3)])
        .await
        .unwrap();
    let logs = group.log_store.try_get_log_entries(..).await.unwrap();
    assert_eq!(logs.iter().map(|e| e.log_id.index).collect::<Vec<_>>(), [3]);
    assert_eq!(
        group
            .log_store
            .get_log_state()
            .await
            .unwrap()
            .last_purged_log_id,
        Some(item(2).log_id)
    );
    assert!(group.log_store.blocking_append([item(5)]).await.is_err());
    let mut other = storage.open_group(GroupId::new(2)).unwrap();
    other
        .log_store
        .blocking_append((0..=2).map(item))
        .await
        .unwrap();
    other.log_store.purge(item(2).log_id).await.unwrap();
    let mut conflict = item(2);
    conflict.log_id = LogId::new(openraft::CommittedLeaderId::new(9, 1), 2);
    assert!(other.log_store.blocking_append([conflict]).await.is_err());
    assert!(
        other
            .log_store
            .try_get_log_entries(..)
            .await
            .unwrap()
            .is_empty()
    );
}
