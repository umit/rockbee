#![allow(clippy::unwrap_used)]

use super::engine::CF_NAMES;
use super::*;

#[cfg(feature = "diagnostics")]
mod append_failure;
mod handoff_fence;
mod metadata;
use crate::types::{Command, Entry, LogId};
use ::rocksdb::Options;
use bytes::Bytes;
use openraft::{
    EntryPayload,
    storage::{RaftLogStorage, RaftLogStorageExt, RaftStateMachine},
};
use rockbee_raft_protocol::{ClusterId, NodeId, ShardConfig};

pub(in crate::storage::rocksdb) fn identity() -> StorageIdentity {
    StorageIdentity {
        cluster_id: ClusterId::new([2; 16]).unwrap(),
        node_id: NodeId::new(1).unwrap(),
        shard_config: ShardConfig::new(4, 42).unwrap(),
    }
}

fn put(index: u64, value: &'static [u8]) -> Entry {
    Entry {
        log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), index),
        payload: EntryPayload::Normal(Command::Put {
            key: Bytes::from_static(b"key"),
            value: Bytes::from_static(value),
        }),
    }
}

#[tokio::test]
async fn failing_second_write_keeps_only_the_complete_first_apply_chunk() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(
        dir.path().join("db"),
        identity(),
        StorageOptions {
            io_chunk_entries: 2,
            ..StorageOptions::default()
        },
        OpenMode::CreateNew,
    )
    .await
    .unwrap();
    let mut stores = storage.open_group(GroupId::new(1)).unwrap();
    stores
        .state_machine
        .group
        .test_writes
        .lock()
        .unwrap()
        .extend([TestWriteAction::Pass, TestWriteAction::Fail]);
    assert!(
        stores
            .state_machine
            .apply([
                put(0, b"zero"),
                put(1, b"one"),
                put(2, b"two"),
                put(3, b"three")
            ])
            .await
            .is_err()
    );
    let read = stores
        .state_machine
        .reader()
        .read_local(Bytes::from_static(b"key"))
        .await
        .unwrap();
    assert_eq!(read.applied.unwrap().index, 1);
    assert_eq!(read.value, Some(Bytes::from_static(b"one")));
    assert!(
        stores
            .log_store
            .save_vote(&openraft::Vote::new(9, 1))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn cancelled_append_keeps_native_work_owned_and_retires_the_group() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(
        dir.path().join("db"),
        identity(),
        StorageOptions::default(),
        OpenMode::CreateNew,
    )
    .await
    .unwrap();
    let stores = storage.open_group(GroupId::new(1)).unwrap();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    stores
        .log_store
        .group
        .test_writes
        .lock()
        .unwrap()
        .push_back(TestWriteAction::Hold {
            entered: entered_tx,
            resume: resume_rx,
        });
    let mut log = stores.log_store;
    let write = tokio::spawn(async move { log.blocking_append([put(0, b"durable")]).await });
    entered_rx.await.unwrap();
    write.abort();
    assert!(write.await.unwrap_err().is_cancelled());
    let mut state = stores.state_machine;
    // The state read lane and other group writes remain usable while native log work is held.
    assert!(
        state
            .reader()
            .read_local(Bytes::from_static(b"key"))
            .await
            .unwrap()
            .value
            .is_none()
    );
    let mut other = storage.open_group(GroupId::new(2)).unwrap();
    other
        .log_store
        .save_vote(&openraft::Vote::new(1, 1))
        .await
        .unwrap();
    let waiting = tokio::time::timeout(
        std::time::Duration::from_millis(30),
        state.apply([put(1, b"late")]),
    )
    .await;
    assert!(
        waiting.is_err(),
        "write gate was released before native completion"
    );
    drop(state);
    assert!(storage.open_group(GroupId::new(1)).is_err());
    resume_tx.send(()).unwrap();
    let reopened = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if let Ok(stores) = storage.open_group(GroupId::new(1)) {
                break stores;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut log = reopened.log_store;
    assert_eq!(
        log.get_log_state()
            .await
            .unwrap()
            .last_log_id
            .unwrap()
            .index,
        0
    );
}

#[tokio::test]
async fn failed_native_append_never_completes_successfully() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut storage = Storage::open(
        path.clone(),
        identity(),
        StorageOptions::default(),
        OpenMode::CreateNew,
    )
    .await
    .unwrap();
    let readonly = Db::open_cf_for_read_only(&Options::default(), &path, CF_NAMES, false).unwrap();
    Arc::get_mut(&mut storage.shared).unwrap().db = Arc::new(readonly);
    let mut stores = storage.open_group(GroupId::new(1)).unwrap();
    let error = stores
        .log_store
        .blocking_append([put(0, b"no")])
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("read only") || error.to_string().contains("read-only"),
        "{error}"
    );
    assert!(
        stores
            .log_store
            .get_log_state()
            .await
            .unwrap()
            .last_log_id
            .is_none()
    );
}

/// A native vote write failure retires the affected in-memory group's writes.
/// Its durable vote/data and the sibling's durable state remain recoverable;
/// a shared read-only DB does not prove sibling write availability.
#[tokio::test]
async fn failed_native_vote_write_retires_group_and_preserves_durable_groups() {
    let _ = env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("rockbee_raft_node::storage=debug"),
    )
    .is_test(true)
    .try_init();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut storage = Storage::open(
        path.clone(),
        identity(),
        StorageOptions::default(),
        OpenMode::CreateNew,
    )
    .await
    .unwrap();
    let group_one = GroupId::new(1);
    let group_two = GroupId::new(2);
    let vote_zero = openraft::Vote::new(1, 1);
    let sibling_vote = openraft::Vote::new(2, 1);

    let mut first = storage.open_group(group_one).unwrap();
    first.log_store.save_vote(&vote_zero).await.unwrap();
    first
        .state_machine
        .apply([put(1, b"group-one")])
        .await
        .unwrap();
    let first_applied = first.state_machine.applied_state().await.unwrap().0;
    let first_value = first
        .state_machine
        .reader()
        .read_local(Bytes::from_static(b"key"))
        .await
        .unwrap()
        .value;
    assert_eq!(
        first_applied,
        Some(LogId::new(openraft::CommittedLeaderId::new(1, 1), 1))
    );
    assert_eq!(first_value, Some(Bytes::from_static(b"group-one")));

    let mut sibling = storage.open_group(group_two).unwrap();
    sibling.log_store.save_vote(&sibling_vote).await.unwrap();
    sibling
        .state_machine
        .apply([put(2, b"group-two")])
        .await
        .unwrap();
    let sibling_applied = sibling.state_machine.applied_state().await.unwrap().0;
    let sibling_value = sibling
        .state_machine
        .reader()
        .read_local(Bytes::from_static(b"key"))
        .await
        .unwrap()
        .value;
    assert_eq!(
        sibling_applied,
        Some(LogId::new(openraft::CommittedLeaderId::new(1, 1), 2))
    );
    assert_eq!(sibling_value, Some(Bytes::from_static(b"group-two")));

    // No group owner may retain Shared while the fixture swaps the one DB
    // handle for RocksDB's real read-only handle.
    drop(first);
    drop(sibling);
    let readonly = Db::open_cf_for_read_only(&Options::default(), &path, CF_NAMES, false).unwrap();
    Arc::get_mut(&mut storage.shared)
        .expect("dropped groups leave the shared DB uniquely owned")
        .db = Arc::new(readonly);

    let mut failed = storage.open_group(group_one).unwrap();
    let error = failed
        .log_store
        .save_vote(&openraft::Vote::new(2, 1))
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("read only") || error.to_string().contains("read-only"),
        "{error}"
    );
    let retired = failed
        .log_store
        .save_vote(&openraft::Vote::new(3, 1))
        .await
        .unwrap_err();
    assert!(
        retired
            .to_string()
            .contains("group write admission retired"),
        "{retired}"
    );

    drop(failed);
    drop(storage);
    let storage = Storage::open(
        path,
        identity(),
        StorageOptions::default(),
        OpenMode::OpenExisting,
    )
    .await
    .unwrap();
    let mut first = storage.open_group(group_one).unwrap();
    let mut sibling = storage.open_group(group_two).unwrap();
    assert_eq!(first.log_store.read_vote().await.unwrap(), Some(vote_zero));
    assert_eq!(
        first.state_machine.applied_state().await.unwrap().0,
        first_applied
    );
    assert_eq!(
        first
            .state_machine
            .reader()
            .read_local(Bytes::from_static(b"key"))
            .await
            .unwrap()
            .value,
        first_value
    );
    assert_eq!(
        sibling.log_store.read_vote().await.unwrap(),
        Some(sibling_vote)
    );
    assert_eq!(
        sibling.state_machine.applied_state().await.unwrap().0,
        sibling_applied
    );
    assert_eq!(
        sibling
            .state_machine
            .reader()
            .read_local(Bytes::from_static(b"key"))
            .await
            .unwrap()
            .value,
        sibling_value
    );
    ::log::info!(
        "raft_vote_native_failure_recovery old_vote={vote_zero:?} sibling_vote={sibling_vote:?} group_applied={first_applied:?} sibling_applied={sibling_applied:?} values_preserved=true"
    );
}

#[tokio::test]
async fn failed_membership_chunk_preserves_data_applied_and_membership_together_after_reopen() {
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
    let mut stores = storage.open_group(GroupId::new(1)).unwrap();
    let membership = |index, ids| Entry {
        log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), index),
        payload: EntryPayload::Membership(openraft::Membership::new(
            vec![std::collections::BTreeSet::from(ids)],
            None,
        )),
    };
    stores
        .state_machine
        .group
        .test_writes
        .lock()
        .unwrap()
        .extend([TestWriteAction::Pass, TestWriteAction::Fail]);
    assert!(
        stores
            .state_machine
            .apply([
                put(0, b"before"),
                membership(1, [1, 2, 3]),
                put(2, b"after"),
                membership(3, [4, 5, 6]),
            ])
            .await
            .is_err()
    );
    drop(stores);
    drop(storage);
    let storage = Storage::open(path, identity(), options, OpenMode::OpenExisting)
        .await
        .unwrap();
    let mut stores = storage.open_group(GroupId::new(1)).unwrap();
    let read = stores
        .state_machine
        .reader()
        .read_local(Bytes::from_static(b"key"))
        .await
        .unwrap();
    assert_eq!(read.applied.unwrap().index, 1);
    assert_eq!(read.value, Some(Bytes::from_static(b"before")));
    let (_, membership) = stores.state_machine.applied_state().await.unwrap();
    assert_eq!(membership.log_id().unwrap().index, 1);
    assert_eq!(
        membership.membership().voter_ids().collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
}

#[tokio::test]
async fn concurrent_purge_and_log_state_read_never_mix_marker_and_tail() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(
        dir.path().join("db"),
        identity(),
        StorageOptions::default(),
        OpenMode::CreateNew,
    )
    .await
    .unwrap();
    let mut stores = storage.open_group(GroupId::new(1)).unwrap();
    stores
        .log_store
        .blocking_append((0..100).map(|i| put(i, b"value")))
        .await
        .unwrap();
    // This private test-only handle exercises the actual read capability while
    // the sole real writer owns purge. No second writer is exposed publicly.
    let mut read_handle = LogStore {
        group: stores.log_store.group.clone(),
    };
    let writer = tokio::spawn(async move {
        for i in 0..100 {
            stores
                .log_store
                .purge(LogId::new(openraft::CommittedLeaderId::new(1, 1), i))
                .await
                .unwrap();
        }
    });
    for _ in 0..200 {
        let state = read_handle.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id.unwrap().index, 99);
        assert!(
            state
                .last_purged_log_id
                .is_none_or(|purged| purged.index <= 99)
        );
    }
    writer.await.unwrap();
    assert_eq!(
        read_handle
            .get_log_state()
            .await
            .unwrap()
            .last_purged_log_id
            .unwrap()
            .index,
        99
    );
}
