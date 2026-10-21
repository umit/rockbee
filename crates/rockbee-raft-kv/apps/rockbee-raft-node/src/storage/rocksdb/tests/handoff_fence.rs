//! Adapter tests: applying a supplied fence is not proof of Raft commitment.
#![allow(clippy::unwrap_used)]

use crate::storage::rocksdb::{tests::identity, *};
use crate::types::{ApplyResult, Command, Entry, LogId, StoredMembership};
use bytes::Bytes;
use openraft::{
    EntryPayload,
    storage::{RaftSnapshotBuilder, RaftStateMachine},
};
use rockbee_raft_protocol::control::MoveJobId;

fn entry(index: u64, payload: EntryPayload<crate::types::TypeConfig>) -> Entry {
    Entry {
        log_id: LogId::new(openraft::CommittedLeaderId::new(7, 2), index),
        payload,
    }
}

fn put(index: u64, key: &'static [u8], value: &'static [u8]) -> Entry {
    entry(
        index,
        EntryPayload::Normal(Command::Put {
            key: Bytes::from_static(key),
            value: Bytes::from_static(value),
        }),
    )
}

fn fence(index: u64) -> Entry {
    entry(
        index,
        EntryPayload::Normal(Command::HandoffFence {
            predecessor_revision: 1,
            predecessor_job_id: MoveJobId::new([1; 16]).unwrap(),
            successor_job_id: MoveJobId::new([2; 16]).unwrap(),
        }),
    )
}

async fn assert_data(stores: &mut GroupStores, applied: LogId, membership: &StoredMembership) {
    assert_eq!(
        stores.state_machine.applied_state().await.unwrap(),
        (Some(applied), membership.clone())
    );
    for (key, expected) in [
        (b"kept".as_slice(), Some(Bytes::from_static(b"value"))),
        (b"deleted".as_slice(), None),
    ] {
        let read = stores
            .state_machine
            .reader()
            .read_local(Bytes::copy_from_slice(key))
            .await
            .unwrap();
        assert_eq!(read.applied, Some(applied));
        assert_eq!(read.value, expected);
    }
}

#[tokio::test]
async fn handoff_fence_preserves_kv_membership_and_sibling_across_reopen_and_snapshot() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("source");
    let storage = Storage::open(
        path.clone(),
        identity(),
        StorageOptions::default(),
        OpenMode::CreateNew,
    )
    .await
    .unwrap();
    let mut group = storage.open_group(GroupId::new(1)).unwrap();
    let mut sibling = storage.open_group(GroupId::new(2)).unwrap();
    sibling
        .state_machine
        .apply([put(0, b"kept", b"sibling")])
        .await
        .unwrap();
    let membership =
        openraft::Membership::new(vec![std::collections::BTreeSet::from([1, 2, 4])], None);
    group
        .state_machine
        .apply([
            entry(0, EntryPayload::Membership(membership.clone())),
            put(1, b"deleted", b"old"),
        ])
        .await
        .unwrap();
    let results = group
        .state_machine
        .apply([
            put(2, b"kept", b"value"),
            fence(3),
            entry(
                4,
                EntryPayload::Normal(Command::Delete {
                    key: Bytes::from_static(b"deleted"),
                }),
            ),
            fence(5),
        ])
        .await
        .unwrap();
    assert_eq!(
        results,
        vec![
            ApplyResult::Applied,
            ApplyResult::HandoffFenced,
            ApplyResult::Applied,
            ApplyResult::HandoffFenced
        ]
    );
    let applied = fence(5).log_id;
    let expected_membership =
        StoredMembership::new(Some(entry(0, EntryPayload::Blank).log_id), membership);
    assert_data(&mut group, applied, &expected_membership).await;
    let image = group
        .state_machine
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap();
    assert_eq!(image.meta.last_log_id, Some(applied));
    assert_eq!(image.meta.last_membership, expected_membership);
    drop(group);
    drop(sibling);
    drop(storage);

    let reopened = Storage::open(
        path,
        identity(),
        StorageOptions::default(),
        OpenMode::OpenExisting,
    )
    .await
    .unwrap();
    let mut group = reopened.open_group(GroupId::new(1)).unwrap();
    let sibling = reopened.open_group(GroupId::new(2)).unwrap();
    assert_data(&mut group, applied, &expected_membership).await;
    let read = sibling
        .state_machine
        .reader()
        .read_local(Bytes::from_static(b"kept"))
        .await
        .unwrap();
    assert_eq!(read.value, Some(Bytes::from_static(b"sibling")));
    assert_eq!(read.applied, Some(put(0, b"kept", b"sibling").log_id));

    let target_storage = Storage::open(
        root.path().join("target"),
        identity(),
        StorageOptions::default(),
        OpenMode::CreateNew,
    )
    .await
    .unwrap();
    let mut target = target_storage.open_group(GroupId::new(1)).unwrap();
    target
        .state_machine
        .apply([put(0, b"deleted", b"must disappear")])
        .await
        .unwrap();
    target
        .state_machine
        .install_snapshot(&image.meta, image.snapshot)
        .await
        .unwrap();
    assert_data(&mut target, applied, &expected_membership).await;
}

#[tokio::test]
async fn handoff_fence_on_metadata_group_rejects_whole_apply_chunk() {
    let root = tempfile::tempdir().unwrap();
    let storage = Storage::open(
        root.path().join("db"),
        identity(),
        StorageOptions::default(),
        OpenMode::CreateNew,
    )
    .await
    .unwrap();
    let mut group = storage.open_group(GroupId::new(0)).unwrap();
    group
        .state_machine
        .apply([put(0, b"kept", b"before")])
        .await
        .unwrap();
    let rejected = group
        .state_machine
        .apply([put(1, b"kept", b"must not persist"), fence(2)])
        .await
        .is_err();
    let read = group
        .state_machine
        .reader()
        .read_local(Bytes::from_static(b"kept"))
        .await
        .unwrap();
    assert!(
        rejected,
        "data-group fence cannot be applied to metadata group"
    );
    assert_eq!(read.value, Some(Bytes::from_static(b"before")));
    assert_eq!(read.applied, Some(put(0, b"kept", b"before").log_id));
    drop(group);
    drop(storage);
    let storage = Storage::open(
        root.path().join("db"),
        identity(),
        StorageOptions::default(),
        OpenMode::OpenExisting,
    )
    .await
    .unwrap();
    let group = storage.open_group(GroupId::new(0)).unwrap();
    let read = group
        .state_machine
        .reader()
        .read_local(Bytes::from_static(b"kept"))
        .await
        .unwrap();
    assert_eq!(read.value, Some(Bytes::from_static(b"before")));
    assert_eq!(read.applied, Some(put(0, b"kept", b"before").log_id));
}

#[tokio::test]
async fn handoff_fence_persist_failure_returns_no_success_and_keeps_previous_state() {
    let root = tempfile::tempdir().unwrap();
    let storage = Storage::open(
        root.path().join("db"),
        identity(),
        StorageOptions::default(),
        OpenMode::CreateNew,
    )
    .await
    .unwrap();
    let mut group = storage.open_group(GroupId::new(1)).unwrap();
    group
        .state_machine
        .apply([put(0, b"kept", b"before")])
        .await
        .unwrap();
    group
        .state_machine
        .group
        .test_writes
        .lock()
        .unwrap()
        .push_back(TestWriteAction::Fail);
    assert!(group.state_machine.apply([fence(1)]).await.is_err());
    drop(group);
    drop(storage);
    let storage = Storage::open(
        root.path().join("db"),
        identity(),
        StorageOptions::default(),
        OpenMode::OpenExisting,
    )
    .await
    .unwrap();
    let group = storage.open_group(GroupId::new(1)).unwrap();
    let read = group
        .state_machine
        .reader()
        .read_local(Bytes::from_static(b"kept"))
        .await
        .unwrap();
    assert_eq!(read.value, Some(Bytes::from_static(b"before")));
    assert_eq!(read.applied, Some(put(0, b"kept", b"before").log_id));
}
