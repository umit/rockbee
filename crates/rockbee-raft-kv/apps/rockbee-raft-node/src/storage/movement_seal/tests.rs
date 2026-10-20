#![allow(clippy::unwrap_used)]

use super::*;
use crate::storage::{OpenMode, Storage, StorageOptions, tests::identity};
use rockbee_raft_protocol::GroupId;
use std::{future::Future, task::Poll};

fn movement(revision: u64, byte: u8) -> MovementIdentity {
    MovementIdentity {
        revision,
        job_id: MoveJobId::new([byte; 16]).unwrap(),
    }
}

#[tokio::test]
async fn sealed_identity_stays_rejected_after_all_owners_drop_and_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let storage = Storage::open(
        path.clone(),
        identity(),
        StorageOptions::default(),
        OpenMode::CreateNew,
    )
    .await
    .unwrap();
    let stores = storage.open_group(GroupId::new(1)).unwrap();
    let control = stores.state_machine.reader().membership_control();
    let predecessor = movement(1, 1);
    control.seal(predecessor).await.unwrap();
    let rejected_live = control.begin(predecessor).await.is_err();
    let successor_allowed = control.begin(movement(2, 2)).await.is_ok();
    drop(control);
    drop(stores);
    drop(storage);
    let reopened = Storage::open(
        path,
        identity(),
        StorageOptions::default(),
        OpenMode::OpenExisting,
    )
    .await
    .unwrap();
    let stores = reopened.open_group(GroupId::new(1)).unwrap();
    let control = stores.state_machine.reader().membership_control();
    let rejected_reopened = control.begin(predecessor).await.is_err();
    drop(control);
    drop(stores);
    drop(reopened);
    assert!(
        rejected_live,
        "missing durable predecessor seal at live admission"
    );
    assert!(
        rejected_reopened,
        "predecessor seal must survive complete reopen"
    );
    assert!(
        successor_allowed,
        "local watermark permits a newer identity; metadata still controls acceptance"
    );
}

#[tokio::test]
async fn seal_cannot_acknowledge_while_native_membership_permit_is_owned() {
    let root = tempfile::tempdir().unwrap();
    let storage = Storage::open(
        root.path().join("db"),
        identity(),
        StorageOptions::default(),
        OpenMode::CreateNew,
    )
    .await
    .unwrap();
    let stores = storage.open_group(GroupId::new(1)).unwrap();
    let control = stores.state_machine.reader().membership_control();
    let predecessor = movement(1, 1);
    let native_owner = control.begin(predecessor).await.unwrap();
    let gate_owned = !control.native_permit_available_for_test();
    let (waited, queued_rejected) = {
        let mut seal = std::pin::pin!(control.seal(predecessor));
        let pending =
            std::future::poll_fn(|cx| Poll::Ready(seal.as_mut().poll(cx).is_pending())).await;
        // Poll an old caller behind the seal while the original permit is
        // still held. It must revalidate durable bytes after acquiring.
        let mut queued = std::pin::pin!(control.begin(predecessor));
        let queued_pending =
            std::future::poll_fn(|cx| Poll::Ready(queued.as_mut().poll(cx).is_pending())).await;
        assert!(queued_pending);
        drop(native_owner);
        if pending {
            seal.await.unwrap();
        }
        let rejected = queued.await.is_err();
        (pending, rejected)
    };
    let rejected = control.begin(predecessor).await.is_err();
    drop(control);
    drop(stores);
    drop(storage);
    assert!(
        gate_owned,
        "original native owner must hold the independent gate"
    );
    assert!(
        waited,
        "seal acknowledged before the original native owner released its permit"
    );
    assert!(
        queued_rejected,
        "queued old caller must revalidate after seal"
    );
    assert!(
        rejected,
        "acknowledged predecessor must reject future native entry"
    );
}

async fn fixture() -> (tempfile::TempDir, Storage, crate::storage::GroupStores) {
    let root = tempfile::tempdir().unwrap();
    let storage = Storage::open(
        root.path().join("db"),
        identity(),
        StorageOptions::default(),
        OpenMode::CreateNew,
    )
    .await
    .unwrap();
    let stores = storage.open_group(GroupId::new(1)).unwrap();
    (root, storage, stores)
}

#[tokio::test]
async fn seal_is_monotonic_and_logical_conflicts_do_not_retire_storage() {
    let (_root, storage, stores) = fixture().await;
    let control = stores.state_machine.reader().membership_control();
    let first = movement(2, 2);
    control.seal(first).await.unwrap();
    control.seal(first).await.unwrap();
    control.seal(movement(1, 1)).await.unwrap();
    assert_eq!(load(&control.group).unwrap(), Some(first));
    assert!(control.seal(movement(2, 9)).await.is_err());
    assert!(control.seal(movement(0, 1)).await.is_err());
    assert!(!control.group.retired.load(Ordering::Acquire));
    let second = movement(3, 3);
    control.seal(second).await.unwrap();
    assert_eq!(load(&control.group).unwrap(), Some(second));
    assert!(control.begin(first).await.is_err());
    assert!(control.begin(second).await.is_err());
    drop(control.begin(movement(4, 4)).await.unwrap());
    let metadata = storage.open_group(GroupId::new(0)).unwrap();
    assert!(
        metadata
            .state_machine
            .reader()
            .membership_control()
            .seal(first)
            .await
            .is_err()
    );
    assert!(!metadata.state_machine.group.retired.load(Ordering::Acquire));
}

#[tokio::test]
async fn malformed_or_oversized_seal_fails_closed_without_a_write_session() {
    let (_root, _storage, stores) = fixture().await;
    let control = stores.state_machine.reader().membership_control();
    for bytes in [
        vec![0; MAX_SEAL_BYTES + 1],
        encode(
            &SealRecord {
                version: 9,
                identity: movement(1, 1),
            },
            ErrorSubject::Store,
        )
        .unwrap(),
        vec![255],
    ] {
        let session = control.group.reserve(ErrorSubject::Store).await.unwrap();
        control
            .group
            .write(&session, Lane::Log, ErrorSubject::Store, move |group| {
                let mut batch = WriteBatch::default();
                batch.put_cf(
                    group.cf("raft_meta")?,
                    keys::meta(group.id, keys::MOVEMENT_SEAL),
                    bytes,
                );
                group.persist(batch, ErrorSubject::Store)
            })
            .await
            .unwrap();
        session.complete();
        assert!(control.begin(movement(2, 2)).await.is_err());
        assert!(control.seal(movement(2, 2)).await.is_err());
        assert!(!control.group.retired.load(Ordering::Acquire));
    }
}

#[tokio::test]
async fn failed_seal_persistence_retires_entry_but_sibling_remains_writable() {
    use openraft::storage::RaftLogStorage;
    let (_root, storage, stores) = fixture().await;
    let control = stores.state_machine.reader().membership_control();
    control
        .group
        .test_writes
        .lock()
        .unwrap()
        .push_back(crate::storage::TestWriteAction::Fail);
    let result = control.seal(movement(1, 1)).await;
    let rejected = control.begin(movement(2, 2)).await.is_err();
    let mut sibling = storage.open_group(GroupId::new(2)).unwrap();
    let vote = openraft::Vote::new(4, 2);
    sibling.log_store.save_vote(&vote).await.unwrap();
    assert_eq!(sibling.log_store.read_vote().await.unwrap(), Some(vote));
    assert!(result.is_err());
    assert!(rejected);
    assert!(control.group.retired.load(Ordering::Acquire));
    assert!(control.native_permit_available_for_test());
}

#[tokio::test]
async fn cancelled_seal_retains_native_gate_until_persist_finishes_then_rejects() {
    let (root, storage, stores) = fixture().await;
    let control = stores.state_machine.reader().membership_control();
    let predecessor = movement(1, 1);
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    control
        .group
        .test_writes
        .lock()
        .unwrap()
        .push_back(crate::storage::TestWriteAction::Hold {
            entered: entered_tx,
            resume: resume_rx,
        });
    let task_control = control.clone();
    let task = tokio::spawn(async move { task_control.seal(predecessor).await });
    let entered = tokio::time::timeout(std::time::Duration::from_secs(5), entered_rx).await;
    if !matches!(&entered, Ok(Ok(()))) {
        // Release even if the hook has not been consumed yet, then join this
        // exact observer before reporting the failed phase premise.
        let _ = resume_tx.send(());
        let outcome = task.await;
        drop(control);
        drop(stores);
        drop(storage);
        assert!(
            matches!(entered, Ok(Ok(()))),
            "native seal hook was not reached: {outcome:?}"
        );
        return;
    }
    // Abort only the async observer; the blocking native closure still owns
    // its group, write gate and cloned native-membership gate permit.
    task.abort();
    let cancelled = task.await.unwrap_err().is_cancelled();
    let gate_retained = !control.native_permit_available_for_test();
    let retired_before_release = control.group.retired.load(Ordering::Acquire);
    let (begin_waited, rejected) = {
        let mut begin = std::pin::pin!(control.begin(movement(2, 2)));
        let pending =
            std::future::poll_fn(|cx| Poll::Ready(begin.as_mut().poll(cx).is_pending())).await;
        resume_tx.send(()).unwrap();
        let rejected = if pending { begin.await.is_err() } else { false };
        (pending, rejected)
    };
    drop(control);
    drop(stores);
    // The aborted observer does not own the blocking closure's join handle.
    // Wait for the exact Group registry owner to disappear before reopening DB.
    let drained = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            match storage.open_group(GroupId::new(1)) {
                Ok(group) => break Ok(group),
                Err(crate::storage::StorageOpenError::ActiveGroup(1)) => {
                    tokio::task::yield_now().await
                }
                Err(error) => return Err(error),
            }
        }
    })
    .await
    .unwrap()
    .unwrap();
    drop(drained);
    drop(storage);
    let reopened = Storage::open(
        root.path().join("db"),
        identity(),
        StorageOptions::default(),
        OpenMode::OpenExisting,
    )
    .await
    .unwrap();
    let stores = reopened.open_group(GroupId::new(1)).unwrap();
    let control = stores.state_machine.reader().membership_control();
    let actual_seal = load(&control.group).unwrap();
    let old_rejected = control.begin(predecessor).await.is_err();
    drop(control.begin(movement(2, 2)).await.unwrap());
    drop(control);
    drop(stores);
    drop(reopened);
    assert!(cancelled && gate_retained && retired_before_release && begin_waited && rejected);
    assert_eq!(actual_seal, Some(predecessor));
    assert!(old_rejected);
}

#[tokio::test]
async fn installed_snapshot_replaces_state_but_preserves_local_seal_and_sibling() {
    use crate::types::{Command, Entry, LogId};
    use bytes::Bytes;
    use openraft::{
        EntryPayload,
        storage::{RaftSnapshotBuilder, RaftStateMachine},
    };
    let entry = |index, payload| Entry {
        log_id: LogId::new(openraft::CommittedLeaderId::new(3, 1), index),
        payload,
    };
    let put = |key, value| {
        EntryPayload::Normal(Command::Put {
            key: Bytes::from_static(key),
            value: Bytes::from_static(value),
        })
    };
    let (_source_root, source_storage, mut source) = fixture().await;
    let membership =
        openraft::Membership::new(vec![std::collections::BTreeSet::from([1, 2, 3])], None);
    source
        .state_machine
        .apply([
            entry(1, put(b"kept", b"snapshot-value")),
            entry(2, put(b"deleted", b"was-present")),
            entry(
                3,
                EntryPayload::Normal(Command::Delete {
                    key: Bytes::from_static(b"deleted"),
                }),
            ),
            entry(4, EntryPayload::Membership(membership)),
        ])
        .await
        .unwrap();
    let image = source
        .state_machine
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap();
    let (root, storage, mut target) = fixture().await;
    target
        .state_machine
        .apply([entry(0, put(b"old-only", b"old"))])
        .await
        .unwrap();
    let mut sibling = storage.open_group(GroupId::new(2)).unwrap();
    sibling
        .state_machine
        .apply([entry(0, put(b"sibling", b"untouched"))])
        .await
        .unwrap();
    let control = target.state_machine.reader().membership_control();
    let sealed = movement(7, 7);
    control.seal(sealed).await.unwrap();
    target
        .state_machine
        .install_snapshot(&image.meta, image.snapshot)
        .await
        .unwrap();
    assert_eq!(load(&control.group).unwrap(), Some(sealed));
    assert!(control.begin(sealed).await.is_err());
    assert_eq!(
        target.state_machine.applied_state().await.unwrap(),
        (image.meta.last_log_id, image.meta.last_membership.clone())
    );
    assert_eq!(
        target
            .state_machine
            .reader()
            .read_local(Bytes::from_static(b"kept"))
            .await
            .unwrap()
            .value,
        Some(Bytes::from_static(b"snapshot-value"))
    );
    assert_eq!(
        target
            .state_machine
            .reader()
            .read_local(Bytes::from_static(b"deleted"))
            .await
            .unwrap()
            .value,
        None
    );
    assert_eq!(
        target
            .state_machine
            .reader()
            .read_local(Bytes::from_static(b"old-only"))
            .await
            .unwrap()
            .value,
        None
    );
    assert_eq!(
        sibling
            .state_machine
            .reader()
            .read_local(Bytes::from_static(b"sibling"))
            .await
            .unwrap()
            .value,
        Some(Bytes::from_static(b"untouched"))
    );
    drop(control);
    drop(target);
    drop(sibling);
    drop(storage);
    drop(source);
    drop(source_storage);
    let reopened = Storage::open(
        root.path().join("db"),
        identity(),
        StorageOptions::default(),
        OpenMode::OpenExisting,
    )
    .await
    .unwrap();
    let mut target = reopened.open_group(GroupId::new(1)).unwrap();
    let sibling = reopened.open_group(GroupId::new(2)).unwrap();
    let control = target.state_machine.reader().membership_control();
    assert_eq!(load(&control.group).unwrap(), Some(sealed));
    assert!(control.begin(sealed).await.is_err());
    let read = target
        .state_machine
        .reader()
        .read_local(Bytes::from_static(b"kept"))
        .await
        .unwrap();
    assert_eq!(read.applied, image.meta.last_log_id);
    assert_eq!(read.value, Some(Bytes::from_static(b"snapshot-value")));
    assert_eq!(
        target
            .state_machine
            .reader()
            .read_local(Bytes::from_static(b"deleted"))
            .await
            .unwrap()
            .value,
        None
    );
    assert_eq!(
        target
            .state_machine
            .reader()
            .read_local(Bytes::from_static(b"old-only"))
            .await
            .unwrap()
            .value,
        None
    );
    assert_eq!(
        target.state_machine.applied_state().await.unwrap(),
        (image.meta.last_log_id, image.meta.last_membership)
    );
    let sibling_read = sibling
        .state_machine
        .reader()
        .read_local(Bytes::from_static(b"sibling"))
        .await
        .unwrap();
    assert_eq!(
        sibling_read.applied,
        Some(LogId::new(openraft::CommittedLeaderId::new(3, 1), 0))
    );
    assert_eq!(sibling_read.value, Some(Bytes::from_static(b"untouched")));
}

#[tokio::test]
async fn dropping_a_seal_waiter_before_gate_acquisition_dispatches_no_write() {
    let (_root, _storage, stores) = fixture().await;
    let control = stores.state_machine.reader().membership_control();
    let predecessor = movement(1, 1);
    let native_owner = control.begin(predecessor).await.unwrap();
    {
        let mut waiting = std::pin::pin!(control.seal(predecessor));
        let pending =
            std::future::poll_fn(|cx| Poll::Ready(waiting.as_mut().poll(cx).is_pending())).await;
        assert!(pending);
    }
    assert_eq!(load(&control.group).unwrap(), None);
    assert!(!control.group.retired.load(Ordering::Acquire));
    drop(native_owner);
    drop(control.begin(predecessor).await.unwrap());
    control.seal(predecessor).await.unwrap();
    assert!(control.begin(predecessor).await.is_err());
}
