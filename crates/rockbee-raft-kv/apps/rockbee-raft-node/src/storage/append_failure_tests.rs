//! An adapter acknowledgment error after a real synchronous native write.
//! This is neither a physical RocksDB failure nor a public-client outcome test.
#![allow(clippy::panic, clippy::unwrap_used)]

use std::sync::Arc;

use bytes::Bytes;
use openraft::{
    EntryPayload, RaftLogReader, Vote,
    storage::{RaftLogStorage, RaftLogStorageExt, RaftStateMachine},
};
use rockbee_raft_protocol::GroupId;

use super::{AppendFailure, GroupStores, OpenMode, Storage, StorageOptions, tests::identity};
use crate::types::{Command, Entry, LogId};

fn put(term: u64, index: u64, key: &'static [u8], value: &'static [u8]) -> Entry {
    Entry {
        log_id: LogId::new(openraft::CommittedLeaderId::new(term, 1), index),
        payload: EntryPayload::Normal(Command::Put {
            key: Bytes::from_static(key),
            value: Bytes::from_static(value),
        }),
    }
}

async fn seed(stores: &mut GroupStores, vote: Vote<u64>, baseline: &Entry) {
    stores.log_store.save_vote(&vote).await.unwrap();
    stores
        .log_store
        .blocking_append([baseline.clone()])
        .await
        .unwrap();
    stores
        .log_store
        .save_committed(Some(baseline.log_id))
        .await
        .unwrap();
    stores
        .state_machine
        .apply([baseline.clone()])
        .await
        .unwrap();
}

async fn assert_contents(
    stores: &mut GroupStores,
    vote: Vote<u64>,
    baseline: &Entry,
    expected_logs: &[Entry],
) {
    assert_eq!(stores.log_store.read_vote().await.unwrap(), Some(vote));
    assert_eq!(
        stores.log_store.read_committed().await.unwrap(),
        Some(baseline.log_id)
    );
    assert_eq!(
        stores.log_store.try_get_log_entries(..).await.unwrap(),
        expected_logs
    );
    let log_state = stores.log_store.get_log_state().await.unwrap();
    assert_eq!(
        log_state.last_log_id,
        expected_logs.last().map(|e| e.log_id)
    );
    assert_eq!(log_state.last_purged_log_id, None);
    let applied = stores.state_machine.applied_state().await.unwrap();
    assert_eq!(applied.0, Some(baseline.log_id));
    assert_eq!(applied.1, Default::default());
    let EntryPayload::Normal(Command::Put { key, value }) = &baseline.payload else {
        panic!("baseline is a PUT")
    };
    let read = stores
        .state_machine
        .reader()
        .read_local(key.clone())
        .await
        .unwrap();
    assert_eq!(read.applied, Some(baseline.log_id));
    assert_eq!(read.value.as_ref(), Some(value));
}

#[tokio::test]
async fn post_write_append_error_keeps_durable_log_and_sibling_recoverable() {
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
    let target_group = GroupId::new(1);
    let target_vote = Vote::new(7, 1);
    let sibling_vote = Vote::new(8, 1);
    let sibling_next_vote = Vote::new(9, 1);
    let baseline = put(7, 0, b"target", b"old");
    let appended = put(7, 1, b"target", b"durable-but-unacknowledged");
    let sibling_baseline = put(8, 0, b"sibling", b"untouched");
    let mut target = storage.open_group(target_group).unwrap();
    let mut sibling = storage.open_group(GroupId::new(2)).unwrap();
    seed(&mut target, target_vote, &baseline).await;
    seed(&mut sibling, sibling_vote, &sibling_baseline).await;
    assert_contents(
        &mut target,
        target_vote,
        &baseline,
        std::slice::from_ref(&baseline),
    )
    .await;
    assert_contents(
        &mut sibling,
        sibling_vote,
        &sibling_baseline,
        std::slice::from_ref(&sibling_baseline),
    )
    .await;

    let failure = Arc::new(AppendFailure::after_persist(
        target_group,
        Bytes::from_static(b"target"),
        Bytes::from_static(b"durable-but-unacknowledged"),
    ));
    {
        let mut slot = target.log_store.group.append_failure.lock().unwrap();
        assert!(slot.is_none());
        *slot = Some(Arc::clone(&failure));
    }
    // Await the real adapter call. blocking_append's append Err short-circuits
    // its callback receiver, so callback evidence comes from the owned callback
    // invocation itself, not merely from this returned error.
    let append_result = target.log_store.blocking_append([appended.clone()]).await;
    let receipt = failure.receipt().unwrap();
    // Repeating V0 keeps the durable baseline unchanged even in the RED stub,
    // where the omitted injection has not retired the group yet.
    let next_target_write = target.log_store.save_vote(&target_vote).await;
    // A retired target group must not retire the shared DB's sibling group.
    sibling
        .log_store
        .save_vote(&sibling_next_vote)
        .await
        .unwrap();

    // The completed call has joined its native closure. Release every DB owner
    // before reopening; the receipt Arc contains no Group/Shared/DB reference.
    drop(target);
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
    let mut target = reopened.open_group(target_group).unwrap();
    let mut sibling = reopened.open_group(GroupId::new(2)).unwrap();
    assert_contents(
        &mut target,
        target_vote,
        &baseline,
        &[baseline.clone(), appended.clone()],
    )
    .await;
    assert_contents(
        &mut sibling,
        sibling_next_vote,
        &sibling_baseline,
        std::slice::from_ref(&sibling_baseline),
    )
    .await;
    // Only after proving the unchanged durable baseline, show that recovery
    // creates a usable write session. This is not a second restart claim.
    let recovered_vote = Vote::new(10, 1);
    target.log_store.save_vote(&recovered_vote).await.unwrap();
    assert_eq!(
        target.log_store.read_vote().await.unwrap(),
        Some(recovered_vote)
    );
    drop(target);
    drop(sibling);
    drop(reopened);

    eprintln!(
        "post_write_append receipt={receipt:?} target_key=target target_value=durable-but-unacknowledged append={append_result:?} next_target={next_target_write:?} reopen=exact sibling_vote_advanced=true recovered_vote={recovered_vote:?}"
    );
    // Assert the returned error separately from the actual callback receipt.
    let error = append_result
        .expect_err("missing post-write acknowledgment injection: expected append Err");
    assert!(
        error
            .to_string()
            .contains("injected post-write acknowledgment failure")
    );
    assert_eq!(receipt.group_id, target_group);
    assert_eq!(receipt.log_id, appended.log_id);
    assert!(receipt.native_persist_succeeded);
    assert!(!receipt.persist_failed);
    assert!(receipt.injected_ack_error);
    assert!(receipt.callback_failed);
    let error = next_target_write.expect_err("failed append must retire target write admission");
    assert!(error.to_string().contains("group write admission retired"));
}
