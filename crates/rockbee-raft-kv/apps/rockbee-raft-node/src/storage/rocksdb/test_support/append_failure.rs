//! Exact, one-shot adapter failure; this does not simulate a RocksDB error.
#![allow(clippy::unwrap_used)]

use std::sync::Mutex;

use bytes::Bytes;
use openraft::{EntryPayload, ErrorSubject, ErrorVerb};
use rockbee_raft_protocol::GroupId;

use crate::storage::rocksdb::{Group, TestWriteAction, error};
use crate::types::{Command, Entry, LogId, StoreError};

#[derive(Clone, Debug)]
pub(crate) struct AppendFailureReceipt {
    pub(crate) group_id: GroupId,
    pub(crate) log_id: LogId,
    pub(crate) persist_failed: bool,
    pub(crate) native_persist_succeeded: bool,
    pub(crate) injected_ack_error: bool,
    pub(crate) callback_failed: bool,
}

#[derive(Clone, Copy)]
enum FailurePhase {
    BeforePersist,
    AfterPersist,
}

pub(crate) struct AppendFailure {
    group_id: GroupId,
    key: Bytes,
    value: Bytes,
    phase: FailurePhase,
    receipt: Mutex<Option<AppendFailureReceipt>>,
}

impl AppendFailure {
    pub(in crate::storage::rocksdb) fn new(group_id: GroupId, key: Bytes, value: Bytes) -> Self {
        Self {
            group_id,
            key,
            value,
            phase: FailurePhase::BeforePersist,
            receipt: Mutex::new(None),
        }
    }

    pub(in crate::storage::rocksdb) fn after_persist(
        group_id: GroupId,
        key: Bytes,
        value: Bytes,
    ) -> Self {
        Self {
            phase: FailurePhase::AfterPersist,
            ..Self::new(group_id, key, value)
        }
    }

    pub(crate) fn receipt(&self) -> Option<AppendFailureReceipt> {
        self.receipt.lock().unwrap().clone()
    }

    pub(in crate::storage::rocksdb) fn matching_log_id(&self, entry: &Entry) -> Option<LogId> {
        match &entry.payload {
            EntryPayload::Normal(Command::Put { key, value })
                if key == &self.key && value == &self.value =>
            {
                Some(entry.log_id)
            }
            _ => None,
        }
    }

    fn claim(&self, log_id: LogId) -> bool {
        let mut receipt = self.receipt.lock().unwrap();
        if receipt.is_some() {
            return false;
        }
        *receipt = Some(AppendFailureReceipt {
            group_id: self.group_id,
            log_id,
            persist_failed: false,
            native_persist_succeeded: false,
            injected_ack_error: false,
            callback_failed: false,
        });
        true
    }

    // Called immediately before persist, with the existing group write gate
    // held. Only the pre-write phase queues an action for this exact persist.
    pub(in crate::storage::rocksdb) fn arm(&self, group: &Group, log_id: LogId) -> bool {
        assert_eq!(self.group_id, GroupId::new(group.id));
        let mut actions = group.test_writes.lock().unwrap();
        assert!(
            actions.is_empty(),
            "targeted failure cannot share a write hook"
        );
        if !self.claim(log_id) {
            return false;
        }
        if matches!(self.phase, FailurePhase::BeforePersist) {
            actions.push_back(TestWriteAction::Fail);
        }
        true
    }

    pub(in crate::storage::rocksdb) fn finish_persist(
        &self,
        result: Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        let mut receipt = self.receipt.lock().unwrap();
        let receipt = receipt.as_mut().unwrap();
        receipt.native_persist_succeeded = result.is_ok();
        receipt.persist_failed = result.is_err();
        match self.phase {
            FailurePhase::BeforePersist => {
                let err = result
                    .as_ref()
                    .expect_err("armed append must fail persistence");
                assert!(err.to_string().contains("injected durable write failure"));
            }
            FailurePhase::AfterPersist if result.is_ok() => {
                receipt.injected_ack_error = true;
                return Err(error(
                    ErrorSubject::Logs,
                    ErrorVerb::Write,
                    "injected post-write acknowledgment failure",
                ));
            }
            // Preserve a real native failure rather than relabeling it as an
            // injected acknowledgment error after a successful write.
            FailurePhase::AfterPersist => {}
        }
        result
    }

    // Called only after the actual final callback receives this exact result.
    pub(in crate::storage::rocksdb) fn record_callback_result(
        &self,
        result: &Result<(), StoreError>,
    ) {
        let mut receipt = self.receipt.lock().unwrap();
        let receipt = receipt.as_mut().unwrap();
        receipt.callback_failed = result.is_err();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(payload: EntryPayload<crate::types::TypeConfig>, index: u64) -> Entry {
        Entry {
            log_id: LogId::new(openraft::CommittedLeaderId::new(7, 2), index),
            payload,
        }
    }

    fn put(key: &'static [u8], value: &'static [u8], index: u64) -> Entry {
        entry(
            EntryPayload::Normal(Command::Put {
                key: Bytes::from_static(key),
                value: Bytes::from_static(value),
            }),
            index,
        )
    }

    #[test]
    fn post_write_hook_preserves_original_native_error() {
        let failure = AppendFailure::after_persist(
            GroupId::new(1),
            Bytes::from_static(b"target"),
            Bytes::from_static(b"value"),
        );
        let target = put(b"target", b"value", 11);
        assert!(failure.claim(target.log_id));
        let original = error(
            ErrorSubject::Logs,
            ErrorVerb::Write,
            "native result sentinel",
        );
        let expected = format!("{original:?}");
        let returned = failure.finish_persist(Err(original)).unwrap_err();
        assert_eq!(format!("{returned:?}"), expected);
        let receipt = failure.receipt().unwrap();
        assert_eq!(receipt.group_id, GroupId::new(1));
        assert_eq!(receipt.log_id, target.log_id);
        assert!(receipt.persist_failed);
        assert!(!receipt.native_persist_succeeded);
        assert!(!receipt.injected_ack_error);
        assert!(!receipt.callback_failed);
    }

    #[test]
    fn only_exact_put_matches_and_receipt_claim_is_once() {
        let failure = AppendFailure::new(
            GroupId::new(1),
            Bytes::from_static(b"target"),
            Bytes::from_static(b"value"),
        );
        for other in [
            put(b"other", b"value", 10),
            put(b"target", b"other", 10),
            entry(EntryPayload::Blank, 10),
            entry(
                EntryPayload::Normal(Command::Delete {
                    key: Bytes::from_static(b"target"),
                }),
                10,
            ),
        ] {
            assert!(failure.matching_log_id(&other).is_none());
            assert!(failure.receipt().is_none());
        }
        let target = put(b"target", b"value", 11);
        assert_eq!(failure.matching_log_id(&target), Some(target.log_id));
        assert!(failure.claim(target.log_id));
        assert!(!failure.claim(put(b"target", b"value", 12).log_id));
        let receipt = failure.receipt().unwrap();
        assert_eq!(receipt.group_id, GroupId::new(1));
        assert_eq!(receipt.log_id, target.log_id);
        assert!(!receipt.persist_failed);
        assert!(!receipt.callback_failed);
    }
}
