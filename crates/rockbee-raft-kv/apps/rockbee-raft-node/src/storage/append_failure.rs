//! Exact, one-shot adapter failure; this does not simulate a RocksDB error.
#![allow(clippy::unwrap_used)]

use std::sync::Mutex;

use bytes::Bytes;
use openraft::EntryPayload;
use rockbee_raft_protocol::GroupId;

use super::{Group, TestWriteAction};
use crate::types::{Command, Entry, LogId, StoreError};

#[derive(Clone, Debug)]
pub(crate) struct AppendFailureReceipt {
    pub(crate) group_id: GroupId,
    pub(crate) log_id: LogId,
    pub(crate) persist_failed: bool,
    pub(crate) callback_failed: bool,
}

pub(crate) struct AppendFailure {
    group_id: GroupId,
    key: Bytes,
    value: Bytes,
    receipt: Mutex<Option<AppendFailureReceipt>>,
}

impl AppendFailure {
    pub(super) fn new(group_id: GroupId, key: Bytes, value: Bytes) -> Self {
        Self {
            group_id,
            key,
            value,
            receipt: Mutex::new(None),
        }
    }

    pub(crate) fn receipt(&self) -> Option<AppendFailureReceipt> {
        self.receipt.lock().unwrap().clone()
    }

    pub(super) fn matching_log_id(&self, entry: &Entry) -> Option<LogId> {
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
            callback_failed: false,
        });
        true
    }

    // Called immediately before persist, with the existing group write gate
    // held. No other persistence operation can consume this queued action.
    pub(super) fn arm(&self, group: &Group, log_id: LogId) -> bool {
        assert_eq!(self.group_id, GroupId::new(group.id));
        let mut actions = group.test_writes.lock().unwrap();
        assert!(
            actions.is_empty(),
            "targeted failure cannot share a write hook"
        );
        if !self.claim(log_id) {
            return false;
        }
        actions.push_back(TestWriteAction::Fail);
        true
    }

    pub(super) fn record_persist_result(&self, result: &Result<(), StoreError>) {
        let err = result
            .as_ref()
            .expect_err("armed append must fail persistence");
        assert!(err.to_string().contains("injected durable write failure"));
        self.receipt
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .persist_failed = true;
    }

    // Called only after the actual final callback receives this persist Err.
    pub(super) fn record_failed_callback(&self) {
        let mut receipt = self.receipt.lock().unwrap();
        let receipt = receipt.as_mut().unwrap();
        assert!(receipt.persist_failed);
        receipt.callback_failed = true;
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
