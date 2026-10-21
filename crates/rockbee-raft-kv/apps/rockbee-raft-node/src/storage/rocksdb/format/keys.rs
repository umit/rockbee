//! Bytewise ordered group boundaries. No boundary adds one to a group ID.
use std::ops::{Bound, RangeBounds};

pub(in crate::storage::rocksdb) const VOTE: u8 = 1;
pub(in crate::storage::rocksdb) const PURGED: u8 = 2;
pub(in crate::storage::rocksdb) const COMMITTED: u8 = 3;
pub(in crate::storage::rocksdb) const IDENTITY: u8 = 0x80;
pub(in crate::storage::rocksdb) const MOVEMENT_SEAL: u8 = 0x81;

pub(in crate::storage::rocksdb) fn meta(group: u64, tag: u8) -> Vec<u8> {
    let mut key = group.to_be_bytes().to_vec();
    key.push(tag);
    key
}

pub(in crate::storage::rocksdb) fn log(group: u64, index: u64) -> Vec<u8> {
    let mut key = group.to_be_bytes().to_vec();
    key.extend_from_slice(&index.to_be_bytes());
    key
}

pub(in crate::storage::rocksdb) fn after_log(group: u64, index: u64) -> Vec<u8> {
    let mut key = log(group, index);
    key.push(0);
    key
}

pub(in crate::storage::rocksdb) fn log_end(group: u64) -> Vec<u8> {
    after_log(group, u64::MAX)
}

pub(in crate::storage::rocksdb) fn data(group: u64, generation: u64, key: &[u8]) -> Vec<u8> {
    let mut result = meta(group, 1);
    result.extend_from_slice(&generation.to_be_bytes());
    result.extend_from_slice(key);
    result
}

pub(in crate::storage::rocksdb) struct LogRange {
    pub start: Vec<u8>,
    pub end: Vec<u8>,
}

impl LogRange {
    pub(in crate::storage::rocksdb) fn new(
        group: u64,
        range: impl RangeBounds<u64>,
    ) -> Option<Self> {
        let first = match range.start_bound() {
            Bound::Included(i) => *i,
            Bound::Excluded(i) => i.checked_add(1)?,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(i) => after_log(group, *i),
            Bound::Excluded(i) => log(group, *i),
            Bound::Unbounded => log_end(group),
        };
        let start = log(group, first);
        (start < end).then_some(Self { start, end })
    }
}
