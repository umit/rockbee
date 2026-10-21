//! Node storage API. The RocksDB implementation lives in `rocksdb/`.
//!
//! ```text
//! Client -> Gateway -> OpenRaft -> storage::rocksdb -> RocksDB
//! ```
//!
//! Callers use `Storage`, `GroupStores` and the settings exported here.
//! Start reading the implementation at `rocksdb/mod.rs`; its module guide
//! separates the native engine, Raft adapters, snapshots and stored layout.
mod options;
mod rocksdb;

pub use options::{OpenMode, StorageIdentity, StorageOptions};
#[cfg(all(test, feature = "diagnostics"))]
pub(crate) use rocksdb::AppendFailure;
pub use rocksdb::{
    GroupStores, LogReader, LogStore, STORAGE_FORMAT, SnapshotBuilder, StateMachine, StateRead,
    StateReader, Storage,
};
#[cfg(test)]
pub(crate) use rocksdb::{JointApplyObservation, JointApplyPause};
pub(crate) use rocksdb::{MembershipControl, MovementIdentity, metadata_entry_bytes};

#[derive(Debug, thiserror::Error)]
pub enum StorageOpenError {
    #[error("invalid storage options: {0}")]
    InvalidOptions(String),
    #[error("storage open failed: {0}")]
    Open(String),
    #[error("group {0} still has active handles or blocking jobs")]
    ActiveGroup(u64),
}
