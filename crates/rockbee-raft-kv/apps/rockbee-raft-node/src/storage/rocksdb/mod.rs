//! RocksDB implementation of the node storage API.
//!
//! Start with `Storage::open`, then `Storage::open_group`.
//!
//! ```text
//! OpenRaft -> raft/{log, state} -> engine/group -> RocksDB
//! ```
//!
//! - `engine`: native database handle, opening, memory budgets and blocking I/O.
//! - `raft`: OpenRaft log/state-machine interfaces backed by this database.
//! - `control`: Rockbee metadata and durable group-movement fencing.
//! - `snapshot`: image files, installation and recovery.
//! - `format`: stored encoding, key layout and bounded entry-batch layout.
//! - `tests` / `test_support`: backend scenarios and owned test hooks.
//!
//! Every group shares the node's database, block cache and memtable budget.
//! Native writes retain the group owner and gate even if the async caller leaves.
// OpenRaft's capability traits require StorageError by value; preserve its
// contextual error instead of allocating a second wrapper at every boundary.
#![allow(clippy::result_large_err)]

mod control;
mod engine;
mod format;
mod raft;
mod snapshot;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use super::{OpenMode, StorageIdentity, StorageOpenError, StorageOptions};

pub(crate) use control::movement_seal::{MembershipControl, MovementIdentity};
pub use format::STORAGE_FORMAT;
pub(crate) use format::batch::metadata_entry_bytes;
pub use raft::{LogReader, LogStore, StateMachine, StateRead, StateReader};
pub use snapshot::SnapshotBuilder;
#[cfg(all(test, feature = "diagnostics"))]
pub(crate) use test_support::append_failure::AppendFailure;
#[cfg(test)]
pub(crate) use test_support::joint_apply_pause::{JointApplyObservation, JointApplyPause};

use control::metadata;
#[cfg(test)]
use engine::TestWriteAction;
use engine::{Db, Group, Lane, Shared, open_database};
use format::batch::{ChunkMetadata, EntryChunks};
// Settings validate the same on-disk batch bounds before opening the database.
pub(super) use format::batch::{
    ENTRY_BATCH_OVERHEAD, PreparedEntry, STATE_BATCH_OVERHEAD, STATE_RECORD_OVERHEAD,
};
use format::{decode, encode, keys};
use raft::state;
#[cfg(test)]
use test_support::joint_apply_pause;

use crate::types::StoreError;
use ::log::{info, warn};
#[cfg(test)]
use ::rocksdb::IteratorMode;
use openraft::{ErrorSubject, ErrorVerb};
use rockbee_raft_protocol::GroupId;
use std::{
    collections::HashMap,
    fmt,
    path::PathBuf,
    sync::{Arc, Mutex, Weak},
};

/// Owns one node database and hands out one log/state adapter pair per Raft group.
pub struct Storage {
    shared: Arc<Shared>,
    groups: Mutex<HashMap<u64, Weak<Group>>>,
}

/// The two storage capabilities passed to OpenRaft for one group.
pub struct GroupStores {
    pub log_store: LogStore,
    pub state_machine: StateMachine,
}

impl Storage {
    /// Open RocksDB off the async worker and verify its persistent identity.
    pub async fn open(
        path: PathBuf,
        identity: StorageIdentity,
        options: StorageOptions,
        mode: OpenMode,
    ) -> Result<Self, StorageOpenError> {
        options.validate()?;
        let node_id = identity.node_id.get();
        let shared =
            tokio::task::spawn_blocking(move || open_database(path, identity, options, mode))
                .await
                .map_err(|e| StorageOpenError::Open(format!("database open worker: {e}")))??;
        info!("raft_storage_opened node={}", node_id);
        Ok(Self {
            shared,
            groups: Mutex::new(HashMap::new()),
        })
    }

    /// Reserve one live group owner shared by its log and state-machine adapters.
    pub fn open_group(&self, group: GroupId) -> Result<GroupStores, StorageOpenError> {
        let mut groups = self
            .groups
            .lock()
            .map_err(|_| StorageOpenError::Open("group registry poisoned".into()))?;
        if groups.get(&group.get()).and_then(Weak::upgrade).is_some() {
            warn!("raft_storage_group_already_active group={}", group.get());
            return Err(StorageOpenError::ActiveGroup(group.get()));
        }
        let group = Arc::new(Group::new(group.get(), self.shared.clone()));
        groups.insert(group.id, Arc::downgrade(&group));
        Ok(GroupStores {
            log_store: LogStore {
                group: group.clone(),
            },
            state_machine: StateMachine { group },
        })
    }

    /// Test-only evidence for physical prefix deletion without creating a
    /// second mutable owner for an already running Raft group.
    #[cfg(test)]
    pub(crate) async fn test_log_prefix_is_empty(
        &self,
        id: GroupId,
        through: u64,
    ) -> Result<bool, StoreError> {
        let group = self
            .groups
            .lock()
            .map_err(|_| {
                error(
                    ErrorSubject::Logs,
                    ErrorVerb::Read,
                    "group registry poisoned",
                )
            })?
            .get(&id.get())
            .and_then(Weak::upgrade)
            .ok_or_else(|| {
                error(
                    ErrorSubject::Logs,
                    ErrorVerb::Read,
                    "test group is not active",
                )
            })?;
        group
            .read(ErrorSubject::Logs, move |group| {
                let range = keys::LogRange::new(group.id, 0..=through).ok_or_else(|| {
                    error(
                        ErrorSubject::Logs,
                        ErrorVerb::Read,
                        "invalid test log range",
                    )
                })?;
                let mut options = ::rocksdb::ReadOptions::default();
                options.set_iterate_lower_bound(range.start);
                options.set_iterate_upper_bound(range.end);
                let snapshot = group.shared.db.snapshot();
                match snapshot
                    .iterator_cf_opt(group.cf("raft_log")?, options, IteratorMode::Start)
                    .next()
                {
                    None => Ok(true),
                    Some(Ok(_)) => Ok(false),
                    Some(Err(cause)) => Err(error(ErrorSubject::Logs, ErrorVerb::Read, cause)),
                }
            })
            .await
    }
}

fn error(subject: ErrorSubject<u64>, verb: ErrorVerb, message: impl fmt::Display) -> StoreError {
    StoreError::from_io_error(subject, verb, std::io::Error::other(message.to_string()))
}
