//! Public storage settings and validation; no RocksDB calls or I/O here.
use super::{
    StorageOpenError,
    rocksdb::{ENTRY_BATCH_OVERHEAD, PreparedEntry, STATE_BATCH_OVERHEAD, STATE_RECORD_OVERHEAD},
};
use rockbee_raft_protocol::{ClusterId, NodeId, ShardConfig};
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

/// Persistent identity checked when opening an existing node database.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageIdentity {
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
    pub shard_config: ShardConfig,
}

/// Whether the node creates a database or verifies and reopens an existing one.
#[derive(Clone, Copy, Debug)]
pub enum OpenMode {
    CreateNew,
    OpenExisting,
}

/// Node-wide cache, memtable and native I/O limits, shared by all Raft groups.
#[derive(Clone, Debug)]
pub struct StorageOptions {
    pub block_cache_bytes: usize,
    pub write_buffer_budget_bytes: usize,
    pub log_io_slots: usize,
    pub state_io_slots: usize,
    pub snapshot_io_slots: usize,
    pub io_chunk_bytes: usize,
    pub io_chunk_entries: usize,
    pub max_entry_bytes: usize,
}

impl Default for StorageOptions {
    fn default() -> Self {
        Self {
            block_cache_bytes: 64 * 1024 * 1024,
            write_buffer_budget_bytes: 32 * 1024 * 1024,
            log_io_slots: 2,
            state_io_slots: 2,
            snapshot_io_slots: 1,
            io_chunk_bytes: 4 * 1024 * 1024,
            io_chunk_entries: 128,
            max_entry_bytes: 2 * 1024 * 1024,
        }
    }
}

impl StorageOptions {
    /// Production control operations may expand a small command into metadata
    /// effects alongside the largest durable membership/state record.
    pub(crate) fn metadata_minimum_chunk_bytes(&self) -> Result<usize, StorageOpenError> {
        self.max_entry_bytes
            .checked_add(STATE_RECORD_OVERHEAD)
            .and_then(|bytes| bytes.checked_add(STATE_BATCH_OVERHEAD))
            .and_then(|bytes| bytes.checked_add(ENTRY_BATCH_OVERHEAD))
            .and_then(|bytes| bytes.checked_add(crate::control::METADATA_EFFECT_RESERVATION))
            .ok_or_else(|| StorageOpenError::InvalidOptions("metadata budget overflow".into()))
    }

    pub fn validate(&self) -> Result<(), StorageOpenError> {
        if [
            self.block_cache_bytes,
            self.write_buffer_budget_bytes,
            self.log_io_slots,
            self.state_io_slots,
            self.snapshot_io_slots,
            self.io_chunk_bytes,
            self.io_chunk_entries,
            self.max_entry_bytes,
        ]
        .contains(&0)
        {
            return Err(StorageOpenError::InvalidOptions(
                "all budgets and slot counts must be nonzero".into(),
            ));
        }
        if self.log_io_slots > Semaphore::MAX_PERMITS
            || self.state_io_slots > Semaphore::MAX_PERMITS
            || self.snapshot_io_slots > Semaphore::MAX_PERMITS
        {
            return Err(StorageOpenError::InvalidOptions(
                "I/O slots exceed semaphore capacity".into(),
            ));
        }
        if self.io_chunk_bytes < self.max_entry_bytes {
            return Err(StorageOpenError::InvalidOptions(
                "chunk bytes must cover max entry bytes".into(),
            ));
        }
        // A state chunk must fit existing membership metadata and one legal KV
        // command. A new membership replaces that metadata (see ChunkMetadata).
        // Public KV limits bound normal entries more tightly than arbitrary
        // membership entries, so the default 4 MiB chunk remains sufficient.
        let max_normal_entry = rockbee_raft_protocol::MAX_KEY_BYTES
            .checked_add(rockbee_raft_protocol::MAX_VALUE_BYTES)
            .and_then(|bytes| bytes.checked_add(128))
            .ok_or_else(|| StorageOpenError::InvalidOptions("normal entry size overflow".into()))?
            .min(self.max_entry_bytes);
        let minimum_chunk = self
            .max_entry_bytes
            .checked_add(STATE_RECORD_OVERHEAD)
            .and_then(|bytes| bytes.checked_add(max_normal_entry))
            .and_then(|bytes| bytes.checked_add(ENTRY_BATCH_OVERHEAD + STATE_BATCH_OVERHEAD))
            .ok_or_else(|| {
                StorageOpenError::InvalidOptions("chunk budget arithmetic overflow".into())
            })?;
        if self.io_chunk_bytes < minimum_chunk {
            return Err(StorageOpenError::InvalidOptions(format!(
                "chunk bytes must be at least {minimum_chunk} to fit one entry and durable state metadata"
            )));
        }
        // Account for input, encoding, native batch and one lookahead without wrapping.
        if self.io_chunk_bytes.checked_mul(4).is_none()
            || self
                .io_chunk_entries
                .checked_mul(size_of::<PreparedEntry>())
                .is_none()
            || self
                .max_entry_bytes
                .checked_add(STATE_RECORD_OVERHEAD)
                .is_none()
            || self
                .block_cache_bytes
                .checked_add(self.write_buffer_budget_bytes)
                .is_none()
        {
            return Err(StorageOpenError::InvalidOptions(
                "byte budget arithmetic overflow".into(),
            ));
        }
        Ok(())
    }
}
