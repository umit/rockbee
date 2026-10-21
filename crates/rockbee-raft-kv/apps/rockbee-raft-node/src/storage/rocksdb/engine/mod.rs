//! RocksDB ownership, shared memory budgets, opening and synchronous write policy.
//!
//! `Db` below is the actual RocksDB handle. OpenRaft adapters in `log`, `state`
//! and `snapshot` read it directly; `Group::persist` submits their write batches.
mod group;

#[cfg(test)]
pub(super) use group::TestWriteAction;
pub(super) use group::{Group, Lane};

use super::{
    OpenMode, StorageIdentity, StorageOpenError, StorageOptions, decode, encode, keys, snapshot,
};
use ::rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, DBWithThreadMode, IteratorMode, Options,
    SingleThreaded, WriteBufferManager, WriteOptions,
};
use openraft::ErrorSubject;
use std::{path::PathBuf, sync::Arc};
use tokio::sync::Semaphore;

pub(in crate::storage::rocksdb) type Db = DBWithThreadMode<SingleThreaded>;
pub(in crate::storage::rocksdb) const CF_NAMES: [&str; 5] =
    ["default", "raft_log", "raft_meta", "state", "snapshot_meta"];

/// One node database shared by all groups; fields retain their original drop order.
pub(in crate::storage::rocksdb) struct Shared {
    pub(in crate::storage::rocksdb) db: Arc<Db>,
    pub(in crate::storage::rocksdb) path: PathBuf,
    pub(in crate::storage::rocksdb) identity: StorageIdentity,
    pub(in crate::storage::rocksdb) options: StorageOptions,
    pub(in crate::storage::rocksdb) log_lane: Arc<Semaphore>,
    pub(in crate::storage::rocksdb) state_lane: Arc<Semaphore>,
    pub(in crate::storage::rocksdb) snapshot_lane: Arc<Semaphore>,
    pub(in crate::storage::rocksdb) _cache: Cache,
    pub(in crate::storage::rocksdb) _write_buffer_manager: WriteBufferManager,
}

pub(in crate::storage::rocksdb) fn open_database(
    path: PathBuf,
    identity: StorageIdentity,
    options: StorageOptions,
    mode: OpenMode,
) -> Result<Arc<Shared>, StorageOpenError> {
    let open_error = |e: String| StorageOpenError::Open(format!("{}: {e}", path.display()));
    match mode {
        OpenMode::CreateNew => {
            if path.exists()
                && std::fs::read_dir(&path)
                    .map_err(|e| open_error(e.to_string()))?
                    .next()
                    .is_some()
            {
                return Err(open_error(
                    "create requires a missing or empty directory".into(),
                ));
            }
        }
        OpenMode::OpenExisting => {
            let mut found =
                Db::list_cf(&Options::default(), &path).map_err(|e| open_error(e.to_string()))?;
            let mut expected = CF_NAMES.map(str::to_string).to_vec();
            found.sort();
            expected.sort();
            if found != expected {
                return Err(open_error(format!("unexpected column families: {found:?}")));
            }
        }
    }
    let cache = Cache::new_lru_cache(options.block_cache_bytes);
    let manager =
        WriteBufferManager::new_write_buffer_manager(options.write_buffer_budget_bytes, true);
    let mut db_options = engine_options(&cache, &manager);
    let creating = matches!(mode, OpenMode::CreateNew);
    db_options.create_if_missing(creating);
    db_options.create_missing_column_families(creating);
    db_options.set_error_if_exists(creating);
    let descriptors =
        CF_NAMES.map(|name| ColumnFamilyDescriptor::new(name, engine_options(&cache, &manager)));
    let db = Db::open_cf_descriptors(&db_options, &path, descriptors)
        .map_err(|e| open_error(e.to_string()))?;
    if let Some(item) = db.iterator(IteratorMode::Start).next() {
        item.map_err(|e| open_error(e.to_string()))?;
        return Err(open_error("default column family must be empty".into()));
    }
    let meta = db
        .cf_handle("raft_meta")
        .ok_or_else(|| open_error("missing raft_meta column family".into()))?;
    let identity_key = keys::meta(0, keys::IDENTITY);
    if creating {
        let value =
            encode(&identity, ErrorSubject::Store).map_err(|e| open_error(e.to_string()))?;
        db.put_cf_opt(meta, identity_key, value, &sync_options())
            .map_err(|e| open_error(e.to_string()))?;
    } else {
        let bytes = db
            .get_cf(meta, identity_key)
            .map_err(|e| open_error(e.to_string()))?
            .ok_or_else(|| {
                open_error("missing storage identity; partial creation is not reset".into())
            })?;
        let actual: StorageIdentity =
            decode(&bytes, ErrorSubject::Store).map_err(|e| open_error(e.to_string()))?;
        if actual != identity {
            return Err(open_error(
                "cluster, node or shard identity mismatch".into(),
            ));
        }
    }
    let shared = Arc::new(Shared {
        db: Arc::new(db),
        path: path.clone(),
        identity,
        log_lane: Arc::new(Semaphore::new(options.log_io_slots)),
        state_lane: Arc::new(Semaphore::new(options.state_io_slots)),
        snapshot_lane: Arc::new(Semaphore::new(options.snapshot_io_slots)),
        options,
        _cache: cache,
        _write_buffer_manager: manager,
    });
    snapshot::recover(&shared).map_err(|e| open_error(e.to_string()))?;
    Ok(shared)
}

fn engine_options(cache: &Cache, manager: &WriteBufferManager) -> Options {
    let mut block = BlockBasedOptions::default();
    block.set_block_cache(cache);
    let mut options = Options::default();
    options.set_block_based_table_factory(&block);
    options.set_write_buffer_manager(manager);
    options
}

/// Require RocksDB to synchronize its WAL before a successful write returns.
/// Both identity initialization and Group::persist use this policy.
pub(in crate::storage::rocksdb) fn sync_options() -> WriteOptions {
    let mut options = WriteOptions::default();
    options.set_sync(true);
    options
}
