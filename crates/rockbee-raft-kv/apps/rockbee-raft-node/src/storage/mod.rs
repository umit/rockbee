//! Durable OpenRaft capabilities sharing one RocksDB, cache and memtable budget.
//!
//! Native work runs in bounded blocking lanes. Every dispatched write retains
//! both its group owner and write gate until the native operation has finished.
// OpenRaft's capability traits require StorageError by value; preserve its
// contextual error instead of allocating a second wrapper at every boundary.
#![allow(clippy::result_large_err)]
#[cfg(test)]
mod joint_apply_pause;
mod keys;
mod log;
mod metadata;
#[cfg(test)]
mod metadata_tests;
mod snapshot;
mod state;
#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) use joint_apply_pause::{JointApplyObservation, JointApplyPause};
pub use log::{LogReader, LogStore};
pub use snapshot::SnapshotBuilder;
pub use state::{StateMachine, StateRead, StateReader};

use crate::{
    diagnostics::Span,
    types::{Entry, StoreError},
};
use ::log::{debug, info, warn};
use openraft::{ErrorSubject, ErrorVerb};
use rockbee_raft_protocol::{ClusterId, GroupId, NodeId, ShardConfig};
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamily, ColumnFamilyDescriptor, DBWithThreadMode, IteratorMode,
    Options, SingleThreaded, WriteBatch, WriteBufferManager, WriteOptions,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    collections::HashMap,
    fmt,
    path::PathBuf,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

type Db = DBWithThreadMode<SingleThreaded>;
const CF_NAMES: [&str; 5] = ["default", "raft_log", "raft_meta", "state", "snapshot_meta"];
pub const STORAGE_FORMAT: u8 = 1;
const ENTRY_BATCH_OVERHEAD: usize = 64;
const STATE_BATCH_OVERHEAD: usize = 128;
const STATE_RECORD_OVERHEAD: usize = 256;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageIdentity {
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
    pub shard_config: ShardConfig,
}

#[derive(Clone, Copy, Debug)]
pub enum OpenMode {
    CreateNew,
    OpenExisting,
}

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

/// Counts the entire durable envelope with the largest possible assigned LogId.
/// Candidate validation runs before cloning/serializing its bounded payload.
pub(crate) fn metadata_entry_bytes(
    command: &crate::control::MetadataCommand,
) -> Result<usize, StorageOpenError> {
    command
        .validate()
        .map_err(|error| StorageOpenError::InvalidOptions(error.to_string()))?;
    let entry = Entry {
        log_id: crate::types::LogId::new(
            openraft::CommittedLeaderId::new(u64::MAX, u64::MAX),
            u64::MAX,
        ),
        payload: openraft::EntryPayload::Normal(crate::types::Command::Metadata(Box::new(
            command.clone(),
        ))),
    };
    postcard::experimental::serialized_size(&(STORAGE_FORMAT, &entry))
        .map_err(|error| StorageOpenError::InvalidOptions(error.to_string()))
}

#[derive(Debug, thiserror::Error)]
pub enum StorageOpenError {
    #[error("invalid storage options: {0}")]
    InvalidOptions(String),
    #[error("storage open failed: {0}")]
    Open(String),
    #[error("group {0} still has active handles or blocking jobs")]
    ActiveGroup(u64),
}

pub struct Storage {
    shared: Arc<Shared>,
    groups: Mutex<HashMap<u64, Weak<Group>>>,
}

struct Shared {
    db: Arc<Db>,
    path: PathBuf,
    identity: StorageIdentity,
    options: StorageOptions,
    log_lane: Arc<Semaphore>,
    state_lane: Arc<Semaphore>,
    snapshot_lane: Arc<Semaphore>,
    _cache: Cache,
    _write_buffer_manager: WriteBufferManager,
}

pub struct GroupStores {
    pub log_store: LogStore,
    pub state_machine: StateMachine,
}

impl Storage {
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
                let mut options = rocksdb::ReadOptions::default();
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

fn open_database(
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

fn sync_options() -> WriteOptions {
    let mut options = WriteOptions::default();
    options.set_sync(true);
    options
}

fn error(subject: ErrorSubject<u64>, verb: ErrorVerb, message: impl fmt::Display) -> StoreError {
    StoreError::from_io_error(subject, verb, std::io::Error::other(message.to_string()))
}

fn encode<T: Serialize>(value: &T, subject: ErrorSubject<u64>) -> Result<Vec<u8>, StoreError> {
    postcard::to_stdvec(&(STORAGE_FORMAT, value)).map_err(|e| error(subject, ErrorVerb::Write, e))
}

fn decode<T: DeserializeOwned>(bytes: &[u8], subject: ErrorSubject<u64>) -> Result<T, StoreError> {
    let ((version, value), rest): ((u8, T), _) =
        postcard::take_from_bytes(bytes).map_err(|e| error(subject.clone(), ErrorVerb::Read, e))?;
    if version != STORAGE_FORMAT || !rest.is_empty() {
        return Err(error(
            subject,
            ErrorVerb::Read,
            "unsupported storage format or trailing bytes",
        ));
    }
    Ok(value)
}

struct Group {
    id: u64,
    shared: Arc<Shared>,
    gate: Arc<Semaphore>,
    retired: AtomicBool,
    publication: Mutex<()>,
    live_images: Mutex<std::collections::HashSet<u64>>,
    #[cfg(test)]
    snapshot_hooks: Mutex<std::collections::VecDeque<(snapshot::Stage, TestWriteAction)>>,
    #[cfg(test)]
    snapshot_peak_batch: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    snapshot_peak_records: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    snapshot_peak_record_bytes: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    test_writes: Mutex<std::collections::VecDeque<TestWriteAction>>,
    #[cfg(test)]
    joint_apply_pause: Mutex<Option<Arc<JointApplyPause>>>,
}

#[derive(Clone, Copy)]
enum Lane {
    Log,
    State,
    Snapshot,
}

struct WriteSession {
    group: Arc<Group>,
    permit: Arc<OwnedSemaphorePermit>,
    completed: bool,
}
impl WriteSession {
    fn complete(mut self) {
        self.completed = true;
    }
}
impl Drop for WriteSession {
    fn drop(&mut self) {
        if !self.completed {
            self.group.retired.store(true, Ordering::Release);
        }
    }
}

impl Group {
    fn new(id: u64, shared: Arc<Shared>) -> Self {
        Self {
            id,
            shared,
            gate: Arc::new(Semaphore::new(1)),
            retired: AtomicBool::new(false),
            publication: Mutex::new(()),
            live_images: Mutex::new(std::collections::HashSet::new()),
            #[cfg(test)]
            snapshot_hooks: Mutex::new(std::collections::VecDeque::new()),
            #[cfg(test)]
            snapshot_peak_batch: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            snapshot_peak_records: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            snapshot_peak_record_bytes: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            test_writes: Mutex::new(std::collections::VecDeque::new()),
            #[cfg(test)]
            joint_apply_pause: Mutex::new(None),
        }
    }
    fn cf(&self, name: &str) -> Result<&ColumnFamily, StoreError> {
        self.shared.db.cf_handle(name).ok_or_else(|| {
            error(
                ErrorSubject::Store,
                ErrorVerb::Read,
                format!("missing {name} column family"),
            )
        })
    }

    async fn reserve(
        self: &Arc<Self>,
        subject: ErrorSubject<u64>,
    ) -> Result<WriteSession, StoreError> {
        let mut span = Span::start(
            self.shared.identity.node_id.get(),
            self.id,
            None,
            "storage_reserve",
            true,
        );
        span.phase("gate_wait");
        let permit = match self.gate.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(cause) => {
                span.failure(&cause);
                warn!(
                    "raft_storage_gate_failed node={} group={} error={}",
                    self.shared.identity.node_id.get(),
                    self.id,
                    cause
                );
                return Err(error(subject, ErrorVerb::Write, cause));
            }
        };
        span.phase("retired_check");
        if self.retired.load(Ordering::Acquire) {
            let failure = error(
                subject,
                ErrorVerb::Write,
                "group write admission retired after failed or abandoned operation; reopen and recover",
            );
            span.failure(&failure);
            warn!(
                "raft_storage_group_retired node={} group={}",
                self.shared.identity.node_id.get(),
                self.id
            );
            return Err(failure);
        }
        span.success();
        Ok(WriteSession {
            group: self.clone(),
            permit: Arc::new(permit),
            completed: false,
        })
    }

    async fn read<T: Send + 'static>(
        self: &Arc<Self>,
        subject: ErrorSubject<u64>,
        work: impl FnOnce(&Group) -> Result<T, StoreError> + Send + 'static,
    ) -> Result<T, StoreError> {
        let started = ::log::log_enabled!(::log::Level::Debug).then(Instant::now);
        let mut caller = Span::start(
            self.shared.identity.node_id.get(),
            self.id,
            None,
            "storage_read",
            true,
        );
        caller.phase("lane_wait");
        let lane = match self.shared.state_lane.clone().acquire_owned().await {
            Ok(lane) => lane,
            Err(cause) => {
                caller.failure(&cause);
                return Err(error(subject, ErrorVerb::Read, cause));
            }
        };
        let group = self.clone();
        let mut native = Span::start(
            self.shared.identity.node_id.get(),
            self.id,
            None,
            "storage_read_native",
            true,
        );
        native.phase("blocking_queue");
        caller.phase("native_wait");
        let result = tokio::task::spawn_blocking(move || {
            let _lane = lane;
            // This span moves into the native job. Dropping the caller's waiter
            // cannot misreport the still-owned native work as canceled.
            native.phase("native_work");
            let result = work(&group);
            match &result {
                Ok(_) => native.success(),
                Err(error) => native.failure(error),
            }
            result
        })
        .await
        .map_err(|e| error(subject, ErrorVerb::Read, e))
        .and_then(|result| result);
        match &result {
            Ok(_) => caller.success(),
            Err(error) => {
                caller.failure(error);
                debug!(
                    "raft_storage_read_failed node={} group={} elapsed_ms={} error={}",
                    self.shared.identity.node_id.get(),
                    self.id,
                    elapsed_ms(started),
                    error
                );
            }
        }
        result
    }

    async fn write<T: Send + 'static>(
        self: &Arc<Self>,
        session: &WriteSession,
        lane: Lane,
        subject: ErrorSubject<u64>,
        work: impl FnOnce(&Group) -> Result<T, StoreError> + Send + 'static,
    ) -> Result<T, StoreError> {
        let started = ::log::log_enabled!(::log::Level::Debug).then(Instant::now);
        let (lanes, operation, native_operation) = match lane {
            Lane::Log => (
                &self.shared.log_lane,
                "storage_write_log",
                "storage_write_log_native",
            ),
            Lane::State => (
                &self.shared.state_lane,
                "storage_write_state",
                "storage_write_state_native",
            ),
            Lane::Snapshot => (
                &self.shared.snapshot_lane,
                "storage_write_snapshot",
                "storage_write_snapshot_native",
            ),
        };
        let mut caller = Span::start(
            self.shared.identity.node_id.get(),
            self.id,
            None,
            operation,
            true,
        );
        caller.phase("lane_wait");
        let lane = match lanes.clone().acquire_owned().await {
            Ok(lane) => lane,
            Err(cause) => {
                caller.failure(&cause);
                return Err(error(subject, ErrorVerb::Write, cause));
            }
        };
        let group = self.clone();
        let gate = session.permit.clone();
        let mut native = Span::start(
            self.shared.identity.node_id.get(),
            self.id,
            None,
            native_operation,
            true,
        );
        native.phase("blocking_queue");
        caller.phase("native_wait");
        // Cancellation drops the waiter, not native I/O. The closure retains the
        // lane, group registry owner and gate; the session retires future writes.
        let result = tokio::task::spawn_blocking(move || {
            let _lane = lane;
            let _gate = gate;
            native.phase("native_work");
            let result = work(&group);
            match &result {
                Ok(_) => native.success(),
                Err(error) => native.failure(error),
            }
            result
        })
        .await
        .map_err(|e| error(subject, ErrorVerb::Write, e))
        .and_then(|result| result);
        match &result {
            Ok(_) => caller.success(),
            Err(error) => {
                caller.failure(error);
                debug!(
                    "raft_storage_write_failed node={} group={} operation={} elapsed_ms={} error={}",
                    self.shared.identity.node_id.get(),
                    self.id,
                    operation,
                    elapsed_ms(started),
                    error
                );
            }
        }
        result
    }

    fn persist(&self, batch: WriteBatch, subject: ErrorSubject<u64>) -> Result<(), StoreError> {
        if batch.size_in_bytes() > self.shared.options.io_chunk_bytes {
            return Err(error(
                subject,
                ErrorVerb::Write,
                "native write batch exceeds configured chunk byte budget",
            ));
        }
        #[cfg(test)]
        self.before_test_write(subject.clone())?;
        let started = ::log::log_enabled!(::log::Level::Debug).then(Instant::now);
        let mut span = Span::start(
            self.shared.identity.node_id.get(),
            self.id,
            None,
            "storage_persist",
            true,
        );
        span.phase("rocksdb_sync_write");
        let result = self
            .shared
            .db
            .write_opt(batch, &sync_options())
            .map_err(|e| error(subject, ErrorVerb::Write, e));
        if let Some(started) = started {
            let elapsed = started.elapsed();
            if elapsed.as_millis() >= 25 {
                debug!(
                    "raft_storage_sync_write_slow node={} group={} elapsed_us={} success={}",
                    self.shared.identity.node_id.get(),
                    self.id,
                    elapsed.as_micros(),
                    result.is_ok()
                );
            }
        }
        match &result {
            Ok(()) => span.success(),
            Err(error) => {
                span.failure(error);
                warn!(
                    "raft_storage_persist_failed node={} group={} error={}",
                    self.shared.identity.node_id.get(),
                    self.id,
                    error
                );
            }
        }
        result
    }

    #[cfg(test)]
    fn before_test_write(&self, subject: ErrorSubject<u64>) -> Result<(), StoreError> {
        let action = self
            .test_writes
            .lock()
            .map_err(|_| {
                error(
                    subject.clone(),
                    ErrorVerb::Write,
                    "test write hook poisoned",
                )
            })?
            .pop_front();
        match action {
            Some(TestWriteAction::Fail) => Err(error(
                subject,
                ErrorVerb::Write,
                "injected durable write failure",
            )),
            Some(TestWriteAction::Hold { entered, resume }) => {
                let _ = entered.send(());
                resume
                    .recv()
                    .map_err(|e| error(subject, ErrorVerb::Write, e))
            }
            Some(TestWriteAction::Pass) | None => Ok(()),
        }
    }
}

fn elapsed_ms(started: Option<Instant>) -> u128 {
    started.map_or(0, |started| started.elapsed().as_millis())
}

#[cfg(test)]
enum TestWriteAction {
    Pass,
    Fail,
    Hold {
        entered: tokio::sync::oneshot::Sender<()>,
        resume: std::sync::mpsc::Receiver<()>,
    },
}

struct PreparedEntry {
    entry: Entry,
    encoded: Vec<u8>,
}

enum ChunkMetadata {
    Log,
    State { encoded_record_bytes: usize },
}

fn prepare_entry(entry: Entry, options: &StorageOptions) -> Result<PreparedEntry, StoreError> {
    let subject = ErrorSubject::Log(entry.log_id);
    if let openraft::EntryPayload::Normal(command) = &entry.payload {
        command
            .validate()
            .map_err(|e| error(subject.clone(), ErrorVerb::Write, e))?;
    }
    let length = postcard::experimental::serialized_size(&(STORAGE_FORMAT, &entry))
        .map_err(|e| error(subject.clone(), ErrorVerb::Write, e))?;
    if length > options.max_entry_bytes {
        return Err(error(
            subject,
            ErrorVerb::Write,
            "encoded entry exceeds configured entry byte limit",
        ));
    }
    Ok(PreparedEntry {
        encoded: encode(&entry, subject)?,
        entry,
    })
}

/// Only one entry is retained across chunk boundaries. API response vectors and
/// full-range read vectors are intentionally outside this bounded work buffer.
struct EntryChunks<I: Iterator<Item = Entry>> {
    input: std::iter::Peekable<I>,
    pending: Option<PreparedEntry>,
    previous: Option<u64>,
}

impl<I: Iterator<Item = Entry>> EntryChunks<I> {
    fn new(input: I) -> Self {
        Self {
            input: input.peekable(),
            pending: None,
            previous: None,
        }
    }

    fn next(
        &mut self,
        options: &StorageOptions,
        metadata: ChunkMetadata,
    ) -> Result<Option<Vec<PreparedEntry>>, StoreError> {
        let mut chunk = Vec::new();
        let state_mode = matches!(metadata, ChunkMetadata::State { .. });
        let (mut bytes, mut replaced_record_bytes) = match metadata {
            ChunkMetadata::Log => (12, 0),
            ChunkMetadata::State {
                encoded_record_bytes,
            } => (
                encoded_record_bytes
                    .checked_add(STATE_BATCH_OVERHEAD)
                    .ok_or_else(|| {
                        error(
                            ErrorSubject::StateMachine,
                            ErrorVerb::Write,
                            "state metadata size overflow",
                        )
                    })?,
                encoded_record_bytes,
            ),
        };
        while chunk.len() < options.io_chunk_entries {
            let next = match self.pending.take() {
                Some(entry) => entry,
                None => match self.input.next() {
                    Some(entry) => prepare_entry(entry, options)?,
                    None => break,
                },
            };
            let metadata_entry = state_mode
                && matches!(
                    &next.entry.payload,
                    openraft::EntryPayload::Normal(crate::types::Command::Metadata(_))
                );
            // Do not advance previous for lookahead: the next call consumes
            // this entry once, after the preceding chunk is durable.
            if metadata_entry && !chunk.is_empty() {
                self.pending = Some(next);
                break;
            }
            // The old state record is not written when this chunk replaces its
            // membership. Only subtract its native-write reservation: ownership
            // of prior state/input/encoded buffers is retained until consumed.
            let replaces_record =
                matches!(next.entry.payload, openraft::EntryPayload::Membership(_));
            let batch_bytes = if replaces_record {
                bytes - replaced_record_bytes
            } else {
                bytes
            };
            let entry_charge = if metadata_entry {
                crate::control::METADATA_EFFECT_RESERVATION
            } else {
                next.encoded.len()
            };
            let next_bytes = batch_bytes
                .checked_add(entry_charge)
                .and_then(|n| n.checked_add(ENTRY_BATCH_OVERHEAD))
                .ok_or_else(|| {
                    error(ErrorSubject::Logs, ErrorVerb::Write, "chunk size overflow")
                })?;
            if next_bytes > options.io_chunk_bytes {
                if chunk.is_empty() {
                    return Err(error(
                        ErrorSubject::Logs,
                        ErrorVerb::Write,
                        if metadata_entry {
                            "metadata effect reservation exceeds chunk byte budget"
                        } else {
                            "entry and metadata exceed chunk byte budget"
                        },
                    ));
                }
                self.pending = Some(next);
                break;
            }
            if let Some(previous) = self.previous
                && previous.checked_add(1) != Some(next.entry.log_id.index)
            {
                return Err(error(
                    ErrorSubject::Logs,
                    ErrorVerb::Write,
                    "nonconsecutive entries in input",
                ));
            }
            self.previous = Some(next.entry.log_id.index);
            if replaces_record {
                replaced_record_bytes = 0;
            }
            bytes = next_bytes;
            chunk.push(next);
            if metadata_entry {
                break;
            }
        }
        Ok((!chunk.is_empty()).then_some(chunk))
    }

    fn is_finished(&mut self) -> bool {
        self.pending.is_none() && self.input.peek().is_none()
    }
}
