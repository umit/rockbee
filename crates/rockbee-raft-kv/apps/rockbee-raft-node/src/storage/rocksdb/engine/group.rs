//! Per-group write admission and execution of blocking RocksDB work.
//!
//! `reserve` owns a write session, `write` schedules its blocking work, and
//! `persist` calls RocksDB. Completing the session keeps the group writable;
//! dropping an incomplete session retires it until reopen/recovery.
#[cfg(all(test, feature = "diagnostics"))]
use crate::storage::rocksdb::AppendFailure;
#[cfg(test)]
use crate::storage::rocksdb::{JointApplyPause, snapshot};
use crate::storage::rocksdb::{Shared, engine::sync_options, error};
use crate::{diagnostics::Span, types::StoreError};
use ::log::{debug, warn};
use ::rocksdb::{ColumnFamily, WriteBatch};
use openraft::{ErrorSubject, ErrorVerb};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Shared owner for one Raft group, including its write and membership gates.
pub(in crate::storage::rocksdb) struct Group {
    pub(in crate::storage::rocksdb) id: u64,
    pub(in crate::storage::rocksdb) shared: Arc<Shared>,
    pub(in crate::storage::rocksdb) gate: Arc<Semaphore>,
    pub(in crate::storage::rocksdb) membership_gate: Arc<Semaphore>,
    pub(in crate::storage::rocksdb) retired: AtomicBool,
    pub(in crate::storage::rocksdb) publication: Mutex<()>,
    pub(in crate::storage::rocksdb) live_images: Mutex<std::collections::HashSet<u64>>,
    #[cfg(test)]
    pub(in crate::storage::rocksdb) snapshot_hooks:
        Mutex<std::collections::VecDeque<(snapshot::Stage, TestWriteAction)>>,
    #[cfg(test)]
    pub(in crate::storage::rocksdb) snapshot_peak_batch: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    pub(in crate::storage::rocksdb) snapshot_peak_records: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    pub(in crate::storage::rocksdb) snapshot_peak_record_bytes: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    pub(in crate::storage::rocksdb) test_writes: Mutex<std::collections::VecDeque<TestWriteAction>>,
    #[cfg(test)]
    pub(in crate::storage::rocksdb) joint_apply_pause: Mutex<Option<Arc<JointApplyPause>>>,
    #[cfg(all(test, feature = "diagnostics"))]
    pub(in crate::storage::rocksdb) append_failure: Mutex<Option<Arc<AppendFailure>>>,
}

#[derive(Clone, Copy)]
pub(in crate::storage::rocksdb) enum Lane {
    Log,
    State,
    Snapshot,
}

/// An admitted write: abandoning it retires future writes for this group.
pub(in crate::storage::rocksdb) struct WriteSession {
    group: Arc<Group>,
    permit: Arc<OwnedSemaphorePermit>,
    completed: bool,
}
impl WriteSession {
    pub(in crate::storage::rocksdb) fn complete(mut self) {
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
    pub(in crate::storage::rocksdb) fn new(id: u64, shared: Arc<Shared>) -> Self {
        Self {
            id,
            shared,
            gate: Arc::new(Semaphore::new(1)),
            membership_gate: Arc::new(Semaphore::new(1)),
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
            #[cfg(all(test, feature = "diagnostics"))]
            append_failure: Mutex::new(None),
        }
    }
    pub(in crate::storage::rocksdb) fn cf(&self, name: &str) -> Result<&ColumnFamily, StoreError> {
        self.shared.db.cf_handle(name).ok_or_else(|| {
            error(
                ErrorSubject::Store,
                ErrorVerb::Read,
                format!("missing {name} column family"),
            )
        })
    }

    pub(in crate::storage::rocksdb) async fn reserve(
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

    /// Run a RocksDB read on a bounded blocking worker, retaining its owner.
    pub(in crate::storage::rocksdb) async fn read<T: Send + 'static>(
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

    /// Schedule native work; cancellation cannot release its lane or write gate early.
    pub(in crate::storage::rocksdb) async fn write<T: Send + 'static>(
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

    /// The actual RocksDB batch write. Call only inside the native worker.
    pub(in crate::storage::rocksdb) fn persist(
        &self,
        batch: WriteBatch,
        subject: ErrorSubject<u64>,
    ) -> Result<(), StoreError> {
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
pub(in crate::storage::rocksdb) enum TestWriteAction {
    Pass,
    Fail,
    Hold {
        entered: tokio::sync::oneshot::Sender<()>,
        resume: std::sync::mpsc::Receiver<()>,
    },
}
