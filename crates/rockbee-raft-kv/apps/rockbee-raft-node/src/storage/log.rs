//! The log store persists votes and logs; readers expose only read capability.
use super::{ChunkMetadata, Db, EntryChunks, Group, Lane, decode, encode, error, keys};
use crate::types::{Entry, LogId, StoreError, TypeConfig};
use openraft::{
    ErrorSubject, ErrorVerb, LogState, OptionalSend, RaftLogReader, Vote,
    storage::{LogFlushed, RaftLogStorage},
};
use rocksdb::{IteratorMode, ReadOptions, SnapshotWithThreadMode, WriteBatch};
use serde::{Serialize, de::DeserializeOwned};
use std::{fmt::Debug, ops::RangeBounds, sync::Arc};

pub struct LogStore {
    pub(super) group: Arc<Group>,
}
#[derive(Clone)]
pub struct LogReader {
    group: Arc<Group>,
}

impl LogReader {
    async fn entries(
        &self,
        range: Option<keys::LogRange>,
        limited: bool,
    ) -> Result<Vec<Entry>, StoreError> {
        let Some(range) = range else {
            return Ok(Vec::new());
        };
        self.group
            .read(ErrorSubject::Logs, move |group| {
                let snapshot = group.shared.db.snapshot();
                let mut options = ReadOptions::default();
                options.set_iterate_lower_bound(range.start);
                options.set_iterate_upper_bound(range.end);
                let mut result = Vec::new();
                let mut bytes = 0usize;
                for item in
                    snapshot.iterator_cf_opt(group.cf("raft_log")?, options, IteratorMode::Start)
                {
                    let (key, value) =
                        item.map_err(|e| error(ErrorSubject::Logs, ErrorVerb::Read, e))?;
                    let entry = decode_log_entry(group, &key, &value)?;
                    if limited
                        && !result.is_empty()
                        && (result.len() == group.shared.options.io_chunk_entries
                            || bytes.saturating_add(value.len())
                                > group.shared.options.io_chunk_bytes)
                    {
                        break;
                    }
                    bytes = bytes.saturating_add(value.len());
                    result.push(entry);
                }
                Ok(result)
            })
            .await
    }
}

impl RaftLogReader<TypeConfig> for LogReader {
    async fn try_get_log_entries<RB>(&mut self, range: RB) -> Result<Vec<Entry>, StoreError>
    where
        RB: RangeBounds<u64> + Clone + Debug + OptionalSend,
    {
        self.entries(keys::LogRange::new(self.group.id, range), false)
            .await
    }
    async fn limited_get_log_entries(
        &mut self,
        start: u64,
        end: u64,
    ) -> Result<Vec<Entry>, StoreError> {
        self.entries(keys::LogRange::new(self.group.id, start..end), true)
            .await
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB>(&mut self, range: RB) -> Result<Vec<Entry>, StoreError>
    where
        RB: RangeBounds<u64> + Clone + Debug + OptionalSend,
    {
        LogReader {
            group: self.group.clone(),
        }
        .try_get_log_entries(range)
        .await
    }
    async fn limited_get_log_entries(
        &mut self,
        start: u64,
        end: u64,
    ) -> Result<Vec<Entry>, StoreError> {
        LogReader {
            group: self.group.clone(),
        }
        .limited_get_log_entries(start, end)
        .await
    }
}

impl LogStore {
    async fn write_meta<T: Serialize>(
        &self,
        tag: u8,
        value: &T,
        subject: ErrorSubject<u64>,
    ) -> Result<(), StoreError> {
        let session = self.group.reserve(subject.clone()).await?;
        let encoded = encode(value, subject.clone())?;
        self.group
            .write(&session, Lane::Log, subject.clone(), move |group| {
                let mut batch = WriteBatch::default();
                batch.put_cf(group.cf("raft_meta")?, keys::meta(group.id, tag), encoded);
                group.persist(batch, subject)
            })
            .await?;
        session.complete();
        Ok(())
    }

    async fn read_meta<T: DeserializeOwned + Send + 'static>(
        &self,
        tag: u8,
        subject: ErrorSubject<u64>,
    ) -> Result<Option<T>, StoreError> {
        self.group
            .read(subject.clone(), move |group| {
                let snapshot = group.shared.db.snapshot();
                read_meta(group, &snapshot, tag, subject)
            })
            .await
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = LogReader;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StoreError> {
        self.group
            .read(ErrorSubject::Logs, |group| {
                log_state(group, &group.shared.db.snapshot())
            })
            .await
    }
    async fn get_log_reader(&mut self) -> LogReader {
        LogReader {
            group: self.group.clone(),
        }
    }
    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StoreError> {
        self.write_meta(keys::VOTE, vote, ErrorSubject::Vote).await
    }
    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StoreError> {
        self.read_meta(keys::VOTE, ErrorSubject::Vote).await
    }
    async fn save_committed(&mut self, committed: Option<LogId>) -> Result<(), StoreError> {
        self.write_meta(keys::COMMITTED, &committed, ErrorSubject::Logs)
            .await
    }
    async fn read_committed(&mut self) -> Result<Option<LogId>, StoreError> {
        Ok(self
            .read_meta::<Option<LogId>>(keys::COMMITTED, ErrorSubject::Logs)
            .await?
            .flatten())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StoreError>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut completion = Some(AppendCompletion(Some(callback)));
        let session = self.group.reserve(ErrorSubject::Logs).await?;
        let mut chunks = EntryChunks::new(entries.into_iter());
        loop {
            let chunk = chunks
                .next(&self.group.shared.options, ChunkMetadata::Log)?
                .unwrap_or_default();
            let finished = chunks.is_finished();
            let final_callback = if finished { completion.take() } else { None };
            self.group
                .write(&session, Lane::Log, ErrorSubject::Logs, move |group| {
                    #[cfg(all(test, feature = "diagnostics"))]
                    let mut failure = None;
                    let result = append_chunk(
                        group,
                        chunk,
                        #[cfg(all(test, feature = "diagnostics"))]
                        final_callback.is_some(),
                        #[cfg(all(test, feature = "diagnostics"))]
                        &mut failure,
                    );
                    if let Some(callback) = final_callback {
                        callback.complete(&result);
                        #[cfg(all(test, feature = "diagnostics"))]
                        if let Some(failure) = failure {
                            failure.record_failed_callback();
                        }
                    }
                    result
                })
                .await?;
            if finished {
                break;
            }
        }
        session.complete();
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId) -> Result<(), StoreError> {
        let session = self.group.reserve(ErrorSubject::Logs).await?;
        self.group
            .write(&session, Lane::Log, ErrorSubject::Logs, move |group| {
                let mut batch = WriteBatch::default();
                batch.delete_range_cf(
                    group.cf("raft_log")?,
                    keys::log(group.id, log_id.index),
                    keys::log_end(group.id),
                );
                group.persist(batch, ErrorSubject::Logs)
            })
            .await?;
        session.complete();
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId) -> Result<(), StoreError> {
        let session = self.group.reserve(ErrorSubject::Logs).await?;
        self.group
            .write(&session, Lane::Log, ErrorSubject::Logs, move |group| {
                let snapshot = group.shared.db.snapshot();
                let old: Option<LogId> =
                    read_meta(group, &snapshot, keys::PURGED, ErrorSubject::Logs)?;
                if old.is_some_and(|old| old.index > log_id.index) {
                    return Err(error(
                        ErrorSubject::Logs,
                        ErrorVerb::Delete,
                        "purge marker cannot move backwards",
                    ));
                }
                let mut batch = WriteBatch::default();
                batch.delete_range_cf(
                    group.cf("raft_log")?,
                    keys::log(group.id, 0),
                    keys::after_log(group.id, log_id.index),
                );
                batch.put_cf(
                    group.cf("raft_meta")?,
                    keys::meta(group.id, keys::PURGED),
                    encode(&log_id, ErrorSubject::Logs)?,
                );
                group.persist(batch, ErrorSubject::Logs)
            })
            .await?;
        session.complete();
        Ok(())
    }
}

fn read_meta<T: DeserializeOwned>(
    group: &Group,
    snapshot: &SnapshotWithThreadMode<'_, Db>,
    tag: u8,
    subject: ErrorSubject<u64>,
) -> Result<Option<T>, StoreError> {
    snapshot
        .get_cf(group.cf("raft_meta")?, keys::meta(group.id, tag))
        .map_err(|e| error(subject.clone(), ErrorVerb::Read, e))?
        .map(|bytes| decode(&bytes, subject))
        .transpose()
}

fn log_state(
    group: &Group,
    snapshot: &SnapshotWithThreadMode<'_, Db>,
) -> Result<LogState<TypeConfig>, StoreError> {
    let last_purged_log_id: Option<LogId> =
        read_meta(group, snapshot, keys::PURGED, ErrorSubject::Logs)?;
    let mut options = ReadOptions::default();
    options.set_iterate_lower_bound(keys::log(group.id, 0));
    options.set_iterate_upper_bound(keys::log_end(group.id));
    let tail = snapshot
        .iterator_cf_opt(group.cf("raft_log")?, options, IteratorMode::End)
        .next()
        .transpose()
        .map_err(|e| error(ErrorSubject::Logs, ErrorVerb::Read, e))?;
    let last_log_id = tail
        .map(|(key, bytes)| decode_log_entry(group, &key, &bytes).map(|entry| entry.log_id))
        .transpose()?
        .or(last_purged_log_id);
    Ok(LogState {
        last_purged_log_id,
        last_log_id,
    })
}

fn decode_log_entry(group: &Group, key: &[u8], value: &[u8]) -> Result<Entry, StoreError> {
    if key.len() != 16
        || !key.starts_with(&group.id.to_be_bytes())
        || value.len() > group.shared.options.max_entry_bytes
    {
        return Err(error(
            ErrorSubject::Logs,
            ErrorVerb::Read,
            "invalid log key or oversized persisted entry",
        ));
    }
    let entry: Entry = decode(value, ErrorSubject::Logs)?;
    if key[8..] != entry.log_id.index.to_be_bytes() {
        return Err(error(
            ErrorSubject::Logs,
            ErrorVerb::Read,
            "entry identity disagrees with log key",
        ));
    }
    if let openraft::EntryPayload::Normal(command) = &entry.payload {
        command
            .validate()
            .map_err(|e| error(ErrorSubject::Logs, ErrorVerb::Read, e))?;
    }
    Ok(entry)
}

fn append_chunk(
    group: &Group,
    entries: Vec<super::PreparedEntry>,
    #[cfg(all(test, feature = "diagnostics"))] final_chunk: bool,
    #[cfg(all(test, feature = "diagnostics"))] claimed_failure: &mut Option<
        Arc<super::AppendFailure>,
    >,
) -> Result<(), StoreError> {
    let snapshot = group.shared.db.snapshot();
    let state = log_state(group, &snapshot)?;
    // Recovery may present a stale prefix already covered by the durable purge
    // marker. It is a no-op, never permission to resurrect removed history.
    if let Some(purged) = state.last_purged_log_id
        && entries
            .iter()
            .any(|entry| entry.entry.log_id.index == purged.index && entry.entry.log_id != purged)
    {
        return Err(error(
            ErrorSubject::Logs,
            ErrorVerb::Write,
            "append conflicts with durable purge identity",
        ));
    }
    let mut entries = entries
        .into_iter()
        .filter(|entry| {
            state
                .last_purged_log_id
                .is_none_or(|purged| entry.entry.log_id.index > purged.index)
        })
        .peekable();
    if let Some(first) = entries.peek() {
        let first_index = first.entry.log_id.index;
        if state.last_log_id.is_some_and(|tail| {
            first_index > tail.index && tail.index.checked_add(1) != Some(first_index)
        }) {
            return Err(error(
                ErrorSubject::Logs,
                ErrorVerb::Write,
                "append would create a log hole",
            ));
        }
    }
    let mut batch = WriteBatch::default();
    #[cfg(all(test, feature = "diagnostics"))]
    let armed = group
        .append_failure
        .lock()
        .expect("append failure lock")
        .clone();
    #[cfg(all(test, feature = "diagnostics"))]
    let mut matching = None;
    for prepared in entries {
        #[cfg(all(test, feature = "diagnostics"))]
        if let Some(log_id) = armed
            .as_ref()
            .and_then(|f| f.matching_log_id(&prepared.entry))
        {
            assert!(
                matching.replace(log_id).is_none(),
                "one target PUT per batch"
            );
        }
        batch.put_cf(
            group.cf("raft_log")?,
            keys::log(group.id, prepared.entry.log_id.index),
            prepared.encoded,
        );
    }
    #[cfg(all(test, feature = "diagnostics"))]
    if let (Some(failure), Some(log_id)) = (armed, matching) {
        assert!(
            final_chunk,
            "targeted append failure requires the final chunk"
        );
        assert!(batch.size_in_bytes() <= group.shared.options.io_chunk_bytes);
        if failure.arm(group, log_id) {
            *claimed_failure = Some(failure);
        }
    }
    let result = group.persist(batch, ErrorSubject::Logs);
    #[cfg(all(test, feature = "diagnostics"))]
    if let Some(failure) = claimed_failure {
        failure.record_persist_result(&result);
    }
    result
}

struct AppendCompletion(Option<LogFlushed<TypeConfig>>);
impl AppendCompletion {
    fn complete(mut self, result: &Result<(), StoreError>) {
        if let Some(callback) = self.0.take() {
            callback.log_io_completed(
                result
                    .as_ref()
                    .map(|_| ())
                    .map_err(|e| std::io::Error::other(e.to_string())),
            );
        }
    }
}
impl Drop for AppendCompletion {
    fn drop(&mut self) {
        if let Some(callback) = self.0.take() {
            callback.log_io_completed(Err(std::io::Error::other(
                "append failed or was abandoned before durable completion",
            )));
        }
    }
}
