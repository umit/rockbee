//! Atomic committed state chunks and coherent, explicitly local read views.
use super::{
    ChunkMetadata, Db, EntryChunks, Group, Lane, STATE_RECORD_OVERHEAD, SnapshotBuilder, decode,
    encode, error, keys, snapshot,
};
use crate::types::{ApplyResult, Command, Entry, LogId, StoreError, StoredMembership, TypeConfig};
use bytes::Bytes;
use openraft::{
    EntryPayload, ErrorSubject, ErrorVerb, OptionalSend, Snapshot, SnapshotMeta,
    storage::RaftStateMachine,
};
use rocksdb::{Direction, IteratorMode, SnapshotWithThreadMode, WriteBatch};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub struct StateMachine {
    pub(super) group: Arc<Group>,
}
#[derive(Clone)]
pub struct StateReader {
    group: Arc<Group>,
}

#[derive(Debug)]
pub struct StateRead {
    pub generation: u64,
    pub applied: Option<LogId>,
    pub value: Option<Bytes>,
}

pub(crate) struct MetadataRead {
    pub applied: Option<LogId>,
    pub view: crate::control::MetadataView,
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub(super) struct StateRecord {
    pub generation: u64,
    pub applied: Option<LogId>,
    pub membership: StoredMembership,
}

impl StateMachine {
    pub fn reader(&self) -> StateReader {
        StateReader {
            group: self.group.clone(),
        }
    }
}

impl StateReader {
    #[cfg(test)]
    #[allow(clippy::unwrap_used)]
    pub(crate) fn pause_next_joint_apply_return(
        &self,
        expected: Vec<std::collections::BTreeSet<u64>>,
    ) -> Arc<super::JointApplyPause> {
        assert_eq!(
            expected.len(),
            2,
            "pause requires one exact joint configuration"
        );
        let pause = Arc::new(super::JointApplyPause::new(expected));
        let mut slot = self.group.joint_apply_pause.lock().unwrap();
        assert!(
            slot.is_none(),
            "one pause may be armed per actual group instance"
        );
        *slot = Some(Arc::clone(&pause));
        pause
    }

    /// Applied identity and membership from one coherent native snapshot.
    /// Callers establish current group authority before trusting this local state.
    pub(crate) async fn read_membership(
        &self,
    ) -> Result<(Option<LogId>, StoredMembership), StoreError> {
        self.group
            .read(ErrorSubject::StateMachine, |group| {
                let state = read_record(group, &group.shared.db.snapshot())?;
                Ok((state.applied, state.membership))
            })
            .await
    }

    /// Reads identity and the bounded validated metadata view from one snapshot.
    /// The caller must establish fresh group-zero authority before using it.
    pub(crate) async fn read_metadata(&self) -> Result<MetadataRead, StoreError> {
        self.group
            .read(ErrorSubject::StateMachine, |group| {
                if group.id != 0 {
                    return Err(error(
                        ErrorSubject::StateMachine,
                        ErrorVerb::Read,
                        "metadata reads require group zero",
                    ));
                }
                let snapshot = group.shared.db.snapshot();
                let state = read_record(group, &snapshot)?;
                let view = super::metadata::load_view(group, &snapshot, state.generation)?;
                Ok(MetadataRead {
                    applied: state.applied,
                    view,
                })
            })
            .await
    }

    /// Returns a coherent local view. A Raft authority barrier is required for strong GETs.
    pub async fn read_local(&self, key: Bytes) -> Result<StateRead, StoreError> {
        if key.is_empty() || key.len() > rockbee_raft_protocol::MAX_KEY_BYTES {
            return Err(error(
                ErrorSubject::StateMachine,
                ErrorVerb::Read,
                "invalid local read key length",
            ));
        }
        self.group
            .read(ErrorSubject::StateMachine, move |group| {
                let snapshot = group.shared.db.snapshot();
                let state = read_record(group, &snapshot)?;
                let value = snapshot
                    .get_cf(
                        group.cf("state")?,
                        keys::data(group.id, state.generation, &key),
                    )
                    .map_err(|e| error(ErrorSubject::StateMachine, ErrorVerb::Read, e))?
                    .map(Bytes::from);
                Ok(StateRead {
                    generation: state.generation,
                    applied: state.applied,
                    value,
                })
            })
            .await
    }
}

impl RaftStateMachine<TypeConfig> for StateMachine {
    type SnapshotBuilder = SnapshotBuilder;

    async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMembership), StoreError> {
        self.group
            .read(ErrorSubject::StateMachine, |group| {
                let state = read_record(group, &group.shared.db.snapshot())?;
                Ok((state.applied, state.membership))
            })
            .await
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<ApplyResult>, StoreError>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let session = self.group.reserve(ErrorSubject::StateMachine).await?;
        let mut state = self
            .group
            .read(ErrorSubject::StateMachine, |group| {
                read_record(group, &group.shared.db.snapshot())
            })
            .await?;
        let mut chunks = EntryChunks::new(entries.into_iter());
        let mut responses = Vec::new();
        loop {
            // Existing membership and conservative record/batch overhead are
            // counted before admitting the next bounded chunk.
            let metadata = ChunkMetadata::State {
                encoded_record_bytes: encode(&state, ErrorSubject::StateMachine)?.len(),
            };
            let Some(chunk) = chunks.next(&self.group.shared.options, metadata)? else {
                break;
            };
            let (next_state, results) = self
                .group
                .write(
                    &session,
                    Lane::State,
                    ErrorSubject::StateMachine,
                    move |group| apply_chunk(group, state, chunk),
                )
                .await?;
            state = next_state;
            responses.extend(results);
        }
        session.complete();
        #[cfg(test)]
        super::joint_apply_pause::after_apply(&self.group, &state).await;
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> SnapshotBuilder {
        SnapshotBuilder {
            group: self.group.clone(),
        }
    }
    async fn begin_receiving_snapshot(&mut self) -> Result<Box<tokio::fs::File>, StoreError> {
        snapshot::begin_receive(&self.group).await
    }
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, openraft::BasicNode>,
        snapshot: Box<tokio::fs::File>,
    ) -> Result<(), StoreError> {
        snapshot::install(&self.group, meta.clone(), *snapshot).await
    }
    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, StoreError> {
        snapshot::current(&self.group).await
    }
}

pub(super) fn read_record(
    group: &Group,
    snapshot: &SnapshotWithThreadMode<'_, Db>,
) -> Result<StateRecord, StoreError> {
    let cf = group.cf("state")?;
    if let Some(value) = snapshot
        .get_cf(cf, keys::meta(group.id, 0))
        .map_err(|e| error(ErrorSubject::StateMachine, ErrorVerb::Read, e))?
    {
        if value.len()
            > group
                .shared
                .options
                .max_entry_bytes
                .saturating_add(STATE_RECORD_OVERHEAD)
        {
            return Err(error(
                ErrorSubject::StateMachine,
                ErrorVerb::Read,
                "oversized state record",
            ));
        }
        return decode(&value, ErrorSubject::StateMachine);
    }
    let prefix = keys::meta(group.id, 1);
    if let Some(item) = snapshot
        .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward))
        .next()
    {
        let (key, _) = item.map_err(|e| error(ErrorSubject::StateMachine, ErrorVerb::Read, e))?;
        if key.starts_with(&prefix) {
            return Err(error(
                ErrorSubject::StateMachine,
                ErrorVerb::Read,
                "orphan generation data without a state record",
            ));
        }
    }
    Ok(StateRecord::default())
}

fn apply_chunk(
    group: &Group,
    mut state: StateRecord,
    entries: Vec<super::PreparedEntry>,
) -> Result<(StateRecord, Vec<ApplyResult>), StoreError> {
    if entries.len() == 1
        && matches!(
            &entries[0].entry.payload,
            EntryPayload::Normal(Command::Metadata(_))
        )
    {
        let prepared = entries.into_iter().next().ok_or_else(|| {
            error(
                ErrorSubject::StateMachine,
                ErrorVerb::Write,
                "missing standalone metadata entry",
            )
        })?;
        return super::metadata::apply(group, state, prepared);
    }
    let cf = group.cf("state")?;
    let mut batch = WriteBatch::default();
    let mut results = Vec::with_capacity(entries.len());
    for prepared in entries {
        let entry = prepared.entry;
        if state
            .applied
            .is_some_and(|applied| entry.log_id.index <= applied.index)
        {
            return Err(error(
                ErrorSubject::Apply(entry.log_id),
                ErrorVerb::Write,
                "state apply cannot replay or move applied progress backwards",
            ));
        }
        match entry.payload {
            EntryPayload::Blank => {}
            EntryPayload::Membership(membership) => {
                state.membership = StoredMembership::new(Some(entry.log_id), membership)
            }
            EntryPayload::Normal(Command::Put { key, value }) => {
                batch.put_cf(cf, keys::data(group.id, state.generation, &key), value)
            }
            EntryPayload::Normal(Command::Delete { key }) => {
                batch.delete_cf(cf, keys::data(group.id, state.generation, &key))
            }
            EntryPayload::Normal(Command::Metadata(_)) => {
                return Err(error(
                    ErrorSubject::StateMachine,
                    ErrorVerb::Write,
                    "metadata entry must occupy its own apply chunk",
                ));
            }
        }
        state.applied = Some(entry.log_id);
        results.push(ApplyResult::Applied);
    }
    batch.put_cf(
        cf,
        keys::meta(group.id, 0),
        encode(&state, ErrorSubject::StateMachine)?,
    );
    group.persist(batch, ErrorSubject::StateMachine)?;
    Ok((state, results))
}
