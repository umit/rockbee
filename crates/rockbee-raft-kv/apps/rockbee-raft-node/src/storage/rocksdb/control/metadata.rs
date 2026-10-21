//! One coherent metadata snapshot and one synchronous durable apply batch.
//!
//! Apply runs inside the existing admitted native write closure. The coherent
//! StateReader also uses load_view from its admitted snapshot read. Neither
//! keeps a process cache or a second encoded copy of the job collection.
use std::collections::BTreeMap;

use ::rocksdb::{SnapshotWithThreadMode, WriteBatch};
use openraft::{EntryPayload, ErrorSubject, ErrorVerb};
use rockbee_raft_protocol::{GroupId, routing::ClusterDirectory};
use serde::de::DeserializeOwned;

use crate::storage::rocksdb::{Db, Group, PreparedEntry, encode, error, keys, state::StateRecord};
use crate::{
    control::{
        DIRECTORY_KEY, JOB_KEY_PREFIX, MAX_DIRECTORY_BYTES, MetadataCommand, MetadataView,
        ORIGIN_KEY, StoredMoves, decode_stored_moves,
    },
    types::{ApplyResult, Command, StoreError},
};

pub(in crate::storage::rocksdb) fn apply(
    group: &Group,
    mut state: StateRecord,
    prepared: PreparedEntry,
) -> Result<(StateRecord, Vec<ApplyResult>), StoreError> {
    if group.id != 0 {
        return Err(write_error("metadata commands require group zero"));
    }
    let entry = prepared.entry;
    if state
        .applied
        .is_some_and(|applied| entry.log_id.index <= applied.index)
    {
        return Err(write_error(
            "state apply cannot replay or move applied progress backwards",
        ));
    }
    let EntryPayload::Normal(Command::Metadata(command)) = entry.payload else {
        return Err(write_error("standalone metadata chunk has another payload"));
    };
    if let MetadataCommand::InitializeDirectoryIfAbsent { initial_directory } = command.as_ref() {
        validate_identity(group, initial_directory)?;
    }
    let snapshot = group.shared.db.snapshot();
    let mut view = load_view(group, &snapshot, state.generation)?;
    let transition = view.apply(*command).map_err(write_error)?;
    let cf = group.cf("state")?;
    let mut batch = WriteBatch::default();
    for effect in transition.effects {
        batch.put_cf(
            cf,
            keys::data(group.id, state.generation, &effect.key),
            effect.value,
        );
    }
    state.applied = Some(entry.log_id);
    batch.put_cf(
        cf,
        keys::meta(group.id, 0),
        encode(&state, ErrorSubject::StateMachine)?,
    );
    // Group::persist checks the final native batch size before its synchronous
    // write. EntryChunks already reserved expanded effects and StateRecord.
    group.persist(batch, ErrorSubject::StateMachine)?;
    Ok((state, vec![ApplyResult::Metadata(transition.result)]))
}

pub(in crate::storage::rocksdb) fn load_view(
    group: &Group,
    snapshot: &SnapshotWithThreadMode<'_, Db>,
    generation: u64,
) -> Result<MetadataView, StoreError> {
    let current: Option<ClusterDirectory> = read_value(
        group,
        snapshot,
        generation,
        DIRECTORY_KEY,
        MAX_DIRECTORY_BYTES,
    )?;
    let origin: Option<ClusterDirectory> =
        read_value(group, snapshot, generation, ORIGIN_KEY, MAX_DIRECTORY_BYTES)?;
    for directory in current.iter().chain(origin.iter()) {
        validate_identity(group, directory)?;
    }
    let limit = current
        .as_ref()
        .map_or(0, |directory| usize::from(directory.shard_config.count()));
    let jobs = read_jobs(group, snapshot, generation, limit)?;
    MetadataView::from_stored_parts(current, origin, jobs).map_err(write_error)
}

fn read_value<T: DeserializeOwned>(
    group: &Group,
    snapshot: &SnapshotWithThreadMode<'_, Db>,
    generation: u64,
    logical_key: &[u8],
    limit: usize,
) -> Result<Option<T>, StoreError> {
    let value = snapshot
        .get_pinned_cf(
            group.cf("state")?,
            keys::data(group.id, generation, logical_key),
        )
        .map_err(write_error)?;
    value
        .as_deref()
        .map(|bytes| decode_value(bytes, limit))
        .transpose()
}

fn read_jobs(
    group: &Group,
    snapshot: &SnapshotWithThreadMode<'_, Db>,
    generation: u64,
    limit: usize,
) -> Result<BTreeMap<GroupId, StoredMoves>, StoreError> {
    let prefix = keys::data(group.id, generation, JOB_KEY_PREFIX);
    let mut iterator = snapshot.raw_iterator_cf(group.cf("state")?);
    iterator.seek(&prefix);
    let mut jobs = BTreeMap::new();
    while iterator.valid() {
        let key = iterator
            .key()
            .ok_or_else(|| write_error("metadata iterator has no key"))?;
        if !key.starts_with(&prefix) {
            break;
        }
        if jobs.len() >= limit {
            return Err(write_error("stored move count exceeds configured groups"));
        }
        let suffix: [u8; 8] = key[prefix.len()..]
            .try_into()
            .map_err(|_| write_error("invalid move key suffix"))?;
        let group_id = GroupId::new(u64::from_be_bytes(suffix));
        let value = iterator
            .value()
            .ok_or_else(|| write_error("metadata iterator has no value"))?;
        // Bounds are checked while the native iterator lends its bytes, before
        // serde decodes or the iterator advances; no serialized Vec is made.
        let record = decode_stored_moves(value).map_err(write_error)?;
        jobs.insert(group_id, record);
        iterator.next();
    }
    iterator.status().map_err(write_error)?;
    Ok(jobs)
}

fn decode_value<T: DeserializeOwned>(bytes: &[u8], limit: usize) -> Result<T, StoreError> {
    if bytes.len() > limit {
        return Err(write_error("stored metadata value exceeds encoded limit"));
    }
    rockbee_raft_protocol::decode_frame(bytes).map_err(write_error)
}

fn validate_identity(group: &Group, directory: &ClusterDirectory) -> Result<(), StoreError> {
    let identity = &group.shared.identity;
    if directory.cluster_id != identity.cluster_id
        || directory.shard_config != identity.shard_config
    {
        return Err(write_error(
            "metadata directory differs from storage identity",
        ));
    }
    Ok(())
}

fn write_error(message: impl std::fmt::Display) -> StoreError {
    error(ErrorSubject::StateMachine, ErrorVerb::Write, message)
}
