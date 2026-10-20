//! Streamed immutable images and atomic generation activation. Files become
//! durable before their RocksDB descriptor; incomplete imports stay invisible.
mod format;
#[cfg(test)]
mod process_tests;
#[cfg(test)]
mod tests;
use self::format::{Fields, Header, ImageReader, ImageWriter, Meta, Summary};
use super::{
    Group, Lane, Shared, StorageIdentity, decode, encode, error, keys,
    state::{self, StateRecord},
};
use crate::types::{LogId, StoreError, StoredMembership, TypeConfig};
use log::{debug, info};
use openraft::{ErrorSubject, ErrorVerb, RaftSnapshotBuilder, Snapshot};
use rocksdb::{Direction, IteratorMode, WriteBatch};
use std::{
    cmp::Ordering,
    fs::{self, File, OpenOptions},
    io::{Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::Arc,
};

const CURRENT: u8 = 0;
const SEQUENCE: u8 = 1;
const DESCRIPTOR_MAGIC: &[u8; 8] = b"RBRDESC1";

#[derive(Clone)]
pub struct SnapshotBuilder {
    pub(super) group: Arc<Group>,
}

#[derive(Clone)]
struct Descriptor {
    image: u64,
    generation: u64,
    header: Header,
    summary: Summary,
}

/// A live build can wait for publication without retaining the scan lane. Its
/// image remains protected from GC until the waiter or native owner drops it.
struct ImageLease {
    group: Arc<Group>,
    image: u64,
}
impl ImageLease {
    fn new(group: Arc<Group>, image: u64) -> Result<Self, StoreError> {
        group.live_images.lock().map_err(fail)?.insert(image);
        Ok(Self { group, image })
    }
}
impl Drop for ImageLease {
    fn drop(&mut self) {
        if let Ok(mut images) = self.group.live_images.lock() {
            images.remove(&self.image);
        }
    }
}
struct PreparedImage {
    descriptor: Descriptor,
    lease: ImageLease,
}

fn fail(message: impl std::fmt::Display) -> StoreError {
    error(ErrorSubject::Snapshot(None), ErrorVerb::Write, message)
}
fn header_for(group: &Group, state: &StateRecord, image: u64) -> Header {
    let identity = &group.shared.identity;
    Header {
        cluster: identity.cluster_id.get(),
        group: group.id,
        hash_version: identity.shard_config.version(),
        shards: identity.shard_config.count(),
        seed: identity.shard_config.seed(),
        meta: Meta {
            last_log_id: state.applied,
            last_membership: state.membership.clone(),
            snapshot_id: format!("{}-{}-{image}", identity.node_id.get(), group.id),
        },
    }
}
fn validate_identity(
    header: &Header,
    group: u64,
    identity: &StorageIdentity,
) -> Result<(), StoreError> {
    if header.cluster != identity.cluster_id.get()
        || header.group != group
        || header.hash_version != identity.shard_config.version()
        || header.shards != identity.shard_config.count()
        || header.seed != identity.shard_config.seed()
    {
        return Err(fail(
            "snapshot cluster, group or hash configuration mismatch",
        ));
    }
    Ok(())
}

impl RaftSnapshotBuilder<TypeConfig> for SnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StoreError> {
        debug!(
            "raft_snapshot_build_start node={} group={}",
            self.group.shared.identity.node_id.get(),
            self.group.id
        );
        let session = self.group.reserve(ErrorSubject::Snapshot(None)).await?;
        let image = self
            .group
            .write(
                &session,
                Lane::Log,
                ErrorSubject::Snapshot(None),
                reserve_sequence,
            )
            .await?;
        let lease = ImageLease::new(self.group.clone(), image)?;
        session.complete();
        let prepared = scan_job(&self.group, move |_| write_image(lease)).await?;
        // Critical lock order: the scan job has ended and released its lane.
        // Installers may hold the group gate while waiting for that same lane.
        #[cfg(test)]
        {
            let group = self.group.clone();
            tokio::task::spawn_blocking(move || checkpoint(&group, Stage::BeforePublication))
                .await
                .map_err(fail)??;
        }
        let session = self.group.reserve(ErrorSubject::Snapshot(None)).await?;
        let selected = self
            .group
            .write(
                &session,
                Lane::Snapshot,
                ErrorSubject::Snapshot(None),
                move |group| publish_image(group, prepared),
            )
            .await?;
        session.complete();
        info!(
            "raft_snapshot_published node={} group={}",
            self.group.shared.identity.node_id.get(),
            self.group.id
        );
        Ok(as_snapshot(selected))
    }
}

async fn scan_job<T: Send + 'static>(
    group: &Arc<Group>,
    work: impl FnOnce(&Group) -> Result<T, StoreError> + Send + 'static,
) -> Result<T, StoreError> {
    let lane = group
        .shared
        .snapshot_lane
        .clone()
        .acquire_owned()
        .await
        .map_err(fail)?;
    let group = group.clone();
    tokio::task::spawn_blocking(move || {
        let _lane = lane;
        work(&group)
    })
    .await
    .map_err(fail)?
}

pub(super) async fn begin_receive(group: &Arc<Group>) -> Result<Box<tokio::fs::File>, StoreError> {
    let session = group.reserve(ErrorSubject::Snapshot(None)).await?;
    let file = group
        .write(
            &session,
            Lane::Snapshot,
            ErrorSubject::Snapshot(None),
            |group| {
                let image = reserve_sequence(group)?;
                let directory = ensure_directory(group)?;
                let path = directory.join(format!("receive-{image:016x}.partial"));
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(&path)
                    .map_err(fail)?;
                // Unix open handles survive unlink. Receiving owns this handle only;
                // install always copies the actual passed handle into its own image.
                fs::remove_file(path).map_err(fail)?;
                sync_directory(&directory)?;
                Ok(file)
            },
        )
        .await?;
    session.complete();
    Ok(Box::new(tokio::fs::File::from_std(file)))
}

pub(super) async fn install(
    group: &Arc<Group>,
    meta: Meta,
    file: tokio::fs::File,
) -> Result<(), StoreError> {
    let session = group.reserve(ErrorSubject::Snapshot(None)).await?;
    let file = file.into_std().await;
    group
        .write(
            &session,
            Lane::Snapshot,
            ErrorSubject::Snapshot(None),
            move |group| validate_and_import(group, &meta, file),
        )
        .await?;
    session.complete();
    Ok(())
}

pub(super) async fn current(
    group: &Arc<Group>,
) -> Result<Option<Snapshot<TypeConfig>>, StoreError> {
    let selected = scan_job(group, |group| {
        let selected = {
            let _publication = group.publication.lock().map_err(fail)?;
            open_current(group)?
        };
        if let Some((descriptor, file)) = &selected {
            validate_image(group, descriptor, file.try_clone().map_err(fail)?, false)?;
        }
        if let Some((_, file)) = &selected {
            file.try_clone()
                .map_err(fail)?
                .seek(SeekFrom::Start(0))
                .map_err(fail)?;
        }
        Ok(selected)
    })
    .await?;
    Ok(selected.map(as_snapshot))
}

fn as_snapshot((descriptor, file): (Descriptor, File)) -> Snapshot<TypeConfig> {
    Snapshot {
        meta: descriptor.header.meta,
        snapshot: Box::new(tokio::fs::File::from_std(file)),
    }
}
fn directory(group: &Group) -> PathBuf {
    group
        .shared
        .path
        .join("snapshots")
        .join(format!("group-{:016x}", group.id))
}
fn final_path(group: &Group, image: u64) -> PathBuf {
    directory(group).join(format!("image-{image:016x}.snap"))
}
fn partial_path(group: &Group, image: u64) -> PathBuf {
    directory(group).join(format!("image-{image:016x}.partial"))
}
fn sync_directory(path: &Path) -> Result<(), StoreError> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(fail)
}
fn ensure_directory(group: &Group) -> Result<PathBuf, StoreError> {
    let root = &group.shared.path;
    if let Some(parent) = root.parent() {
        let parent = if parent.as_os_str().is_empty() {
            Path::new(".")
        } else {
            parent
        };
        sync_directory(parent)?;
    }
    let snapshots = root.join("snapshots");
    for (path, parent) in [(&snapshots, root), (&directory(group), &snapshots)] {
        match fs::create_dir(path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists && path.is_dir() => {}
            Err(e) => return Err(fail(e)),
        }
        sync_directory(parent)?;
    }
    Ok(directory(group))
}
fn reserve_sequence(group: &Group) -> Result<u64, StoreError> {
    let cf = group.cf("snapshot_meta")?;
    let previous = group
        .shared
        .db
        .get_cf(cf, keys::meta(group.id, SEQUENCE))
        .map_err(fail)?
        .map(|bytes| decode::<u64>(&bytes, ErrorSubject::Snapshot(None)))
        .transpose()?
        .unwrap_or(0);
    let image = previous
        .checked_add(1)
        .ok_or_else(|| fail("snapshot sequence exhausted"))?;
    let mut batch = WriteBatch::default();
    batch.put_cf(
        cf,
        keys::meta(group.id, SEQUENCE),
        encode(&image, ErrorSubject::Snapshot(None))?,
    );
    if group
        .shared
        .db
        .get_cf(group.cf("state")?, keys::meta(group.id, 0))
        .map_err(fail)?
        .is_none()
    {
        let state = state::read_record(group, &group.shared.db.snapshot())?;
        batch.put_cf(
            group.cf("state")?,
            keys::meta(group.id, 0),
            encode(&state, ErrorSubject::StateMachine)?,
        );
    }
    group.persist(batch, ErrorSubject::Snapshot(None))?;
    Ok(image)
}
fn create_partial(group: &Group, image: u64) -> Result<File, StoreError> {
    ensure_directory(group)?;
    OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(partial_path(group, image))
        .map_err(fail)
}
fn finish_image(group: &Group, image: u64, file: File) -> Result<(), StoreError> {
    checkpoint(group, Stage::BeforeFileSync)?;
    file.sync_all().map_err(fail)?;
    drop(file);
    let destination = final_path(group, image);
    if destination.exists() {
        return Err(fail("reserved snapshot filename already exists"));
    }
    fs::rename(partial_path(group, image), destination).map_err(fail)?;
    sync_directory(&directory(group))?;
    checkpoint(group, Stage::AfterRename)
}

fn write_image(lease: ImageLease) -> Result<PreparedImage, StoreError> {
    let group = &lease.group;
    let snapshot = group.shared.db.snapshot();
    let state = state::read_record(group, &snapshot)?;
    if compare_logs(*state.membership.log_id(), state.applied)? == Ordering::Greater {
        return Err(fail("state membership is ahead of applied progress"));
    }
    let header = header_for(group, &state, lease.image);
    let mut output = ImageWriter::new(
        create_partial(group, lease.image)?,
        &header,
        group.shared.options.max_entry_bytes,
    )
    .map_err(fail)?;
    let prefix = keys::data(group.id, state.generation, &[]);
    for row in snapshot.iterator_cf(
        group.cf("state")?,
        IteratorMode::From(&prefix, Direction::Forward),
    ) {
        let (key, value) = row.map_err(fail)?;
        if !key.starts_with(&prefix) {
            break;
        }
        output.record(&key[prefix.len()..], &value).map_err(fail)?;
    }
    let (file, summary) = output.finish().map_err(fail)?;
    finish_image(group, lease.image, file)?;
    drop(snapshot);
    Ok(PreparedImage {
        descriptor: Descriptor {
            image: lease.image,
            generation: state.generation,
            header,
            summary,
        },
        lease,
    })
}

/// Index orders history; committed leader identity cannot move in the opposite
/// direction. Equal indices must retain the entire committed identity.
fn compare_logs(left: Option<LogId>, right: Option<LogId>) -> Result<Ordering, StoreError> {
    match (left, right) {
        (Some(a), Some(b)) if a.index == b.index && a != b => {
            Err(fail("conflicting snapshot log identity at equal index"))
        }
        (Some(a), Some(b)) => {
            let index_order = a.index.cmp(&b.index);
            let leader_order = a.leader_id.cmp(&b.leader_id);
            if matches!(
                (index_order, leader_order),
                (Ordering::Less, Ordering::Greater) | (Ordering::Greater, Ordering::Less)
            ) {
                return Err(fail(
                    "snapshot index and committed leader identity contradict each other",
                ));
            }
            Ok(index_order)
        }
        (a, b) => Ok(a.map(|id| id.index).cmp(&b.map(|id| id.index))),
    }
}
fn check_same_progress(left: &Meta, right: &Meta) -> Result<Ordering, StoreError> {
    let order = compare_logs(left.last_log_id, right.last_log_id)?;
    if order == Ordering::Equal && left.last_membership != right.last_membership {
        return Err(fail("conflicting membership at equal snapshot index"));
    }
    Ok(order)
}
fn validate_membership_progress(
    incoming: &StoredMembership,
    previous: &StoredMembership,
) -> Result<(), StoreError> {
    let order = compare_logs(*incoming.log_id(), *previous.log_id())?;
    if order == Ordering::Less || (order == Ordering::Equal && incoming != previous) {
        return Err(fail(
            "snapshot membership regresses or conflicts with known applied membership",
        ));
    }
    Ok(())
}

fn publish_image(group: &Group, prepared: PreparedImage) -> Result<(Descriptor, File), StoreError> {
    let selected = {
        let _publication = group.publication.lock().map_err(fail)?;
        let state = state::read_record(group, &group.shared.db.snapshot())?;
        let current = read_descriptor(group)?;
        let superseded = if let Some(current) = &current {
            check_same_progress(&current.header.meta, &prepared.descriptor.header.meta)?
                != Ordering::Less
        } else {
            false
        };
        if state.generation != prepared.descriptor.generation || superseded {
            let current =
                current.ok_or_else(|| fail("changed generation has no current snapshot"))?;
            if check_same_progress(&current.header.meta, &prepared.descriptor.header.meta)?
                == Ordering::Less
            {
                return Err(fail(
                    "replaced generation has an older snapshot than the captured view",
                ));
            }
            open_descriptor(group, current)?
        } else {
            if compare_logs(state.applied, prepared.descriptor.header.meta.last_log_id)?
                == Ordering::Less
            {
                return Err(fail("snapshot capture is ahead of active state"));
            }
            let mut batch = WriteBatch::default();
            put_descriptor(group, &mut batch, &prepared.descriptor)?;
            group.persist(batch, ErrorSubject::Snapshot(None))?;
            open_descriptor(group, prepared.descriptor)?
        }
    };
    drop(prepared.lease);
    collect_obsolete(group)?;
    Ok(selected)
}

struct ImportBatch<'a> {
    group: &'a Group,
    generation: u64,
    batch: WriteBatch,
    entries: usize,
}
impl ImportBatch<'_> {
    fn record(&mut self, key: &[u8], value: &[u8]) -> Result<(), StoreError> {
        let charge = key
            .len()
            .checked_add(value.len())
            .and_then(|n| n.checked_add(64))
            .ok_or_else(|| fail("import batch length overflow"))?;
        if self.entries != 0
            && (self.entries == self.group.shared.options.io_chunk_entries
                || self
                    .batch
                    .size_in_bytes()
                    .checked_add(charge)
                    .is_none_or(|n| n > self.group.shared.options.io_chunk_bytes))
        {
            self.flush()?;
        }
        if charge
            .checked_add(12)
            .is_none_or(|n| n > self.group.shared.options.io_chunk_bytes)
        {
            return Err(fail("maximum snapshot record exceeds import chunk budget"));
        }
        self.batch.put_cf(
            self.group.cf("state")?,
            keys::data(self.group.id, self.generation, key),
            value,
        );
        self.entries += 1;
        #[cfg(test)]
        {
            self.group.snapshot_peak_batch.fetch_max(
                self.batch.size_in_bytes(),
                std::sync::atomic::Ordering::Relaxed,
            );
            self.group
                .snapshot_peak_records
                .fetch_max(self.entries, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(())
    }
    fn flush(&mut self) -> Result<(), StoreError> {
        if self.entries != 0 {
            self.group.persist(
                std::mem::take(&mut self.batch),
                ErrorSubject::Snapshot(None),
            )?;
            self.entries = 0;
        }
        Ok(())
    }
}
fn validate_and_import(group: &Group, supplied: &Meta, mut source: File) -> Result<(), StoreError> {
    let active = state::read_record(group, &group.shared.db.snapshot())?;
    if compare_logs(supplied.last_log_id, active.applied)? == Ordering::Less {
        return Err(fail("snapshot installation would roll back applied state"));
    }
    let image = reserve_sequence(group)?;
    source.seek(SeekFrom::Start(0)).map_err(fail)?;
    let mut input = ImageReader::new(source, Some(create_partial(group, image)?));
    let header = input
        .header(group.shared.options.max_entry_bytes)
        .map_err(fail)?;
    validate_identity(&header, group.id, &group.shared.identity)?;
    if &header.meta != supplied {
        return Err(fail("supplied snapshot metadata differs from image header"));
    }
    validate_membership_progress(&supplied.last_membership, &active.membership)?;
    if compare_logs(supplied.last_log_id, active.applied)? == Ordering::Equal
        && supplied.last_membership != active.membership
    {
        return Err(fail(
            "snapshot membership conflicts with active applied state",
        ));
    }
    let mut importer = ImportBatch {
        group,
        generation: image,
        batch: WriteBatch::default(),
        entries: 0,
    };
    let active_view = group.shared.db.snapshot();
    let active_prefix = keys::data(group.id, active.generation, &[]);
    let mut active_rows = active_view.iterator_cf(
        group.cf("state")?,
        IteratorMode::From(&active_prefix, Direction::Forward),
    );
    let same_progress = supplied.last_log_id == active.applied;
    let summary = input
        .records(|key, value| {
            if same_progress {
                let (existing_key, existing_value) = active_rows
                    .next()
                    .ok_or_else(|| format::invalid("same applied snapshot adds a state record"))?
                    .map_err(format::invalid)?;
                if existing_key.as_ref() != keys::data(group.id, active.generation, key)
                    || existing_value.as_ref() != value
                {
                    return Err(format::invalid(
                        "same applied snapshot changes logical state",
                    ));
                }
            }
            importer.record(key, value).map_err(format::invalid)
        })
        .map_err(fail)?;
    if same_progress
        && let Some(row) = active_rows.next()
        && row.map_err(fail)?.0.starts_with(&active_prefix)
    {
        return Err(fail("same applied snapshot removes a state record"));
    }
    importer.flush()?;
    #[cfg(test)]
    group.snapshot_peak_record_bytes.fetch_max(
        input.peak_record_bytes(),
        std::sync::atomic::Ordering::Relaxed,
    );
    let file = input
        .finish_copy()
        .map_err(fail)?
        .ok_or_else(|| fail("missing owned snapshot copy"))?;
    finish_image(group, image, file)?;
    let descriptor = Descriptor {
        image,
        generation: image,
        header,
        summary,
    };
    activate_generation(group, &descriptor)?;
    collect_obsolete(group)
}
fn activate_generation(group: &Group, descriptor: &Descriptor) -> Result<(), StoreError> {
    checkpoint(group, Stage::BeforeActivation)?;
    let _publication = group.publication.lock().map_err(fail)?;
    let state = StateRecord {
        generation: descriptor.generation,
        applied: descriptor.header.meta.last_log_id,
        membership: descriptor.header.meta.last_membership.clone(),
    };
    let mut batch = WriteBatch::default();
    batch.put_cf(
        group.cf("state")?,
        keys::meta(group.id, 0),
        encode(&state, ErrorSubject::StateMachine)?,
    );
    put_descriptor(group, &mut batch, descriptor)?;
    // Both pointers become durable together. No log/vote/purge metadata changes.
    group.persist(batch, ErrorSubject::Snapshot(None))?;
    drop(_publication);
    checkpoint(group, Stage::AfterActivation)
}

fn encode_descriptor(group: &Group, descriptor: &Descriptor) -> Result<Vec<u8>, StoreError> {
    let header = format::encode_header(&descriptor.header, group.shared.options.max_entry_bytes)
        .map_err(fail)?;
    let mut bytes = Vec::with_capacity(header.len() + 48);
    bytes.extend_from_slice(DESCRIPTOR_MAGIC);
    for value in [
        descriptor.image,
        descriptor.generation,
        descriptor.summary.length,
        descriptor.summary.count,
    ] {
        bytes.extend_from_slice(&value.to_be_bytes());
    }
    bytes.extend_from_slice(&descriptor.summary.checksum.to_be_bytes());
    bytes.extend_from_slice(&u32::try_from(header.len()).map_err(fail)?.to_be_bytes());
    bytes.extend_from_slice(&header);
    Ok(bytes)
}
fn read_descriptor(group: &Group) -> Result<Option<Descriptor>, StoreError> {
    let Some(bytes) = group
        .shared
        .db
        .get_cf(group.cf("snapshot_meta")?, keys::meta(group.id, CURRENT))
        .map_err(fail)?
    else {
        return Ok(None);
    };
    if bytes.len() > group.shared.options.max_entry_bytes.saturating_add(48) {
        return Err(fail("oversized snapshot descriptor"));
    }
    let mut fields = Fields::new(&bytes);
    if fields.take(8).map_err(fail)? != DESCRIPTOR_MAGIC {
        return Err(fail("unsupported snapshot descriptor format"));
    }
    let image = fields.u64().map_err(fail)?;
    let generation = fields.u64().map_err(fail)?;
    let length = fields.u64().map_err(fail)?;
    let count = fields.u64().map_err(fail)?;
    let checksum = fields.u32().map_err(fail)?;
    let header_len = usize::try_from(fields.u32().map_err(fail)?).map_err(fail)?;
    let header = format::decode_header(fields.take(header_len).map_err(fail)?).map_err(fail)?;
    fields.finish().map_err(fail)?;
    validate_identity(&header, group.id, &group.shared.identity)?;
    Ok(Some(Descriptor {
        image,
        generation,
        header,
        summary: Summary {
            length,
            count,
            checksum,
        },
    }))
}
fn put_descriptor(
    group: &Group,
    batch: &mut WriteBatch,
    descriptor: &Descriptor,
) -> Result<(), StoreError> {
    batch.put_cf(
        group.cf("snapshot_meta")?,
        keys::meta(group.id, CURRENT),
        encode_descriptor(group, descriptor)?,
    );
    Ok(())
}
fn open_descriptor(
    group: &Group,
    descriptor: Descriptor,
) -> Result<(Descriptor, File), StoreError> {
    let file = File::open(final_path(group, descriptor.image))
        .map_err(|e| fail(format!("open published snapshot {}: {e}", descriptor.image)))?;
    Ok((descriptor, file))
}
fn open_current(group: &Group) -> Result<Option<(Descriptor, File)>, StoreError> {
    read_descriptor(group)?
        .map(|descriptor| open_descriptor(group, descriptor))
        .transpose()
}

fn validate_image(
    group: &Group,
    descriptor: &Descriptor,
    mut file: File,
    compare_state: bool,
) -> Result<(), StoreError> {
    file.seek(SeekFrom::Start(0)).map_err(fail)?;
    let mut input = ImageReader::new(file, None);
    let header = input
        .header(group.shared.options.max_entry_bytes)
        .map_err(fail)?;
    if header != descriptor.header {
        return Err(fail("published image header differs from descriptor"));
    }
    let view = group.shared.db.snapshot();
    let mut rows = view.iterator_cf(
        group.cf("state")?,
        IteratorMode::From(
            &keys::data(group.id, descriptor.generation, &[]),
            Direction::Forward,
        ),
    );
    let prefix = keys::data(group.id, descriptor.generation, &[]);
    let summary = input
        .records(|key, value| {
            if compare_state {
                let Some(row) = rows.next() else {
                    return Err(format::invalid(
                        "active generation is missing a snapshot record",
                    ));
                };
                let (stored_key, stored_value) = row.map_err(format::invalid)?;
                if stored_key.as_ref() != keys::data(group.id, descriptor.generation, key)
                    || stored_value.as_ref() != value
                {
                    return Err(format::invalid("active generation differs from snapshot"));
                }
            }
            Ok(())
        })
        .map_err(fail)?;
    if summary != descriptor.summary {
        return Err(fail(
            "published image length/count/checksum differs from descriptor",
        ));
    }
    if compare_state
        && let Some(row) = rows.next()
        && row.map_err(fail)?.0.starts_with(&prefix)
    {
        return Err(fail("active generation has extra snapshot records"));
    }
    Ok(())
}

/// No live jobs exist during startup. Validate all authoritative records before
/// deleting any orphan, preserving evidence if another group is corrupt.
pub(super) fn recover(shared: &Arc<Shared>) -> Result<(), StoreError> {
    visit_snapshot_groups(shared, |group| {
        validate_reservations(group)?;
        let current = open_current(group)?;
        if let Some((descriptor, file)) = current {
            let state = state::read_record(group, &shared.db.snapshot())?;
            if state.generation != descriptor.generation {
                return Err(fail("snapshot descriptor and active generation disagree"));
            }
            let order = compare_logs(state.applied, descriptor.header.meta.last_log_id)?;
            if order == Ordering::Less {
                return Err(fail("active state is behind published snapshot"));
            }
            if order == Ordering::Equal
                && state.membership != descriptor.header.meta.last_membership
            {
                return Err(fail(
                    "active membership differs at snapshot applied progress",
                ));
            }
            validate_membership_progress(
                &state.membership,
                &descriptor.header.meta.last_membership,
            )?;
            validate_image(group, &descriptor, file, order == Ordering::Equal)?;
        }
        Ok(())
    })?;
    let image_root = shared.path.join("snapshots");
    if image_root.exists() {
        for entry in fs::read_dir(image_root).map_err(fail)? {
            let entry = entry.map_err(fail)?;
            let name = entry.file_name();
            let Some(digits) = name.to_str().and_then(|name| name.strip_prefix("group-")) else {
                continue;
            };
            if digits.len() == 16
                && let Ok(id) = u64::from_str_radix(digits, 16)
            {
                validate_reservations(&Group::new(id, shared.clone()))?;
            }
        }
    }
    // An activated generation must never survive without its atomic descriptor.
    let cf = shared
        .db
        .cf_handle("state")
        .ok_or_else(|| fail("missing state column family"))?;
    let mut rows = shared.db.raw_iterator_cf(cf);
    rows.seek_to_first();
    while rows.valid() {
        let key = rows
            .key()
            .ok_or_else(|| fail("missing state iterator key"))?;
        if key.len() < 9 {
            return Err(fail("malformed state key"));
        }
        let id = u64::from_be_bytes(key[..8].try_into().map_err(fail)?);
        let group = Group::new(id, shared.clone());
        if key.len() == 9 && key[8] == 0 {
            let state = state::read_record(&group, &shared.db.snapshot())?;
            if state.generation != 0 && read_descriptor(&group)?.is_none() {
                return Err(fail(
                    "active imported generation has no snapshot descriptor",
                ));
            }
        }
        if let Some(next) = id.checked_add(1) {
            rows.seek(next.to_be_bytes());
        } else {
            break;
        }
    }
    rows.status().map_err(fail)?;
    visit_snapshot_groups(shared, collect_obsolete)
}
fn visit_snapshot_groups(
    shared: &Arc<Shared>,
    mut visit: impl FnMut(&Group) -> Result<(), StoreError>,
) -> Result<(), StoreError> {
    let cf = shared
        .db
        .cf_handle("snapshot_meta")
        .ok_or_else(|| fail("missing snapshot metadata column family"))?;
    let mut previous = None;
    for row in shared.db.iterator_cf(cf, IteratorMode::Start) {
        let (key, value) = row.map_err(fail)?;
        if key.len() != 9 || ![CURRENT, SEQUENCE].contains(&key[8]) {
            return Err(fail("unknown snapshot metadata key"));
        }
        let id = u64::from_be_bytes(key[..8].try_into().map_err(fail)?);
        if key[8] == SEQUENCE {
            let _: u64 = decode(&value, ErrorSubject::Snapshot(None))?;
        }
        if previous != Some(id) {
            visit(&Group::new(id, shared.clone()))?;
            previous = Some(id);
        }
    }
    Ok(())
}

fn validate_reservations(group: &Group) -> Result<(), StoreError> {
    let sequence = group
        .shared
        .db
        .get_cf(group.cf("snapshot_meta")?, keys::meta(group.id, SEQUENCE))
        .map_err(fail)?
        .map(|bytes| decode::<u64>(&bytes, ErrorSubject::Snapshot(None)))
        .transpose()?;
    if sequence == Some(0) {
        return Err(fail("snapshot sequence is zero"));
    }
    let sequence = sequence.unwrap_or(0);
    if let Some(current) = read_descriptor(group)?
        && (current.image == 0 || current.image > sequence || current.generation > sequence)
    {
        return Err(fail(
            "published snapshot exceeds durable sequence reservation",
        ));
    }
    let prefix = keys::meta(group.id, 1);
    let mut rows = group.shared.db.raw_iterator_cf(group.cf("state")?);
    rows.seek(&prefix);
    while rows.valid() {
        let key = rows
            .key()
            .ok_or_else(|| fail("missing generation iterator key"))?;
        if !key.starts_with(&prefix) {
            break;
        }
        if key.len() < 18 {
            return Err(fail("malformed generation key"));
        }
        let generation = u64::from_be_bytes(key[9..17].try_into().map_err(fail)?);
        if generation > sequence {
            return Err(fail(
                "state generation exceeds durable sequence reservation",
            ));
        }
        if let Some(next) = generation.checked_add(1) {
            rows.seek(keys::data(group.id, next, &[]));
        } else {
            break;
        }
    }
    rows.status().map_err(fail)?;
    let path = directory(group);
    if path.exists() {
        for entry in fs::read_dir(path).map_err(fail)? {
            let entry = entry.map_err(fail)?;
            if let Some(image) = entry.file_name().to_str().and_then(owned_image_id)
                && (image == 0 || image > sequence)
            {
                return Err(fail("owned image exceeds durable sequence reservation"));
            }
        }
    }
    Ok(())
}

fn collect_obsolete(group: &Group) -> Result<(), StoreError> {
    let active = state::read_record(group, &group.shared.db.snapshot())?;
    let prefix = keys::meta(group.id, 1);
    let mut batch = WriteBatch::default();
    let mut count = 0;
    let mut rows = group.shared.db.raw_iterator_cf(group.cf("state")?);
    rows.seek(&prefix);
    while rows.valid() {
        let key = rows
            .key()
            .ok_or_else(|| fail("missing generation iterator key"))?;
        if !key.starts_with(&prefix) {
            break;
        }
        if key.len() < 18 {
            return Err(fail("malformed generation key"));
        }
        let generation = u64::from_be_bytes(key[9..17].try_into().map_err(fail)?);
        if generation == active.generation {
            if let Some(next) = generation.checked_add(1) {
                rows.seek(keys::data(group.id, next, &[]));
                continue;
            }
            break;
        }
        if count != 0
            && (count == group.shared.options.io_chunk_entries
                || batch.size_in_bytes().saturating_add(key.len() + 32)
                    > group.shared.options.io_chunk_bytes)
        {
            checkpoint(group, Stage::DuringCleanup)?;
            group.persist(std::mem::take(&mut batch), ErrorSubject::Snapshot(None))?;
            count = 0;
        }
        batch.delete_cf(group.cf("state")?, key);
        count += 1;
        rows.next();
    }
    rows.status().map_err(fail)?;
    if count != 0 {
        checkpoint(group, Stage::DuringCleanup)?;
        group.persist(batch, ErrorSubject::Snapshot(None))?;
    }
    let path = directory(group);
    if !path.exists() {
        return Ok(());
    }
    let mut changed = false;
    for entry in fs::read_dir(&path).map_err(fail)? {
        let entry = entry.map_err(fail)?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Some(image) = owned_image_id(&name) else {
            continue;
        };
        // Lock only selection + unlink for one file, not the directory scan.
        // A reader that already opened this file remains valid after unlink.
        let _publication = group.publication.lock().map_err(fail)?;
        let current = read_descriptor(group)?.map(|d| d.image);
        let live = group.live_images.lock().map_err(fail)?;
        if Some(image) != current && !live.contains(&image) {
            fs::remove_file(entry.path()).map_err(fail)?;
            changed = true;
        }
    }
    if changed {
        sync_directory(&path)?;
    }
    Ok(())
}
fn owned_image_id(name: &str) -> Option<u64> {
    let suffix = name
        .strip_prefix("image-")
        .or_else(|| name.strip_prefix("receive-"))?;
    let digits = suffix
        .strip_suffix(".snap")
        .or_else(|| suffix.strip_suffix(".partial"))?;
    if digits.len() != 16 {
        return None;
    }
    u64::from_str_radix(digits, 16).ok()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Stage {
    BeforeFileSync,
    AfterRename,
    BeforeActivation,
    AfterActivation,
    DuringCleanup,
    #[cfg(test)]
    BeforePublication,
}
#[cfg(not(test))]
fn checkpoint(_group: &Group, _stage: Stage) -> Result<(), StoreError> {
    Ok(())
}
#[cfg(test)]
fn checkpoint(group: &Group, stage: Stage) -> Result<(), StoreError> {
    let action = {
        let mut hooks = group.snapshot_hooks.lock().map_err(fail)?;
        if hooks
            .front()
            .is_some_and(|(expected, _)| *expected == stage)
        {
            hooks.pop_front().map(|(_, action)| action)
        } else {
            None
        }
    };
    match action {
        Some(super::TestWriteAction::Fail) => {
            Err(fail(format!("injected snapshot failure at {stage:?}")))
        }
        Some(super::TestWriteAction::Hold { entered, resume }) => {
            let _ = entered.send(());
            resume.recv().map_err(fail)
        }
        Some(super::TestWriteAction::Pass) | None => Ok(()),
    }
}
