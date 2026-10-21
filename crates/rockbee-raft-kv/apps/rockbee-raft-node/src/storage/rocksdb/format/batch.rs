//! Validate and pack Raft entries into bounded write chunks; no I/O here.
use crate::storage::rocksdb::{STORAGE_FORMAT, StorageOpenError, StorageOptions, encode, error};
use crate::types::{Entry, StoreError};
use openraft::{ErrorSubject, ErrorVerb};

pub(in crate::storage) const ENTRY_BATCH_OVERHEAD: usize = 64;
pub(in crate::storage) const STATE_BATCH_OVERHEAD: usize = 128;
pub(in crate::storage) const STATE_RECORD_OVERHEAD: usize = 256;

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

pub(in crate::storage) struct PreparedEntry {
    pub(in crate::storage::rocksdb) entry: Entry,
    pub(in crate::storage::rocksdb) encoded: Vec<u8>,
}

pub(in crate::storage::rocksdb) enum ChunkMetadata {
    Log,
    State { encoded_record_bytes: usize },
}

pub(in crate::storage::rocksdb) fn prepare_entry(
    entry: Entry,
    options: &StorageOptions,
) -> Result<PreparedEntry, StoreError> {
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
pub(in crate::storage::rocksdb) struct EntryChunks<I: Iterator<Item = Entry>> {
    input: std::iter::Peekable<I>,
    pending: Option<PreparedEntry>,
    previous: Option<u64>,
}

impl<I: Iterator<Item = Entry>> EntryChunks<I> {
    pub(in crate::storage::rocksdb) fn new(input: I) -> Self {
        Self {
            input: input.peekable(),
            pending: None,
            previous: None,
        }
    }

    pub(in crate::storage::rocksdb) fn next(
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

    pub(in crate::storage::rocksdb) fn is_finished(&mut self) -> bool {
        self.pending.is_none() && self.input.peek().is_none()
    }
}
