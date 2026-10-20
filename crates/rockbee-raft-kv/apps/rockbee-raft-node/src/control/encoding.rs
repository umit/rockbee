//! Count every output before allocating any framed effect buffer.
use rockbee_raft_protocol::{GroupId, control::MoveRecord, routing::ClusterDirectory};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use super::model::{
    AcceptedSuccessor, DIRECTORY_KEY, JOB_KEY_PREFIX, MAX_DIRECTORY_BYTES, MAX_JOB_BYTES,
    MAX_STORED_MOVES_BYTES, METADATA_EFFECT_RESERVATION, MetadataEffect, MetadataError, ORIGIN_KEY,
    PreparingSuccessor, StoredMoves, SuccessorState,
};

/// Conservative per-effect allowance for generation prefix and native put
/// framing. Storage integration still checks the actual native batch size.
const NATIVE_EFFECT_OVERHEAD: usize = 64;
const MAX_EFFECTS: usize = 3;

pub(super) fn check_size(value: &impl Serialize, limit: usize) -> Result<usize, MetadataError> {
    let length = rockbee_raft_protocol::encoded_payload_size(value)?
        .checked_add(4)
        .ok_or(MetadataError::EncodedLimit)?;
    if length > limit {
        return Err(MetadataError::EncodedLimit);
    }
    Ok(length)
}

pub(super) fn decode_bounded<T: DeserializeOwned>(
    bytes: &[u8],
    limit: usize,
) -> Result<T, MetadataError> {
    if bytes.len() > limit {
        return Err(MetadataError::EncodedLimit);
    }
    Ok(rockbee_raft_protocol::decode_frame(bytes)?)
}

/// Encodes the bounded logical group key. The storage adapter prefixes it with
/// its active generation exactly as it does for ordinary data state values.
pub fn job_key(group: GroupId) -> Vec<u8> {
    let mut key = Vec::with_capacity(JOB_KEY_PREFIX.len() + 8);
    key.extend_from_slice(JOB_KEY_PREFIX);
    key.extend_from_slice(&group.get().to_be_bytes());
    key
}

pub(super) fn initialize_effects(
    directory: &ClusterDirectory,
) -> Result<Vec<MetadataEffect>, MetadataError> {
    encode_effects(&[
        PlannedEffect::Directory {
            key: DIRECTORY_KEY,
            directory,
        },
        PlannedEffect::Directory {
            key: ORIGIN_KEY,
            directory,
        },
    ])
}

pub(super) fn origin_effect(
    directory: &ClusterDirectory,
) -> Result<Vec<MetadataEffect>, MetadataError> {
    encode_effects(&[PlannedEffect::Directory {
        key: ORIGIN_KEY,
        directory,
    }])
}

pub(super) fn job_effect(
    group: GroupId,
    record: &MoveRecord,
) -> Result<Vec<MetadataEffect>, MetadataError> {
    encode_effects(&[PlannedEffect::Job { group, record }])
}

pub(super) fn completion_effects(
    directory: &ClusterDirectory,
    group: GroupId,
    record: &MoveRecord,
) -> Result<Vec<MetadataEffect>, MetadataError> {
    encode_effects(&[
        PlannedEffect::Directory {
            key: DIRECTORY_KEY,
            directory,
        },
        PlannedEffect::Job { group, record },
    ])
}

pub(super) fn stored_effect(
    group: GroupId,
    moves: &StoredMoves,
) -> Result<Vec<MetadataEffect>, MetadataError> {
    encode_effects(&[PlannedEffect::Stored { group, moves }])
}

pub(super) fn successor_completion_effects(
    directory: &ClusterDirectory,
    group: GroupId,
    moves: &StoredMoves,
) -> Result<Vec<MetadataEffect>, MetadataError> {
    encode_effects(&[
        PlannedEffect::Directory {
            key: DIRECTORY_KEY,
            directory,
        },
        PlannedEffect::Stored { group, moves },
    ])
}

enum PlannedEffect<'a> {
    Stored {
        group: GroupId,
        moves: &'a StoredMoves,
    },
    Directory {
        key: &'static [u8],
        directory: &'a ClusterDirectory,
    },
    Job {
        group: GroupId,
        record: &'a MoveRecord,
    },
}

fn encode_effects(plans: &[PlannedEffect<'_>]) -> Result<Vec<MetadataEffect>, MetadataError> {
    if plans.len() > MAX_EFFECTS {
        return Err(MetadataError::EncodedLimit);
    }
    let mut total = 0usize;
    for plan in plans {
        let (key_bytes, value_bytes) = match plan {
            PlannedEffect::Stored { moves, .. } => (JOB_KEY_PREFIX.len() + 8, stored_size(moves)?),
            PlannedEffect::Directory { key, directory } => {
                (key.len(), check_size(directory, MAX_DIRECTORY_BYTES)?)
            }
            PlannedEffect::Job { record, .. } => {
                (JOB_KEY_PREFIX.len() + 8, check_size(record, MAX_JOB_BYTES)?)
            }
        };
        total = total
            .checked_add(key_bytes)
            .and_then(|size| size.checked_add(value_bytes))
            .and_then(|size| size.checked_add(NATIVE_EFFECT_OVERHEAD))
            .ok_or(MetadataError::EncodedLimit)?;
    }
    if total > METADATA_EFFECT_RESERVATION {
        return Err(MetadataError::EncodedLimit);
    }
    // Both frames in an initialization/completion have been counted already;
    // failure in a later frame cannot leave a partially updated typed view.
    plans
        .iter()
        .map(|plan| match plan {
            PlannedEffect::Stored { group, moves } => Ok(MetadataEffect {
                key: job_key(*group),
                value: super::model::encode_stored_moves(moves)?,
            }),
            PlannedEffect::Directory { key, directory } => Ok(MetadataEffect {
                key: key.to_vec(),
                value: rockbee_raft_protocol::encode_frame(directory)?,
            }),
            PlannedEffect::Job { group, record } => Ok(MetadataEffect {
                key: job_key(*group),
                value: rockbee_raft_protocol::encode_frame(record)?,
            }),
        })
        .collect()
}

// The all-zero first field is invalid as the legacy MoveIntent's MoveJobId.
// Keep the ordinary four-byte frame; the discriminator belongs to its payload.
const PREPARED_MARKER: [u8; 16] = [0; 16];
const PREPARED_VERSION: u8 = 2;
const ACCEPTED_VERSION: u8 = 3;

#[derive(Serialize, Deserialize)]
struct PreparedMoves<First, Preparing> {
    marker: [u8; 16],
    version: u8,
    first: First,
    preparing: Preparing,
}

fn prepared_wire<'a>(
    first: &'a MoveRecord,
    preparing: &'a PreparingSuccessor,
) -> PreparedMoves<&'a MoveRecord, &'a PreparingSuccessor> {
    PreparedMoves {
        marker: PREPARED_MARKER,
        version: PREPARED_VERSION,
        first,
        preparing,
    }
}

pub(super) fn prepared_size(
    first: &MoveRecord,
    preparing: &PreparingSuccessor,
) -> Result<usize, MetadataError> {
    first.validate()?;
    preparing.intent.validate()?;
    check_size(first, MAX_JOB_BYTES)?;
    check_size(&prepared_wire(first, preparing), MAX_STORED_MOVES_BYTES)
}

#[derive(Serialize, Deserialize)]
struct AcceptedMoves<First, Accepted> {
    marker: [u8; 16],
    version: u8,
    first: First,
    accepted: Accepted,
}

fn accepted_wire<'a>(
    first: &'a MoveRecord,
    accepted: &'a AcceptedSuccessor,
) -> AcceptedMoves<&'a MoveRecord, &'a AcceptedSuccessor> {
    AcceptedMoves {
        marker: PREPARED_MARKER,
        version: ACCEPTED_VERSION,
        first,
        accepted,
    }
}

pub(super) fn accepted_size(
    first: &MoveRecord,
    accepted: &AcceptedSuccessor,
) -> Result<usize, MetadataError> {
    first.validate()?;
    accepted.record.validate()?;
    check_size(first, MAX_JOB_BYTES)?;
    check_size(&accepted.record, MAX_JOB_BYTES)?;
    check_size(&accepted_wire(first, accepted), MAX_STORED_MOVES_BYTES)
}

fn stored_size(moves: &StoredMoves) -> Result<usize, MetadataError> {
    match &moves.successor {
        Some(SuccessorState::Preparing(preparing)) => prepared_size(&moves.first, preparing),
        Some(SuccessorState::Accepted(accepted)) => accepted_size(&moves.first, accepted),
        None => {
            moves.first.validate()?;
            check_size(&moves.first, MAX_JOB_BYTES)
        }
    }
}

pub(super) fn encode_stored_moves(moves: &StoredMoves) -> Result<Vec<u8>, MetadataError> {
    stored_size(moves)?;
    Ok(match &moves.successor {
        Some(SuccessorState::Preparing(preparing)) => {
            rockbee_raft_protocol::encode_frame(&prepared_wire(&moves.first, preparing))?
        }
        Some(SuccessorState::Accepted(accepted)) => {
            rockbee_raft_protocol::encode_frame(&accepted_wire(&moves.first, accepted))?
        }
        None => rockbee_raft_protocol::encode_frame(&moves.first)?,
    })
}

pub(super) fn decode_stored_moves(bytes: &[u8]) -> Result<StoredMoves, MetadataError> {
    if bytes.len() > MAX_STORED_MOVES_BYTES {
        return Err(MetadataError::EncodedLimit);
    }
    if bytes.get(4..20) != Some(PREPARED_MARKER.as_slice()) {
        return Ok(StoredMoves {
            first: decode_bounded(bytes, MAX_JOB_BYTES)?,
            successor: None,
        });
    }
    // V2's concrete Preparing payload is frozen. V3 is Accepted-only, with no
    // encoded enum tag and no fallback after any marked parse/version failure.
    match bytes.get(20).copied() {
        Some(PREPARED_VERSION) => {
            let wire: PreparedMoves<MoveRecord, PreparingSuccessor> =
                decode_bounded(bytes, MAX_STORED_MOVES_BYTES)?;
            prepared_size(&wire.first, &wire.preparing)?;
            Ok(StoredMoves {
                first: wire.first,
                successor: Some(SuccessorState::Preparing(Box::new(wire.preparing))),
            })
        }
        Some(ACCEPTED_VERSION) => {
            let wire: AcceptedMoves<MoveRecord, AcceptedSuccessor> =
                decode_bounded(bytes, MAX_STORED_MOVES_BYTES)?;
            accepted_size(&wire.first, &wire.accepted)?;
            Ok(StoredMoves {
                first: wire.first,
                successor: Some(SuccessorState::Accepted(Box::new(wire.accepted))),
            })
        }
        _ => Err(MetadataError::Corrupt(
            "unknown or missing stored move version",
        )),
    }
}
