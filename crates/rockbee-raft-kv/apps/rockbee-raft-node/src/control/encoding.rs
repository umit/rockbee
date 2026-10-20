//! Count every output before allocating any framed effect buffer.
use rockbee_raft_protocol::{GroupId, control::MoveRecord, routing::ClusterDirectory};
use serde::{Serialize, de::DeserializeOwned};

use super::model::{
    DIRECTORY_KEY, JOB_KEY_PREFIX, MAX_DIRECTORY_BYTES, MAX_JOB_BYTES, METADATA_EFFECT_RESERVATION,
    MetadataEffect, MetadataError, ORIGIN_KEY,
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

enum PlannedEffect<'a> {
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
