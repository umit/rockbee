//! Types persisted in each group's replicated log; GET is never a log command.
use crate::control::{MetadataCommand, MetadataResult};
use bytes::Bytes;
use rockbee_raft_protocol::control::MoveJobId;
use rockbee_raft_protocol::{MAX_KEY_BYTES, MAX_VALUE_BYTES, ProtocolError};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    Put {
        key: Bytes,
        value: Bytes,
    },
    Delete {
        key: Bytes,
    },
    /// Appended tag two; boxing keeps ordinary KV command payloads compact.
    Metadata(Box<MetadataCommand>),
    /// Appended tag three. Orders predecessor handoff without touching user KV.
    HandoffFence {
        predecessor_revision: u64,
        predecessor_job_id: MoveJobId,
        successor_job_id: MoveJobId,
    },
}

impl Command {
    /// Checks intrinsic command constraints again at the durable boundary.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        let key = match self {
            Self::Put { key, .. } | Self::Delete { key } => key,
            Self::Metadata(command) => return command.validate(),
            Self::HandoffFence {
                predecessor_revision,
                predecessor_job_id,
                successor_job_id,
            } => {
                return if *predecessor_revision == 0 || predecessor_job_id == successor_job_id {
                    Err(ProtocolError::InvalidRouting(
                        "invalid handoff fence identity",
                    ))
                } else {
                    Ok(())
                };
            }
        };
        if key.is_empty() || key.len() > MAX_KEY_BYTES {
            return Err(ProtocolError::InvalidKeyLength(key.len()));
        }
        if let Self::Put { value, .. } = self
            && value.len() > MAX_VALUE_BYTES
        {
            return Err(ProtocolError::ValueTooLarge(value.len()));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ApplyResult {
    Applied,
    /// Appended tag one; historical Applied remains tag zero.
    Metadata(MetadataResult),
    /// Appended tag two; only the internal handoff command produces this result.
    HandoffFenced,
}

// This macro wires command/result/node/snapshot/runtime types into OpenRaft.
// It is a type configuration, like implementing associated types on an interface;
// no application business flow or persistence operation is hidden here.
openraft::declare_raft_types!(
    pub TypeConfig:
        D = Command,
        R = ApplyResult,
        NodeId = u64,
        Node = openraft::BasicNode,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = tokio::fs::File,
        AsyncRuntime = openraft::TokioRuntime,
        Responder = openraft::impls::OneshotResponder<TypeConfig>,
);

pub type Entry = openraft::Entry<TypeConfig>;
pub type LogId = openraft::LogId<u64>;
pub type StoredMembership = openraft::StoredMembership<u64, openraft::BasicNode>;
pub type StoreError = openraft::StorageError<u64>;

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize, Deserialize)]
    enum LegacyCommand {
        Put { key: Bytes, value: Bytes },
        Delete { key: Bytes },
    }

    #[test]
    fn handoff_fence_rejects_zero_revision_and_same_job_identity() {
        let first = MoveJobId::new([1; 16]).expect("first job");
        let next = MoveJobId::new([2; 16]).expect("next job");
        for (revision, successor, valid) in [(1, next, true), (0, next, false), (1, first, false)] {
            assert_eq!(
                Command::HandoffFence {
                    predecessor_revision: revision,
                    predecessor_job_id: first,
                    successor_job_id: successor,
                }
                .validate()
                .is_ok(),
                valid
            );
        }
    }

    #[test]
    fn handoff_fence_appends_tags_and_preserves_inline_kv_size() {
        let command = Command::HandoffFence {
            predecessor_revision: u64::MAX,
            predecessor_job_id: MoveJobId::new([1; 16]).expect("first job"),
            successor_job_id: MoveJobId::new([2; 16]).expect("next job"),
        };
        let bytes = postcard::to_stdvec(&command).expect("fence encoding");
        assert_eq!(bytes[0], 3);
        assert_eq!(
            postcard::from_bytes::<Command>(&bytes).expect("fence decode"),
            command
        );
        assert_eq!(
            postcard::to_stdvec(&ApplyResult::HandoffFenced).expect("result"),
            [2]
        );
        assert!(size_of::<Command>() <= size_of::<LegacyCommand>() + size_of::<usize>());
    }

    #[test]
    fn metadata_append_preserves_historical_command_result_tags_and_kv_footprint() {
        let put = [0, 1, b'k', 1, b'v'];
        let delete = [1, 1, b'k'];
        assert_eq!(
            postcard::from_bytes::<Command>(&put).expect("historical Put"),
            Command::Put {
                key: Bytes::from_static(b"k"),
                value: Bytes::from_static(b"v")
            }
        );
        assert_eq!(
            postcard::from_bytes::<Command>(&delete).expect("historical Delete"),
            Command::Delete {
                key: Bytes::from_static(b"k")
            }
        );
        assert_eq!(
            postcard::from_bytes::<ApplyResult>(&[0]).expect("historical Applied"),
            ApplyResult::Applied
        );
        let outcome = ApplyResult::Metadata(MetadataResult::Conflict {
            reason: crate::control::ConflictReason::UnknownMove,
        });
        assert_eq!(postcard::to_stdvec(&outcome).expect("new outcome")[0], 1);
        assert!(
            size_of::<Command>() <= size_of::<LegacyCommand>() + size_of::<usize>(),
            "boxing bounds inline growth to one machine word without embedding MetadataCommand"
        );
        assert!(size_of::<MetadataCommand>() > size_of::<Box<MetadataCommand>>());
    }
}
