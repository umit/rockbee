//! Types persisted in each group's replicated log; GET is never a log command.
use bytes::Bytes;
use rockbee_raft_protocol::{MAX_KEY_BYTES, MAX_VALUE_BYTES, ProtocolError};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    Put { key: Bytes, value: Bytes },
    Delete { key: Bytes },
}

impl Command {
    /// Checks public KV limits again at the durable-command boundary.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        let key = match self {
            Self::Put { key, .. } | Self::Delete { key } => key,
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
