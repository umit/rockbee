//! Bounded wire-only equivalents of OpenRaft messages.
//!
//! OpenRaft's request structs are excellent in-memory API types, but several
//! contain unbounded `Vec`, map and string fields. They are never deserialized
//! directly from a peer socket. These DTOs validate every collection before
//! reserving it, then convert into OpenRaft types after decoding succeeds.
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    marker::PhantomData,
};

use bytes::Bytes;
use openraft::{
    BasicNode, CommittedLeaderId, Entry, EntryPayload, LeaderId, LogId, Membership,
    StoredMembership, Vote,
    raft::{AppendEntriesRequest, InstallSnapshotRequest},
    storage::SnapshotMeta,
};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{Error as _, SeqAccess, Visitor},
};

use super::PeerError;
use crate::types::{Command, TypeConfig};

/// A sequence that refuses its declared count before it reserves a `Vec`.
#[derive(Clone, Debug)]
pub(crate) struct BoundedVec<T, const MAX: usize>(pub(crate) Vec<T>);

impl<T: Serialize, const MAX: usize> Serialize for BoundedVec<T, MAX> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de, T: Deserialize<'de>, const MAX: usize> Deserialize<'de> for BoundedVec<T, MAX> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct BoundedVisitor<T, const MAX: usize>(PhantomData<T>);
        impl<'de, T: Deserialize<'de>, const MAX: usize> Visitor<'de> for BoundedVisitor<T, MAX> {
            type Value = BoundedVec<T, MAX>;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "a sequence containing at most {MAX} values")
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> Result<Self::Value, A::Error> {
                let declared = sequence.size_hint().unwrap_or(0);
                if declared > MAX {
                    return Err(A::Error::custom(
                        "peer collection exceeds its configured limit",
                    ));
                }
                let mut values = Vec::with_capacity(declared);
                while let Some(value) = sequence.next_element()? {
                    if values.len() == MAX {
                        return Err(A::Error::custom(
                            "peer collection exceeds its configured limit",
                        ));
                    }
                    values.push(value);
                }
                Ok(BoundedVec(values))
            }
        }
        deserializer.deserialize_seq(BoundedVisitor(PhantomData))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct WireVote {
    term: u64,
    node_id: u64,
    committed: bool,
}
impl From<&Vote<u64>> for WireVote {
    fn from(value: &Vote<u64>) -> Self {
        Self {
            term: value.leader_id.term,
            node_id: value.leader_id.node_id,
            committed: value.committed,
        }
    }
}
impl From<WireVote> for Vote<u64> {
    fn from(value: WireVote) -> Self {
        Self {
            leader_id: LeaderId {
                term: value.term,
                node_id: value.node_id,
            },
            committed: value.committed,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct WireLogId {
    term: u64,
    leader_node_id: u64,
    index: u64,
}
impl From<&LogId<u64>> for WireLogId {
    fn from(value: &LogId<u64>) -> Self {
        Self {
            term: value.leader_id.term,
            leader_node_id: value.leader_id.node_id,
            index: value.index,
        }
    }
}
impl From<WireLogId> for LogId<u64> {
    fn from(value: WireLogId) -> Self {
        LogId::new(
            CommittedLeaderId::new(value.term, value.leader_node_id),
            value.index,
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum WireCommand {
    Put {
        key: BoundedVec<u8, 65536>,
        value: BoundedVec<u8, 1_048_576>,
    },
    Delete {
        key: BoundedVec<u8, 65536>,
    },
}
impl From<&Command> for WireCommand {
    fn from(value: &Command) -> Self {
        match value {
            Command::Put { key, value } => Self::Put {
                key: BoundedVec(key.to_vec()),
                value: BoundedVec(value.to_vec()),
            },
            Command::Delete { key } => Self::Delete {
                key: BoundedVec(key.to_vec()),
            },
        }
    }
}
impl TryFrom<WireCommand> for Command {
    type Error = PeerError;
    fn try_from(value: WireCommand) -> Result<Self, Self::Error> {
        let command = match value {
            WireCommand::Put { key, value } => Command::Put {
                key: Bytes::from(key.0),
                value: Bytes::from(value.0),
            },
            WireCommand::Delete { key } => Command::Delete {
                key: Bytes::from(key.0),
            },
        };
        command
            .validate()
            .map_err(|error| PeerError::Protocol(error.to_string()))?;
        Ok(command)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct WireNode {
    id: u64,
    endpoint: BoundedVec<u8, 256>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct WireMembership {
    configs: BoundedVec<BoundedVec<u64, 16>, 2>,
    nodes: BoundedVec<WireNode, 16>,
}
impl From<&Membership<u64, BasicNode>> for WireMembership {
    fn from(value: &Membership<u64, BasicNode>) -> Self {
        Self {
            configs: BoundedVec(
                value
                    .get_joint_config()
                    .iter()
                    .map(|config| BoundedVec(config.iter().copied().collect()))
                    .collect(),
            ),
            nodes: BoundedVec(
                value
                    .nodes()
                    .map(|(id, node)| WireNode {
                        id: *id,
                        endpoint: BoundedVec(node.addr.as_bytes().to_vec()),
                    })
                    .collect(),
            ),
        }
    }
}
impl TryFrom<WireMembership> for Membership<u64, BasicNode> {
    type Error = PeerError;
    fn try_from(value: WireMembership) -> Result<Self, Self::Error> {
        let configs: Result<Vec<BTreeSet<u64>>, PeerError> = value
            .configs
            .0
            .into_iter()
            .map(|config| {
                let set: BTreeSet<u64> = config.0.into_iter().collect();
                if set.iter().any(|id| *id == 0) {
                    Err(PeerError::Protocol("membership node id is zero".into()))
                } else {
                    Ok(set)
                }
            })
            .collect();
        let nodes: Result<BTreeMap<u64, BasicNode>, PeerError> = value
            .nodes
            .0
            .into_iter()
            .map(|node| {
                if node.id == 0 {
                    return Err(PeerError::Protocol("membership node id is zero".into()));
                }
                let endpoint = String::from_utf8(node.endpoint.0)
                    .map_err(|_| PeerError::Protocol("membership endpoint is not UTF-8".into()))?;
                if endpoint.is_empty() {
                    return Err(PeerError::Protocol("membership endpoint is empty".into()));
                }
                Ok((node.id, BasicNode::new(endpoint)))
            })
            .collect();
        Ok(Membership::new(configs?, nodes?))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum WireEntryPayload {
    Blank,
    Normal(WireCommand),
    Membership(WireMembership),
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct WireEntry {
    log_id: WireLogId,
    payload: WireEntryPayload,
}
impl From<&Entry<TypeConfig>> for WireEntry {
    fn from(value: &Entry<TypeConfig>) -> Self {
        let payload = match &value.payload {
            EntryPayload::Blank => WireEntryPayload::Blank,
            EntryPayload::Normal(command) => WireEntryPayload::Normal(WireCommand::from(command)),
            EntryPayload::Membership(membership) => {
                WireEntryPayload::Membership(WireMembership::from(membership))
            }
        };
        Self {
            log_id: WireLogId::from(&value.log_id),
            payload,
        }
    }
}
impl TryFrom<WireEntry> for Entry<TypeConfig> {
    type Error = PeerError;
    fn try_from(value: WireEntry) -> Result<Self, Self::Error> {
        let payload = match value.payload {
            WireEntryPayload::Blank => EntryPayload::Blank,
            WireEntryPayload::Normal(command) => EntryPayload::Normal(command.try_into()?),
            WireEntryPayload::Membership(membership) => {
                EntryPayload::Membership(membership.try_into()?)
            }
        };
        Ok(Entry {
            log_id: value.log_id.into(),
            payload,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct WireAppend {
    vote: WireVote,
    prev_log_id: Option<WireLogId>,
    entries: BoundedVec<WireEntry, 128>,
    leader_commit: Option<WireLogId>,
}
impl From<&AppendEntriesRequest<TypeConfig>> for WireAppend {
    fn from(value: &AppendEntriesRequest<TypeConfig>) -> Self {
        Self {
            vote: WireVote::from(&value.vote),
            prev_log_id: value.prev_log_id.as_ref().map(WireLogId::from),
            entries: BoundedVec(value.entries.iter().map(WireEntry::from).collect()),
            leader_commit: value.leader_commit.as_ref().map(WireLogId::from),
        }
    }
}
impl TryFrom<WireAppend> for AppendEntriesRequest<TypeConfig> {
    type Error = PeerError;
    fn try_from(value: WireAppend) -> Result<Self, Self::Error> {
        Ok(Self {
            vote: value.vote.into(),
            prev_log_id: value.prev_log_id.map(Into::into),
            entries: value
                .entries
                .0
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            leader_commit: value.leader_commit.map(Into::into),
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct WireVoteRequest {
    vote: WireVote,
    last_log_id: Option<WireLogId>,
}
impl From<&openraft::raft::VoteRequest<u64>> for WireVoteRequest {
    fn from(value: &openraft::raft::VoteRequest<u64>) -> Self {
        Self {
            vote: WireVote::from(&value.vote),
            last_log_id: value.last_log_id.as_ref().map(WireLogId::from),
        }
    }
}
impl From<WireVoteRequest> for openraft::raft::VoteRequest<u64> {
    fn from(value: WireVoteRequest) -> Self {
        Self::new(value.vote.into(), value.last_log_id.map(Into::into))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct WireStoredMembership {
    log_id: Option<WireLogId>,
    membership: WireMembership,
}
impl From<&StoredMembership<u64, BasicNode>> for WireStoredMembership {
    fn from(value: &StoredMembership<u64, BasicNode>) -> Self {
        Self {
            log_id: value.log_id().as_ref().map(WireLogId::from),
            membership: WireMembership::from(value.membership()),
        }
    }
}
impl TryFrom<WireStoredMembership> for StoredMembership<u64, BasicNode> {
    type Error = PeerError;
    fn try_from(value: WireStoredMembership) -> Result<Self, Self::Error> {
        Ok(StoredMembership::new(
            value.log_id.map(Into::into),
            value.membership.try_into()?,
        ))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct WireSnapshotMeta {
    last_log_id: Option<WireLogId>,
    last_membership: WireStoredMembership,
    snapshot_id: BoundedVec<u8, 256>,
}
impl From<&SnapshotMeta<u64, BasicNode>> for WireSnapshotMeta {
    fn from(value: &SnapshotMeta<u64, BasicNode>) -> Self {
        Self {
            last_log_id: value.last_log_id.as_ref().map(WireLogId::from),
            last_membership: WireStoredMembership::from(&value.last_membership),
            snapshot_id: BoundedVec(value.snapshot_id.as_bytes().to_vec()),
        }
    }
}
impl TryFrom<WireSnapshotMeta> for SnapshotMeta<u64, BasicNode> {
    type Error = PeerError;
    fn try_from(value: WireSnapshotMeta) -> Result<Self, Self::Error> {
        let snapshot_id = String::from_utf8(value.snapshot_id.0)
            .map_err(|_| PeerError::Protocol("snapshot ID is not UTF-8".into()))?;
        if snapshot_id.is_empty() {
            return Err(PeerError::Protocol("snapshot ID is empty".into()));
        }
        Ok(Self {
            last_log_id: value.last_log_id.map(Into::into),
            last_membership: value.last_membership.try_into()?,
            snapshot_id,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct WireSnapshot {
    vote: WireVote,
    meta: WireSnapshotMeta,
    offset: u64,
    data: BoundedVec<u8, 262_144>,
    done: bool,
}
impl From<&InstallSnapshotRequest<TypeConfig>> for WireSnapshot {
    fn from(value: &InstallSnapshotRequest<TypeConfig>) -> Self {
        Self {
            vote: WireVote::from(&value.vote),
            meta: WireSnapshotMeta::from(&value.meta),
            offset: value.offset,
            data: BoundedVec(value.data.clone()),
            done: value.done,
        }
    }
}
impl TryFrom<WireSnapshot> for InstallSnapshotRequest<TypeConfig> {
    type Error = PeerError;
    fn try_from(value: WireSnapshot) -> Result<Self, Self::Error> {
        Ok(Self {
            vote: value.vote.into(),
            meta: value.meta.try_into()?,
            offset: value.offset,
            data: value.data.0,
            done: value.done,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use openraft::{
        BasicNode, Entry, EntryPayload, Membership, StoredMembership, Vote,
        raft::AppendEntriesRequest, storage::SnapshotMeta,
    };

    use super::{BoundedVec, WireAppend, WireSnapshot, WireVoteRequest};

    fn log_id(term: u64, leader_node_id: u64, index: u64) -> openraft::LogId<u64> {
        openraft::LogId::new(
            openraft::CommittedLeaderId::new(term, leader_node_id),
            index,
        )
    }

    #[test]
    fn bounded_sequence_rejects_a_declared_count_before_reserving_it() {
        let oversized = vec![7_u8; 129];
        let encoded = postcard::to_allocvec(&oversized).expect("encode test sequence");
        assert!(postcard::from_bytes::<BoundedVec<u8, 128>>(&encoded).is_err());
    }

    #[test]
    fn bounded_sequence_preserves_an_allowed_value() {
        let encoded = postcard::to_allocvec(&vec![7_u8; 128]).expect("encode test sequence");
        let decoded =
            postcard::from_bytes::<BoundedVec<u8, 128>>(&encoded).expect("decode bounded sequence");
        assert_eq!(decoded.0.len(), 128);
    }

    #[test]
    fn peer_dtos_preserve_every_committed_leader_node_id() {
        let previous = log_id(10, 11, 12);
        let entry_log = log_id(20, 21, 22);
        let committed = log_id(30, 31, 32);
        let append = AppendEntriesRequest {
            vote: Vote::new(40, 41),
            prev_log_id: Some(previous),
            entries: vec![Entry {
                log_id: entry_log,
                payload: EntryPayload::Blank,
            }],
            leader_commit: Some(committed),
        };
        let append: AppendEntriesRequest<crate::types::TypeConfig> = WireAppend::from(&append)
            .try_into()
            .expect("round trip append");
        assert_eq!(
            append.prev_log_id.expect("previous log").leader_id.node_id,
            11
        );
        assert_eq!(append.entries[0].log_id.leader_id.node_id, 21);
        assert_eq!(
            append.leader_commit.expect("commit log").leader_id.node_id,
            31
        );

        let voters = BTreeSet::from([1, 2]);
        let nodes = BTreeMap::from([
            (1, BasicNode::new("127.0.0.1:1")),
            (2, BasicNode::new("127.0.0.1:2")),
        ]);
        let membership = Membership::new(vec![voters], nodes);
        let snapshot = openraft::raft::InstallSnapshotRequest {
            vote: Vote::new(50, 51),
            meta: SnapshotMeta {
                last_log_id: Some(log_id(60, 61, 62)),
                last_membership: StoredMembership::new(Some(log_id(70, 71, 72)), membership),
                snapshot_id: "snapshot-1".to_owned(),
            },
            offset: 0,
            data: vec![],
            done: true,
        };
        let snapshot: openraft::raft::InstallSnapshotRequest<crate::types::TypeConfig> =
            WireSnapshot::from(&snapshot)
                .try_into()
                .expect("round trip snapshot");
        assert_eq!(
            snapshot
                .meta
                .last_log_id
                .expect("snapshot log")
                .leader_id
                .node_id,
            61
        );
        assert_eq!(
            snapshot
                .meta
                .last_membership
                .log_id()
                .as_ref()
                .expect("membership log")
                .leader_id
                .node_id,
            71
        );

        let vote_request =
            openraft::raft::VoteRequest::new(Vote::new(80, 81), Some(log_id(80, 82, 83)));
        let encoded =
            postcard::to_allocvec(&WireVoteRequest::from(&vote_request)).expect("encode vote DTO");
        let decoded: WireVoteRequest = postcard::from_bytes(&encoded).expect("decode vote DTO");
        let decoded: openraft::raft::VoteRequest<u64> = decoded.into();
        assert_eq!(decoded.vote.leader_id.node_id, 81);
        assert_eq!(
            decoded
                .last_log_id
                .expect("vote last log")
                .leader_id
                .node_id,
            82
        );
    }
}
