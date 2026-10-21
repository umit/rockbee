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
    Metadata(Box<crate::control::MetadataCommand>),
    HandoffFence {
        predecessor_revision: u64,
        predecessor_job_id: rockbee_raft_protocol::control::MoveJobId,
        successor_job_id: rockbee_raft_protocol::control::MoveJobId,
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
            Command::Metadata(command) => Self::Metadata(command.clone()),
            Command::HandoffFence {
                predecessor_revision,
                predecessor_job_id,
                successor_job_id,
            } => Self::HandoffFence {
                predecessor_revision: *predecessor_revision,
                predecessor_job_id: *predecessor_job_id,
                successor_job_id: *successor_job_id,
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
            WireCommand::Metadata(command) => Command::Metadata(command),
            WireCommand::HandoffFence {
                predecessor_revision,
                predecessor_job_id,
                successor_job_id,
            } => Command::HandoffFence {
                predecessor_revision,
                predecessor_job_id,
                successor_job_id,
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

    use super::{BoundedVec, WireAppend, WireCommand, WireSnapshot, WireVoteRequest};

    #[test]
    fn handoff_fence_wire_command_roundtrips_full_width_fields() {
        use crate::types::Command;
        use rockbee_raft_protocol::control::MoveJobId;

        let command = Command::HandoffFence {
            predecessor_revision: u64::MAX,
            predecessor_job_id: MoveJobId::new([255; 16]).expect("predecessor"),
            successor_job_id: MoveJobId::new([254; 16]).expect("successor"),
        };
        let bytes = postcard::to_stdvec(&WireCommand::from(&command)).expect("encode wire fence");
        // Appended tag three, full-width u64 varint, two fixed arrays without lengths.
        let mut expected = vec![3];
        expected.extend_from_slice(&[255; 9]);
        expected.push(1);
        expected.extend_from_slice(&[255; 16]);
        expected.extend_from_slice(&[254; 16]);
        assert_eq!(bytes, expected);
        let wire: WireCommand = postcard::from_bytes(&bytes).expect("decode wire fence");
        assert_eq!(Command::try_from(wire).expect("validated command"), command);
    }

    #[test]
    fn handoff_fence_wire_conversion_rejects_zero_revision_and_same_job() {
        use crate::types::Command;
        use rockbee_raft_protocol::control::MoveJobId;

        let first = MoveJobId::new([1; 16]).expect("predecessor");
        let next = MoveJobId::new([2; 16]).expect("successor");
        for (revision, successor) in [(0, next), (1, first)] {
            let invalid = WireCommand::HandoffFence {
                predecessor_revision: revision,
                predecessor_job_id: first,
                successor_job_id: successor,
            };
            // Intrinsic validation belongs to conversion, after bounded wire decode.
            let bytes = postcard::to_stdvec(&invalid).expect("encode invalid fixture");
            let decoded = postcard::from_bytes::<WireCommand>(&bytes).expect("bounded wire shape");
            assert!(Command::try_from(decoded).is_err());
        }
    }

    #[test]
    fn handoff_append_preserves_historical_put_delete_and_metadata_wire_bytes() {
        use crate::{
            control::{MembershipObservation, MetadataCommand},
            types::Command,
        };
        use rockbee_raft_protocol::{
            GroupId, NodeId,
            control::{CommittedLogPosition, MoveJobId},
            routing::NodeEndpoint,
        };

        let metadata = Command::Metadata(Box::new(MetadataCommand::CompleteMove {
            expected_directory_revision: 1,
            data_group: GroupId::new(1),
            job_id: MoveJobId::new([1; 16]).expect("job"),
            observation: MembershipObservation {
                uniform: true,
                voters: [1, 2, 3].map(|id| NodeId::new(id).expect("voter")),
                replacement_endpoint: NodeEndpoint {
                    node_id: NodeId::new(4).expect("replacement"),
                    peer_address: "127.0.0.1:1".parse().expect("peer"),
                    data_address: "127.0.0.1:2".parse().expect("data"),
                },
                membership: CommittedLogPosition {
                    term: 1,
                    leader_node_id: NodeId::new(1).expect("leader"),
                    index: 1,
                },
            },
        }));
        // Frozen pre-fence tags: Wire Metadata=2, CompleteMove=2. All scalar
        // values fit one byte; SocketAddr V4 tag=0 precedes its four octets/port.
        let mut metadata_bytes = vec![2, 2, 1, 1];
        metadata_bytes.extend_from_slice(&[1; 16]);
        metadata_bytes.extend_from_slice(&[
            1, 1, 2, 3, 4, 0, 127, 0, 0, 1, 1, 0, 127, 0, 0, 1, 2, 1, 1, 1,
        ]);
        for (command, historical) in [
            (
                Command::Put {
                    key: bytes::Bytes::from_static(b"k"),
                    value: bytes::Bytes::from_static(b"v"),
                },
                vec![0, 1, b'k', 1, b'v'],
            ),
            (
                Command::Delete {
                    key: bytes::Bytes::from_static(b"k"),
                },
                vec![1, 1, b'k'],
            ),
            (metadata, metadata_bytes),
        ] {
            assert_eq!(
                postcard::to_stdvec(&WireCommand::from(&command)).expect("wire encode"),
                historical
            );
            let wire = postcard::from_bytes::<WireCommand>(&historical).expect("historical decode");
            assert_eq!(
                Command::try_from(wire).expect("historical command"),
                command
            );
        }
    }

    #[test]
    fn metadata_nested_directory_counts_and_intrinsic_shape_are_rejected() {
        use crate::control::MetadataCommand;
        use rockbee_raft_protocol::{
            ClusterId, NodeId, ShardConfig,
            routing::{ClusterDirectory, NodeEndpoint},
        };
        for corruption in 0..4 {
            let nodes = (1..=3)
                .map(|id| NodeEndpoint {
                    node_id: NodeId::new(id).expect("node"),
                    peer_address: format!("127.0.0.1:{}", 22000 + id * 2)
                        .parse()
                        .expect("peer"),
                    data_address: format!("127.0.0.1:{}", 22001 + id * 2)
                        .parse()
                        .expect("data"),
                })
                .collect();
            let mut directory = ClusterDirectory::initial(
                ClusterId::new([1; 16]).expect("cluster"),
                ShardConfig::new(256, 0).expect("shards"),
                nodes,
            )
            .expect("directory");
            match corruption {
                0 => directory.nodes = (0..17).map(|_| directory.nodes[0].clone()).collect(),
                1 => directory.placements.push(directory.placements[0].clone()),
                2 => directory.placements[0].voters = vec![NodeId::new(1).expect("node"); 17],
                _ => {
                    directory.nodes.truncate(1);
                    for placement in &mut directory.placements {
                        placement.voters.truncate(1);
                    }
                }
            }
            // Bypass the trusted sender to exercise the decoder against a
            // malformed peer; collection counts are refused during decode.
            let wire =
                WireCommand::Metadata(Box::new(MetadataCommand::InitializeDirectoryIfAbsent {
                    initial_directory: directory,
                }));
            let bytes = postcard::to_stdvec(&wire).expect("encode malicious fixture");
            let decoded = postcard::from_bytes::<WireCommand>(&bytes);
            if corruption < 3 {
                assert!(decoded.is_err());
            } else {
                assert!(
                    crate::types::Command::try_from(decoded.expect("bounded shape decodes"))
                        .is_err()
                );
            }
        }
    }

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
