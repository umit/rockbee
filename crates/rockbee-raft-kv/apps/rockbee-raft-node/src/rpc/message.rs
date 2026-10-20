//! Bounded private wire messages.
//!
//! The top-level frame remains the common four-byte-length postcard frame. This
//! extra header is the peer boundary's routing contract: a receiver rejects a
//! request before it reaches OpenRaft when the cluster, target node, group, or
//! message kind is wrong. It prevents accidental cross-cluster misrouting; it is
//! not authentication.
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use rockbee_raft_protocol::{ClusterId, GroupId, NodeId, VERSION};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::types::TypeConfig;

use super::PeerError;
use super::dto::{WireAppend, WireSnapshot, WireVoteRequest};

/// The three OpenRaft calls that can cross the private peer listener.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum PeerKind {
    AppendEntries,
    Vote,
    InstallSnapshot,
}

/// Fixed routing information carried with every private request and response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PeerHeader {
    pub version: u8,
    pub cluster_id: ClusterId,
    pub source: NodeId,
    pub target: NodeId,
    pub group_id: GroupId,
    pub kind: PeerKind,
}

/// Bytes before the postcard body: version + cluster + source + target + group + kind.
/// The receiver reads this small, fixed-size prelude before it reserves a lane or
/// allocates the potentially large AppendEntries/snapshot body.
pub(crate) const PEER_HEADER_BYTES: usize = 42;

impl PeerHeader {
    pub(crate) fn request(
        cluster_id: ClusterId,
        source: NodeId,
        target: NodeId,
        group_id: GroupId,
        kind: PeerKind,
    ) -> Self {
        Self {
            version: VERSION,
            cluster_id,
            source,
            target,
            group_id,
            kind,
        }
    }

    pub(crate) fn response_for(request: &Self, source: NodeId) -> Self {
        Self::request(
            request.cluster_id,
            source,
            request.source,
            request.group_id,
            request.kind,
        )
    }

    pub(crate) fn validate_for_receiver(
        &self,
        cluster_id: ClusterId,
        node_id: NodeId,
    ) -> Result<(), PeerError> {
        if self.version != VERSION {
            return Err(PeerError::Identity("protocol version"));
        }
        if self.cluster_id != cluster_id {
            return Err(PeerError::Identity("cluster"));
        }
        if self.target != node_id {
            return Err(PeerError::Identity("target node"));
        }
        if self.source.get() == 0 {
            return Err(PeerError::Identity("source node"));
        }
        Ok(())
    }

    pub(crate) fn validate_response_for(&self, request: &Self) -> Result<(), PeerError> {
        if self.version != VERSION || self.cluster_id != request.cluster_id {
            return Err(PeerError::Identity("response protocol or cluster"));
        }
        if self.source != request.target || self.target != request.source {
            return Err(PeerError::Identity("response source or target"));
        }
        if self.group_id != request.group_id || self.kind != request.kind {
            return Err(PeerError::Identity("response group or kind"));
        }
        Ok(())
    }

    pub(crate) fn encode_fixed(&self, output: &mut [u8; PEER_HEADER_BYTES]) {
        output[0] = self.version;
        output[1..17].copy_from_slice(&self.cluster_id.get());
        output[17..25].copy_from_slice(&self.source.get().to_be_bytes());
        output[25..33].copy_from_slice(&self.target.get().to_be_bytes());
        output[33..41].copy_from_slice(&self.group_id.get().to_be_bytes());
        output[41] = match self.kind {
            PeerKind::AppendEntries => 0,
            PeerKind::Vote => 1,
            PeerKind::InstallSnapshot => 2,
        };
    }

    pub(crate) fn decode_fixed(input: [u8; PEER_HEADER_BYTES]) -> Result<Self, PeerError> {
        let cluster_id = ClusterId::new(
            input[1..17]
                .try_into()
                .expect("fixed cluster header length"),
        )
        .map_err(|_| PeerError::Identity("cluster"))?;
        let source = NodeId::new(u64::from_be_bytes(
            input[17..25]
                .try_into()
                .expect("fixed source header length"),
        ))
        .map_err(|_| PeerError::Identity("source node"))?;
        let target = NodeId::new(u64::from_be_bytes(
            input[25..33]
                .try_into()
                .expect("fixed target header length"),
        ))
        .map_err(|_| PeerError::Identity("target node"))?;
        let group_id = GroupId::new(u64::from_be_bytes(
            input[33..41].try_into().expect("fixed group header length"),
        ));
        let kind = match input[41] {
            0 => PeerKind::AppendEntries,
            1 => PeerKind::Vote,
            2 => PeerKind::InstallSnapshot,
            _ => return Err(PeerError::Protocol("unknown peer RPC kind".into())),
        };
        Ok(Self {
            version: input[0],
            cluster_id,
            source,
            target,
            group_id,
            kind,
        })
    }
}

/// A single private request. The enum makes the header/body kind check explicit.
#[derive(Debug, Clone)]
pub(crate) struct PeerRequest {
    pub header: PeerHeader,
    pub body: PeerRequestBody,
}

impl PeerRequest {
    pub(crate) fn validate_shape(&self, snapshot_chunk_limit: usize) -> Result<(), PeerError> {
        if self.header.kind != self.body.kind() {
            return Err(PeerError::KindMismatch);
        }
        let vote_node_id = match &self.body {
            PeerRequestBody::AppendEntries(request) => request.vote.leader_id.node_id,
            PeerRequestBody::Vote(request) => request.vote.leader_id.node_id,
            PeerRequestBody::InstallSnapshot(request) => request.vote.leader_id.node_id,
        };
        if vote_node_id == 0 || vote_node_id != self.header.source.get() {
            return Err(PeerError::Identity("header source and Raft vote"));
        }
        match &self.body {
            PeerRequestBody::AppendEntries(request) if request.entries.len() > 128 => {
                Err(PeerError::Protocol("append entry count exceeds 128".into()))
            }
            PeerRequestBody::AppendEntries(request) => {
                // The outgoing network calls this gate before WireAppend
                // clones payloads. Incoming DTO conversion repeats intrinsic
                // command validation after bounded deserialization.
                for entry in &request.entries {
                    if let openraft::EntryPayload::Normal(command) = &entry.payload {
                        command
                            .validate()
                            .map_err(|error| PeerError::Protocol(error.to_string()))?;
                    }
                }
                Ok(())
            }
            PeerRequestBody::InstallSnapshot(request)
                if request.data.len() > snapshot_chunk_limit =>
            {
                Err(PeerError::FrameTooLarge {
                    actual: request.data.len(),
                    maximum: snapshot_chunk_limit,
                })
            }
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum PeerRequestBody {
    AppendEntries(AppendEntriesRequest<TypeConfig>),
    Vote(VoteRequest<u64>),
    InstallSnapshot(InstallSnapshotRequest<TypeConfig>),
}

impl PeerRequestBody {
    pub(crate) const fn kind(&self) -> PeerKind {
        match self {
            Self::AppendEntries(_) => PeerKind::AppendEntries,
            Self::Vote(_) => PeerKind::Vote,
            Self::InstallSnapshot(_) => PeerKind::InstallSnapshot,
        }
    }
}

/// A response is always either a typed OpenRaft response or an explicit peer failure.
/// It can never silently manufacture a Raft success after a transport failure.
#[derive(Debug)]
pub(crate) struct PeerResponse {
    pub header: PeerHeader,
    pub body: PeerResponseBody,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(bound = "")]
pub(crate) enum PeerResponseBody {
    AppendEntries(AppendEntriesResponse<u64>),
    Vote(VoteResponse<u64>),
    InstallSnapshot(InstallSnapshotResponse<u64>),
    Failure { message: String },
}

impl PeerResponseBody {
    pub(crate) const fn kind(&self) -> Option<PeerKind> {
        match self {
            Self::AppendEntries(_) => Some(PeerKind::AppendEntries),
            Self::Vote(_) => Some(PeerKind::Vote),
            Self::InstallSnapshot(_) => Some(PeerKind::InstallSnapshot),
            Self::Failure { .. } => None,
        }
    }
}

pub(crate) fn encode_request(
    request: &PeerRequest,
    max_frame_bytes: usize,
) -> Result<Vec<u8>, PeerError> {
    let wire = match &request.body {
        PeerRequestBody::AppendEntries(request) => {
            WireRequestBody::AppendEntries(WireAppend::from(request))
        }
        PeerRequestBody::Vote(request) => WireRequestBody::Vote(WireVoteRequest::from(request)),
        PeerRequestBody::InstallSnapshot(request) => {
            WireRequestBody::InstallSnapshot(WireSnapshot::from(request))
        }
    };
    encode_wire(&request.header, &wire, max_frame_bytes)
}

/// Counts the complete largest legal one-entry metadata append, and also
/// covers the network adapter's conservative prefix admission before DTO copies.
pub(crate) fn metadata_append_bytes(
    command: &crate::control::MetadataCommand,
) -> Result<usize, PeerError> {
    command
        .validate()
        .map_err(|error| PeerError::Protocol(error.to_string()))?;
    let log_id = openraft::LogId::new(
        openraft::CommittedLeaderId::new(u64::MAX, u64::MAX),
        u64::MAX,
    );
    let append = AppendEntriesRequest {
        vote: openraft::Vote::new(u64::MAX, u64::MAX),
        prev_log_id: Some(log_id),
        entries: vec![crate::types::Entry {
            log_id,
            payload: openraft::EntryPayload::Normal(crate::types::Command::Metadata(Box::new(
                command.clone(),
            ))),
        }],
        leader_commit: Some(log_id),
    };
    let wire = WireRequestBody::AppendEntries(WireAppend::from(&append));
    let actual = postcard::experimental::serialized_size(&wire)
        .map_err(|error| PeerError::Protocol(error.to_string()))?
        .checked_add(PEER_HEADER_BYTES)
        .ok_or_else(|| PeerError::Protocol("metadata frame size overflow".into()))?;
    let reserved = super::network::APPEND_ENVELOPE_RESERVATION
        .checked_add(super::network::metadata_entry_reservation(command))
        .ok_or_else(|| PeerError::Protocol("metadata append reservation overflow".into()))?;
    Ok(actual.max(reserved))
}

pub(crate) fn encode_response(
    response: &PeerResponse,
    max_frame_bytes: usize,
) -> Result<Vec<u8>, PeerError> {
    encode_wire(&response.header, &response.body, max_frame_bytes)
}

fn encode_wire<T: Serialize>(
    header: &PeerHeader,
    body: &T,
    max_frame_bytes: usize,
) -> Result<Vec<u8>, PeerError> {
    let body =
        postcard::to_allocvec(body).map_err(|error| PeerError::Protocol(error.to_string()))?;
    let total = PEER_HEADER_BYTES
        .checked_add(body.len())
        .ok_or_else(|| PeerError::Protocol("peer frame length overflow".into()))?;
    if total == PEER_HEADER_BYTES || total > max_frame_bytes {
        return Err(PeerError::FrameTooLarge {
            actual: total,
            maximum: max_frame_bytes,
        });
    }
    let mut frame = Vec::with_capacity(4 + total);
    frame.extend_from_slice(&(total as u32).to_be_bytes());
    let mut fixed = [0; PEER_HEADER_BYTES];
    header.encode_fixed(&mut fixed);
    frame.extend_from_slice(&fixed);
    frame.extend_from_slice(&body);
    Ok(frame)
}

pub(crate) fn decode_body<T: DeserializeOwned>(body: &[u8]) -> Result<T, PeerError> {
    let (value, remaining) =
        postcard::take_from_bytes(body).map_err(|error| PeerError::Protocol(error.to_string()))?;
    if !remaining.is_empty() {
        return Err(PeerError::Protocol("trailing bytes in peer body".into()));
    }
    Ok(value)
}

pub(crate) fn decode_request_body(
    kind: PeerKind,
    body: &[u8],
) -> Result<PeerRequestBody, PeerError> {
    let wire: WireRequestBody = decode_body(body)?;
    match (kind, wire) {
        (PeerKind::AppendEntries, WireRequestBody::AppendEntries(request)) => {
            Ok(PeerRequestBody::AppendEntries(request.try_into()?))
        }
        (PeerKind::Vote, WireRequestBody::Vote(request)) => {
            Ok(PeerRequestBody::Vote(request.into()))
        }
        (PeerKind::InstallSnapshot, WireRequestBody::InstallSnapshot(request)) => {
            Ok(PeerRequestBody::InstallSnapshot(request.try_into()?))
        }
        _ => Err(PeerError::KindMismatch),
    }
}

#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
enum WireRequestBody {
    AppendEntries(WireAppend),
    Vote(WireVoteRequest),
    InstallSnapshot(WireSnapshot),
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use openraft::{
        CommittedLeaderId, Entry, EntryPayload, LogId, Vote,
        raft::{AppendEntriesRequest, VoteRequest},
    };

    use crate::types::Command;

    use super::*;

    fn identity(byte: u8) -> ClusterId {
        ClusterId::new([byte; 16]).expect("nonzero test cluster identity")
    }

    fn metadata_test_directory() -> rockbee_raft_protocol::routing::ClusterDirectory {
        let nodes = (1..=3)
            .map(|id| rockbee_raft_protocol::routing::NodeEndpoint {
                node_id: NodeId::new(id).expect("node"),
                peer_address: format!("127.0.0.1:{}", 21000 + id * 2)
                    .parse()
                    .expect("peer"),
                data_address: format!("127.0.0.1:{}", 21001 + id * 2)
                    .parse()
                    .expect("data"),
            })
            .collect();
        rockbee_raft_protocol::routing::ClusterDirectory::initial(
            identity(1),
            rockbee_raft_protocol::ShardConfig::new(256, u64::MAX).expect("shards"),
            nodes,
        )
        .expect("directory")
    }

    fn metadata_test_request(command: crate::control::MetadataCommand) -> PeerRequest {
        let mut header = header(PeerKind::AppendEntries);
        header.group_id = GroupId::new(0);
        PeerRequest {
            header,
            body: PeerRequestBody::AppendEntries(AppendEntriesRequest {
                vote: Vote::new(3, 1),
                prev_log_id: None,
                entries: vec![Entry {
                    log_id: LogId::new(CommittedLeaderId::new(3, 1), 99),
                    payload: EntryPayload::Normal(Command::Metadata(Box::new(command))),
                }],
                leader_commit: None,
            }),
        }
    }

    #[test]
    fn metadata_private_append_preserves_every_command_and_full_membership_identity() {
        use crate::control::{MembershipObservation, MetadataCommand};
        use rockbee_raft_protocol::control::{CommittedLogPosition, MoveJobId, MoveReplica};
        let directory = metadata_test_directory();
        let intent = MoveReplica {
            job_id: MoveJobId::new([u8::MAX; 16]).expect("job"),
            data_group: GroupId::new(256),
            remove_node: NodeId::new(3).expect("old voter"),
            add_node: rockbee_raft_protocol::routing::NodeEndpoint {
                node_id: NodeId::new(4).expect("new voter"),
                peer_address: "[feff:ffff:ffff:ffff:ffff:ffff:ffff:ffff]:65535"
                    .parse()
                    .expect("peer"),
                data_address: "[feff:ffff:ffff:ffff:ffff:ffff:ffff:ffff]:65534"
                    .parse()
                    .expect("data"),
            },
            expected_directory_revision: 1,
        }
        .derive_intent(&directory)
        .expect("intent");
        let commands = [
            MetadataCommand::InitializeDirectoryIfAbsent {
                initial_directory: directory,
            },
            MetadataCommand::BeginMove {
                intent: intent.clone(),
            },
            MetadataCommand::CompleteMove {
                expected_directory_revision: u64::MAX,
                data_group: intent.data_group,
                job_id: intent.job_id,
                observation: MembershipObservation {
                    uniform: true,
                    voters: intent.target_voters,
                    replacement_endpoint: intent.add_node,
                    membership: CommittedLogPosition {
                        term: u64::MAX,
                        leader_node_id: NodeId::new(u64::MAX).expect("leader"),
                        index: u64::MAX,
                    },
                },
            },
        ];
        for command in commands {
            let request = metadata_test_request(command.clone());
            request
                .validate_shape(256 * 1024)
                .expect("valid command shape");
            let frame = encode_request(&request, rockbee_raft_protocol::MAX_FRAME_PAYLOAD)
                .expect("encode metadata");
            let decoded =
                decode_request_body(PeerKind::AppendEntries, &frame[4 + PEER_HEADER_BYTES..])
                    .expect("decode metadata");
            let PeerRequestBody::AppendEntries(decoded) = decoded else {
                panic!("append expected")
            };
            assert_eq!(
                decoded.entries[0].log_id,
                LogId::new(CommittedLeaderId::new(3, 1), 99)
            );
            assert_eq!(
                decoded.entries[0].payload,
                EntryPayload::Normal(Command::Metadata(Box::new(command)))
            );
        }
    }

    #[test]
    fn metadata_outgoing_shape_is_rejected_before_dto_conversion() {
        let mut directory = metadata_test_directory();
        directory.nodes.truncate(1);
        for placement in &mut directory.placements {
            placement.voters.truncate(1);
        }
        directory
            .validate()
            .expect("generic directory remains valid");
        let command = crate::control::MetadataCommand::InitializeDirectoryIfAbsent {
            initial_directory: directory,
        };
        assert!(
            command.validate().is_err(),
            "intrinsically invalid bootstrap"
        );
        let request = metadata_test_request(command);
        assert!(
            request.validate_shape(256 * 1024).is_err(),
            "the outgoing network shape gate runs before cloning a WireAppend"
        );
    }

    fn header(kind: PeerKind) -> PeerHeader {
        PeerHeader::request(
            identity(1),
            NodeId::new(1).expect("test node id"),
            NodeId::new(2).expect("test node id"),
            GroupId::new(1),
            kind,
        )
    }

    #[test]
    fn header_round_trip_preserves_routing_identity() {
        let request = PeerRequest {
            header: header(PeerKind::Vote),
            body: PeerRequestBody::Vote(VoteRequest::new(Vote::new(3, 1), None)),
        };
        let frame = encode_request(&request, rockbee_raft_protocol::MAX_FRAME_PAYLOAD)
            .expect("encode bounded request");
        let fixed: [u8; PEER_HEADER_BYTES] = frame[4..4 + PEER_HEADER_BYTES]
            .try_into()
            .expect("fixed header length");
        let decoded = PeerRequest {
            header: PeerHeader::decode_fixed(fixed).expect("decode fixed header"),
            body: decode_request_body(PeerKind::Vote, &frame[4 + PEER_HEADER_BYTES..])
                .expect("decode bounded request"),
        };
        decoded
            .header
            .validate_for_receiver(identity(1), NodeId::new(2).expect("test node id"))
            .expect("receiver identity matches");
        decoded.validate_shape(256 * 1024).expect("matching kind");
    }

    #[test]
    fn metadata_append_round_trip_rejects_trailing_declared_body_bytes() {
        let nodes = (1..=3)
            .map(|id| rockbee_raft_protocol::routing::NodeEndpoint {
                node_id: NodeId::new(id).expect("node"),
                peer_address: format!("127.0.0.1:{}", 10000 + id * 2)
                    .parse()
                    .expect("peer"),
                data_address: format!("127.0.0.1:{}", 10001 + id * 2)
                    .parse()
                    .expect("data"),
            })
            .collect();
        let directory = rockbee_raft_protocol::routing::ClusterDirectory::initial(
            identity(1),
            rockbee_raft_protocol::ShardConfig::new(256, u64::MAX).expect("shards"),
            nodes,
        )
        .expect("directory");
        let command = Command::Metadata(Box::new(
            crate::control::MetadataCommand::InitializeDirectoryIfAbsent {
                initial_directory: directory,
            },
        ));
        let mut header = header(PeerKind::AppendEntries);
        header.group_id = GroupId::new(0);
        let request = PeerRequest {
            header,
            body: PeerRequestBody::AppendEntries(AppendEntriesRequest {
                vote: Vote::new(3, 1),
                prev_log_id: None,
                entries: vec![Entry {
                    log_id: LogId::new(CommittedLeaderId::new(3, 1), 1),
                    payload: EntryPayload::Normal(command.clone()),
                }],
                leader_commit: None,
            }),
        };
        let frame = encode_request(&request, rockbee_raft_protocol::MAX_FRAME_PAYLOAD)
            .expect("metadata frame");
        let body = &frame[4 + PEER_HEADER_BYTES..];
        let decoded = decode_request_body(PeerKind::AppendEntries, body).expect("metadata decode");
        let PeerRequestBody::AppendEntries(decoded) = decoded else {
            panic!("append expected")
        };
        assert_eq!(decoded.entries[0].payload, EntryPayload::Normal(command));
        let mut trailing = body.to_vec();
        trailing.push(0);
        assert!(
            decode_request_body(PeerKind::AppendEntries, &trailing).is_err(),
            "a declared frame body must consume exactly one bounded request"
        );
    }

    #[test]
    fn target_or_kind_mismatch_is_rejected_before_dispatch() {
        let mut request = PeerRequest {
            header: header(PeerKind::Vote),
            body: PeerRequestBody::Vote(VoteRequest::new(Vote::new(3, 1), None)),
        };
        assert!(
            request
                .header
                .validate_for_receiver(identity(2), NodeId::new(2).expect("test node id"))
                .is_err()
        );
        request.header.kind = PeerKind::AppendEntries;
        assert!(matches!(
            request.validate_shape(256 * 1024),
            Err(PeerError::KindMismatch)
        ));
    }

    #[test]
    fn one_maximum_legal_value_fits_the_private_frame() {
        let entry = Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), 1),
            payload: EntryPayload::Normal(Command::Put {
                key: Bytes::from(vec![b'k'; rockbee_raft_protocol::MAX_KEY_BYTES]),
                value: Bytes::from(vec![b'v'; rockbee_raft_protocol::MAX_VALUE_BYTES]),
            }),
        };
        let request = PeerRequest {
            header: header(PeerKind::AppendEntries),
            body: PeerRequestBody::AppendEntries(AppendEntriesRequest {
                vote: Vote::new(1, 1),
                prev_log_id: None,
                entries: vec![entry],
                leader_commit: None,
            }),
        };
        let frame = encode_request(&request, rockbee_raft_protocol::MAX_FRAME_PAYLOAD)
            .expect("maximum legal command must replicate in one peer frame");
        assert!(frame.len() <= rockbee_raft_protocol::MAX_FRAME_PAYLOAD + 4);
    }
}
