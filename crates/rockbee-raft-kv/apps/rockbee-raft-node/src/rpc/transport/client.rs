//! OpenRaft network trait implementations.
use std::{
    error::Error,
    sync::Arc,
    time::{Duration, Instant},
};

use log::{debug, trace};
use openraft::{
    BasicNode, EntryPayload,
    error::{NetworkError, PayloadTooLarge, RPCError, RaftError, Timeout, Unreachable},
    network::{RPCOption, RPCTypes, RaftNetwork, RaftNetworkFactory},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
};
use rockbee_raft_protocol::{GroupId, NodeId};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};

use crate::{diagnostics::Span, types::TypeConfig};

use crate::rpc::{
    PeerError, PeerIdentity, PeerResources,
    admission::{ControlCheckout, ControlConnectionKey, IdleControlConnection},
    wire::message::{
        PEER_HEADER_BYTES, PeerHeader, PeerKind, PeerRequest, PeerRequestBody, PeerResponseBody,
        decode_body, encode_request,
    },
};

/// Builds one lazy peer client for a specific remote node and local Raft group.
#[derive(Clone)]
pub struct PeerNetworkFactory {
    identity: PeerIdentity,
    group_id: GroupId,
    resources: Arc<PeerResources>,
}

impl PeerNetworkFactory {
    pub fn new(identity: PeerIdentity, group_id: GroupId, resources: Arc<PeerResources>) -> Self {
        Self {
            identity,
            group_id,
            resources,
        }
    }
}

impl RaftNetworkFactory<TypeConfig> for PeerNetworkFactory {
    type Network = PeerNetwork;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        PeerNetwork {
            identity: self.identity,
            group_id: self.group_id,
            target,
            address: node.addr.clone(),
            resources: Arc::clone(&self.resources),
        }
    }
}

/// A lazy OpenRaft peer client.
///
/// Vote and append requests borrow one exclusive process-shared control stream.
/// Snapshots always use a fresh stream, so a large transfer cannot occupy the
/// stream needed by elections and heartbeats. A failed or cancelled call drops
/// its stream; this adapter intentionally has no reconnect or replay policy.
pub struct PeerNetwork {
    identity: PeerIdentity,
    group_id: GroupId,
    target: u64,
    address: String,
    resources: Arc<PeerResources>,
}

impl PeerNetwork {
    fn target_node_id(&self) -> Result<NodeId, PeerError> {
        NodeId::new(self.target).map_err(|_| PeerError::Identity("target node"))
    }

    async fn call(
        &self,
        body: PeerRequestBody,
        option: RPCOption,
    ) -> Result<PeerResponseBody, PeerError> {
        let deadline = option.hard_ttl();
        let operation = rpc_operation(body.kind());
        let started = log::log_enabled!(log::Level::Debug).then(Instant::now);
        trace!(
            "raft_rpc_start node={} group={} target={} op={}",
            self.identity.node_id.get(),
            self.group_id.get(),
            self.target,
            operation,
        );
        let mut span = Span::start(
            self.identity.node_id.get(),
            self.group_id.get(),
            Some(self.target),
            rpc_operation(body.kind()),
            false,
        );
        let result = timeout(deadline, self.call_before_deadline(body, &mut span)).await;
        match result {
            Ok(Ok(response)) => {
                if let PeerResponseBody::Failure { message } = &response {
                    let error = PeerError::Protocol(message.clone());
                    span.failure(&error);
                    debug!(
                        "raft_rpc_remote_failure node={} group={} target={} op={} elapsed_ms={} error={}",
                        self.identity.node_id.get(),
                        self.group_id.get(),
                        self.target,
                        operation,
                        elapsed_ms(started),
                        error,
                    );
                } else {
                    span.success();
                    trace!(
                        "raft_rpc_complete node={} group={} target={} op={} elapsed_ms={}",
                        self.identity.node_id.get(),
                        self.group_id.get(),
                        self.target,
                        operation,
                        elapsed_ms(started),
                    );
                }
                Ok(response)
            }
            Ok(Err(error)) => {
                span.failure(&error);
                debug!(
                    "raft_rpc_failed node={} group={} target={} op={} elapsed_ms={} error={}",
                    self.identity.node_id.get(),
                    self.group_id.get(),
                    self.target,
                    operation,
                    elapsed_ms(started),
                    error,
                );
                Err(error)
            }
            Err(_) => {
                let error = PeerError::Deadline;
                span.failure(&error);
                debug!(
                    "raft_rpc_deadline node={} group={} target={} op={} elapsed_ms={}",
                    self.identity.node_id.get(),
                    self.group_id.get(),
                    self.target,
                    operation,
                    elapsed_ms(started),
                );
                Err(error)
            }
        }
    }

    async fn call_before_deadline(
        &self,
        body: PeerRequestBody,
        span: &mut Span,
    ) -> Result<PeerResponseBody, PeerError> {
        let kind = body.kind();
        span.phase("lane_admission");
        let _lane = match kind {
            PeerKind::InstallSnapshot => self.resources.acquire_outgoing_snapshot().await?,
            _ => self.resources.acquire_outgoing_control().await?,
        };
        // Take the classified lane before the generic connection permit. A
        // backlog of snapshots therefore waits only in the bulk lane and cannot
        // occupy every shared connection slot needed by votes/heartbeats.
        // Cheap shape validation happens before admission. Encoding waits until
        // a physical connection is owned, keeping concurrent frame buffers
        // within the process-wide connection budget.
        let target = self.target_node_id()?;
        let request = PeerRequest {
            header: PeerHeader::request(
                self.identity.cluster_id,
                self.identity.node_id,
                target,
                self.group_id,
                kind,
            ),
            body,
        };
        request.validate_shape(self.resources.limits().max_snapshot_chunk_bytes)?;
        let control_key = ControlConnectionKey::new(self.identity, target, self.address.clone());
        let mut control_connection = match kind {
            PeerKind::InstallSnapshot => {
                span.phase("connection_admission");
                let permit = self.resources.acquire_outgoing_connection().await?;
                span.phase("connect");
                let stream = TcpStream::connect(&self.address).await?;
                IdleControlConnection::new(control_key, stream, permit)
            }
            _ => {
                span.phase("connection_admission");
                match self
                    .resources
                    .checkout_outgoing_control(control_key)
                    .await?
                {
                    ControlCheckout::Reused(connection) => connection,
                    ControlCheckout::Fresh(permit) => {
                        span.phase("connect");
                        let stream = TcpStream::connect(&self.address).await?;
                        IdleControlConnection::new(
                            ControlConnectionKey::new(self.identity, target, self.address.clone()),
                            stream,
                            permit,
                        )
                    }
                }
            }
        };
        span.phase("encode");
        let frame = encode_request(&request, self.resources.limits().max_frame_bytes)?;
        span.phase("write");
        control_connection.stream_mut().write_all(&frame).await?;
        control_connection.stream_mut().flush().await?;
        span.phase("read_header");
        let (header, body_len) = read_peer_header(
            control_connection.stream_mut(),
            self.resources.limits().max_frame_bytes,
        )
        .await?;
        span.phase("read_body");
        let body = read_peer_body(control_connection.stream_mut(), body_len).await?;
        span.phase("decode");
        header.validate_response_for(&request.header)?;
        let response: PeerResponseBody = decode_body(&body)?;
        if let Some(kind) = response.kind()
            && kind != request.header.kind
        {
            return Err(PeerError::KindMismatch);
        }
        if kind != PeerKind::InstallSnapshot {
            self.resources
                .return_outgoing_control(control_connection)
                .await;
        }
        Ok(response)
    }

    fn rpc_error<E>(
        &self,
        error: PeerError,
        action: RPCTypes,
        timeout: Duration,
    ) -> RPCError<u64, BasicNode, RaftError<u64, E>>
    where
        E: Error + Send + Sync + 'static,
    {
        match error {
            PeerError::Deadline => RPCError::Timeout(Timeout {
                action,
                id: self.identity.node_id.get(),
                target: self.target,
                timeout,
            }),
            error => RPCError::Unreachable(Unreachable::new(&error)),
        }
    }
}

fn elapsed_ms(started: Option<Instant>) -> u128 {
    started.map_or(0, |started| started.elapsed().as_millis())
}

fn rpc_operation(kind: PeerKind) -> &'static str {
    match kind {
        PeerKind::AppendEntries => "append_entries",
        PeerKind::Vote => "vote",
        PeerKind::InstallSnapshot => "install_snapshot",
    }
}

impl RaftNetwork<TypeConfig> for PeerNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let timeout = option.hard_ttl();
        let entries = rpc.entries.len();
        if entries > 128 {
            return Err(PayloadTooLarge::new_entries_hint(128).into());
        }
        let fitting_entries = fitting_append_prefix(&rpc, self.resources.limits().max_frame_bytes);
        if fitting_entries < entries {
            if fitting_entries == 0 {
                return Err(self.rpc_error(
                    PeerError::FrameTooLarge {
                        actual: self.resources.limits().max_frame_bytes.saturating_add(1),
                        maximum: self.resources.limits().max_frame_bytes,
                    },
                    RPCTypes::AppendEntries,
                    timeout,
                ));
            }
            return Err(PayloadTooLarge::new_entries_hint(fitting_entries as u64).into());
        }
        match self.call(PeerRequestBody::AppendEntries(rpc), option).await {
            Ok(PeerResponseBody::AppendEntries(response)) => Ok(response),
            Ok(PeerResponseBody::Failure { message }) => Err(RPCError::Network(NetworkError::new(
                &PeerError::Protocol(message),
            ))),
            Ok(_) => Err(self.rpc_error(PeerError::KindMismatch, RPCTypes::AppendEntries, timeout)),
            Err(PeerError::FrameTooLarge { .. }) if entries > 1 => {
                Err(PayloadTooLarge::new_entries_hint((entries / 2).max(1) as u64).into())
            }
            Err(error) => Err(self.rpc_error(error, RPCTypes::AppendEntries, timeout)),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let timeout = option.hard_ttl();
        match self.call(PeerRequestBody::Vote(rpc), option).await {
            Ok(PeerResponseBody::Vote(response)) => Ok(response),
            Ok(PeerResponseBody::Failure { message }) => Err(RPCError::Network(NetworkError::new(
                &PeerError::Protocol(message),
            ))),
            Ok(_) => Err(self.rpc_error(PeerError::KindMismatch, RPCTypes::Vote, timeout)),
            Err(error) => Err(self.rpc_error(error, RPCTypes::Vote, timeout)),
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, openraft::error::InstallSnapshotError>>,
    > {
        let timeout = option.hard_ttl();
        match self
            .call(PeerRequestBody::InstallSnapshot(rpc), option)
            .await
        {
            Ok(PeerResponseBody::InstallSnapshot(response)) => Ok(response),
            Ok(PeerResponseBody::Failure { message }) => Err(RPCError::Network(NetworkError::new(
                &PeerError::Protocol(message),
            ))),
            Ok(_) => {
                Err(self.rpc_error(PeerError::KindMismatch, RPCTypes::InstallSnapshot, timeout))
            }
            Err(error) => Err(self.rpc_error(error, RPCTypes::InstallSnapshot, timeout)),
        }
    }
}

/// Covers the fixed peer header, vote, optional positions and envelope tags.
pub(in crate::rpc) const APPEND_ENVELOPE_RESERVATION: usize = 128;
/// Covers full-width assigned LogId, payload/command tags and collection lengths.
const METADATA_ENTRY_RESERVATION: usize = 128;

pub(in crate::rpc) fn metadata_entry_reservation(
    command: &crate::control::MetadataCommand,
) -> usize {
    rockbee_raft_protocol::encoded_payload_size(command)
        .unwrap_or(usize::MAX)
        .saturating_add(METADATA_ENTRY_RESERVATION)
}

/// Counts conservatively before DTO conversion, splitting before frame copies.
fn fitting_append_prefix(request: &AppendEntriesRequest<TypeConfig>, frame_limit: usize) -> usize {
    let mut total = APPEND_ENVELOPE_RESERVATION;
    let mut fitting = 0;
    for entry in &request.entries {
        let entry_bytes = match &entry.payload {
            EntryPayload::Blank => 64,
            EntryPayload::Normal(command) => match command {
                crate::types::Command::Put { key, value } => {
                    key.len().saturating_add(value.len()).saturating_add(128)
                }
                crate::types::Command::Delete { key } => key.len().saturating_add(96),
                crate::types::Command::Metadata(command) => metadata_entry_reservation(command),
                // Fixed two job IDs, revision, full LogId and payload tags.
                crate::types::Command::HandoffFence { .. } => 128,
            },
            EntryPayload::Membership(membership) => {
                let configs = membership
                    .get_joint_config()
                    .iter()
                    .map(|config| config.len().saturating_mul(16).saturating_add(16))
                    .sum::<usize>();
                let nodes = membership
                    .nodes()
                    .map(|(_, node)| node.addr.len().saturating_add(32))
                    .sum::<usize>();
                configs.saturating_add(nodes).saturating_add(96)
            }
        };
        let next = total.saturating_add(entry_bytes);
        if next > frame_limit {
            break;
        }
        total = next;
        fitting += 1;
    }
    fitting
}

pub(crate) async fn read_peer_header(
    stream: &mut TcpStream,
    payload_limit: usize,
) -> Result<(PeerHeader, usize), PeerError> {
    let mut header = [0_u8; 4];
    stream.read_exact(&mut header).await?;
    let payload_bytes = u32::from_be_bytes(header) as usize;
    if payload_bytes <= PEER_HEADER_BYTES || payload_bytes > payload_limit {
        return Err(PeerError::FrameTooLarge {
            actual: payload_bytes,
            maximum: payload_limit,
        });
    }
    let mut fixed = [0; PEER_HEADER_BYTES];
    stream.read_exact(&mut fixed).await?;
    let header = PeerHeader::decode_fixed(fixed)?;
    Ok((header, payload_bytes - PEER_HEADER_BYTES))
}

pub(crate) async fn read_peer_body(
    stream: &mut TcpStream,
    body_len: usize,
) -> Result<Vec<u8>, PeerError> {
    let mut body = vec![0; body_len];
    stream.read_exact(&mut body).await?;
    Ok(body)
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use bytes::Bytes;
    use openraft::{BasicNode, CommittedLeaderId, Entry, LogId, Vote};
    use tokio::{io::AsyncWriteExt, net::TcpListener};

    use super::*;
    use crate::rpc::{
        PeerLimits,
        wire::{
            WireEntry,
            message::{self, decode_request_body},
        },
    };

    #[test]
    fn handoff_fence_full_width_append_prefix_fits_actual_private_frames() {
        use crate::types::Command;
        use rockbee_raft_protocol::control::MoveJobId;

        let command = Command::HandoffFence {
            predecessor_revision: u64::MAX,
            predecessor_job_id: MoveJobId::new([255; 16]).expect("predecessor"),
            successor_job_id: MoveJobId::new([254; 16]).expect("successor"),
        };
        let previous = LogId::new(CommittedLeaderId::new(u64::MAX, u64::MAX), u64::MAX - 128);
        let request: AppendEntriesRequest<TypeConfig> = AppendEntriesRequest {
            vote: Vote::new(u64::MAX, u64::MAX),
            prev_log_id: Some(previous),
            entries: (0u64..128)
                .map(|offset| Entry {
                    log_id: LogId::new(
                        CommittedLeaderId::new(u64::MAX, u64::MAX),
                        u64::MAX - 127 + offset,
                    ),
                    payload: EntryPayload::Normal(command.clone()),
                })
                .collect(),
            leader_commit: Some(previous),
        };
        for entry in &request.entries {
            let actual = postcard::to_stdvec(&WireEntry::from(entry)).expect("wire entry");
            assert!(
                actual.len() <= 128,
                "full-width fence exceeds its reservation"
            );
        }
        let header = PeerHeader::request(
            test_identity(1).cluster_id,
            NodeId::new(u64::MAX).expect("source"),
            NodeId::new(u64::MAX - 1).expect("target"),
            GroupId::new(256),
            PeerKind::AppendEntries,
        );
        let single_limit = APPEND_ENVELOPE_RESERVATION + 128;
        assert_eq!(fitting_append_prefix(&request, single_limit - 1), 0);
        assert_eq!(fitting_append_prefix(&request, single_limit), 1);
        for limit in [single_limit, 4096, APPEND_ENVELOPE_RESERVATION + 128 * 128] {
            let count = fitting_append_prefix(&request, limit);
            assert_eq!(
                count,
                ((limit - APPEND_ENVELOPE_RESERVATION) / 128).min(128)
            );
            let selected = AppendEntriesRequest {
                vote: request.vote,
                prev_log_id: request.prev_log_id,
                entries: request.entries[..count].to_vec(),
                leader_commit: request.leader_commit,
            };
            let frame = encode_request(
                &PeerRequest {
                    header: header.clone(),
                    body: PeerRequestBody::AppendEntries(selected),
                },
                limit,
            )
            .expect("selected full-width prefix fits its actual frame");
            assert!(frame.len() <= limit + 4);
            let body = &frame[4 + PEER_HEADER_BYTES..];
            let decoded = decode_request_body(PeerKind::AppendEntries, body)
                .expect("decode actual frame body");
            let PeerRequestBody::AppendEntries(decoded) = decoded else {
                unreachable!("append kind yields append body")
            };
            assert_eq!(decoded.vote, request.vote);
            assert_eq!(decoded.prev_log_id, request.prev_log_id);
            assert_eq!(decoded.leader_commit, request.leader_commit);
            assert_eq!(decoded.entries, request.entries[..count]);
        }
    }

    #[test]
    fn oversized_batch_is_split_before_wire_dto_copies() {
        let entry = || Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), 1),
            payload: EntryPayload::Normal(crate::types::Command::Put {
                key: Bytes::from_static(b"key"),
                value: Bytes::from(vec![b'v'; rockbee_raft_protocol::MAX_VALUE_BYTES]),
            }),
        };
        let request = AppendEntriesRequest {
            vote: Vote::new(1, 1),
            prev_log_id: None,
            entries: vec![entry(), entry()],
            leader_commit: None,
        };
        assert_eq!(
            fitting_append_prefix(&request, rockbee_raft_protocol::MAX_FRAME_PAYLOAD),
            1
        );
    }

    #[test]
    fn metadata_maximum_shape_prefix_fits_its_actual_private_frame() {
        use crate::{control::MetadataCommand, types::Command};
        use rockbee_raft_protocol::{
            ClusterId, ShardConfig,
            routing::{ClusterDirectory, NodeEndpoint},
        };
        let nodes = (1u16..=3)
            .map(|offset| NodeEndpoint {
                node_id: NodeId::new(u64::MAX - u64::from(offset)).expect("node"),
                peer_address: std::net::SocketAddr::new(
                    std::net::Ipv6Addr::from(u128::MAX - (1u128 << 120) - u128::from(offset))
                        .into(),
                    u16::MAX - offset * 2,
                ),
                data_address: std::net::SocketAddr::new(
                    std::net::Ipv6Addr::from(u128::MAX - (1u128 << 120) - u128::from(offset))
                        .into(),
                    u16::MAX - offset * 2 - 1,
                ),
            })
            .collect();
        let directory = ClusterDirectory::initial(
            ClusterId::new([u8::MAX; 16]).expect("cluster"),
            ShardConfig::new(256, u64::MAX).expect("shards"),
            nodes,
        )
        .expect("maximum bootstrap");
        let command = Command::Metadata(Box::new(MetadataCommand::InitializeDirectoryIfAbsent {
            initial_directory: directory,
        }));
        let log_id = LogId::new(CommittedLeaderId::new(u64::MAX, u64::MAX), u64::MAX);
        let request: AppendEntriesRequest<TypeConfig> = AppendEntriesRequest {
            vote: Vote::new(u64::MAX, u64::MAX),
            prev_log_id: Some(log_id),
            entries: (0..128)
                .map(|_| Entry {
                    log_id,
                    payload: EntryPayload::Normal(command.clone()),
                })
                .collect(),
            leader_commit: Some(log_id),
        };
        let Command::Metadata(metadata) = &command else {
            unreachable!("fixture is metadata");
        };
        let admitted =
            message::metadata_append_bytes(metadata).expect("count complete worst append envelope");
        let single = AppendEntriesRequest {
            vote: request.vote,
            prev_log_id: request.prev_log_id,
            entries: vec![request.entries[0].clone()],
            leader_commit: request.leader_commit,
        };
        assert_eq!(fitting_append_prefix(&single, admitted), 1);
        assert_eq!(fitting_append_prefix(&single, admitted - 1), 0);
        let single_frame = encode_request(
            &PeerRequest {
                header: PeerHeader::request(
                    test_identity(1).cluster_id,
                    NodeId::new(1).expect("source"),
                    NodeId::new(2).expect("target"),
                    GroupId::new(0),
                    PeerKind::AppendEntries,
                ),
                body: PeerRequestBody::AppendEntries(single),
            },
            admitted,
        )
        .expect("full-width optional positions and body fit shared admission");
        assert!(single_frame.len() <= admitted + 4);
        let limit = 32 * 1024;
        let fitting = fitting_append_prefix(&request, limit);
        assert!(fitting > 0 && fitting < request.entries.len());
        let selected = AppendEntriesRequest {
            entries: request.entries[..fitting].to_vec(),
            ..request
        };
        let header = PeerHeader::request(
            test_identity(1).cluster_id,
            NodeId::new(1).expect("source"),
            NodeId::new(2).expect("target"),
            GroupId::new(0),
            PeerKind::AppendEntries,
        );
        let frame = encode_request(
            &PeerRequest {
                header,
                body: PeerRequestBody::AppendEntries(selected),
            },
            limit,
        )
        .expect("chosen prefix fits real DTO frame");
        assert!(frame.len() <= limit + 4);
    }

    #[test]
    fn local_deadline_maps_to_openraft_timeout() {
        let network = PeerNetwork {
            identity: test_identity(1),
            group_id: GroupId::new(1),
            target: 2,
            address: "127.0.0.1:1".into(),
            resources: Arc::new(
                PeerResources::new(PeerLimits::default()).expect("valid test peer limits"),
            ),
        };

        let ttl = Duration::from_millis(50);
        for action in [
            RPCTypes::AppendEntries,
            RPCTypes::Vote,
            RPCTypes::InstallSnapshot,
        ] {
            let deadline: RPCError<u64, BasicNode, RaftError<u64>> =
                network.rpc_error(PeerError::Deadline, action, ttl);
            match deadline {
                RPCError::Timeout(timeout) => {
                    assert_eq!(timeout.action, action);
                    assert_eq!(timeout.id, 1);
                    assert_eq!(timeout.target, 2);
                    assert_eq!(timeout.timeout, ttl);
                }
                error => panic!("deadline must remain an OpenRaft timeout: {error}"),
            }
        }

        let protocol: RPCError<u64, BasicNode, RaftError<u64>> = network.rpc_error(
            PeerError::Protocol("malformed peer response".into()),
            RPCTypes::Vote,
            ttl,
        );
        assert!(matches!(protocol, RPCError::Unreachable(_)));
    }

    #[tokio::test]
    async fn sequential_control_calls_from_cloned_factories_share_one_connection() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind owned peer listener");
        let address = listener
            .local_addr()
            .expect("read owned listener address")
            .to_string();
        let accepted = Arc::new(AtomicUsize::new(0));
        let server_accepted = Arc::clone(&accepted);
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept first control peer");
            server_accepted.fetch_add(1, Ordering::SeqCst);
            for _ in 0..2 {
                let (header, body_len) =
                    read_peer_header(&mut stream, rockbee_raft_protocol::MAX_FRAME_PAYLOAD)
                        .await
                        .expect("read vote header");
                let body = read_peer_body(&mut stream, body_len)
                    .await
                    .expect("read vote body");
                assert!(matches!(
                    decode_request_body(header.kind, &body),
                    Ok(PeerRequestBody::Vote(_))
                ));
                let response = message::PeerResponse {
                    header: PeerHeader::response_for(
                        &header,
                        NodeId::new(2).expect("test target node"),
                    ),
                    body: PeerResponseBody::Vote(VoteResponse::new(Vote::new(1, 1), None, true)),
                };
                let frame =
                    message::encode_response(&response, rockbee_raft_protocol::MAX_FRAME_PAYLOAD)
                        .expect("encode vote response");
                stream.write_all(&frame).await.expect("write vote response");
                stream.flush().await.expect("flush vote response");
            }
        });

        let resources =
            Arc::new(PeerResources::new(PeerLimits::default()).expect("valid test peer limits"));
        let mut first_factory =
            PeerNetworkFactory::new(test_identity(1), GroupId::new(1), resources);
        let mut second_factory = first_factory.clone();
        let remote = BasicNode::new(address);
        let mut first = first_factory.new_client(2, &remote).await;
        let mut second = second_factory.new_client(2, &remote).await;
        let request = || VoteRequest::new(Vote::new(1, 1), None);
        let option = RPCOption::new(Duration::from_secs(1));

        first
            .vote(request(), option.clone())
            .await
            .expect("first vote succeeds");
        second
            .vote(request(), option)
            .await
            .expect("second vote succeeds");
        server.await.expect("peer server task succeeds");
        assert_eq!(
            accepted.load(Ordering::SeqCst),
            1,
            "one socket serves both votes"
        );
    }

    #[tokio::test]
    async fn concurrent_groups_progress_while_one_peer_response_is_delayed() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind owned peer listener");
        let address = listener
            .local_addr()
            .expect("read owned listener address")
            .to_string();
        let first_received = Arc::new(tokio::sync::Notify::new());
        let release_first = Arc::new(tokio::sync::Notify::new());
        let finish_server = Arc::new(tokio::sync::Notify::new());
        let server_first_received = Arc::clone(&first_received);
        let server_release_first = Arc::clone(&release_first);
        let server_finish = Arc::clone(&finish_server);
        let server = tokio::spawn(async move {
            let (mut first_stream, _) = listener.accept().await.expect("accept first peer");
            let (first_header, first_body_len) =
                read_peer_header(&mut first_stream, rockbee_raft_protocol::MAX_FRAME_PAYLOAD)
                    .await
                    .expect("read first vote header");
            let first_body = read_peer_body(&mut first_stream, first_body_len)
                .await
                .expect("read first vote body");
            assert!(matches!(
                decode_request_body(first_header.kind, &first_body),
                Ok(PeerRequestBody::Vote(_))
            ));
            assert_eq!(first_header.group_id, GroupId::new(1));
            server_first_received.notify_one();

            let (mut second_stream, _) = listener.accept().await.expect("accept second peer");
            let (second_header, second_body_len) =
                read_peer_header(&mut second_stream, rockbee_raft_protocol::MAX_FRAME_PAYLOAD)
                    .await
                    .expect("read second vote header");
            let second_body = read_peer_body(&mut second_stream, second_body_len)
                .await
                .expect("read second vote body");
            assert!(matches!(
                decode_request_body(second_header.kind, &second_body),
                Ok(PeerRequestBody::Vote(_))
            ));
            assert_eq!(second_header.group_id, GroupId::new(2));
            let second_response = message::PeerResponse {
                header: PeerHeader::response_for(
                    &second_header,
                    NodeId::new(2).expect("test target node"),
                ),
                body: PeerResponseBody::Vote(VoteResponse::new(Vote::new(1, 1), None, true)),
            };
            let second_frame = message::encode_response(
                &second_response,
                rockbee_raft_protocol::MAX_FRAME_PAYLOAD,
            )
            .expect("encode second vote response");
            second_stream
                .write_all(&second_frame)
                .await
                .expect("write second vote response");
            second_stream
                .flush()
                .await
                .expect("flush second vote response");

            server_release_first.notified().await;
            let first_response = message::PeerResponse {
                header: PeerHeader::response_for(
                    &first_header,
                    NodeId::new(2).expect("test target node"),
                ),
                body: PeerResponseBody::Vote(VoteResponse::new(Vote::new(1, 1), None, true)),
            };
            let first_frame =
                message::encode_response(&first_response, rockbee_raft_protocol::MAX_FRAME_PAYLOAD)
                    .expect("encode first vote response");
            first_stream
                .write_all(&first_frame)
                .await
                .expect("write first vote response");
            first_stream
                .flush()
                .await
                .expect("flush first vote response");
            server_finish.notified().await;
        });

        let limits = PeerLimits {
            max_connections: 2,
            max_control_operations: 2,
            max_snapshot_operations: 1,
            ..PeerLimits::default()
        };
        let resources = Arc::new(PeerResources::new(limits).expect("valid bounded peer limits"));
        let remote = BasicNode::new(address.clone());
        let mut first =
            PeerNetworkFactory::new(test_identity(1), GroupId::new(1), Arc::clone(&resources))
                .new_client(2, &remote)
                .await;
        let mut second =
            PeerNetworkFactory::new(test_identity(1), GroupId::new(2), Arc::clone(&resources))
                .new_client(2, &remote)
                .await;
        let option = RPCOption::new(Duration::from_secs(5));
        let first_vote = tokio::spawn(async move {
            first
                .vote(VoteRequest::new(Vote::new(1, 1), None), option)
                .await
        });

        timeout(Duration::from_secs(1), first_received.notified())
            .await
            .expect("first vote reaches peer");
        let second_result = second
            .vote(
                VoteRequest::new(Vote::new(1, 1), None),
                RPCOption::new(Duration::from_secs(1)),
            )
            .await;
        assert!(
            second_result.is_ok(),
            "second group must not wait for first reply"
        );
        assert!(
            !first_vote.is_finished(),
            "first vote must remain pending until its delayed response is released"
        );

        release_first.notify_one();
        assert!(first_vote.await.expect("first vote task joins").is_ok());
        let key = ControlConnectionKey::new(
            test_identity(1),
            NodeId::new(2).expect("test target node"),
            address,
        );
        let first_reused = timeout(
            Duration::from_secs(1),
            resources.checkout_outgoing_control(key.clone()),
        )
        .await
        .expect("checkout first returned stream does not wait")
        .expect("checkout first returned stream");
        assert!(matches!(&first_reused, ControlCheckout::Reused(_)));
        let second_reused = timeout(
            Duration::from_secs(1),
            resources.checkout_outgoing_control(key),
        )
        .await
        .expect("checkout second returned stream does not wait")
        .expect("checkout second returned stream");
        assert!(matches!(&second_reused, ControlCheckout::Reused(_)));
        drop(first_reused);
        drop(second_reused);
        let first_permit = timeout(
            Duration::from_secs(1),
            resources.acquire_outgoing_connection(),
        )
        .await
        .expect("first connection permit release does not wait")
        .expect("first connection permit released");
        let second_permit = timeout(
            Duration::from_secs(1),
            resources.acquire_outgoing_connection(),
        )
        .await
        .expect("second connection permit release does not wait")
        .expect("second connection permit released");
        drop(first_permit);
        drop(second_permit);
        finish_server.notify_one();
        server.await.expect("peer server task joins");
    }

    fn test_identity(node_id: u64) -> PeerIdentity {
        PeerIdentity {
            cluster_id: rockbee_raft_protocol::ClusterId::new([7; 16])
                .expect("test cluster identity"),
            node_id: NodeId::new(node_id).expect("test node identity"),
        }
    }
}
