//! Gateway-to-node messages, separate from public client KV and private Raft RPC.
//!
//! A cached leader hint is only a routing suggestion. The destination still
//! checks its authority before reading and classifies writes by submission.
mod handoff;
mod movement;
pub use handoff::{HandoffFenceReceipt, PreparedMoveState, PreparingSuccessor, SealReceipt};
pub use movement::{AppliedMembership, MoveState, ObservedLogPosition, ReconcileStatus};

#[cfg(test)]
mod tests;

use std::num::NonZeroU64;

use serde::{Deserialize, Serialize};

use crate::{
    ClusterId, Failure, GroupId, KvExpectedResponse, KvRequest, KvResponse, NodeId, ProtocolError,
    ReadFailure, WireRequest,
    control::{AdminResult, MoveJobId, MoveReplica},
    routing::ClusterDirectory,
};

/// Certainty of the receiver-owned conditional directory initialization.
/// Public key/value mutation certainty remains a separate contract.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BootstrapOutcome {
    /// Initialization was applied or the same immutable origin already existed.
    Ready {
        /// Current validated directory revision, always nonzero.
        revision: NonZeroU64,
    },
    /// The submitted conditional initialization durably lost its precondition.
    Conflict(Failure),
    /// The receiver proved it had not invoked Raft client_write.
    NotSubmitted(Failure),
    /// Submission may have happened; the final conditional result is unknown.
    Unknown(Failure),
}

impl BootstrapOutcome {
    fn validate(&self) -> Result<(), ProtocolError> {
        match self {
            Self::Ready { .. } => Ok(()),
            Self::Conflict(error) | Self::NotSubmitted(error) | Self::Unknown(error) => {
                error.validate()
            }
        }
    }
}

/// One node operation. Metadata is read through its own operation, not public KV.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BackendOperation {
    /// Linearizable read of the persisted directory from group zero.
    Directory,
    /// Local leader hint, which does not establish read or write authority.
    Leader {
        /// Group whose current leader should be discovered.
        group_id: GroupId,
    },
    /// Public key/value operation already routed to a logical data group.
    Kv {
        /// Destination data group; node verifies the key's configured mapping.
        group_id: GroupId,
        /// Operation carrying opaque key/value bytes.
        request: KvRequest,
    },
    /// Publishes only the receiver's immutable initial directory in group zero.
    /// No caller-supplied metadata or arbitrary reserved-key write is accepted.
    BootstrapDirectory,
    /// Accepts a single immutable lifetime replacement; no voter sets supplied.
    MoveReplica(MoveReplica),
    /// Reads current directory and retained job coherently through group zero.
    GetMove {
        /// Data group whose one lifetime job is requested.
        data_group: GroupId,
    },
    /// Schedules bounded receiver-owned execution of the retained job.
    ReconcileMove {
        /// Data group whose stored target is resumed.
        data_group: GroupId,
        /// Retained immutable job identity.
        job_id: MoveJobId,
    },
    /// Internal authoritative applied observation, never a completion input.
    ObserveMembership {
        /// Actual data authority to observe.
        data_group: GroupId,
    },
    /// Internal trigger; receiver independently obtains completion evidence.
    PublishMove {
        /// Data group whose applied target must be reobserved.
        data_group: GroupId,
        /// Retained immutable job identity, never an observation witness.
        job_id: MoveJobId,
    },
    /// Reads the frozen predecessor and preparing-successor handoff state.
    GetPreparedMove {
        /// Data group whose retained handoff state is requested.
        data_group: GroupId,
    },
    /// Seals the predecessor at one exact executor before the handoff fence.
    SealPredecessor {
        /// Data group whose predecessor is sealed.
        data_group: GroupId,
        /// Directory revision retained by the predecessor move.
        predecessor_revision: u64,
        /// Retained predecessor job identity.
        predecessor_job_id: MoveJobId,
        /// Retained successor job identity.
        successor_job_id: MoveJobId,
    },
    /// Commits the authoritative data-log fence after all predecessor seals.
    CommitHandoffFence {
        /// Data group whose predecessor is fenced.
        data_group: GroupId,
        /// Directory revision retained by the predecessor move.
        predecessor_revision: u64,
        /// Retained predecessor job identity.
        predecessor_job_id: MoveJobId,
        /// Retained successor job identity.
        successor_job_id: MoveJobId,
    },
}

/// Explicit destination identity prevents accidental cross-cluster/node routing.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BackendRequest {
    /// Expected cluster identity.
    pub cluster_id: ClusterId,
    /// Expected node identity, verified independently of its socket address.
    pub target_node: NodeId,
    /// Typed operation to execute.
    pub operation: BackendOperation,
}

/// Typed result of a backend operation; hints never replace actual certainty.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BackendReply {
    /// Successful metadata read or an explicit read failure.
    Directory(Result<ClusterDirectory, ReadFailure>),
    /// A local leader observation, possibly unknown.
    Leader {
        /// Group that the hint describes.
        group_id: GroupId,
        /// Last observed leader; the gateway must validate the destination again.
        leader: Option<NodeId>,
    },
    /// Operation result and an optional routing hint for later safe requests.
    Kv {
        /// Same value/absence/certainty contract as the public client.
        response: KvResponse,
        /// Candidate leader, never permission to replay an uncertain mutation.
        leader_hint: Option<NodeId>,
    },
    /// Conditional initialization result, separate from public mutation results.
    Bootstrap(BootstrapOutcome),
    /// Acceptance or publication result bound to the named job.
    Move {
        /// Group bound to the acceptance or publication request.
        data_group: GroupId,
        /// Job bound to the original request.
        job_id: MoveJobId,
        /// Durable result or explicit bounded conflict/unknown status.
        result: AdminResult,
    },
    /// Coherent metadata view bound to the requested group.
    MoveState {
        /// Group bound to the metadata read request.
        data_group: GroupId,
        /// Coherent directory and retained record, boxed off ordinary KV replies.
        result: Result<Box<MoveState>, ReadFailure>,
    },
    /// Quick admission result; the runner outlives this response.
    Reconcile {
        /// Group bound to the scheduling request.
        data_group: GroupId,
        /// Job bound to the scheduling request.
        job_id: MoveJobId,
        /// Immediate scheduling status, separate from durable completion.
        result: Result<ReconcileStatus, ReadFailure>,
    },
    /// Actual applied membership of the requested group.
    Membership {
        /// Actual group bound to the observation request.
        data_group: GroupId,
        /// Bounded applied proof, boxed to preserve ordinary reply size.
        result: Result<Box<AppliedMembership>, ReadFailure>,
    },
    /// Frozen predecessor and preparing-successor state for a data group.
    PreparedMoveState {
        /// Group bound to the metadata read request.
        data_group: GroupId,
        /// Coherent handoff state, boxed off ordinary KV replies.
        result: Result<Option<Box<PreparedMoveState>>, ReadFailure>,
    },
    /// One receiver's durable predecessor seal receipt.
    PredecessorSealed {
        /// Group bound to the seal request.
        data_group: GroupId,
        /// Retained predecessor job identity.
        predecessor_job_id: MoveJobId,
        /// Retained successor job identity.
        successor_job_id: MoveJobId,
        /// Durable receipt or an explicit read failure.
        result: Result<SealReceipt, ReadFailure>,
    },
    /// The authoritative data-log fence for a predecessor-to-successor handoff.
    HandoffFenced {
        /// Group bound to the fence request.
        data_group: GroupId,
        /// Retained predecessor job identity.
        predecessor_job_id: MoveJobId,
        /// Retained successor job identity.
        successor_job_id: MoveJobId,
        /// Durable receipt or an explicit read failure.
        result: Result<HandoffFenceReceipt, ReadFailure>,
    },
}

/// A backend response identifies the node that produced it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BackendResponse {
    /// Responding cluster identity.
    pub cluster_id: ClusterId,
    /// Responding node identity.
    pub node_id: NodeId,
    /// Typed operation result.
    pub reply: BackendReply,
}

/// Small response-matching token; no request key/value is retained here.
#[derive(Clone, Copy, Debug)]
pub struct BackendExpected {
    cluster_id: ClusterId,
    node_id: NodeId,
    kind: ExpectedKind,
}
#[derive(Clone, Copy, Debug)]
enum ExpectedKind {
    Directory,
    Leader(GroupId),
    Kv(KvExpectedResponse),
    Bootstrap,
    Move(GroupId, MoveJobId),
    MoveState(GroupId),
    Reconcile(GroupId, MoveJobId),
    Membership(GroupId),
    PreparedMoveState(GroupId),
    PredecessorSeal(GroupId, u64, MoveJobId, MoveJobId),
    HandoffFence(GroupId, u64, MoveJobId, MoveJobId),
}

impl WireRequest<BackendResponse> for BackendRequest {
    type Expected = BackendExpected;

    // A directory can contain 257 placement structs and 257*16 voter IDs, plus
    // 16 endpoint structs. Decoded Vecs and validation sets can be much larger
    // than Postcard's compact integers. Reserve 64 KiB before decoding, beyond
    // the ordinary encoded+decoded byte and metadata allowance.
    const RESPONSE_DECODE_HEADROOM: usize = 64 * 1024;

    fn validate(&self) -> Result<(), ProtocolError> {
        match &self.operation {
            BackendOperation::Directory | BackendOperation::BootstrapDirectory => Ok(()),
            BackendOperation::MoveReplica(request) => {
                validate_group(request.data_group, false)?;
                // Round-trip conversion is unnecessary: deriving checks direct construction.
                if request.expected_directory_revision == 0
                    || request.remove_node == request.add_node.node_id
                    || request.add_node.peer_address == request.add_node.data_address
                    || [request.add_node.peer_address, request.add_node.data_address]
                        .iter()
                        .any(|address| {
                            address.port() == 0
                                || address.ip().is_unspecified()
                                || address.ip().is_multicast()
                        })
                {
                    return Err(ProtocolError::InvalidRouting("invalid move request"));
                }
                Ok(())
            }
            BackendOperation::GetMove { data_group }
            | BackendOperation::ReconcileMove { data_group, .. }
            | BackendOperation::ObserveMembership { data_group }
            | BackendOperation::PublishMove { data_group, .. }
            | BackendOperation::GetPreparedMove { data_group } => {
                validate_group(*data_group, false)
            }
            BackendOperation::SealPredecessor {
                data_group,
                predecessor_revision,
                predecessor_job_id,
                successor_job_id,
            }
            | BackendOperation::CommitHandoffFence {
                data_group,
                predecessor_revision,
                predecessor_job_id,
                successor_job_id,
            } => {
                validate_group(*data_group, false)?;
                if *predecessor_revision == 0 {
                    return Err(ProtocolError::InvalidRouting(
                        "invalid predecessor revision",
                    ));
                }
                if predecessor_job_id == successor_job_id {
                    return Err(ProtocolError::InvalidRouting(
                        "predecessor and successor jobs must differ",
                    ));
                }
                Ok(())
            }
            BackendOperation::Leader { group_id } => validate_group(*group_id, true),
            BackendOperation::Kv { group_id, request } => {
                validate_group(*group_id, false)?;
                request.validate()
            }
        }
    }

    fn retained_bytes(&self) -> Result<usize, ProtocolError> {
        self.validate()?;
        let body = match &self.operation {
            BackendOperation::Kv { request, .. } => request.retained_bytes()?,
            _ => 0,
        };
        // Counting the inline KV body twice is deliberately conservative and
        // small; retained byte admission must cover every owned payload.
        Ok(size_of::<Self>() + body)
    }

    fn expected_response(&self) -> Self::Expected {
        BackendExpected {
            cluster_id: self.cluster_id,
            node_id: self.target_node,
            kind: match &self.operation {
                BackendOperation::MoveReplica(request) => {
                    ExpectedKind::Move(request.data_group, request.job_id)
                }
                BackendOperation::GetMove { data_group } => ExpectedKind::MoveState(*data_group),
                BackendOperation::ReconcileMove { data_group, job_id } => {
                    ExpectedKind::Reconcile(*data_group, *job_id)
                }
                BackendOperation::ObserveMembership { data_group } => {
                    ExpectedKind::Membership(*data_group)
                }
                BackendOperation::PublishMove { data_group, job_id } => {
                    ExpectedKind::Move(*data_group, *job_id)
                }
                BackendOperation::GetPreparedMove { data_group } => {
                    ExpectedKind::PreparedMoveState(*data_group)
                }
                BackendOperation::SealPredecessor {
                    data_group,
                    predecessor_revision,
                    predecessor_job_id,
                    successor_job_id,
                } => ExpectedKind::PredecessorSeal(
                    *data_group,
                    *predecessor_revision,
                    *predecessor_job_id,
                    *successor_job_id,
                ),
                BackendOperation::CommitHandoffFence {
                    data_group,
                    predecessor_revision,
                    predecessor_job_id,
                    successor_job_id,
                } => ExpectedKind::HandoffFence(
                    *data_group,
                    *predecessor_revision,
                    *predecessor_job_id,
                    *successor_job_id,
                ),
                BackendOperation::Directory => ExpectedKind::Directory,
                BackendOperation::BootstrapDirectory => ExpectedKind::Bootstrap,
                BackendOperation::Leader { group_id } => ExpectedKind::Leader(*group_id),
                BackendOperation::Kv { request, .. } => {
                    ExpectedKind::Kv(request.expected_response())
                }
            },
        }
    }

    fn validate_response(
        expected: Self::Expected,
        response: &BackendResponse,
    ) -> Result<(), ProtocolError> {
        if response.cluster_id != expected.cluster_id || response.node_id != expected.node_id {
            return Err(ProtocolError::InvalidRouting(
                "backend response identity mismatch",
            ));
        }
        match (expected.kind, &response.reply) {
            (ExpectedKind::Directory, BackendReply::Directory(Ok(directory))) => {
                directory.validate()?;
                if directory.cluster_id != expected.cluster_id {
                    return Err(ProtocolError::InvalidRouting("directory cluster mismatch"));
                }
                Ok(())
            }
            (ExpectedKind::Directory, BackendReply::Directory(Err(error))) => error.0.validate(),
            (ExpectedKind::Leader(expected), BackendReply::Leader { group_id, .. })
                if expected == *group_id =>
            {
                Ok(())
            }
            (ExpectedKind::Kv(expected), BackendReply::Kv { response, .. }) => {
                KvRequest::validate_response(expected, response)
            }
            (ExpectedKind::Bootstrap, BackendReply::Bootstrap(outcome)) => outcome.validate(),
            (
                ExpectedKind::Move(group, job),
                BackendReply::Move {
                    data_group,
                    job_id,
                    result,
                },
            ) if group == *data_group && job == *job_id => {
                result.validate()?;
                let intent = match result {
                    AdminResult::Accepted { intent } | AdminResult::AlreadyAccepted { intent } => {
                        Some(intent)
                    }
                    AdminResult::Completed { record } => Some(&record.intent),
                    _ => None,
                };
                if intent.is_some_and(|intent| intent.data_group != group || intent.job_id != job) {
                    return Err(ProtocolError::ResponseKindMismatch);
                }
                Ok(())
            }
            (ExpectedKind::MoveState(group), BackendReply::MoveState { data_group, result })
                if group == *data_group =>
            {
                match result {
                    Ok(state) => {
                        state.directory.validate()?;
                        if state.directory.cluster_id != expected.cluster_id {
                            return Err(ProtocolError::InvalidRouting(
                                "move directory cluster mismatch",
                            ));
                        }
                        if let Some(record) = &state.record {
                            record.validate()?;
                            if record.intent.data_group != group {
                                return Err(ProtocolError::ResponseKindMismatch);
                            }
                        }
                        Ok(())
                    }
                    Err(error) => error.0.validate(),
                }
            }
            (
                ExpectedKind::Reconcile(group, job),
                BackendReply::Reconcile {
                    data_group,
                    job_id,
                    result,
                },
            ) if group == *data_group && job == *job_id => match result {
                Ok(_) => Ok(()),
                Err(error) => error.0.validate(),
            },
            (ExpectedKind::Membership(group), BackendReply::Membership { data_group, result })
                if group == *data_group =>
            {
                match result {
                    Ok(observation) if observation.data_group == group => observation.validate(),
                    Ok(_) => Err(ProtocolError::ResponseKindMismatch),
                    Err(error) => error.0.validate(),
                }
            }
            (
                ExpectedKind::PreparedMoveState(group),
                BackendReply::PreparedMoveState { data_group, result },
            ) if group == *data_group => match result {
                Ok(Some(state)) => {
                    state.validate()?;
                    if state.directory.cluster_id != expected.cluster_id
                        || state.first.intent.data_group != group
                    {
                        return Err(ProtocolError::ResponseKindMismatch);
                    }
                    Ok(())
                }
                Ok(None) => Ok(()),
                Err(error) => error.0.validate(),
            },
            (
                ExpectedKind::PredecessorSeal(group, revision, predecessor_job, successor_job),
                BackendReply::PredecessorSealed {
                    data_group,
                    predecessor_job_id,
                    successor_job_id,
                    result,
                },
            ) if group == *data_group
                && predecessor_job == *predecessor_job_id
                && successor_job == *successor_job_id =>
            {
                match result {
                    Ok(receipt)
                        if receipt.endpoint.node_id == expected.node_id
                            && receipt.data_group == group
                            && receipt.predecessor_revision == revision
                            && receipt.predecessor_job_id == predecessor_job =>
                    {
                        validate_endpoint(&receipt.endpoint)
                    }
                    Ok(_) => Err(ProtocolError::ResponseKindMismatch),
                    Err(error) => error.0.validate(),
                }
            }
            (
                ExpectedKind::HandoffFence(group, revision, predecessor_job, successor_job),
                BackendReply::HandoffFenced {
                    data_group,
                    predecessor_job_id,
                    successor_job_id,
                    result,
                },
            ) if group == *data_group
                && predecessor_job == *predecessor_job_id
                && successor_job == *successor_job_id =>
            {
                match result {
                    Ok(receipt)
                        if receipt.data_group == group
                            && receipt.predecessor_revision == revision
                            && receipt.predecessor_job_id == predecessor_job
                            && receipt.successor_job_id == successor_job =>
                    {
                        Ok(())
                    }
                    Ok(_) => Err(ProtocolError::ResponseKindMismatch),
                    Err(error) => error.0.validate(),
                }
            }
            _ => Err(ProtocolError::ResponseKindMismatch),
        }
    }
}

fn validate_group(group: GroupId, allow_metadata: bool) -> Result<(), ProtocolError> {
    if group.get() > 256 || (!allow_metadata && group.is_metadata()) {
        return Err(ProtocolError::InvalidRouting("invalid destination group"));
    }
    Ok(())
}

fn validate_endpoint(endpoint: &crate::routing::NodeEndpoint) -> Result<(), ProtocolError> {
    if endpoint.peer_address == endpoint.data_address
        || [endpoint.peer_address, endpoint.data_address]
            .iter()
            .any(|address| {
                address.port() == 0 || address.ip().is_unspecified() || address.ip().is_multicast()
            })
    {
        return Err(ProtocolError::InvalidRouting("invalid seal endpoint"));
    }
    Ok(())
}
