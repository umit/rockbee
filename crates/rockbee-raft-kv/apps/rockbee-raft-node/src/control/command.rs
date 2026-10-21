use rockbee_raft_protocol::{
    GroupId, NodeId,
    backend::AppliedMembership,
    control::{CommittedLogPosition, MoveIntent, MoveJobId},
    routing::{ClusterDirectory, NodeEndpoint},
};
use serde::{Deserialize, Serialize};

use super::{encoding, state::MAX_DIRECTORY_BYTES, validation};

/// Serializable group-zero metadata proposals for singleton durable apply.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MetadataCommand {
    InitializeDirectoryIfAbsent {
        initial_directory: ClusterDirectory,
    },
    BeginMove {
        intent: MoveIntent,
    },
    CompleteMove {
        expected_directory_revision: u64,
        data_group: GroupId,
        job_id: MoveJobId,
        observation: MembershipObservation,
    },
    /// Records preparation only; it does not authorize native membership work.
    PrepareSuccessor {
        predecessor_job_id: MoveJobId,
        intent: MoveIntent,
    },
    /// Internal conditional transition; callers cannot supply it through public KV.
    ActivateSuccessor {
        expected_directory_revision: u64,
        data_group: GroupId,
        predecessor_job_id: MoveJobId,
        successor_job_id: MoveJobId,
        proof: Box<HandoffProof>,
    },
}

impl MetadataCommand {
    /// Checks self-contained shape at durable-log and private-peer boundaries.
    /// Cross-record conditions remain normal apply-time conflicts.
    ///
    /// # Errors
    /// Returns an error for invalid intrinsic fields or oversized encoding.
    pub fn validate(&self) -> Result<(), rockbee_raft_protocol::ProtocolError> {
        use rockbee_raft_protocol::ProtocolError;
        match self {
            Self::InitializeDirectoryIfAbsent { initial_directory } => {
                initial_directory.validate()?;
                validation::validate_bootstrap_shape(initial_directory)
                    .map_err(|_| ProtocolError::InvalidRouting("invalid bootstrap shape"))?;
                encoding::check_size(initial_directory, MAX_DIRECTORY_BYTES)
                    .map_err(|_| ProtocolError::InvalidRouting("oversized bootstrap directory"))?;
            }
            Self::BeginMove { intent } | Self::PrepareSuccessor { intent, .. } => {
                intent.validate()?
            }
            Self::ActivateSuccessor {
                expected_directory_revision,
                data_group,
                predecessor_job_id,
                successor_job_id,
                proof,
            } => {
                proof.observed.validate()?;
                if *expected_directory_revision == 0
                    || data_group.is_metadata()
                    || data_group.get() > 256
                    || predecessor_job_id == successor_job_id
                    || proof.fence.data_group != *data_group
                    || proof.fence.predecessor_job_id != *predecessor_job_id
                    || proof.fence.successor_job_id != *successor_job_id
                    || proof.fence.predecessor_revision == 0
                    || proof.observed.data_group != *data_group
                    || proof.seals.iter().any(|seal| {
                        seal.data_group != *data_group
                            || seal.predecessor_job_id != *predecessor_job_id
                            || seal.predecessor_revision != proof.fence.predecessor_revision
                    })
                {
                    return Err(ProtocolError::InvalidRouting(
                        "invalid successor activation shape",
                    ));
                }
            }
            Self::CompleteMove {
                expected_directory_revision,
                data_group,
                observation,
                ..
            } => {
                if *expected_directory_revision == 0
                    || data_group.is_metadata()
                    || data_group.get() > 256
                    || observation.voters.windows(2).any(|pair| pair[0] >= pair[1])
                {
                    return Err(ProtocolError::InvalidRouting("invalid completion shape"));
                }
                let endpoint = &observation.replacement_endpoint;
                if endpoint.peer_address == endpoint.data_address
                    || [endpoint.peer_address, endpoint.data_address]
                        .iter()
                        .any(|address| {
                            address.port() == 0
                                || address.ip().is_unspecified()
                                || address.ip().is_multicast()
                        })
                {
                    return Err(ProtocolError::InvalidRouting("invalid completion endpoint"));
                }
            }
        }
        Ok(())
    }
}

/// Internal observation of committed membership, never an external certificate.
///
/// Private fields prevent ordinary callers from constructing a witness. Serde
/// exists for trusted internal Raft records only; no external RPC accepts it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MembershipObservation {
    pub(crate) uniform: bool,
    pub(crate) voters: [NodeId; 3],
    pub(crate) replacement_endpoint: NodeEndpoint,
    pub(crate) membership: CommittedLogPosition,
}

/// Small responses; consumers re-read directory or job state when needed.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MetadataResult {
    Initialized {
        revision: u64,
    },
    AlreadyInitialized {
        revision: u64,
    },
    Accepted {
        data_group: GroupId,
        job_id: MoveJobId,
    },
    AlreadyAccepted {
        data_group: GroupId,
        job_id: MoveJobId,
    },
    Completed {
        data_group: GroupId,
        job_id: MoveJobId,
        revision: u64,
    },
    AlreadyCompleted {
        data_group: GroupId,
        job_id: MoveJobId,
        revision: u64,
    },
    Conflict {
        reason: ConflictReason,
    },
    Prepared {
        data_group: GroupId,
        job_id: MoveJobId,
    },
    AlreadyPrepared {
        data_group: GroupId,
        job_id: MoveJobId,
    },
    Activated {
        data_group: GroupId,
        job_id: MoveJobId,
    },
    AlreadyActivated {
        data_group: GroupId,
        job_id: MoveJobId,
    },
}

/// Conditional rejection is normal; it does not change durable state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ConflictReason {
    BootstrapMismatch,
    RevisionMismatch,
    PlacementMismatch,
    ExistingMove,
    UnknownMove,
    InvalidIntent,
    EndpointUnion,
    MembershipMismatch,
    RevisionExhausted,
}

pub use rockbee_raft_protocol::backend::{HandoffFenceReceipt, PreparingSuccessor, SealReceipt};

/// Fixed evidence retained by an internal activation. Static validation checks
/// binding and history coverage; actual seal-before-fence ordering is runtime work.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HandoffProof {
    pub seals: [SealReceipt; 4],
    pub fence: HandoffFenceReceipt,
    pub observed: AppliedMembership,
}
