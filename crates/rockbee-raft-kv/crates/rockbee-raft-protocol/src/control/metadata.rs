use serde::{Deserialize, Serialize};

use crate::{GroupId, MAX_MESSAGE_BYTES, NodeId, ProtocolError};

use super::{MoveIntent, MoveJobId, MoveReplica};

/// Full committed Raft identity, not a metric index or leader hint.
///
/// This is a typed representation of trusted internal observation supplied by
/// the authorized reconciler. Constructing or decoding it cannot prove a commit;
/// metadata apply must never accept it as an external caller witness.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CommittedLogPosition {
    /// Raft term from the committed log identity.
    pub term: u64,
    /// Leader node identity paired with the term.
    pub leader_node_id: NodeId,
    /// Log index paired with the term and leader identity.
    pub index: u64,
}

/// Durable progress of the one allowed lifetime move for a data group.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MoveProgress {
    /// The durable target is accepted but final membership is not published.
    Pending,
    /// Final uniform membership was observed and recorded by the reconciler.
    Completed {
        /// Trusted full log identity of the observed committed membership.
        membership: CommittedLogPosition,
    },
}

/// Metadata state stored for a group. The immutable intent is retained forever.
///
/// Metadata apply owns prospective-catalog union checks across every stored job
/// and applies each conditional transition in its own durable metadata chunk.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MoveRecord {
    /// Immutable placement target retained for the group's lifetime.
    pub intent: MoveIntent,
    /// Current durable observation of the target's completion.
    pub progress: MoveProgress,
}

impl MoveRecord {
    /// Validates the immutable move payload retained by this durable record.
    ///
    /// # Errors
    /// Returns an error when the embedded intent is not a valid fixed
    /// three-voter replacement.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.intent.validate()
    }
}

/// Engine-free control operations. The runtime obtains directory/membership
/// authority and applies these through group-zero conditional metadata logic.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "AdminOperationWire", into = "AdminOperationWire")]
pub enum AdminOperation {
    /// Proposes exactly one fixed replacement for a data group.
    MoveReplica(MoveReplica),
    /// Reads the immutable move record for a data group.
    GetMove {
        /// Target data group, never metadata group zero.
        data_group: GroupId,
    },
    /// Resumes the stored job through a linearizable metadata read.
    ReconcileMove {
        /// Target data group, never metadata group zero.
        data_group: GroupId,
        /// Existing durable job identity to reconcile.
        job_id: MoveJobId,
    },
    /// Requests a trusted current membership observation for a data group.
    ObserveMembership {
        /// Target data group, never metadata group zero.
        data_group: GroupId,
    },
}

/// Serializable admin-operation form validated at the protocol boundary.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum AdminOperationWire {
    MoveReplica(MoveReplica),
    GetMove {
        data_group: GroupId,
    },
    ReconcileMove {
        data_group: GroupId,
        job_id: MoveJobId,
    },
    ObserveMembership {
        data_group: GroupId,
    },
}

impl TryFrom<AdminOperationWire> for AdminOperation {
    type Error = ProtocolError;

    fn try_from(value: AdminOperationWire) -> Result<Self, Self::Error> {
        match value {
            AdminOperationWire::MoveReplica(request) => Ok(Self::MoveReplica(request)),
            AdminOperationWire::GetMove { data_group } => {
                validate_control_group(data_group)?;
                Ok(Self::GetMove { data_group })
            }
            AdminOperationWire::ReconcileMove { data_group, job_id } => {
                validate_control_group(data_group)?;
                Ok(Self::ReconcileMove { data_group, job_id })
            }
            AdminOperationWire::ObserveMembership { data_group } => {
                validate_control_group(data_group)?;
                Ok(Self::ObserveMembership { data_group })
            }
        }
    }
}

impl From<AdminOperation> for AdminOperationWire {
    fn from(value: AdminOperation) -> Self {
        match value {
            AdminOperation::MoveReplica(request) => Self::MoveReplica(request),
            AdminOperation::GetMove { data_group } => Self::GetMove { data_group },
            AdminOperation::ReconcileMove { data_group, job_id } => {
                Self::ReconcileMove { data_group, job_id }
            }
            AdminOperation::ObserveMembership { data_group } => {
                Self::ObserveMembership { data_group }
            }
        }
    }
}

/// Small deterministic control responses; callers re-read durable records.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "AdminResultWire", into = "AdminResultWire")]
pub enum AdminResult {
    /// A conditional metadata proposal persisted this new immutable intent.
    Accepted {
        /// The stored placement target.
        intent: MoveIntent,
    },
    /// The same immutable intent was already durable.
    AlreadyAccepted {
        /// The existing placement target.
        intent: MoveIntent,
    },
    /// Reconciliation observed a durable completed move record.
    Completed {
        /// The record including trusted committed-membership observation.
        record: MoveRecord,
    },
    /// A different immutable target is permanently present for the group.
    Conflict {
        /// Bounded diagnostic for operators; never a retry instruction.
        message: String,
    },
    /// Metadata did not find a requested job or group.
    Unknown {
        /// Bounded diagnostic for operators; never a retry instruction.
        message: String,
    },
}

/// Serializable result form validated before an admin caller sees it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum AdminResultWire {
    Accepted { intent: MoveIntent },
    AlreadyAccepted { intent: MoveIntent },
    Completed { record: MoveRecord },
    Conflict { message: String },
    Unknown { message: String },
}

impl TryFrom<AdminResultWire> for AdminResult {
    type Error = ProtocolError;

    fn try_from(value: AdminResultWire) -> Result<Self, Self::Error> {
        let result = match value {
            AdminResultWire::Accepted { intent } => Self::Accepted { intent },
            AdminResultWire::AlreadyAccepted { intent } => Self::AlreadyAccepted { intent },
            AdminResultWire::Completed { record } => Self::Completed { record },
            AdminResultWire::Conflict { message } => Self::Conflict { message },
            AdminResultWire::Unknown { message } => Self::Unknown { message },
        };
        result.validate()?;
        Ok(result)
    }
}

impl From<AdminResult> for AdminResultWire {
    fn from(value: AdminResult) -> Self {
        match value {
            AdminResult::Accepted { intent } => Self::Accepted { intent },
            AdminResult::AlreadyAccepted { intent } => Self::AlreadyAccepted { intent },
            AdminResult::Completed { record } => Self::Completed { record },
            AdminResult::Conflict { message } => Self::Conflict { message },
            AdminResult::Unknown { message } => Self::Unknown { message },
        }
    }
}

impl AdminResult {
    /// Checks bounded diagnostic fields after direct construction or decoding.
    ///
    /// # Errors
    /// Returns an error when a diagnostic exceeds the protocol message budget,
    /// an embedded intent is invalid, or a completed envelope holds a pending
    /// record.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        match self {
            Self::Accepted { intent } | Self::AlreadyAccepted { intent } => intent.validate(),
            Self::Completed { record } => {
                record.validate()?;
                if !matches!(&record.progress, MoveProgress::Completed { .. }) {
                    return Err(ProtocolError::InvalidRouting(
                        "completed result requires completed move progress",
                    ));
                }
                Ok(())
            }
            Self::Conflict { message } | Self::Unknown { message } => {
                if message.len() > MAX_MESSAGE_BYTES {
                    return Err(ProtocolError::MessageTooLarge(message.len()));
                }
                Ok(())
            }
        }
    }
}

fn validate_control_group(data_group: GroupId) -> Result<(), ProtocolError> {
    if data_group.is_metadata() || data_group.get() > 256 {
        return Err(ProtocolError::InvalidRouting(
            "control operation requires a configured data group",
        ));
    }
    Ok(())
}
