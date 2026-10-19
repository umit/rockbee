//! Remote service failure reasons and bounded diagnostic messages.

use serde::{Deserialize, Serialize};

use crate::{MAX_MESSAGE_BYTES, ProtocolError};

/// Core transport/service reasons, without routing hints or engine details.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum FailureReason {
    /// The operation or fields were invalid at the receiving boundary.
    InvalidRequest,
    /// Admission capacity was unavailable before service submission.
    Overloaded,
    /// The request budget elapsed.
    Deadline,
    /// A transport connection ended before a final result.
    Disconnected,
    /// The service cannot currently process the operation.
    Unavailable,
    /// The receiving node is not the relevant Raft leader.
    NotLeader,
    /// The operation targets another cluster identity.
    WrongCluster,
    /// The operation targets another Raft group.
    WrongGroup,
    /// Framing, correlation or message validation failed.
    Protocol,
    /// The service encountered an internal failure.
    Internal,
}

/// Typed failure with a bounded diagnostic message.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Failure {
    /// Machine-readable failure category.
    pub reason: FailureReason,
    /// Human-readable bounded diagnostic; never a retry instruction.
    pub message: String,
}

impl Failure {
    /// Creates a failure after checking message size.
    ///
    /// # Errors
    /// Returns an error when the message exceeds 1024 UTF-8 bytes.
    pub fn new(reason: FailureReason, message: impl Into<String>) -> Result<Self, ProtocolError> {
        let failure = Self {
            reason,
            message: message.into(),
        };
        failure.validate()?;
        Ok(failure)
    }

    /// Checks a directly constructed or decoded diagnostic.
    ///
    /// # Errors
    /// Returns an error when the message exceeds 1024 UTF-8 bytes.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.message.len() > MAX_MESSAGE_BYTES {
            return Err(ProtocolError::MessageTooLarge(self.message.len()));
        }
        Ok(())
    }
}

/// A failed read has no mutation uncertainty to report.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ReadFailure(pub Failure);
impl ReadFailure {
    /// Wraps a read failure. Response validation checks its message bound.
    pub fn new(failure: Failure) -> Self {
        Self(failure)
    }
}
