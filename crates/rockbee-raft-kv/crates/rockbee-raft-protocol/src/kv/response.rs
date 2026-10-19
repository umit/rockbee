//! Read results and mutation certainty; validate against the originating operation.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::{
    Failure, KvExpectedResponse, KvRequest, ProtocolError, ReadFailure, WireRequest,
    validation::validate_value,
};

/// Mutation completion semantics. Request IDs do not make retries safe.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MutationOutcome {
    /// Durable consensus acknowledgement.
    Committed,
    /// Proven before local transport handoff. Client-local only: received wire
    /// outcomes using this variant are rejected by `validate_for`. A remote
    /// service that proves no consensus submission must use `Rejected`.
    NotSent(Failure),
    /// Proven rejected before consensus submission.
    Rejected(Failure),
    /// May have committed; callers must not automatically replay the mutation.
    Unknown(Failure),
}

/// A missing key is `Value(None)`, distinct from an empty stored value.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum KvResponse {
    /// GET result: `None` is missing; `Some(empty)` is a stored empty value.
    Value(Option<Bytes>),
    /// PUT/DELETE completion with explicit consensus/transport certainty.
    Mutation(MutationOutcome),
    /// A read failed; no mutation outcome is implied.
    ReadError(ReadFailure),
}

impl MutationOutcome {
    /// Checks a received outcome; only local code may produce `NotSent`.
    pub(crate) fn validate_wire(&self) -> Result<(), ProtocolError> {
        match self {
            Self::Committed => Ok(()),
            Self::Rejected(failure) | Self::Unknown(failure) => failure.validate(),
            Self::NotSent(_) => Err(ProtocolError::RemoteNotSent),
        }
    }
}

impl KvResponse {
    /// Checks operation compatibility and the received payload's field bounds.
    pub(crate) fn validate_for(&self, request: &KvRequest) -> Result<(), ProtocolError> {
        self.validate_expected(request.expected_response())
    }

    pub(crate) fn validate_expected(
        &self,
        expected: KvExpectedResponse,
    ) -> Result<(), ProtocolError> {
        match (expected, self) {
            (KvExpectedResponse::Read, Self::Value(value)) => {
                if let Some(value) = value {
                    validate_value(value)?;
                }
                Ok(())
            }
            (KvExpectedResponse::Read, Self::ReadError(failure)) => failure.0.validate(),
            (KvExpectedResponse::Mutation, Self::Mutation(outcome)) => outcome.validate_wire(),
            _ => Err(ProtocolError::ResponseKindMismatch),
        }
    }
}
