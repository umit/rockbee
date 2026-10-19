//! Correlation/version headers around operation payloads.
//! Decode first, then validate the envelope before using its payload.

use serde::{Deserialize, Serialize};

use crate::{
    KvRequest, KvResponse, ProtocolError, RequestId, VERSION,
    validation::{validate_budget, validate_version},
};

/// Request correlation and the caller's remaining service budget.
///
/// Public fields support inspection and direct construction. Call `validate()`
/// at trust boundaries; serde only enforces the private ID type invariants.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RequestEnvelope<R = KvRequest> {
    /// Wire schema version; must equal `VERSION`.
    pub version: u8,
    /// Nonzero ID scoped to this TCP connection; not a retry deduplication token.
    pub request_id: RequestId,
    /// Remaining caller budget, stamped after queueing; no shared wall clock.
    pub budget_ms: u64,
    /// Typed operation body; defaults to public `KvRequest`.
    pub request: R,
}

impl RequestEnvelope<KvRequest> {
    /// Builds and validates a current-version request.
    ///
    /// # Errors
    /// Returns an error for a budget outside `1..=60_000` or invalid fields.
    pub fn new(
        request_id: RequestId,
        budget_ms: u64,
        request: KvRequest,
    ) -> Result<Self, ProtocolError> {
        let envelope = Self {
            version: VERSION,
            request_id,
            budget_ms,
            request,
        };
        envelope.validate()?;
        Ok(envelope)
    }

    /// Validates a directly constructed or decoded request.
    ///
    /// # Errors
    /// Returns an error for an unsupported version, budget, key, or value.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        validate_version(self.version)?;
        validate_budget(self.budget_ms)?;
        self.request.validate()
    }
}

/// A response correlated with one request.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResponseEnvelope<R = KvResponse> {
    /// Wire schema version; must equal `VERSION`.
    pub version: u8,
    /// Nonzero ID scoped to this TCP connection; not a retry deduplication token.
    pub request_id: RequestId,
    /// Typed result body; defaults to public `KvResponse`.
    pub response: R,
}

impl ResponseEnvelope<KvResponse> {
    /// Validates a received wire response against the original request.
    ///
    /// # Errors
    /// Returns an error for invalid request/version, mismatched ID or response
    /// kind, oversized value/diagnostic, or a client-local `NotSent` outcome.
    pub fn validate_for(&self, request: &RequestEnvelope) -> Result<(), ProtocolError> {
        request.validate()?;
        validate_version(self.version)?;
        if self.request_id != request.request_id {
            return Err(ProtocolError::RequestIdMismatch);
        }
        self.response.validate_for(&request.request)
    }
}
