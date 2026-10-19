//! The body contract needed by one multiplexed connection.

use serde::Serialize;

use crate::{KvRequest, KvResponse, ProtocolError};

/// A trusted message family with bounded serialization and response decoding.
///
/// `retained_bytes` includes logical owned heap buffers and the body itself.
/// Implementations must bound decoder allocations by the encoded response size
/// plus the transport's metadata headroom. Arbitrary nested collection visitors
/// are not safe merely because they implement `DeserializeOwned`; see
/// [`crate::decode_frame`]. `Bytes` slices can retain larger caller allocations:
/// the budget bounds logical bytes, not total process RSS.
pub trait WireRequest<Resp>: Serialize + Send + 'static {
    /// A small copyable response-kind token; never a second owned request.
    type Expected: Copy + Send + 'static;

    /// Extra bytes reserved before decoding this family's response collections.
    /// Byte/string-only KV messages need no extra collection allowance. A new
    /// bounded collection schema must account for its decoded representation and
    /// validation scratch space; the encoded length alone is insufficient.
    const RESPONSE_DECODE_HEADROOM: usize = 0;

    /// Checks operation fields before any transport admission or encoding.
    fn validate(&self) -> Result<(), ProtocolError>;
    /// Conservative logical owned bytes, including this body's inline size.
    fn retained_bytes(&self) -> Result<usize, ProtocolError>;
    /// Extracts only the information needed to match a response's body kind.
    fn expected_response(&self) -> Self::Expected;
    /// Validates kind and field bounds after decode, outside the pending lock.
    fn validate_response(expected: Self::Expected, response: &Resp) -> Result<(), ProtocolError>;
}

/// Small compatibility token, without retaining the original key/value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KvExpectedResponse {
    /// A GET expects `Value` or `ReadError`.
    Read,
    /// PUT/DELETE expect a valid remote `Mutation` outcome.
    Mutation,
}

impl WireRequest<KvResponse> for KvRequest {
    type Expected = KvExpectedResponse;
    fn validate(&self) -> Result<(), ProtocolError> {
        self.validate()
    }

    fn retained_bytes(&self) -> Result<usize, ProtocolError> {
        self.validate()?;
        let data = match self {
            Self::Put { key, value } => key.len() + value.len(),
            Self::Get { key } | Self::Delete { key } => key.len(),
        };
        // Validation bounds both operands before this addition.
        Ok(size_of::<Self>() + data)
    }

    fn expected_response(&self) -> Self::Expected {
        match self {
            Self::Get { .. } => KvExpectedResponse::Read,
            Self::Put { .. } | Self::Delete { .. } => KvExpectedResponse::Mutation,
        }
    }

    fn validate_response(
        expected: Self::Expected,
        response: &KvResponse,
    ) -> Result<(), ProtocolError> {
        response.validate_expected(expected)
    }
}
