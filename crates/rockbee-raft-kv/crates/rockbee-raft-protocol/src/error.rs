//! Local validation and codec errors, separate from remote failure outcomes.

use thiserror::Error;

/// Validation and framing errors; local errors, not remote outcomes.
#[derive(Debug, Error)]
pub enum ProtocolError {
    /// A directory or backend identity violates routing invariants.
    #[error("invalid routing data: {0}")]
    InvalidRouting(&'static str),
    #[error("invalid zero identifier: {0}")]
    /// A nonzero typed identifier was constructed or decoded as zero.
    InvalidIdentifier(&'static str),
    #[error("unsupported wire version: {0}")]
    /// The envelope uses an incompatible wire schema version.
    UnsupportedVersion(u8),
    #[error("unsupported hash version: {0}")]
    /// The routing configuration names an unsupported hash algorithm version.
    UnsupportedHashVersion(u8),
    #[error("invalid shard count: {0}")]
    /// The shard count is zero or exceeds the supported routing bound.
    InvalidShardCount(u16),
    #[error("invalid request budget in milliseconds: {0}")]
    /// Remaining request time is outside the allowed millisecond range.
    InvalidBudget(u64),
    #[error("invalid key length: {0}")]
    /// The key is empty or larger than the opaque key limit.
    InvalidKeyLength(usize),
    #[error("value exceeds maximum length: {0}")]
    /// A request or response value exceeds the value byte limit.
    ValueTooLarge(usize),
    #[error("failure message exceeds maximum UTF-8 byte length: {0}")]
    /// A diagnostic exceeds the UTF-8 byte limit.
    MessageTooLarge(usize),
    #[error("response request ID does not match")]
    /// The response refers to a different originating request.
    RequestIdMismatch,
    #[error("response kind does not match request")]
    /// A response case is incompatible with the request operation.
    ResponseKindMismatch,
    #[error("not-sent outcome is client-local and invalid on the wire")]
    /// A peer sent the client-local certainty case, which is invalid on the wire.
    RemoteNotSent,
    #[error("invalid frame length: {0}")]
    /// The advertised or encoded payload length is empty or oversized.
    InvalidFrameLength(usize),
    #[error("frame length does not match header")]
    /// The fixed header and available payload bytes disagree.
    FrameLengthMismatch,
    #[error("trailing postcard payload bytes")]
    /// Extra postcard bytes follow the single expected message.
    TrailingPayload,
    #[error("postcard codec error: {0}")]
    /// Postcard rejected the encoded data or serialization capacity.
    Codec(#[from] postcard::Error),
}
