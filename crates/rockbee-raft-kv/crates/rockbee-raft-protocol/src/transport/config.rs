use thiserror::Error;

use crate::{MAX_FRAME_PAYLOAD, ProtocolError, WireRequest};

// Includes entry, queue, channel, timer nodes and small decoded envelope fields.
// Concrete message families include their inline sizes in validation below.
pub(super) const OWNERSHIP_HEADROOM: usize = 4096;

/// Admission budgets shared by all clones of one connection generation.
/// Byte limits must admit a maximum legal frame plus metadata/decode headroom.
#[derive(Clone, Copy, Debug)]
pub struct LinkConfig {
    /// Retained operations, including unconsumed completion channels.
    pub max_outstanding: usize,
    /// Logical request, encoded frame and operation metadata ownership.
    pub request_bytes: usize,
    /// Encoded/decoded response overlap and unconsumed result ownership.
    pub receive_bytes: usize,
}

impl Default for LinkConfig {
    fn default() -> Self {
        Self {
            max_outstanding: 128,
            request_bytes: 16 * 1024 * 1024,
            receive_bytes: 16 * 1024 * 1024,
        }
    }
}

pub(super) fn ownership_bytes<Req: WireRequest<Resp>, Resp>() -> Option<usize> {
    OWNERSHIP_HEADROOM
        .checked_add(size_of::<super::state::Entry<Resp, Req::Expected>>())?
        .checked_add(size_of::<super::Queued<Req>>())?
        .checked_add(size_of::<super::pending::Completion<Resp>>())
}

impl LinkConfig {
    pub(super) fn validate<Req: WireRequest<Resp>, Resp>(&self) -> Result<(), ConfigError> {
        if self.max_outstanding == 0 || self.max_outstanding > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(ConfigError::Outstanding);
        }
        let minimum = MAX_FRAME_PAYLOAD
            .checked_mul(2)
            .and_then(|n| n.checked_add(ownership_bytes::<Req, Resp>()? + 4))
            .and_then(|n| n.checked_add(size_of::<Req>().max(size_of::<Resp>())))
            .ok_or(ConfigError::Bytes)?;
        let receive_minimum = minimum
            .checked_add(Req::RESPONSE_DECODE_HEADROOM)
            .ok_or(ConfigError::Bytes)?;
        for (capacity, minimum) in [
            (self.request_bytes, minimum),
            (self.receive_bytes, receive_minimum),
        ] {
            if capacity < minimum
                || capacity > u32::MAX as usize
                || capacity > tokio::sync::Semaphore::MAX_PERMITS
            {
                return Err(ConfigError::Bytes);
            }
        }
        Ok(())
    }
}

/// Invalid limits are rejected before opening a socket.
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("outstanding limit must be nonzero and supported by semaphore")]
    /// The operation count is zero or exceeds supported semaphore counters.
    Outstanding,
    #[error(
        "byte limits must admit a maximum frame with ownership/decode headroom and fit semaphore counters"
    )]
    /// A byte limit cannot fit one maximum frame plus ownership/decode headroom.
    Bytes,
}

/// One connection attempt failed; no request was submitted.
#[derive(Debug, Error)]
pub enum ConnectError {
    #[error(transparent)]
    /// Configuration was rejected before opening a connection.
    Config(#[from] ConfigError),
    #[error("connect deadline expired")]
    /// The originating absolute deadline elapsed.
    Deadline,
    #[error("connect failed: {0}")]
    /// The socket operation failed; certainty still depends on handoff state.
    Io(#[from] std::io::Error),
}

/// Every admission failure proves the operation was not enqueued or handed off.
#[derive(Debug, Error)]
pub enum AdmitError {
    #[error("invalid request: {0}")]
    /// Body validation or bounded serialization sizing failed.
    InvalidRequest(ProtocolError),
    #[error("request deadline expired before admission")]
    /// The originating absolute deadline elapsed.
    Deadline,
    #[error("outstanding operation limit reached")]
    /// Retained accepted work/results occupy every operation slot.
    CountLimit,
    #[error("retained request byte limit reached")]
    /// Logical request/frame ownership cannot fit in the remaining byte budget.
    ByteLimit,
    #[error("connection retired; explicitly connect a new client")]
    /// This connection generation was explicitly closed or already retired.
    Closed,
    #[error("request identifiers exhausted; connection retired")]
    /// The checked correlation counter reached its end; IDs are never reused.
    IdExhausted,
}

/// Local transport certainty; this does not infer consensus completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Delivery {
    /// The writer never won the handoff transition.
    NotSent,
    /// Handoff won; even zero observed bytes cannot prove non-delivery.
    MayHaveBeenSent,
}

/// Why a request failed or its entire connection generation retired.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportFailureKind {
    /// The originating absolute deadline elapsed.
    Deadline,
    /// The peer closed the stream before a final response.
    Disconnected,
    /// The socket operation failed; certainty still depends on handoff state.
    Io,
    /// An invalid frame, body, ID or response kind retired the connection.
    Protocol,
    /// This connection generation was explicitly closed or already retired.
    Closed,
    /// The checked correlation counter reached its end; IDs are never reused.
    IdExhausted,
}

/// A typed reason paired with delivery certainty for the originating caller.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
#[error("transport {reason:?}, delivery {delivery:?}")]
pub struct TransportFailure {
    /// Whether the local writer proved no handoff or may have delivered bytes.
    pub delivery: Delivery,
    /// Typed cause of this failure or generation retirement.
    pub reason: TransportFailureKind,
}
