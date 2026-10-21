//! Bounded private peer protocol: errors, DTOs, headers, and postcard frames.
//!
//! OpenRaft request types become bounded DTOs at this socket boundary before
//! they are decoded into in-memory Raft requests.
mod dto;
pub(super) mod error;
pub(super) mod message;

#[cfg(test)]
pub(super) use dto::WireEntry;
pub use error::PeerError;
pub(crate) use message::metadata_append_bytes;
