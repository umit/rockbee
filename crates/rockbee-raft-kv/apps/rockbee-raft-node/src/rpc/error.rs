use std::io;

use thiserror::Error;

/// A failure at the private node-to-node boundary.
///
/// The error is intentionally transport-oriented. OpenRaft turns it into an
/// `RPCError`, which tells Raft to retry or back off; it is never interpreted as
/// an acknowledgement for a client mutation.
#[derive(Debug, Error)]
pub enum PeerError {
    #[error("peer limit {name} must be greater than zero")]
    InvalidLimit { name: &'static str },
    #[error("peer frame is too large: {actual} bytes, maximum is {maximum}")]
    FrameTooLarge { actual: usize, maximum: usize },
    #[error("peer identity did not match: {0}")]
    Identity(&'static str),
    #[error("peer RPC body did not match its header")]
    KindMismatch,
    #[error("peer group {0} is not hosted by this node")]
    UnknownGroup(u64),
    #[error("peer protocol error: {0}")]
    Protocol(String),
    #[error("peer I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("peer operation exceeded its deadline")]
    Deadline,
    #[error("peer server is shutting down")]
    Shutdown,
}
