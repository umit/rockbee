//! Eager, bounded byte-key/value client for Rockbee Raft KV.
//!
//! `put` and `delete` synchronously admit work and return an observer. The
//! operation starts without polling that handle; dropping it does not cancel or
//! roll back accepted work. Admission errors prove local non-submission.
//!
//! A durable server acknowledgement yields `Committed`. Transport loss after
//! handoff yields `Unknown`, which must not be automatically replayed. Expiry of
//! any handed request retires the entire multiplexed connection, so concurrent
//! mutations can also return collateral `Unknown`. Queued operations proven
//! never handed return `NotSent`. A retired client requires an explicit new
//! `connect`; this client never reconnects or replays implicitly.
//!
//! Call `close().await` to retire and join the connection's owned workers.
//! Logical byte budgets cannot bound large backing allocations retained by
//! caller-supplied `Bytes` slices. Values returned to the caller become caller
//! ownership and no longer count against the Link receive budget.

pub mod api;
pub mod config;
mod error;

pub use api::{MutationHandle, RockBeeRaftClient};
pub use config::ClientConfig;
pub use error::{ClientConnectError, ReadError};
pub use rockbee_raft_protocol::{
    MutationOutcome,
    transport::{AdmitError, LinkConfig},
};
