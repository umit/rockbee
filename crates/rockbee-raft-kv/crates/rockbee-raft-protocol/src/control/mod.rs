//! Bounded, engine-free control values for the first durable replica move.
//!
//! A move is intentionally restrictive: each data group accepts one immutable
//! lifetime target. Runtime code performs Raft membership calls; this module
//! only validates and preserves the durable intent and its observations.

mod metadata;
mod movement;

pub use metadata::{AdminOperation, AdminResult, CommittedLogPosition, MoveProgress, MoveRecord};
pub use movement::{MoveIntent, MoveJobId, MoveReplica};
