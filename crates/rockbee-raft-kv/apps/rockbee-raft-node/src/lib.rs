//! Storage-node application boundary for Rockbee Raft KV.
//!
//! Raft types describe the consensus payload; storage implements OpenRaft's
//! log and state-machine capabilities over one process-local RocksDB.

pub mod cluster;
pub mod config;
pub mod control;
#[doc(hidden)]
pub mod diagnostics;
pub mod raft;
pub mod rpc;
pub mod storage;
pub use raft::types;
