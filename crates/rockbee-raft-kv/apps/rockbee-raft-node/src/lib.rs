//! Storage-node application boundary for Rockbee Raft KV.
//!
//! Raft types describe the consensus payload; storage implements OpenRaft's
//! log and state-machine capabilities over one process-local RocksDB.
//!
//! ```text
//! Client -> Gateway -> cluster::NodeService -> OpenRaft -> storage -> RocksDB
//!                                               |
//!                                               +-> rpc -> remote OpenRaft
//! ```
//!
//! Read `cluster/runtime` for startup, `cluster/service` for requests,
//! `rpc/transport` for network I/O, and `storage/rocksdb/engine` for disk I/O.
//! `config` validates startup settings; `control` models metadata decisions.
//! Each public module reexports its supported types from focused child modules.

pub mod cluster;
pub mod config;
pub mod control;
#[doc(hidden)]
pub mod diagnostics;
pub mod raft;
pub mod rpc;
pub mod storage;
pub use raft::types;
