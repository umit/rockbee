//! SlateDB-backed writer and reader nodes.
//!
//! [`writer`] and [`reader`] own role-specific storage operations and app entry
//! points. [`storage::Store`] preserves the shared role-dispatch API; [`server`]
//! provides their TCP service. Process bootstrap is shared in the runtime module.

pub mod backend;
pub mod config;
mod key;
pub mod reader;
mod runtime;
pub mod server;
pub mod storage;
pub mod writer;

pub use rockbee_protocol as protocol;
pub use rockbee_protocol::Result;
pub use rockbee_protocol::transport;
pub use rockbee_rpc as rpc;
pub use rockbee_rpc::RpcConfig;
pub use runtime::{run_compat, run_for_role};
pub use server::Server;
pub use storage::Store;

pub const DEFAULT_ADDR: &str = "127.0.0.1:7000";
