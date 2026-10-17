//! Shared configuration and generic RPC lifecycle for independently owned storage roles.
pub mod backend;
pub mod config;
pub mod key;
pub mod memory;
mod runtime;
pub mod server;
pub mod ttl;
pub use rockbee_protocol as protocol;
pub use rockbee_protocol::{Result, transport};
pub use rockbee_rpc as rpc;
pub use rockbee_rpc::RpcConfig;
pub use runtime::run_for_role;
pub use server::{Server, Storage};
pub const DEFAULT_ADDR: &str = "127.0.0.1:7000";
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Writer,
    Reader,
}
/// Validation failures occur before a mutation reaches SlateDB.
pub fn rejected(
    error: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> protocol::MutationError {
    protocol::MutationError::new(protocol::MutationOutcome::Rejected, error)
}
