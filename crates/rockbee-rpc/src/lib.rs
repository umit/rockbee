//! Shared bounded Protobuf transport. It knows nothing about storage or routing.
mod config;
mod connection;
mod server;
mod shutdown;
mod stats;

pub use config::RpcConfig;
pub use server::RpcServer;
pub use shutdown::wait_for_shutdown;

#[cfg(test)]
mod tests;
