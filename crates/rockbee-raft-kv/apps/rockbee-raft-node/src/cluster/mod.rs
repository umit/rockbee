//! Process lifecycle and group orchestration; storage and RPC keep their own jobs.

mod membership;
mod movement;
mod runtime;
mod service;
#[cfg(test)]
mod tests;

pub use runtime::{NodeError, NodeRuntime};
pub use service::NodeService;
