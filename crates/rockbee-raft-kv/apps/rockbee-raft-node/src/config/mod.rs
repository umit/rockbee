//! Validated configuration for bootstrap, join and the shared startup path.
//! Parsing lives in the binary's `cli` package; execution lives in `cluster`.
mod join;
mod node;
mod startup;
mod validation;

pub use join::{JoinConfig, JoinSeed};
pub use node::NodeConfig;
pub(crate) use startup::ResolvedStartup;
pub(crate) use validation::validate_metadata_candidate;

#[cfg(test)]
mod tests;
