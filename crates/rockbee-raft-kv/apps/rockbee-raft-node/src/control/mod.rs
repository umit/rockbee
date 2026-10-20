//! Deterministic metadata decisions, separate from Raft and network execution.
//!
//! Storage applies this model as a singleton durable metadata transition. Only
//! a future authorized internal reconciler may construct completion observations;
//! an admin request is never membership evidence.

mod encoding;
mod model;
mod validation;

pub use encoding::job_key;
pub use model::*;

#[cfg(test)]
mod tests;
