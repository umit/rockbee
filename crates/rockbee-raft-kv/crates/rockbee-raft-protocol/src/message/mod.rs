//! Correlated wire envelopes and bounded service failure diagnostics.

mod envelope;
mod failure;

pub use envelope::{RequestEnvelope, ResponseEnvelope};
pub use failure::{Failure, FailureReason, ReadFailure};
