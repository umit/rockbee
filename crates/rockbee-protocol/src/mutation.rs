//! Certainty of a failed mutation, separate from its diagnostic cause.

use serde::{Deserialize, Serialize};
use std::{error::Error, fmt};

type Cause = Box<dyn Error + Send + Sync>;

/// What the caller can establish about this particular failed PUT or DELETE.
/// None of these variants is an instruction to retry automatically.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationOutcome {
    /// This hop did not enqueue the request to its socket writer.
    /// At the public Client boundary, the request never left that client.
    NotSent,
    /// A responding server confirms it did not apply this request.
    Rejected,
    /// The mutation may have been applied, or may still be applied later.
    #[default]
    Unknown,
}

/// A failed mutation with machine-readable certainty and its diagnostic cause.
#[derive(Debug)]
pub struct MutationError {
    outcome: MutationOutcome,
    cause: Cause,
}

pub type MutationResult<T = ()> = Result<T, MutationError>;

impl MutationError {
    pub fn new(outcome: MutationOutcome, cause: impl Into<Cause>) -> Self {
        Self {
            outcome,
            cause: cause.into(),
        }
    }

    pub fn outcome(&self) -> MutationOutcome {
        self.outcome
    }

    /// Keep transport certainty when present; unclassified errors are uncertain.
    pub fn from_error(error: Cause) -> Self {
        match error.downcast::<Self>() {
            Ok(error) => *error,
            Err(error) => Self::new(MutationOutcome::Unknown, error),
        }
    }
}

impl fmt::Display for MutationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let status = match self.outcome {
            MutationOutcome::NotSent => "mutation was not sent",
            MutationOutcome::Rejected => "mutation was rejected before application",
            MutationOutcome::Unknown => "mutation outcome is unknown",
        };
        write!(f, "{}; {status}", self.cause)
    }
}

impl Error for MutationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.cause.as_ref())
    }
}
