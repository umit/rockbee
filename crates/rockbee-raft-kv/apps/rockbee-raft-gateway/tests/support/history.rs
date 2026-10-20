//! Bounded, test-only per-key register model. This does not certify the running system.

pub(super) type Value = Option<Vec<u8>>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Operation {
    Get,
    Put(Vec<u8>),
    Delete,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Outcome {
    Value(Value),
    ReadError,
    Committed,
    NotSent,
    Rejected,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum EffectKnowledge {
    MayApply,
    Applied,
    NoEffect,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Notice {
    pub at: u64,
    pub outcome: Outcome,
}

/// Fixture evidence that excludes effects after `ended_by`, not a timeout.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Execution {
    pub ended_by: u64,
    pub effect: EffectKnowledge,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Attempt {
    pub id: u64,
    pub key: Vec<u8>,
    pub operation: Operation,
    pub invoked_at: u64,
    pub notice: Option<Notice>,
    pub execution: Option<Execution>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct History {
    pub initial: Vec<(Vec<u8>, Value)>,
    pub attempts: Vec<Attempt>,
    pub horizon: u64,
    pub complete: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct KeyWitness {
    pub key: Vec<u8>,
    pub linearized: Vec<u64>,
    pub omitted: Vec<u64>,
}
