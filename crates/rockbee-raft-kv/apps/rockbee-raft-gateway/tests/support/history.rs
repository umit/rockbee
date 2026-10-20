//! Bounded, test-only per-key register model. This does not certify the running system.

use std::collections::{BTreeMap, BTreeSet};

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum InputError {
    DuplicateAttempt(u64),
    DuplicateInitialKey,
    MissingInitialState(u64),
    DuplicateEvent(u64),
    Chronology(u64),
    OutcomeMismatch(u64),
    ContradictoryEvidence(u64),
    ReadExecutionEvidence(u64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Inconclusive {
    Attempts,
    Keys,
    PayloadBytes,
    OperationsPerKey,
    PartialCapture,
    BudgetExceeded,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Verdict {
    Legal {
        witnesses: Vec<KeyWitness>,
        visited: usize,
    },
    Illegal {
        key: Vec<u8>,
        attempt_ids: Vec<u64>,
        visited: usize,
    },
    Inconclusive {
        reason: Inconclusive,
        visited: usize,
    },
}

const MAX_ATTEMPTS: usize = 64;
const MAX_KEYS: usize = 64;
const MAX_OPERATIONS_PER_KEY: usize = 16;
const MAX_PAYLOAD_BYTES: usize = 64 * 1024;
const MAX_VISITS: usize = 100_000;

fn inconclusive(reason: Inconclusive, visited: usize) -> Verdict {
    Verdict::Inconclusive { reason, visited }
}

fn mutation_knowledge(a: &Attempt) -> Result<EffectKnowledge, InputError> {
    let client = match a.notice.as_ref().map(|n| &n.outcome) {
        Some(Outcome::Committed) => EffectKnowledge::Applied,
        Some(Outcome::NotSent | Outcome::Rejected) => EffectKnowledge::NoEffect,
        Some(Outcome::Unknown) | None => EffectKnowledge::MayApply,
        Some(Outcome::Value(_) | Outcome::ReadError) => {
            return Err(InputError::OutcomeMismatch(a.id));
        }
    };
    let independent = a.execution.map_or(EffectKnowledge::MayApply, |e| e.effect);
    match (client, independent) {
        (EffectKnowledge::Applied, EffectKnowledge::NoEffect)
        | (EffectKnowledge::NoEffect, EffectKnowledge::Applied) => {
            Err(InputError::ContradictoryEvidence(a.id))
        }
        (EffectKnowledge::MayApply, knowledge) => Ok(knowledge),
        (knowledge, _) => Ok(knowledge),
    }
}

fn add_bytes(total: &mut Option<usize>, bytes: usize) {
    *total = total.and_then(|n| n.checked_add(bytes));
}

/// Validates every record in bounded containers before returning any verdict.
/// Container overflow returns a limit immediately, before scanning arbitrary input.
fn validate(h: &History) -> Result<Option<Inconclusive>, InputError> {
    if h.attempts.len() > MAX_ATTEMPTS {
        return Ok(Some(Inconclusive::Attempts));
    }
    if h.initial.len() > MAX_KEYS {
        return Ok(Some(Inconclusive::Keys));
    }
    let mut initial_keys = BTreeSet::new();
    let mut bytes = Some(0usize);
    for (key, value) in &h.initial {
        if !initial_keys.insert(key.as_slice()) {
            return Err(InputError::DuplicateInitialKey);
        }
        add_bytes(&mut bytes, key.len());
        if let Some(value) = value {
            add_bytes(&mut bytes, value.len());
        }
    }
    let mut ids = BTreeSet::new();
    let mut events = BTreeSet::new();
    let mut counts = BTreeMap::<&[u8], usize>::new();
    for a in &h.attempts {
        if !ids.insert(a.id) {
            return Err(InputError::DuplicateAttempt(a.id));
        }
        if !initial_keys.contains(a.key.as_slice()) {
            return Err(InputError::MissingInitialState(a.id));
        }
        if a.invoked_at >= h.horizon {
            return Err(InputError::Chronology(a.id));
        }
        if !events.insert(a.invoked_at) {
            return Err(InputError::DuplicateEvent(a.invoked_at));
        }
        if let Some(n) = &a.notice {
            if n.at <= a.invoked_at || n.at >= h.horizon {
                return Err(InputError::Chronology(a.id));
            }
            if !events.insert(n.at) {
                return Err(InputError::DuplicateEvent(n.at));
            }
            if let Outcome::Value(Some(value)) = &n.outcome {
                add_bytes(&mut bytes, value.len());
            }
        }
        if let Some(e) = a.execution
            && (e.ended_by < a.invoked_at || e.ended_by > h.horizon)
        {
            return Err(InputError::Chronology(a.id));
        }
        add_bytes(&mut bytes, a.key.len());
        let semantic = match &a.operation {
            Operation::Get => {
                if a.execution.is_some() {
                    return Err(InputError::ReadExecutionEvidence(a.id));
                }
                match a.notice.as_ref().map(|n| &n.outcome) {
                    Some(Outcome::Value(_)) => true,
                    Some(Outcome::ReadError) | None => false,
                    _ => return Err(InputError::OutcomeMismatch(a.id)),
                }
            }
            Operation::Put(value) => {
                add_bytes(&mut bytes, value.len());
                mutation_knowledge(a)? != EffectKnowledge::NoEffect
            }
            Operation::Delete => mutation_knowledge(a)? != EffectKnowledge::NoEffect,
        };
        if semantic {
            *counts.entry(a.key.as_slice()).or_default() += 1;
        }
    }
    // Even an already impossible first key cannot hide a malformed later record.
    if bytes.is_none_or(|n| n > MAX_PAYLOAD_BYTES) {
        return Ok(Some(Inconclusive::PayloadBytes));
    }
    if counts.values().any(|n| *n > MAX_OPERATIONS_PER_KEY) {
        return Ok(Some(Inconclusive::OperationsPerKey));
    }
    if !h.complete {
        return Ok(Some(Inconclusive::PartialCapture));
    }
    Ok(None)
}

#[derive(Clone, Copy, Debug)]
enum Action {
    Read(usize),
    Write(usize),
}

#[derive(Clone, Copy, Debug)]
struct SemanticOperation {
    id: u64,
    invoke: u64,
    end: u64,
    optional: bool,
    action: Action,
    predecessors: u32,
}

// Value index 0 always represents absence. Empty stored bytes get a different index.
fn intern(values: &mut Vec<Value>, value: &Value) -> usize {
    if let Some(index) = values.iter().position(|v| v == value) {
        index
    } else {
        values.push(value.clone());
        values.len() - 1
    }
}

fn normalize(h: &History, key: &[u8], initial: &Value) -> (usize, Vec<SemanticOperation>) {
    let mut values = vec![None];
    let initial = intern(&mut values, initial);
    let mut attempts: Vec<_> = h.attempts.iter().filter(|a| a.key == key).collect();
    attempts.sort_by_key(|a| a.id);
    let mut operations = Vec::new();
    for a in attempts {
        let notice = a.notice.as_ref();
        let (action, optional, client_end) = match &a.operation {
            Operation::Get => {
                let Some(Notice {
                    at,
                    outcome: Outcome::Value(value),
                }) = notice
                else {
                    continue;
                };
                (Action::Read(intern(&mut values, value)), false, Some(*at))
            }
            Operation::Put(_) | Operation::Delete => {
                let knowledge = mutation_knowledge(a).expect("history has been validated");
                if knowledge == EffectKnowledge::NoEffect {
                    continue;
                }
                let value = if let Operation::Put(value) = &a.operation {
                    Some(value.clone())
                } else {
                    None
                };
                let end =
                    notice.and_then(|n| matches!(n.outcome, Outcome::Committed).then_some(n.at));
                (
                    Action::Write(intern(&mut values, &value)),
                    knowledge == EffectKnowledge::MayApply,
                    end,
                )
            }
        };
        let independent_end = a.execution.map(|e| e.ended_by);
        let end = client_end
            .into_iter()
            .chain(independent_end)
            .min()
            .unwrap_or(h.horizon);
        operations.push(SemanticOperation {
            id: a.id,
            invoke: a.invoked_at,
            end,
            optional,
            action,
            predecessors: 0,
        });
    }
    for index in 0..operations.len() {
        operations[index].predecessors =
            operations.iter().enumerate().fold(0, |mask, (prior, a)| {
                if a.end < operations[index].invoke {
                    mask | (1u32 << prior)
                } else {
                    mask
                }
            });
    }
    (initial, operations)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SearchResult {
    Found,
    Exhausted,
    Pruned,
}

struct Search<'a> {
    operations: &'a [SemanticOperation],
    // A failed entry is inserted only following fully exhausted descendants.
    failed: BTreeSet<(u32, usize)>,
    linearized: Vec<u64>,
    omitted: Vec<u64>,
    visited: &'a mut usize,
    budget: usize,
}

impl Search<'_> {
    fn visit(&mut self, remaining: u32, value: usize) -> SearchResult {
        // Charge before the empty-state and memo checks, so memo hits also cost a visit.
        if *self.visited >= self.budget {
            return SearchResult::Pruned;
        }
        *self.visited += 1;
        if remaining == 0 {
            return SearchResult::Found;
        }
        if self.failed.contains(&(remaining, value)) {
            return SearchResult::Exhausted;
        }
        let mut pruned = false;
        for index in 0..self.operations.len() {
            let operation = self.operations[index];
            let bit = 1u32 << index;
            if remaining & bit == 0 {
                continue;
            }
            let next = match operation.action {
                Action::Read(expected) => (expected == value).then_some(value),
                Action::Write(next) => Some(next),
            };
            if operation.predecessors & remaining == 0
                && let Some(next) = next
            {
                self.linearized.push(operation.id);
                match self.visit(remaining & !bit, next) {
                    SearchResult::Found => return SearchResult::Found,
                    SearchResult::Pruned => pruned = true,
                    SearchResult::Exhausted => {}
                }
                self.linearized.pop();
            }
            // Omitting an effect is legal even before its predecessors resolve.
            if operation.optional {
                self.omitted.push(operation.id);
                match self.visit(remaining & !bit, value) {
                    SearchResult::Found => return SearchResult::Found,
                    SearchResult::Pruned => pruned = true,
                    SearchResult::Exhausted => {}
                }
                self.omitted.pop();
            }
        }
        if pruned {
            SearchResult::Pruned
        } else {
            self.failed.insert((remaining, value));
            SearchResult::Exhausted
        }
    }
}

/// Checks complete fixture histories within fixed H1 limits, independently per key.
/// A Legal witness explains only this bounded prefix, not network/proposal replay.
pub(super) fn check(h: &History, requested_budget: usize) -> Result<Verdict, InputError> {
    if let Some(reason) = validate(h)? {
        return Ok(inconclusive(reason, 0));
    }
    let budget = requested_budget.min(MAX_VISITS);
    let mut visited = 0;
    let mut witnesses = Vec::new();
    let mut pruned = false;
    let initial: BTreeMap<_, _> = h
        .initial
        .iter()
        .map(|(key, value)| (key.as_slice(), value))
        .collect();
    for (key, value) in initial {
        let (value, operations) = normalize(h, key, value);
        if operations.is_empty() {
            witnesses.push(KeyWitness {
                key: key.to_vec(),
                linearized: vec![],
                omitted: vec![],
            });
            continue;
        }
        let remaining = (1u32 << operations.len()) - 1;
        let mut search = Search {
            operations: &operations,
            failed: BTreeSet::new(),
            linearized: Vec::new(),
            omitted: Vec::new(),
            visited: &mut visited,
            budget,
        };
        match search.visit(remaining, value) {
            SearchResult::Found => {
                search.omitted.sort_unstable();
                witnesses.push(KeyWitness {
                    key: key.to_vec(),
                    linearized: search.linearized,
                    omitted: search.omitted,
                });
            }
            SearchResult::Exhausted => {
                let mut attempt_ids: Vec<_> = h
                    .attempts
                    .iter()
                    .filter(|a| a.key == key)
                    .map(|a| a.id)
                    .collect();
                attempt_ids.sort_unstable();
                return Ok(Verdict::Illegal {
                    key: key.to_vec(),
                    attempt_ids,
                    visited,
                });
            }
            SearchResult::Pruned => pruned = true,
        }
    }
    if pruned {
        Ok(inconclusive(Inconclusive::BudgetExceeded, visited))
    } else {
        Ok(Verdict::Legal { witnesses, visited })
    }
}
