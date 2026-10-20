//! Fixed-size public observations for the one Fatal/failover schedule.
//! Receipt timestamps describe observation, never the end of an Unknown effect.

use super::history;
use crate::types::LogId;
use bytes::Bytes;
use rockbee_raft_protocol::{ClusterId, Failure, ShardConfig, routing::NodeEndpoint};
use serde::{Deserialize, Serialize};
use std::{
    io::{Read, Write},
    path::Path,
};

const VERSION: u8 = 1;
const MAX_ATTEMPTS: usize = 10;
const KEY_COUNT: usize = 6;
const MAX_FILE_BYTES: usize = 64 * 1024;
const MAX_FIELD_BYTES: usize = 1024;
const MAX_EVENTS: u64 = 64;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct Topology {
    pub(super) cluster_id: ClusterId,
    pub(super) shards: ShardConfig,
    pub(super) revision: u64,
    pub(super) nodes: [NodeEndpoint; 3],
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) enum Operation {
    Put(Bytes),
    Get,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) enum Outcome {
    Committed,
    Rejected(Failure),
    Unknown(Failure),
    NotSent(Failure),
    /// A synchronous public admission error, distinct from a handle's NotSent.
    AdmissionError(String),
    Value(Option<Bytes>),
    ReadError(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct Notice {
    at: u64,
    outcome: Outcome,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct Attempt {
    id: u64,
    key: Bytes,
    operation: Operation,
    invoked_at: u64,
    notice: Option<Notice>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct FaultObservation {
    pub(super) observed_at: u64,
    pub(super) group_id: u64,
    pub(super) log_id: LogId,
    pub(super) persist_failed: bool,
    pub(super) callback_failed: bool,
    pub(super) gate_closed: bool,
    pub(super) available_at_close: usize,
    pub(super) active_at_close: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct AuthorityObservation {
    pub(super) observed_at: u64,
    pub(super) node_id: u64,
    pub(super) group_id: u64,
    pub(super) applied: Option<LogId>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct NativeCounts {
    pub(super) entered: usize,
    pub(super) completed: usize,
    pub(super) active: usize,
    pub(super) dropped: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct Capture {
    version: u8,
    /// This fixture does not infer the build revision. The outer report owns it.
    source_revision: Option<String>,
    topology: Topology,
    initial_absent: [Bytes; KEY_COUNT],
    attempts: [Option<Attempt>; MAX_ATTEMPTS],
    last_event: u64,
    horizon: u64,
    incomplete: bool,
    pub(super) fault: Option<FaultObservation>,
    pub(super) authority: Option<AuthorityObservation>,
    pub(super) final_native: Option<[NativeCounts; 3]>,
    /// Scenario/liveness acceptance is independent of history consistency.
    pub(super) scenario_failure: Option<String>,
}

impl Capture {
    pub(super) fn new(topology: Topology, initial_absent: [Bytes; KEY_COUNT]) -> Self {
        Self {
            version: VERSION,
            source_revision: None,
            topology,
            initial_absent,
            attempts: std::array::from_fn(|_| None),
            last_event: 0,
            horizon: 0,
            incomplete: false,
            fault: None,
            authority: None,
            final_native: None,
            scenario_failure: None,
        }
    }

    pub(super) fn begin(&mut self, key: Bytes, operation: Operation) -> Result<u64, String> {
        let Some(index) = self.attempts.iter().position(Option::is_none) else {
            self.incomplete = true;
            return Err("capture attempt limit".into());
        };
        if self.horizon != 0
            || !self.initial_absent.contains(&key)
            || key.is_empty()
            || key.len() > MAX_FIELD_BYTES
            || matches!(&operation, Operation::Put(value) if value.len() > MAX_FIELD_BYTES)
        {
            self.incomplete = true;
            return Err("invalid capture invocation or sealed capture".into());
        }
        let id = index as u64 + 1;
        let invoked_at = self.event();
        self.attempts[index] = Some(Attempt {
            id,
            key,
            operation,
            invoked_at,
            notice: None,
        });
        Ok(id)
    }

    /// Retains the bounded raw observation, including an incompatible outcome.
    /// H1 owns operation/outcome validation so diagnostic evidence is preserved
    /// without a second, potentially divergent set of semantic rules.
    pub(super) fn notice(&mut self, id: u64, outcome: Outcome) -> Result<(), String> {
        let index = self
            .attempts
            .iter()
            .position(|slot| slot.as_ref().is_some_and(|a| a.id == id));
        let Some(index) = index else {
            self.incomplete = true;
            return Err("notice has no captured invocation".into());
        };
        if self.horizon != 0
            || self.attempts[index].as_ref().unwrap().notice.is_some()
            || !outcome_bounded(&outcome)
        {
            self.incomplete = true;
            return Err("duplicate, oversized or post-seal notice".into());
        }
        let at = self.event();
        self.attempts[index].as_mut().unwrap().notice = Some(Notice { at, outcome });
        Ok(())
    }

    pub(super) fn event(&mut self) -> u64 {
        assert_eq!(self.horizon, 0, "sealed capture cannot observe new events");
        assert!(self.last_event < MAX_EVENTS, "finite fixture event limit");
        self.last_event += 1;
        self.last_event
    }

    pub(super) fn seal(&mut self, interrupted: bool) {
        self.incomplete |= interrupted
            || self
                .attempts
                .iter()
                .any(|slot| slot.as_ref().is_none_or(|attempt| attempt.notice.is_none()));
        self.horizon = self.last_event + 1;
    }

    fn history(&self) -> Result<history::History, String> {
        self.validate()?;
        Ok(history::History {
            initial: self
                .initial_absent
                .iter()
                .map(|key| (key.to_vec(), None))
                .collect(),
            attempts: self
                .attempts
                .iter()
                .flatten()
                .map(|attempt| history::Attempt {
                    id: attempt.id,
                    key: attempt.key.to_vec(),
                    operation: match &attempt.operation {
                        Operation::Put(value) => history::Operation::Put(value.to_vec()),
                        Operation::Get => history::Operation::Get,
                    },
                    invoked_at: attempt.invoked_at,
                    notice: attempt.notice.as_ref().map(|notice| history::Notice {
                        at: notice.at,
                        outcome: match &notice.outcome {
                            Outcome::Committed => history::Outcome::Committed,
                            Outcome::Rejected(_) => history::Outcome::Rejected,
                            Outcome::Unknown(_) => history::Outcome::Unknown,
                            Outcome::NotSent(_) => history::Outcome::NotSent,
                            Outcome::AdmissionError(_) => match &attempt.operation {
                                Operation::Put(_) => history::Outcome::NotSent,
                                Operation::Get => history::Outcome::ReadError,
                            },
                            Outcome::Value(value) => {
                                history::Outcome::Value(value.as_ref().map(|v| v.to_vec()))
                            }
                            Outcome::ReadError(_) => history::Outcome::ReadError,
                        },
                    }),
                    // Storage/authority receipts are diagnostic observations. A
                    // local Fatal does not prove a global Unknown effect ended.
                    execution: None,
                })
                .collect(),
            horizon: self.horizon,
            complete: !self.incomplete
                && self
                    .attempts
                    .iter()
                    .all(|slot| slot.as_ref().is_some_and(|a| a.notice.is_some())),
        })
    }

    pub(super) fn verdict(&self) -> Result<history::Verdict, String> {
        history::check(&self.history()?, 100_000).map_err(|error| format!("{error:?}"))
    }

    /// Attempts, PUTs, GETs, Committed, Rejected, Unknown, returned values.
    pub(super) fn counts(&self) -> [usize; 7] {
        let mut counts = [0; 7];
        for attempt in self.attempts.iter().flatten() {
            counts[0] += 1;
            counts[match &attempt.operation {
                Operation::Put(_) => 1,
                Operation::Get => 2,
            }] += 1;
            match attempt.notice.as_ref().map(|notice| &notice.outcome) {
                Some(Outcome::Committed) => counts[3] += 1,
                Some(Outcome::Rejected(_)) => counts[4] += 1,
                Some(Outcome::Unknown(_)) => counts[5] += 1,
                Some(Outcome::Value(_)) => counts[6] += 1,
                _ => {}
            }
        }
        counts
    }

    pub(super) fn persist(&self, path: &Path) -> Result<(), String> {
        self.validate()?;
        let mut buffer = vec![0; MAX_FILE_BYTES];
        let bytes = postcard::to_slice(self, &mut buffer)
            .map_err(|error| format!("capture encoding byte limit: {error}"))?;
        let temporary = path.with_extension("partial");
        let mut file = std::fs::File::create(&temporary).map_err(|e| e.to_string())?;
        file.write_all(bytes).map_err(|e| e.to_string())?;
        file.sync_all().map_err(|e| e.to_string())?;
        std::fs::rename(temporary, path).map_err(|e| e.to_string())
    }

    pub(super) fn load(path: &Path) -> Result<Self, String> {
        let file = std::fs::File::open(path).map_err(|e| e.to_string())?;
        if file.metadata().map_err(|e| e.to_string())?.len() > MAX_FILE_BYTES as u64 {
            return Err("capture file byte limit".into());
        }
        let mut bytes = Vec::new();
        file.take(MAX_FILE_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|e| e.to_string())?;
        if bytes.len() > MAX_FILE_BYTES {
            return Err("capture file byte limit".into());
        }
        let (capture, remaining): (Self, _) =
            postcard::take_from_bytes(&bytes).map_err(|e| e.to_string())?;
        if !remaining.is_empty() {
            return Err("trailing capture bytes".into());
        }
        capture.validate()?;
        Ok(capture)
    }

    fn validate(&self) -> Result<(), String> {
        if self.version != VERSION {
            return Err("unsupported capture version".into());
        }
        if self.last_event > MAX_EVENTS || self.horizon == 0 || self.horizon != self.last_event + 1
        {
            return Err("invalid capture horizon".into());
        }
        if self.source_revision.as_ref().is_some_and(|v| v.len() > 128)
            || self
                .scenario_failure
                .as_ref()
                .is_some_and(|v| v.len() > MAX_FIELD_BYTES)
            || self
                .initial_absent
                .iter()
                .any(|key| key.is_empty() || key.len() > MAX_FIELD_BYTES)
        {
            return Err("capture field byte limit".into());
        }
        let mut vacant = false;
        for slot in &self.attempts {
            let Some(attempt) = slot else {
                vacant = true;
                continue;
            };
            if vacant {
                return Err("capture slot gap".into());
            }
            if attempt.key.is_empty()
                || attempt.key.len() > MAX_FIELD_BYTES
                || matches!(&attempt.operation, Operation::Put(value) if value.len() > MAX_FIELD_BYTES)
                || attempt
                    .notice
                    .as_ref()
                    .is_some_and(|n| !outcome_bounded(&n.outcome))
            {
                return Err("capture field byte limit".into());
            }
        }
        for at in [
            self.fault.as_ref().map(|r| r.observed_at),
            self.authority.as_ref().map(|r| r.observed_at),
        ]
        .into_iter()
        .flatten()
        {
            if at == 0 || at >= self.horizon {
                return Err("invalid diagnostic observation time".into());
            }
        }
        Ok(())
    }
}

fn outcome_bounded(outcome: &Outcome) -> bool {
    match outcome {
        Outcome::Rejected(f) | Outcome::Unknown(f) | Outcome::NotSent(f) => {
            f.message.len() <= MAX_FIELD_BYTES
        }
        Outcome::AdmissionError(s) | Outcome::ReadError(s) => s.len() <= MAX_FIELD_BYTES,
        Outcome::Value(Some(value)) => value.len() <= MAX_FIELD_BYTES,
        Outcome::Committed | Outcome::Value(None) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockbee_raft_protocol::{FailureReason, NodeId};

    fn failure() -> Failure {
        Failure {
            reason: FailureReason::Unavailable,
            message: "observed fixture outcome".into(),
        }
    }

    fn empty() -> Capture {
        Capture::new(
            Topology {
                cluster_id: ClusterId::new([113; 16]).unwrap(),
                shards: ShardConfig::new(2, 0).unwrap(),
                revision: 1,
                nodes: std::array::from_fn(|index| NodeEndpoint {
                    node_id: NodeId::new(index as u64 + 1).unwrap(),
                    peer_address: format!("127.0.0.1:{}", 1000 + index).parse().unwrap(),
                    data_address: format!("127.0.0.1:{}", 2000 + index).parse().unwrap(),
                }),
            },
            std::array::from_fn(|index| Bytes::from(format!("fresh-key-{index}"))),
        )
    }

    // Hand-proven sequential schedule: three sibling PUT/GET pairs, one
    // Unknown PUT, one rejected PUT, and one new successful failover PUT/GET.
    fn complete() -> Capture {
        let mut capture = empty();
        let value = Bytes::from_static(b"value");
        let rows = [
            (0, Operation::Put(value.clone()), Outcome::Committed),
            (0, Operation::Get, Outcome::Value(Some(value.clone()))),
            (
                3,
                Operation::Put(value.clone()),
                Outcome::Unknown(failure()),
            ),
            (
                4,
                Operation::Put(value.clone()),
                Outcome::Rejected(failure()),
            ),
            (1, Operation::Put(value.clone()), Outcome::Committed),
            (1, Operation::Get, Outcome::Value(Some(value.clone()))),
            (5, Operation::Put(value.clone()), Outcome::Committed),
            (5, Operation::Get, Outcome::Value(Some(value.clone()))),
            (2, Operation::Put(value.clone()), Outcome::Committed),
            (2, Operation::Get, Outcome::Value(Some(value))),
        ];
        for (index, (key, operation, outcome)) in rows.into_iter().enumerate() {
            capture.attempts[index] = Some(Attempt {
                id: index as u64 + 1,
                key: capture.initial_absent[key].clone(),
                operation,
                invoked_at: index as u64 * 2 + 1,
                notice: Some(Notice {
                    at: index as u64 * 2 + 2,
                    outcome,
                }),
            });
        }
        capture.last_event = 20;
        capture.horizon = 21;
        capture
    }

    #[test]
    fn records_every_invocation_and_actual_notice() {
        let expected = complete();
        let mut observed = empty();
        for attempt in expected.attempts.iter().flatten() {
            let id = observed
                .begin(attempt.key.clone(), attempt.operation.clone())
                .unwrap();
            assert_eq!(id, attempt.id);
            observed
                .notice(id, attempt.notice.as_ref().unwrap().outcome.clone())
                .unwrap();
        }
        observed.seal(false);
        assert_eq!(observed, expected);
    }

    #[test]
    fn complete_history_is_legal_and_unknown_has_no_execution_receipt() {
        let mut capture = complete();
        let log_id = LogId::new(openraft::CommittedLeaderId::new(7, 1), 11);
        capture.fault = Some(FaultObservation {
            observed_at: 21,
            group_id: 1,
            log_id,
            persist_failed: true,
            callback_failed: true,
            gate_closed: true,
            available_at_close: 4,
            active_at_close: 1,
        });
        capture.authority = Some(AuthorityObservation {
            observed_at: 22,
            node_id: 2,
            group_id: 1,
            applied: Some(log_id),
        });
        capture.last_event = 22;
        capture.horizon = 23;
        assert!(matches!(
            capture.verdict().unwrap(),
            history::Verdict::Legal { .. }
        ));
        let projected = capture.history().unwrap();
        assert_eq!(projected.attempts.len(), 10);
        assert_eq!(projected.initial.len(), 6);
        assert!(projected.attempts[2].execution.is_none());
        assert!(matches!(
            projected.attempts[2].notice.as_ref().unwrap().outcome,
            history::Outcome::Unknown
        ));
    }

    #[test]
    fn complete_client_failures_are_not_capture_truncation() {
        let mut capture = complete();
        capture.attempts[0]
            .as_mut()
            .unwrap()
            .notice
            .as_mut()
            .unwrap()
            .outcome = Outcome::NotSent(failure());
        capture.attempts[1]
            .as_mut()
            .unwrap()
            .notice
            .as_mut()
            .unwrap()
            .outcome = Outcome::Value(None);
        capture.attempts[5]
            .as_mut()
            .unwrap()
            .notice
            .as_mut()
            .unwrap()
            .outcome = Outcome::ReadError("EOF observed by client".into());
        capture.attempts[3]
            .as_mut()
            .unwrap()
            .notice
            .as_mut()
            .unwrap()
            .outcome = Outcome::AdmissionError("closed before submission".into());
        assert!(matches!(
            capture.verdict().unwrap(),
            history::Verdict::Legal { .. }
        ));
    }

    #[test]
    fn fully_observed_liveness_failure_can_have_a_legal_history() {
        let mut capture = complete();
        capture.attempts[6]
            .as_mut()
            .unwrap()
            .notice
            .as_mut()
            .unwrap()
            .outcome = Outcome::Rejected(failure());
        capture.attempts[7]
            .as_mut()
            .unwrap()
            .notice
            .as_mut()
            .unwrap()
            .outcome = Outcome::Value(None);
        capture.scenario_failure = Some("new public PUT did not reach authoritative B".into());
        capture.seal(false);
        assert!(!capture.incomplete);
        assert!(matches!(
            capture.verdict().unwrap(),
            history::Verdict::Legal { .. }
        ));
        assert!(capture.scenario_failure.is_some());
    }

    #[test]
    fn open_attempt_and_overflow_are_inconclusive_without_invented_notice() {
        let mut capture = complete();
        capture.attempts[9].as_mut().unwrap().notice = None;
        capture.seal(true);
        assert!(capture.attempts[9].as_ref().unwrap().notice.is_none());
        assert!(matches!(
            capture.verdict().unwrap(),
            history::Verdict::Inconclusive {
                reason: history::Inconclusive::PartialCapture,
                ..
            }
        ));
        let mut overflow = complete();
        overflow.horizon = 0;
        assert!(
            overflow
                .begin(overflow.initial_absent[0].clone(), Operation::Get)
                .is_err()
        );
        overflow.seal(false);
        assert!(matches!(
            overflow.verdict().unwrap(),
            history::Verdict::Inconclusive {
                reason: history::Inconclusive::PartialCapture,
                ..
            }
        ));
    }

    #[test]
    fn wrong_read_is_illegal_and_invalid_identity_is_an_input_error() {
        let mut capture = complete();
        capture.attempts[1]
            .as_mut()
            .unwrap()
            .notice
            .as_mut()
            .unwrap()
            .outcome = Outcome::Value(Some(Bytes::from_static(b"wrong")));
        assert!(matches!(
            capture.verdict().unwrap(),
            history::Verdict::Illegal { .. }
        ));
        let mut duplicate = complete();
        duplicate.attempts[1].as_mut().unwrap().id = 1;
        assert!(
            duplicate
                .verdict()
                .unwrap_err()
                .contains("DuplicateAttempt")
        );
        let mut missing = complete();
        missing.attempts[1].as_mut().unwrap().key = Bytes::from_static(b"undeclared-key");
        assert!(
            missing
                .verdict()
                .unwrap_err()
                .contains("MissingInitialState")
        );
    }

    #[test]
    fn incompatible_notice_is_an_input_error_not_a_normalized_outcome() {
        for (index, incompatible) in [(0, Outcome::Value(None)), (1, Outcome::Committed)] {
            let mut expected = complete();
            expected.attempts[index]
                .as_mut()
                .unwrap()
                .notice
                .as_mut()
                .unwrap()
                .outcome = incompatible;
            let mut observed = empty();
            for attempt in expected.attempts.iter().flatten() {
                let id = observed
                    .begin(attempt.key.clone(), attempt.operation.clone())
                    .unwrap();
                observed
                    .notice(id, attempt.notice.as_ref().unwrap().outcome.clone())
                    .unwrap();
            }
            observed.seal(false);
            assert_eq!(
                observed, expected,
                "raw incompatible evidence must survive recording"
            );
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("history.bin");
            observed.persist(&path).unwrap();
            let reloaded = Capture::load(&path).unwrap();
            assert_eq!(
                reloaded, observed,
                "reload must not normalize the raw outcome"
            );
            assert!(reloaded.verdict().unwrap_err().contains("OutcomeMismatch"));
        }
    }

    #[tokio::test]
    async fn outer_capture_keeps_open_invocation_when_observer_is_aborted() {
        let capture = std::sync::Arc::new(std::sync::Mutex::new(empty()));
        let observed = std::sync::Arc::clone(&capture);
        let (invoked, reached) = tokio::sync::oneshot::channel();
        let observer = tokio::spawn(async move {
            {
                let mut capture = observed.lock().unwrap();
                let key = capture.initial_absent[0].clone();
                capture.begin(key, Operation::Get).unwrap();
            }
            invoked.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        reached.await.unwrap();
        observer.abort();
        assert!(observer.await.unwrap_err().is_cancelled());
        let mut capture = capture.lock().unwrap();
        capture.seal(true);
        assert!(capture.attempts[0].as_ref().unwrap().notice.is_none());
        assert!(matches!(
            capture.verdict().unwrap(),
            history::Verdict::Inconclusive {
                reason: history::Inconclusive::PartialCapture,
                ..
            }
        ));
    }

    #[test]
    fn persisted_capture_is_exact_and_reload_rejects_trailing_or_oversized_input() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("history.bin");
        let capture = complete();
        capture.persist(&path).unwrap();
        assert_eq!(Capture::load(&path).unwrap(), capture);
        let mut encoded = std::fs::read(&path).unwrap();
        encoded.push(0);
        std::fs::write(&path, encoded).unwrap();
        assert!(Capture::load(&path).unwrap_err().contains("trailing"));
        std::fs::write(&path, vec![0; MAX_FILE_BYTES + 1]).unwrap();
        assert!(Capture::load(&path).unwrap_err().contains("byte limit"));
    }

    #[test]
    fn reload_rejects_unknown_schema_and_oversized_payload() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("history.bin");
        let mut capture = complete();
        capture.version += 1;
        std::fs::write(&path, postcard::to_stdvec(&capture).unwrap()).unwrap();
        assert!(Capture::load(&path).unwrap_err().contains("version"));
        let mut capture = complete();
        capture.attempts[0].as_mut().unwrap().operation =
            Operation::Put(Bytes::from(vec![0; MAX_FILE_BYTES + 1]));
        assert!(capture.persist(&path).is_err());
    }
}
