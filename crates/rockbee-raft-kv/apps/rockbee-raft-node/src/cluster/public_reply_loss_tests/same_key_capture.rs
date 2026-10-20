//! Six public attempts on one initially absent key; raw receipts retain provenance.

use super::{capture::persist_reload, history, proxy::ProxyReceipt};
use bytes::Bytes;
use rockbee_raft_protocol::MutationOutcome;
use serde::{Deserialize, Serialize};
use std::path::Path;

const ATTEMPTS: usize = 6;
const FIELD_BYTES: usize = 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) enum Operation {
    Put(Bytes),
    Get,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) enum Outcome {
    Mutation(MutationOutcome),
    AdmissionError(String),
    Value(Option<Bytes>),
    ReadError(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Attempt {
    operation: Operation,
    invoked_at: u64,
    notice: Option<(u64, Outcome)>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Capture {
    version: u8,
    key: Bytes,
    attempts: [Option<Attempt>; ATTEMPTS],
    horizon: u64,
    incomplete: bool,
    pub(super) proxy: Option<ProxyReceipt>,
    pub(super) native_calls: Option<usize>,
    pub(super) scenario_failure: Option<String>,
}

impl Capture {
    pub(super) fn new(key: Bytes) -> Self {
        Self {
            version: 1,
            key,
            attempts: std::array::from_fn(|_| None),
            horizon: 0,
            incomplete: false,
            proxy: None,
            native_calls: None,
            scenario_failure: None,
        }
    }

    pub(super) fn begin(&mut self, operation: Operation, at: u64) -> Result<usize, String> {
        if self.horizon != 0
            || at == 0
            || at >= 128
            || matches!(&operation, Operation::Put(v) if v.len() > FIELD_BYTES)
        {
            self.incomplete = true;
            return Err("invalid, oversized or sealed invocation".into());
        }
        let Some(slot) = self.attempts.iter().position(Option::is_none) else {
            self.incomplete = true;
            return Err("six-attempt capture limit".into());
        };
        self.attempts[slot] = Some(Attempt {
            operation,
            invoked_at: at,
            notice: None,
        });
        Ok(slot)
    }

    pub(super) fn notice(&mut self, slot: usize, outcome: Outcome, at: u64) -> Result<(), String> {
        let attempt = self.attempts.get_mut(slot).and_then(Option::as_mut);
        if self.horizon != 0
            || at == 0
            || at >= 128
            || !outcome_bounded(&outcome)
            || attempt.as_ref().is_none_or(|a| a.notice.is_some())
        {
            self.incomplete = true;
            return Err("missing invocation, duplicate, oversized or sealed notice".into());
        }
        // Keep incompatible raw notices too. H1 owns semantic validation.
        attempt.unwrap().notice = Some((at, outcome));
        Ok(())
    }

    pub(super) fn seal(&mut self, horizon: u64, interrupted: bool) {
        self.horizon = horizon;
        self.incomplete |= interrupted;
    }

    pub(super) fn persist_reload(&self, path: &Path) -> Self {
        self.validate_bounds()
            .expect("bounded capture before persistence");
        let loaded = persist_reload(path, self);
        assert_eq!(*self, loaded, "raw observed capture must roundtrip exactly");
        loaded
    }

    pub(super) fn history(&self) -> Result<history::History, String> {
        self.validate_bounds()?;
        let executions = self.correlate_committed_replies()?;
        Ok(history::History {
            initial: vec![(self.key.to_vec(), None)],
            attempts: self
                .attempts
                .iter()
                .enumerate()
                .filter_map(|(slot, attempt)| {
                    attempt.as_ref().map(|a| history::Attempt {
                        id: slot as u64 + 1,
                        key: self.key.to_vec(),
                        operation: match &a.operation {
                            Operation::Put(value) => history::Operation::Put(value.to_vec()),
                            Operation::Get => history::Operation::Get,
                        },
                        invoked_at: a.invoked_at,
                        notice: a.notice.as_ref().map(|(at, outcome)| history::Notice {
                            at: *at,
                            outcome: match outcome {
                                Outcome::Mutation(MutationOutcome::Committed) => {
                                    history::Outcome::Committed
                                }
                                Outcome::Mutation(MutationOutcome::Unknown(_)) => {
                                    history::Outcome::Unknown
                                }
                                Outcome::Mutation(MutationOutcome::Rejected(_)) => {
                                    history::Outcome::Rejected
                                }
                                Outcome::Mutation(MutationOutcome::NotSent(_)) => {
                                    history::Outcome::NotSent
                                }
                                Outcome::AdmissionError(_) => match a.operation {
                                    Operation::Put(_) => history::Outcome::NotSent,
                                    Operation::Get => history::Outcome::ReadError,
                                },
                                Outcome::Value(value) => {
                                    history::Outcome::Value(value.as_ref().map(|v| v.to_vec()))
                                }
                                Outcome::ReadError(_) => history::Outcome::ReadError,
                            },
                        }),
                        execution: executions[slot],
                    })
                })
                .collect(),
            horizon: self.horizon,
            complete: !self.incomplete
                && self
                    .attempts
                    .iter()
                    .all(|a| a.as_ref().is_some_and(|a| a.notice.is_some()))
                && executions[..2].iter().all(Option::is_some),
        })
    }

    /// Each receipt independently proves its matching PUT applied by that time.
    /// Neither the wire slot nor response order implies A/B execution order.
    fn correlate_committed_replies(
        &self,
    ) -> Result<[Option<history::Execution>; ATTEMPTS], String> {
        let mut executions = [None; ATTEMPTS];
        let Some(proxy) = &self.proxy else {
            return Ok(executions);
        };
        let mut seen_ids = [None; 2];
        for (index, receipt) in proxy.matched_puts.iter().flatten().enumerate() {
            if seen_ids.contains(&Some(receipt.request_id))
                || receipt.key.as_slice() != self.key.as_ref()
            {
                return Err("duplicate receipt id or mismatched key".into());
            }
            seen_ids[index] = Some(receipt.request_id);
            let matching: Vec<_> = self.attempts[..2].iter().enumerate().filter_map(|(slot, a)| {
                a.as_ref().filter(|a| matches!(&a.operation, Operation::Put(v) if v.as_ref() == receipt.value.as_slice())).map(|a| (slot, a))
            }).collect();
            let [(slot, attempt)] = matching.as_slice() else {
                return Err("receipt must match exactly one of the two eager PUTs".into());
            };
            if executions[*slot].is_some() {
                return Err("duplicate receipt for one PUT".into());
            }
            let (Some(received), Some(forwarded)) =
                (proxy.both_received_at, proxy.forward_started_at)
            else {
                return Err("missing pair forwarding receipt".into());
            };
            if !(attempt.invoked_at < received
                && received < forwarded
                && forwarded < receipt.committed_at
                && receipt.committed_at < self.horizon)
            {
                return Err("invalid actual commit receipt chronology".into());
            }
            executions[*slot] = Some(history::Execution {
                ended_by: receipt.committed_at,
                effect: history::EffectKnowledge::Applied,
            });
        }
        Ok(executions)
    }

    fn validate_bounds(&self) -> Result<(), String> {
        if self.version != 1
            || self.key.is_empty()
            || self.key.len() > FIELD_BYTES
            || self.horizon == 0
            || self.horizon > 128
            || self
                .scenario_failure
                .as_ref()
                .is_some_and(|s| s.len() > FIELD_BYTES)
        {
            return Err("invalid capture version, horizon or field size".into());
        }
        let mut vacant = false;
        for attempt in &self.attempts {
            let Some(a) = attempt else {
                vacant = true;
                continue;
            };
            if vacant
                || matches!(&a.operation, Operation::Put(v) if v.len() > FIELD_BYTES)
                || a.notice.as_ref().is_some_and(|(_, n)| !outcome_bounded(n))
            {
                return Err("capture slot gap or oversized observation".into());
            }
        }
        if self.proxy.as_ref().is_some_and(|p| {
            p.matched_puts
                .iter()
                .flatten()
                .any(|r| r.key.len() > FIELD_BYTES || r.value.len() > FIELD_BYTES)
        }) {
            return Err("oversized proxy receipt".into());
        }
        Ok(())
    }
}

fn outcome_bounded(outcome: &Outcome) -> bool {
    match outcome {
        Outcome::Mutation(
            MutationOutcome::Unknown(f)
            | MutationOutcome::Rejected(f)
            | MutationOutcome::NotSent(f),
        ) => f.message.len() <= FIELD_BYTES,
        Outcome::AdmissionError(s) | Outcome::ReadError(s) => s.len() <= FIELD_BYTES,
        Outcome::Value(Some(v)) => v.len() <= FIELD_BYTES,
        Outcome::Mutation(MutationOutcome::Committed) | Outcome::Value(None) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::super::proxy::MatchedPut;
    use super::*;
    use rockbee_raft_protocol::{Failure, FailureReason, RequestId};

    fn unknown() -> Outcome {
        Outcome::Mutation(MutationOutcome::Unknown(Failure {
            reason: FailureReason::Disconnected,
            message: "observed disconnect".into(),
        }))
    }

    fn complete(first_read: Option<Bytes>, last_read: Option<Bytes>) -> Capture {
        let mut capture = Capture::new(Bytes::from_static(b"same-key"));
        let first = capture
            .begin(Operation::Put(Bytes::from_static(b"A")), 1)
            .unwrap();
        let second = capture
            .begin(Operation::Put(Bytes::from_static(b"B")), 2)
            .unwrap();
        capture.notice(first, unknown(), 7).unwrap();
        capture.notice(second, unknown(), 8).unwrap();
        for (operation, outcome, at) in [
            (Operation::Get, Outcome::ReadError("closed".into()), 9),
            (Operation::Get, Outcome::Value(first_read), 11),
            (
                Operation::Put(Bytes::from_static(b"C")),
                Outcome::Mutation(MutationOutcome::Committed),
                13,
            ),
            (Operation::Get, Outcome::Value(last_read), 15),
        ] {
            let slot = capture.begin(operation, at).unwrap();
            capture.notice(slot, outcome, at + 1).unwrap();
        }
        capture.proxy = Some(ProxyReceipt {
            accepted_connections: 1,
            forwarded_put_frames: 2,
            suppressed_response_bytes: 10,
            original_eof_observed: true,
            both_received_at: Some(3),
            forward_started_at: Some(4),
            matched_puts: [
                Some(MatchedPut {
                    request_id: RequestId::new(42).unwrap(),
                    key: b"same-key".to_vec(),
                    value: b"B".to_vec(),
                    committed_at: 5,
                }),
                Some(MatchedPut {
                    request_id: RequestId::new(41).unwrap(),
                    key: b"same-key".to_vec(),
                    value: b"A".to_vec(),
                    committed_at: 6,
                }),
            ],
            ..ProxyReceipt::default()
        });
        capture.native_calls = Some(3);
        capture.seal(17, false);
        capture
    }

    fn reload(capture: &Capture) -> Capture {
        let dir = tempfile::tempdir().unwrap();
        let reloaded = capture.persist_reload(&dir.path().join("history.bin"));
        assert_eq!(*capture, reloaded);
        reloaded
    }

    #[test]
    fn either_overlapping_write_can_win_without_inventing_receipt_order() {
        for value in [b"A", b"B"] {
            let captured = reload(&complete(
                Some(Bytes::copy_from_slice(value)),
                Some(Bytes::from_static(b"C")),
            ));
            let model = captured.history().unwrap();
            assert_eq!(model.attempts.len(), 6);
            for (index, ended_by) in [6, 5].into_iter().enumerate() {
                assert_eq!(
                    model.attempts[index].execution,
                    Some(history::Execution {
                        ended_by,
                        effect: history::EffectKnowledge::Applied
                    })
                );
            }
            let history::Verdict::Legal { witnesses, .. } =
                history::check(&model, 100_000).unwrap()
            else {
                panic!("both overlap orders must be legal")
            };
            assert!(witnesses[0].omitted.is_empty());
            assert_eq!(witnesses[0].linearized.len(), 5);
            assert!(witnesses[0].linearized.contains(&1));
            assert!(witnesses[0].linearized.contains(&2));
        }
    }

    #[test]
    fn absent_first_read_or_stale_final_read_is_illegal() {
        let absent = complete(None, Some(Bytes::from_static(b"C")));
        assert!(matches!(
            history::check(&reload(&absent).history().unwrap(), 100_000).unwrap(),
            history::Verdict::Illegal { .. }
        ));
        for stale in [
            None,
            Some(Bytes::from_static(b"A")),
            Some(Bytes::from_static(b"B")),
        ] {
            let captured = complete(Some(Bytes::from_static(b"B")), stale);
            assert!(matches!(
                history::check(&reload(&captured).history().unwrap(), 100_000).unwrap(),
                history::Verdict::Illegal { .. }
            ));
        }
    }

    #[test]
    fn each_missing_receipt_leaves_a_partial_history() {
        for missing in 0..2 {
            let mut captured = complete(
                Some(Bytes::from_static(b"A")),
                Some(Bytes::from_static(b"C")),
            );
            captured.proxy.as_mut().unwrap().matched_puts[missing] = None;
            let model = reload(&captured).history().unwrap();
            assert_eq!(
                model
                    .attempts
                    .iter()
                    .filter(|a| a.execution.is_some())
                    .count(),
                1
            );
            assert!(matches!(
                history::check(&model, 100_000).unwrap(),
                history::Verdict::Inconclusive {
                    reason: history::Inconclusive::PartialCapture,
                    ..
                }
            ));
        }
    }

    #[test]
    fn mismatched_receipts_are_preserved_but_never_become_effect_proof() {
        for kind in 0..4 {
            let mut captured = complete(
                Some(Bytes::from_static(b"A")),
                Some(Bytes::from_static(b"C")),
            );
            let receipt = captured.proxy.as_mut().unwrap().matched_puts[0]
                .as_mut()
                .unwrap();
            match kind {
                0 => receipt.key = b"another-key".to_vec(),
                1 => receipt.value = b"another-value".to_vec(),
                2 => receipt.request_id = RequestId::new(41).unwrap(),
                _ => receipt.committed_at = 2,
            }
            assert!(reload(&captured).history().is_err());
        }
    }

    #[test]
    fn actual_incompatible_notice_survives_reload_for_h1_validation() {
        for (operation, outcome) in [
            (
                Operation::Put(Bytes::from_static(b"A")),
                Outcome::Value(None),
            ),
            (
                Operation::Get,
                Outcome::Mutation(MutationOutcome::Committed),
            ),
        ] {
            let mut captured = Capture::new(Bytes::from_static(b"same-key"));
            let slot = captured.begin(operation, 1).unwrap();
            captured.notice(slot, outcome, 2).unwrap();
            captured.seal(3, true);
            assert_eq!(
                history::check(&reload(&captured).history().unwrap(), 100_000),
                Err(history::InputError::OutcomeMismatch(1))
            );
        }
    }

    #[tokio::test]
    async fn observer_abort_preserves_the_open_public_invocation() {
        let capture = std::sync::Arc::new(std::sync::Mutex::new(Capture::new(Bytes::from_static(
            b"same-key",
        ))));
        let observed = std::sync::Arc::clone(&capture);
        let (ready, received) = tokio::sync::oneshot::channel();
        let observer = tokio::spawn(async move {
            observed
                .lock()
                .unwrap()
                .begin(Operation::Put(Bytes::from_static(b"A")), 1)
                .unwrap();
            ready.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        received.await.unwrap();
        observer.abort();
        assert!(observer.await.unwrap_err().is_cancelled());
        let mut captured = capture.lock().unwrap();
        captured.seal(2, true);
        let model = reload(&captured).history().unwrap();
        assert_eq!(model.attempts.len(), 1);
        assert!(model.attempts[0].notice.is_none());
        assert!(matches!(
            history::check(&model, 100_000).unwrap(),
            history::Verdict::Inconclusive {
                reason: history::Inconclusive::PartialCapture,
                ..
            }
        ));
    }

    #[test]
    fn bounded_recorder_retains_prior_notice_on_duplicate_and_rejects_sealed_changes() {
        let mut captured = Capture::new(Bytes::from_static(b"same-key"));
        assert!(
            captured
                .begin(Operation::Put(Bytes::from(vec![0; FIELD_BYTES + 1])), 1)
                .is_err()
        );
        let slot = captured.begin(Operation::Get, 2).unwrap();
        captured.notice(slot, Outcome::Value(None), 3).unwrap();
        assert!(
            captured
                .notice(slot, Outcome::ReadError("duplicate".into()), 4)
                .is_err()
        );
        captured.seal(5, false);
        assert!(captured.begin(Operation::Get, 6).is_err());
        let model = reload(&captured).history().unwrap();
        assert_eq!(
            model.attempts[0].notice.as_ref().unwrap().outcome,
            history::Outcome::Value(None)
        );
    }
}
