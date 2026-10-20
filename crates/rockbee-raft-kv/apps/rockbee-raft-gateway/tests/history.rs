#[path = "support/history.rs"]
mod history;

use history::{
    Attempt, EffectKnowledge, Execution, History, KeyWitness, Notice, Operation, Outcome, Value,
};
use history::{Inconclusive, InputError, Verdict, check};
use std::collections::BTreeSet;

fn value(bytes: &[u8]) -> Value {
    Some(bytes.to_vec())
}

fn attempt(id: u64, operation: Operation, invoke: u64, end: u64, outcome: Outcome) -> Attempt {
    Attempt {
        id,
        key: b"k".to_vec(),
        operation,
        invoked_at: invoke,
        notice: Some(Notice { at: end, outcome }),
        execution: None,
    }
}

fn put(id: u64, bytes: &[u8], invoke: u64, end: u64) -> Attempt {
    attempt(
        id,
        Operation::Put(bytes.to_vec()),
        invoke,
        end,
        Outcome::Committed,
    )
}

fn unknown(id: u64, bytes: &[u8], invoke: u64, notice: u64) -> Attempt {
    attempt(
        id,
        Operation::Put(bytes.to_vec()),
        invoke,
        notice,
        Outcome::Unknown,
    )
}

fn get(id: u64, expected: Value, invoke: u64, end: u64) -> Attempt {
    attempt(id, Operation::Get, invoke, end, Outcome::Value(expected))
}

fn delete(id: u64, invoke: u64, end: u64) -> Attempt {
    attempt(id, Operation::Delete, invoke, end, Outcome::Committed)
}

fn bounded(mut attempt: Attempt, ended_by: u64, effect: EffectKnowledge) -> Attempt {
    attempt.execution = Some(Execution { ended_by, effect });
    attempt
}

fn fixture(initial: Value, attempts: Vec<Attempt>) -> History {
    History {
        initial: vec![(b"k".to_vec(), initial)],
        attempts,
        horizon: 1000,
        complete: true,
    }
}

struct Case {
    name: &'static str,
    history: History,
    // Explicit orders are hand proofs; None denotes an impossible history.
    proof: Option<(Vec<u64>, Vec<u64>)>,
}

fn hand_cases() -> Vec<Case> {
    let mut cases = vec![
        Case {
            name: "V01 sequential success: PUT before GET",
            history: fixture(None, vec![put(1, b"v", 1, 2), get(2, value(b"v"), 3, 4)]),
            proof: Some((vec![1, 2], vec![])),
        },
        Case {
            name: "V02 stale: mandatory PUT precedes GET",
            history: fixture(None, vec![put(1, b"v", 1, 2), get(2, None, 3, 4)]),
            proof: None,
        },
        Case {
            name: "V03 delayed Unknown: GET, PUT, GET",
            history: fixture(
                None,
                vec![
                    unknown(1, b"v", 1, 2),
                    get(2, None, 3, 4),
                    get(3, value(b"v"), 5, 6),
                ],
            ),
            proof: Some((vec![2, 1, 3], vec![])),
        },
        Case {
            name: "V04 Unknown omitted",
            history: fixture(None, vec![unknown(1, b"v", 1, 2), get(2, None, 3, 4)]),
            proof: Some((vec![2], vec![1])),
        },
        Case {
            name: "V05 Unknown included",
            history: fixture(
                None,
                vec![unknown(1, b"v", 1, 2), get(2, value(b"v"), 3, 4)],
            ),
            proof: Some((vec![1, 2], vec![])),
        },
        Case {
            name: "V06 end bound: include breaks first GET; omit breaks second",
            history: fixture(
                None,
                vec![
                    bounded(unknown(1, b"v", 1, 2), 3, EffectKnowledge::MayApply),
                    get(2, None, 4, 5),
                    get(3, value(b"v"), 6, 7),
                ],
            ),
            proof: None,
        },
        Case {
            name: "V07 Applied evidence forces effect before GET",
            history: fixture(
                None,
                vec![
                    bounded(unknown(1, b"v", 1, 2), 3, EffectKnowledge::Applied),
                    get(2, None, 4, 5),
                ],
            ),
            proof: None,
        },
        Case {
            name: "V08 one Unknown cannot resurrect after DELETE",
            history: fixture(
                None,
                vec![
                    unknown(1, b"v", 1, 2),
                    get(2, value(b"v"), 3, 4),
                    delete(3, 5, 6),
                    get(4, value(b"v"), 7, 8),
                ],
            ),
            proof: None,
        },
        Case {
            name: "V09 successful DELETE",
            history: fixture(
                None,
                vec![put(1, b"v", 1, 2), delete(2, 3, 4), get(3, None, 5, 6)],
            ),
            proof: Some((vec![1, 2, 3], vec![])),
        },
        Case {
            name: "V10 DELETE forbids resurrection",
            history: fixture(
                None,
                vec![
                    put(1, b"v", 1, 2),
                    delete(2, 3, 4),
                    get(3, value(b"v"), 5, 6),
                ],
            ),
            proof: None,
        },
        Case {
            name: "V11 distinct PUT recreates",
            history: fixture(
                None,
                vec![
                    put(1, b"v1", 1, 2),
                    delete(2, 3, 4),
                    put(3, b"v2", 5, 6),
                    get(4, value(b"v2"), 7, 8),
                ],
            ),
            proof: Some((vec![1, 2, 3, 4], vec![])),
        },
        Case {
            name: "V12 overlapping writes: first PUT then second",
            history: fixture(
                None,
                vec![
                    put(1, b"a", 1, 4),
                    put(2, b"b", 2, 3),
                    get(3, value(b"b"), 5, 6),
                ],
            ),
            proof: Some((vec![1, 2, 3], vec![])),
        },
        Case {
            name: "V13 overlapping writes: second PUT then first",
            history: fixture(
                None,
                vec![
                    put(1, b"a", 1, 4),
                    put(2, b"b", 2, 3),
                    get(3, value(b"a"), 5, 6),
                ],
            ),
            proof: Some((vec![2, 1, 3], vec![])),
        },
        Case {
            name: "V14 both mandatory PUTs prevent absence",
            history: fixture(
                None,
                vec![put(1, b"a", 1, 4), put(2, b"b", 2, 3), get(3, None, 5, 6)],
            ),
            proof: None,
        },
        Case {
            name: "V15 overlapping GET may see old state",
            history: fixture(None, vec![get(1, None, 1, 4), put(2, b"v", 2, 3)]),
            proof: Some((vec![1, 2], vec![])),
        },
        Case {
            name: "V18 no-effect attempts harmless",
            history: fixture(
                None,
                vec![
                    attempt(1, Operation::Put(b"v".to_vec()), 1, 2, Outcome::NotSent),
                    attempt(2, Operation::Delete, 3, 4, Outcome::Rejected),
                    get(3, None, 5, 6),
                ],
            ),
            proof: Some((vec![3], vec![])),
        },
        Case {
            name: "V19 two distinct Unknowns may each apply once",
            history: fixture(
                None,
                vec![
                    unknown(1, b"v", 1, 2),
                    get(2, value(b"v"), 3, 4),
                    delete(3, 5, 6),
                    unknown(4, b"v", 7, 8),
                    get(5, value(b"v"), 9, 10),
                ],
            ),
            proof: Some((vec![1, 2, 3, 4, 5], vec![])),
        },
        Case {
            name: "V20 stored empty is present",
            history: fixture(None, vec![put(1, b"", 1, 2), get(2, value(b""), 3, 4)]),
            proof: Some((vec![1, 2], vec![])),
        },
        Case {
            name: "V20 empty cannot explain missing",
            history: fixture(None, vec![put(1, b"", 1, 2), get(2, None, 3, 4)]),
            proof: None,
        },
    ];
    for (outcome, put_name, delete_name) in [
        (
            Outcome::NotSent,
            "V16 NotSent PUT has no effect",
            "V17 NotSent DELETE has no effect",
        ),
        (
            Outcome::Rejected,
            "V16 Rejected PUT has no effect",
            "V17 Rejected DELETE has no effect",
        ),
    ] {
        cases.push(Case {
            name: put_name,
            history: fixture(
                None,
                vec![
                    attempt(1, Operation::Put(b"v".to_vec()), 1, 2, outcome.clone()),
                    get(2, value(b"v"), 3, 4),
                ],
            ),
            proof: None,
        });
        cases.push(Case {
            name: delete_name,
            history: fixture(
                value(b"v"),
                vec![
                    attempt(1, Operation::Delete, 1, 2, outcome),
                    get(2, None, 3, 4),
                ],
            ),
            proof: None,
        });
    }
    cases
}

// This intentionally uses raw records, pairwise order checks and actual bytes.
// It does not call checker normalization, predecessor masks, interning or DFS.
fn replay(history: &History, witness: &KeyWitness) -> bool {
    let Some((_, initial)) = history.initial.iter().find(|(key, _)| key == &witness.key) else {
        return false;
    };
    let mut state = initial.clone();
    let mut selected = BTreeSet::new();
    let omitted: BTreeSet<_> = witness.omitted.iter().copied().collect();
    if omitted.len() != witness.omitted.len() {
        return false;
    }
    let records: Vec<_> = history
        .attempts
        .iter()
        .filter(|a| a.key == witness.key)
        .collect();
    for id in &witness.linearized {
        if !selected.insert(*id) || omitted.contains(id) {
            return false;
        }
        let Some(a) = records.iter().find(|a| a.id == *id) else {
            return false;
        };
        let outcome = a.notice.as_ref().map(|n| &n.outcome);
        if matches!(
            outcome,
            Some(Outcome::NotSent | Outcome::Rejected | Outcome::ReadError)
        ) || a
            .execution
            .is_some_and(|e| e.effect == EffectKnowledge::NoEffect)
        {
            return false;
        }
        match (&a.operation, outcome) {
            (Operation::Get, Some(Outcome::Value(expected))) if &state == expected => {}
            (Operation::Put(bytes), _) => state = Some(bytes.clone()),
            (Operation::Delete, _) => state = None,
            _ => return false,
        }
    }
    for a in &records {
        let outcome = a.notice.as_ref().map(|n| &n.outcome);
        let mandatory = matches!(outcome, Some(Outcome::Committed | Outcome::Value(_)))
            || a.execution
                .is_some_and(|e| e.effect == EffectKnowledge::Applied);
        let optional = !matches!(a.operation, Operation::Get)
            && !mandatory
            && !matches!(outcome, Some(Outcome::NotSent | Outcome::Rejected))
            && !a
                .execution
                .is_some_and(|e| e.effect == EffectKnowledge::NoEffect);
        if (mandatory && !selected.contains(&a.id))
            || (optional && selected.contains(&a.id) == omitted.contains(&a.id))
            || (!optional && omitted.contains(&a.id))
        {
            return false;
        }
    }
    if omitted
        .iter()
        .any(|id| !records.iter().any(|a| a.id == *id))
    {
        return false;
    }
    for (index, id) in witness.linearized.iter().enumerate() {
        let a = records
            .iter()
            .find(|a| a.id == *id)
            .expect("selected record");
        let client_end = a.notice.as_ref().and_then(|n| match n.outcome {
            Outcome::Committed | Outcome::Value(_) => Some(n.at),
            _ => None,
        });
        let end = match (client_end, a.execution) {
            (Some(c), Some(e)) => c.min(e.ended_by),
            (Some(c), None) => c,
            (None, Some(e)) => e.ended_by,
            (None, None) => history.horizon,
        };
        // A must not appear after B if A definitively ended before B started.
        for earlier in &witness.linearized[..index] {
            let b = records
                .iter()
                .find(|a| a.id == *earlier)
                .expect("selected record");
            if end < b.invoked_at {
                return false;
            }
        }
    }
    true
}

#[test]
fn hand_proven_legal_orders_replay_without_the_search() {
    for case in hand_cases() {
        if let Some((linearized, omitted)) = case.proof {
            assert!(
                replay(
                    &case.history,
                    &KeyWitness {
                        key: b"k".to_vec(),
                        linearized,
                        omitted
                    }
                ),
                "{}",
                case.name
            );
        }
    }
}

#[test]
fn independent_replayer_rejects_missing_repeated_and_misordered_effects() {
    let h = fixture(None, vec![put(1, b"v", 1, 2), get(2, value(b"v"), 3, 4)]);
    for linearized in [vec![2], vec![1, 1, 2], vec![2, 1]] {
        assert!(!replay(
            &h,
            &KeyWitness {
                key: b"k".to_vec(),
                linearized,
                omitted: vec![]
            }
        ));
    }
    let stale = fixture(
        None,
        vec![put(1, b"v", 1, 2), get(2, None, 3, 4), delete(3, 5, 6)],
    );
    // Its final None is plausible, but neither a stale read nor reversed real time is.
    for linearized in [vec![1, 2, 3], vec![2, 1, 3]] {
        assert!(!replay(
            &stale,
            &KeyWitness {
                key: b"k".to_vec(),
                linearized,
                omitted: vec![]
            }
        ));
    }
}

#[test]
fn failed_and_pending_gets_assert_no_value_and_cannot_be_replayed_as_reads() {
    let failed = attempt(1, Operation::Get, 1, 2, Outcome::ReadError);
    let pending = Attempt {
        id: 2,
        key: b"k".to_vec(),
        operation: Operation::Get,
        invoked_at: 3,
        notice: None,
        execution: None,
    };
    // The error/pending observations neither erase the stored value nor assert None.
    let h = fixture(
        value(b"v"),
        vec![failed, pending, get(3, value(b"v"), 4, 5)],
    );
    assert!(replay(
        &h,
        &KeyWitness {
            key: b"k".to_vec(),
            linearized: vec![3],
            omitted: vec![],
        }
    ));
    for id in [1, 2] {
        assert!(!replay(
            &h,
            &KeyWitness {
                key: b"k".to_vec(),
                linearized: vec![id, 3],
                omitted: vec![],
            }
        ));
    }
}

fn assert_legal(h: &History) -> Vec<KeyWitness> {
    let verdict = check(h, 100_000).expect("valid fixture");
    let witnesses = match verdict {
        Verdict::Legal { witnesses, .. } => witnesses,
        other => {
            assert!(
                matches!(other, Verdict::Legal { .. }),
                "expected legal: {other:?}; history={h:?}"
            );
            return vec![];
        }
    };
    for witness in &witnesses {
        assert!(replay(h, witness), "invalid returned witness: {witness:?}");
    }
    witnesses
}

fn assert_illegal(h: &History) {
    assert!(
        matches!(check(h, 100_000), Ok(Verdict::Illegal { .. })),
        "history={h:?}"
    );
}

#[test]
fn checker_matches_all_twenty_hand_proven_verifier_cases() {
    for case in hand_cases() {
        let verdict = check(&case.history, 100_000).expect("valid hand fixture");
        match verdict {
            Verdict::Legal { witnesses, .. } => {
                assert!(case.proof.is_some(), "{} incorrectly accepted", case.name);
                assert_eq!(witnesses.len(), 1);
                assert!(
                    replay(&case.history, &witnesses[0]),
                    "{} invalid witness",
                    case.name
                );
            }
            Verdict::Illegal {
                key, attempt_ids, ..
            } => {
                assert!(case.proof.is_none(), "{} incorrectly rejected", case.name);
                assert_eq!(key, b"k");
                assert_eq!(attempt_ids.len(), case.history.attempts.len());
            }
            other => assert!(
                matches!(other, Verdict::Legal { .. } | Verdict::Illegal { .. }),
                "{} unresolved: {other:?}",
                case.name
            ),
        }
    }
}

#[test]
fn pending_mutation_can_apply_after_a_read_and_unknown_end_can_precede_notice() {
    let mut pending = unknown(1, b"v", 1, 2);
    pending.notice = None;
    let h = fixture(
        None,
        vec![pending, get(2, None, 3, 4), get(3, value(b"v"), 5, 6)],
    );
    assert_legal(&h);
    let u = bounded(unknown(1, b"v", 1, 5), 2, EffectKnowledge::Applied);
    assert_legal(&fixture(None, vec![u.clone(), get(2, value(b"v"), 3, 4)]));
    assert_illegal(&fixture(None, vec![u, get(2, None, 3, 4)]));
    let u = bounded(unknown(1, b"v", 1, 5), 2, EffectKnowledge::MayApply);
    let witness = assert_legal(&fixture(None, vec![u, get(2, None, 3, 4)]));
    assert_eq!(witness[0].omitted, vec![1]);
}

#[test]
fn certainty_merges_knowledge_without_treating_may_apply_as_a_contradiction() {
    for (notice, evidence, initial, expected) in [
        (Outcome::Unknown, EffectKnowledge::NoEffect, None, None),
        (
            Outcome::Unknown,
            EffectKnowledge::Applied,
            None,
            value(b"v"),
        ),
        (
            Outcome::Committed,
            EffectKnowledge::MayApply,
            None,
            value(b"v"),
        ),
        (Outcome::Rejected, EffectKnowledge::MayApply, None, None),
        (Outcome::NotSent, EffectKnowledge::NoEffect, None, None),
    ] {
        let a = bounded(
            attempt(1, Operation::Put(b"v".to_vec()), 1, 2, notice),
            2,
            evidence,
        );
        assert_legal(&fixture(initial, vec![a, get(2, expected, 3, 4)]));
    }
    // Ended is optional, even if every possible effect is before the read.
    let h = fixture(
        None,
        vec![
            bounded(unknown(1, b"v", 1, 2), 3, EffectKnowledge::MayApply),
            get(2, None, 4, 5),
        ],
    );
    assert_eq!(assert_legal(&h)[0].omitted, vec![1]);
}

#[test]
fn optional_omission_is_allowed_before_its_unresolved_predecessor() {
    let h = fixture(
        None,
        vec![
            put(2, b"a", 1, 2),
            bounded(unknown(1, b"b", 3, 4), 5, EffectKnowledge::MayApply),
            get(3, value(b"a"), 6, 7),
        ],
    );
    let witness = assert_legal(&h);
    assert_eq!(witness[0].linearized, vec![2, 3]);
    assert_eq!(witness[0].omitted, vec![1]);
}

#[test]
fn initial_state_empty_absent_and_opaque_bytes_are_distinct() {
    for (initial, expected, legal) in [
        (None, None, true),
        (value(b""), value(b""), true),
        (None, value(b""), false),
        (value(b""), None, false),
        (value(b"a"), value(b"b"), false),
    ] {
        let h = fixture(initial, vec![get(1, expected, 1, 2)]);
        if legal {
            assert_legal(&h);
        } else {
            assert_illegal(&h);
        }
    }
    let binary = vec![0, 255, 128, 0];
    let key = vec![255, 0, 128];
    let mut h = fixture(
        None,
        vec![put(1, &binary, 1, 2), get(2, Some(binary), 3, 4)],
    );
    h.initial[0].0 = key.clone();
    for a in &mut h.attempts {
        a.key.clone_from(&key);
    }
    assert_eq!(assert_legal(&h)[0].key, key);
}

#[test]
fn overlapping_long_put_can_linearize_after_an_intervening_read() {
    assert_legal(&fixture(
        None,
        vec![
            put(1, b"a", 1, 6),
            put(2, b"b", 2, 3),
            get(3, value(b"b"), 4, 5),
            get(4, value(b"a"), 7, 8),
        ],
    ));
    assert_illegal(&fixture(
        value(b"a"),
        vec![put(1, b"b", 1, 2), get(2, value(b"a"), 3, 4)],
    ));
    assert_illegal(&fixture(
        value(b"v"),
        vec![
            delete(1, 1, 2),
            get(2, None, 3, 4),
            get(3, value(b"v"), 5, 6),
        ],
    ));
}

#[test]
fn key_histories_do_not_share_values_or_effects() {
    let mut h = fixture(value(b"a"), vec![get(1, value(b"a"), 1, 2)]);
    h.initial.push((b"z".to_vec(), value(b"b")));
    let mut second = get(2, value(b"b"), 3, 4);
    second.key = b"z".to_vec();
    h.attempts.push(second);
    assert_eq!(assert_legal(&h).len(), 2);
    h.attempts[1].notice.as_mut().expect("notice").outcome = Outcome::Value(value(b"a"));
    assert!(matches!(check(&h,100_000),Ok(Verdict::Illegal { key, .. }) if key == b"z"));
}

#[test]
fn validation_precedes_illegal_and_partial_capture_verdicts() {
    let illegal = fixture(None, vec![get(1, value(b"v"), 1, 2)]);
    let mut h = illegal.clone();
    h.attempts.push(get(1, None, 3, 4));
    assert_eq!(check(&h, 0), Err(InputError::DuplicateAttempt(1)));
    h.complete = false;
    assert_eq!(check(&h, 0), Err(InputError::DuplicateAttempt(1)));
    let mut h = illegal.clone();
    h.initial.push((b"k".to_vec(), None));
    assert_eq!(check(&h, 0), Err(InputError::DuplicateInitialKey));
    let mut h = illegal.clone();
    h.initial.clear();
    assert_eq!(check(&h, 0), Err(InputError::MissingInitialState(1)));
    let mut h = illegal.clone();
    h.attempts
        .push(attempt(2, Operation::Get, 3, 4, Outcome::Committed));
    assert_eq!(check(&h, 0), Err(InputError::OutcomeMismatch(2)));
    let mut h = illegal;
    h.attempts.push(attempt(
        2,
        Operation::Put(vec![]),
        3,
        4,
        Outcome::Value(None),
    ));
    assert_eq!(check(&h, 0), Err(InputError::OutcomeMismatch(2)));
}

#[test]
fn malformed_chronology_and_conflicting_certainty_are_input_errors() {
    for a in [
        get(1, None, 3, 2),
        get(1, None, 2, 2),
        get(1, None, 1, 1000),
        get(1, None, 1000, 1001),
        bounded(unknown(1, b"v", 3, 4), 2, EffectKnowledge::MayApply),
        bounded(unknown(1, b"v", 1, 2), 1001, EffectKnowledge::MayApply),
    ] {
        assert_eq!(
            check(&fixture(None, vec![a]), 100_000),
            Err(InputError::Chronology(1))
        );
    }
    let h = fixture(None, vec![get(1, None, 1, 2), get(2, None, 2, 3)]);
    assert_eq!(check(&h, 100_000), Err(InputError::DuplicateEvent(2)));
    for (notice, evidence) in [
        (Outcome::Committed, EffectKnowledge::NoEffect),
        (Outcome::Rejected, EffectKnowledge::Applied),
        (Outcome::NotSent, EffectKnowledge::Applied),
    ] {
        let a = bounded(attempt(1, Operation::Delete, 1, 2, notice), 2, evidence);
        assert_eq!(
            check(&fixture(None, vec![a]), 100_000),
            Err(InputError::ContradictoryEvidence(1))
        );
    }
    let a = bounded(get(1, None, 1, 2), 2, EffectKnowledge::MayApply);
    assert_eq!(
        check(&fixture(None, vec![a]), 100_000),
        Err(InputError::ReadExecutionEvidence(1))
    );
    let a = bounded(unknown(1, b"v", 1, 2), 1000, EffectKnowledge::MayApply);
    assert_legal(&fixture(None, vec![a, get(2, None, 3, 4)]));
}

#[test]
fn failed_and_pending_gets_and_proven_no_effects_need_no_search_budget() {
    let mut pending = get(2, None, 3, 4);
    pending.notice = None;
    let h = fixture(
        value(b"v"),
        vec![
            attempt(1, Operation::Get, 1, 2, Outcome::ReadError),
            pending,
            attempt(3, Operation::Delete, 5, 6, Outcome::NotSent),
        ],
    );
    assert!(matches!(
        check(&h, 0),
        Ok(Verdict::Legal { visited: 0, .. })
    ));
    let h = fixture(
        value(b"v"),
        vec![
            attempt(1, Operation::Get, 1, 2, Outcome::ReadError),
            get(2, None, 3, 4),
        ],
    );
    assert_illegal(&h); // ReadError is not a successful observation of absence.
}

#[test]
fn budget_pruning_is_inconclusive_and_a_found_witness_stops_search() {
    let h = fixture(None, vec![put(1, b"v", 1, 2), get(2, value(b"v"), 3, 4)]);
    for budget in [0, 1, 2] {
        assert_eq!(
            check(&h, budget),
            Ok(Verdict::Inconclusive {
                reason: Inconclusive::BudgetExceeded,
                visited: budget
            })
        );
    }
    assert!(matches!(
        check(&h, 3),
        Ok(Verdict::Legal { visited: 3, .. })
    ));
    let h = fixture(
        None,
        vec![unknown(1, b"v", 1, 2), get(2, value(b"v"), 3, 4)],
    );
    // First include branch succeeds; omission and other orders need no exploration.
    assert!(matches!(
        check(&h, 3),
        Ok(Verdict::Legal { visited: 3, .. })
    ));
    let h = fixture(None, vec![unknown(1, b"v", 1, 2), get(2, None, 3, 4)]);
    assert!(matches!(check(&h, 2), Ok(Verdict::Inconclusive { .. })));
    assert_legal(&h);
}

#[test]
fn budget_is_global_across_keys_and_results_are_repeatable() {
    let mut h = fixture(None, vec![get(1, None, 1, 2)]);
    h.initial.push((b"z".to_vec(), None));
    let mut a = get(2, None, 3, 4);
    a.key = b"z".to_vec();
    h.attempts.push(a);
    assert_eq!(
        check(&h, 2),
        Ok(Verdict::Inconclusive {
            reason: Inconclusive::BudgetExceeded,
            visited: 2
        })
    );
    assert!(matches!(
        check(&h, 4),
        Ok(Verdict::Legal { visited: 4, .. })
    ));
    let baseline = check(&h, 100_000);
    h.initial.reverse();
    h.attempts.reverse();
    assert_eq!(check(&h, 100_000), baseline);
    for _ in 0..3 {
        assert_eq!(check(&h, 100_000), baseline);
    }
}

fn assert_limit(h: &History, reason: Inconclusive) {
    assert_eq!(
        check(h, 100_000),
        Ok(Verdict::Inconclusive { reason, visited: 0 })
    );
}

#[test]
fn container_semantic_payload_and_partial_capture_limits_are_explicit() {
    let mut h = fixture(
        None,
        (0..64)
            .map(|n| {
                let mut a = attempt(n, Operation::Get, n * 2 + 1, n * 2 + 2, Outcome::ReadError);
                a.key = b"k".to_vec();
                a
            })
            .collect(),
    );
    assert_legal(&h); // 64 total failed reads are not 64 semantic effects.
    h.attempts
        .push(attempt(64, Operation::Get, 129, 130, Outcome::ReadError));
    assert_limit(&h, Inconclusive::Attempts);
    let mut h = fixture(None, vec![]);
    h.initial = (0u8..64).map(|n| (vec![n], None)).collect();
    assert_legal(&h);
    h.initial.push((vec![64], None));
    assert_limit(&h, Inconclusive::Keys);
    let h = fixture(
        None,
        (0..16)
            .map(|n| get(n, None, n * 2 + 1, n * 2 + 2))
            .collect(),
    );
    assert_legal(&h);
    let mut h = h;
    h.attempts.push(get(16, None, 33, 34));
    assert_limit(&h, Inconclusive::OperationsPerKey);
    let mut h = fixture(Some(vec![0; 65_535]), vec![]); // one key byte makes 64 KiB.
    assert_legal(&h);
    h.initial[0].1.as_mut().expect("initial").push(0);
    assert_limit(&h, Inconclusive::PayloadBytes);
    let mut h = fixture(None, vec![get(1, value(b"missing"), 1, 2)]);
    h.complete = false;
    assert_limit(&h, Inconclusive::PartialCapture); // Cannot report the prefix Illegal.
    h.attempts.clear();
    assert_limit(&h, Inconclusive::PartialCapture); // Nor certify a truncated empty prefix.
}

#[test]
fn every_supplied_key_and_value_counts_toward_payload_limit() {
    let mut h = fixture(None, vec![put(1, &vec![0; 65_533], 1, 2)]);
    assert_legal(&h); // initial key + attempt key + payload = 65_535.
    h.attempts[0].key.push(0);
    h.initial.push((h.attempts[0].key.clone(), None));
    assert_limit(&h, Inconclusive::PayloadBytes);
    let h = fixture(None, vec![get(1, Some(vec![0; 65_535]), 1, 2)]);
    assert_limit(&h, Inconclusive::PayloadBytes); // successful GET bytes count before search.
    let mut h = fixture(None, vec![]);
    h.initial.push((vec![0; 65_535], None));
    assert_legal(&h); // unused initial keys count, exactly at payload cap.
    h.initial[1].0.push(0);
    assert_limit(&h, Inconclusive::PayloadBytes);
}

#[test]
fn memo_hits_consume_budget_and_pruned_states_do_not_become_illegal() {
    // The two overlap orders converge on the same remaining GET/current value.
    // Calls: root; P1; P2; root's P2; P1 (memo hit). Neither order can read None.
    let h = fixture(
        None,
        vec![put(1, b"v", 1, 4), put(2, b"v", 2, 3), get(3, None, 5, 6)],
    );
    assert_eq!(
        check(&h, 4),
        Ok(Verdict::Inconclusive {
            reason: Inconclusive::BudgetExceeded,
            visited: 4
        })
    );
    assert!(matches!(
        check(&h, 5),
        Ok(Verdict::Illegal { visited: 5, .. })
    ));
    // Every fresh run must recover the exhausted verdict after a pruned run.
    assert_illegal(&h);
}

#[test]
fn requested_budget_cannot_raise_the_hard_global_call_cap() {
    // Fifteen mutually overlapping PUTs can appear in any order, but the final
    // GET(None) is impossible. Memo hits still make full exploration >100k calls.
    let mut attempts: Vec<_> = (0..15).map(|n| put(n, b"v", n + 1, 30 - n)).collect();
    attempts.push(get(15, None, 31, 32));
    let h = fixture(None, attempts);
    let expected = Ok(Verdict::Inconclusive {
        reason: Inconclusive::BudgetExceeded,
        visited: 100_000,
    });
    assert_eq!(check(&h, usize::MAX), expected);
    assert_eq!(check(&h, 100_000), expected);
}
