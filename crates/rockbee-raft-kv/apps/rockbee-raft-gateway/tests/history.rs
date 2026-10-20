#[path = "support/history.rs"]
mod history;

use history::{
    Attempt, EffectKnowledge, Execution, History, KeyWitness, Notice, Operation, Outcome, Value,
};
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
