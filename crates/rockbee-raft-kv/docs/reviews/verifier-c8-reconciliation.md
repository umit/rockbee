# Missing post-failure recovery and reconciliation gate

Read-only follow-up; no source/build/run changes. The stable soak must retain its original availability **FAIL**. Reconciliation adds a separately reported safety/recovery observation and cannot turn Unknown into success or prove an unobserved outcome.

```text
availability FAIL -> join workers -> bounded read reconciliation -> report FAIL
                                  -> separate recovery canary keys
```

## Exact current gap

`tests/support/soak.rs:270–277` retains aggregate metrics and only an in-flight step, not the final per-key acknowledged state or uncertain intent. `Worker::run` at 389–419 constructs deterministic worker/group keys and cycle/version payloads, stops dependent operations after failure, and completes worker drain. The owner at 735–747 then proceeds directly to resource closure. It does not ask whether the failed group recovers read authority, verify the remaining acknowledged state, or observe an Unknown operation afterward. Existing process recovery is a separate successful-write/SIGKILL/reopen case, not reconciliation after this availability failure.

The original reports therefore cannot retrospectively establish individual Unknown outcomes from aggregate counts alone. Diagnostic events carry worker/group/step but not the cycle identity necessary to reconstruct exact versioned payloads without additional evidence.

## Smallest bounded test-only addition

Keep a final ledger of four keys per worker: at current concurrency cap 64 this is at most 256 records, not an append-only history. Store seed/worker/group/cycle/step descriptors and regenerate the deterministic 1-KiB payload when needed. Each record contains the latest acknowledged mutation state plus an optional unresolved mutation intent. Register intent before eager submission, update from the observed terminal result, and keep a canceled/in-flight mutation conservatively unresolved. A pending read does not create mutation uncertainty. There is at most one unresolved write per owned key because the workload stops its dependent cycle after failure.

After workers have joined, before closing resources, perform a read-only reconciliation phase with one global monotonic deadline, bounded per-request deadline and bounded number of sequential observations per key. Honor existing signal/cleanup ownership; do not spawn detached work or let 256 independent retry deadlines multiply the phase bound. Retry read *availability errors* within that bound, and preserve each failure plus each successful observation. Do not retry a successful unexpected value into a passing result. A fresh public Client connection may be needed after retirement; it performs only new GETs and does not replay prior mutations.

For an ACK-only key require the exact latest acknowledged state. For one uncertain mutation, permitted observations are:

| Uncertain operation | Before state | After state |
|---|---|---|
| Initial PUT | None | exact cycle/version-1 bytes |
| Update PUT | exact cycle/version-1 bytes | exact cycle/version-2 bytes |
| DELETE | exact cycle/version-2 bytes | None |

A successful barrier read may see before, followed by a later successful read seeing after: an Unknown notification did not end the execution interval. An old-only window is **unresolved**, not proof of non-commit. After observing after, observing before on a fresh sequential successful strong read with no intervening writes is forbidden. Any other value is a safety failure. Report zero successful reads before the deadline as recovery unavailable/inconclusive, never as safe absence.

Use separate owned canary keys for any subsequent one-shot PUT(Committed)/exact GET/DELETE(Committed)/GET(None) recovery exercise across all four groups. Never modify uncertain ledger keys, and never retry a canary mutation after Unknown. Canary success demonstrates new-operation progress in that phase; it does not settle an older Unknown operation or establish uninterrupted availability. Keep its duration and samples outside soak TPS/latency populations.

The ledger provides final-state checks for acknowledged prefixes, not a general full-history linearizability checker. Source inspection establishes that this worker does not issue dependent replay after failure (`soak.rs:414–416`); runtime proof of no duplicate Raft proposal still requires proposal-attempt instrumentation. Gateway fixture counts alone do not provide that proof for real nodes.

## Focused acceptance before implementation claims

Test the reconciliation checker with controlled histories: Unknown old->new is legal; new->old and foreign bytes fail; ACK-only missing/wrong bytes fail; unavailable-only and old-only Unknown remain unresolved; canceled mutation keeps intent; signal/global deadline still join and clean up; original availability FAIL remains FAIL even when all later reads succeed. A small dropped-reply real-node test can then supply an actual Unknown and verify later exact visibility without a second mutation submission. None of these have been run by this review.

Advisor directly concurred with these semantics, including the open Unknown interval and separate canary keys. Judge was sent the source-established adapter error-class/backoff issue and the need for this distinct post-failure gate. No source fix or incident-root-cause conclusion follows from this note.
