# Coordinator test evidence audit — current Raft work

Status: finite read-only inventory on `feat/raft-kv-multiraft`. This audit
reviews the test evidence that matters to the current public-path, soak, and
peer-control work. It does not rerun Cargo, alter source, make a commit, or
turn historical green logs into evidence for source that changed afterward.

```text
Public client connections -> one in-process gateway -> three CLI node processes
                                      |                    |
                                      +-- backend links ----+-- private peer RPC
                                                               -> Raft groups -> RocksDB
```

## Evidence inventory

| Evidence | Command/checkpoint described by artifact | Result | What it establishes | Current limitation |
| --- | --- | --- | --- | --- |
| `reports/workspace-after-writer-fix.log` | Whole nested-workspace test checkpoint after the writer-exit fix | PASS: 143 tests; manual soak ignored | Existing unit, integration, protocol, gateway, storage, peer and runtime cases passed at that source revision | It predates the active bounded peer-control connection changes; it is not a green result for those edits. |
| `reports/soak-60s-c1.log`, `reports/soak/stable-mBkDks/` | One client, one worker, one gateway, three node processes, 60-second warm closed loop | PASS: 72,954 successful operations in 60.005 s; clean shutdown | This exact c1 workload made sustained progress; HDR output is an observed warm closed-loop rate only | One worker and prior source; no c8 or new peer-control qualification. |
| `reports/soak-unit-green.log` and `reports/soak-processes-green.log` | Focused deterministic soak/accounting/cleanup tests | PASS: five soak unit checks; seven process checks, with manual soak ignored | Accounting preserves successful prefixes, invalid settings reject, worker shutdown owns cleanup, process recovery fixture passes | These are fixture checks, not a sustained availability result. |
| `reports/soak-diagnostics-tests.log` | Bounded diagnostic fixture checks | PASS: seven tests, manual soak ignored | Diagnostic history has explicit bounded retention/omission and cleanup remains owned | Diagnostic probing changes workload; it is not a baseline. |
| `reports/soak-clippy.log` | Node Clippy checkpoint | PASS | The checked node source had no Clippy warnings | It predates current peer-control edits. |
| `reports/soak-60s-c8.log`, `reports/soak/stable-c0OU7U/` | Original c8 shared-client soak | FAIL at 50.993 s: three `Unknown` mutations | Availability failed; no automatic replay occurred and checked successful values were not wrong | It does not identify the initiating phase or reconcile the eventual outcome of each Unknown mutation. |
| `reports/soak-60s-c8-diagnostic.log`, `reports/soak/stable-4xGY5g/` | First bounded c8 diagnostic repetition | FAIL at 28.002 s: three quorum-read failures and two Unknown mutations | Failure reproduced; observer tasks/links closed and no wrong checked value was recorded | Extra diagnostic traffic means this is not a clean performance baseline; trigger unresolved. |
| `reports/soak-60s-c8-peer-diagnostic.log`, `reports/soak/stable-bBn2R9/` | Second bounded c8 peer diagnostic | FAIL at 44.761 s: one quorum-read failure; 113,219 successful operations | Fresh peer-connect waits near 51 ms and peer deadline events are recorded; shutdown was clean | This identifies a failed phase candidate, not TCP exhaustion or the root cause. It is a failed correctness soak, never throughput success. |
| `docs/reviews/soak-c8-investigation.md`, `advisor-c8-analysis.md`, `judge-c8-analysis.md` | Read-only cross-review of the failed c8 evidence | OPEN | Advisor/verifier/Judge agree Unknown/no-replay remains correct and causal tracing must precede tuning | Agreement does not diagnose the incident; no vote changes the FAIL result. |
| Peer worker isolated control-TCP regression | Red/green checkpoint is actively being prepared in an isolated target | OPEN | A concrete connection-lifecycle hypothesis is under implementation | No command/result is accepted yet; this audit records no fix claim. |
| Storage worker multiple-client fixture | Test-only 1/2/3-client matrix support is actively being prepared | OPEN | The next matrix will separate independent client connections from shared-client c8 multiplexing | No matrix artifact exists yet. |

## Evidence interpretation

The three c8 artifacts are failures. Their passing sub-operations, zero observed
wrong-value results, HDR samples, and clean shutdown cannot be promoted into a
passing stability or capacity claim. `Unknown` means an accepted mutation may
have committed; the tests must retain that uncertainty and must not replay it.

The current peer-control implementation changes the private-RPC connection
lifecycle. Therefore the 143-test workspace log, c1 soak, and earlier Clippy
logs are baseline evidence only. They must be repeated in the final source
window before the sequential 1/2/3-client matrix or c8 regression can be called
green.

HDR histogram output is appropriate for the planned soak report. It must state
the profile, architecture, workload, duration, number of independent client
connections, number of workers, warm/cold condition, completed/censored/error
counts, and percentiles. It is not a maximum-throughput benchmark.

## Required review packet after the active checkpoints freeze

1. **Peer worker:** exact diff, focused red reproduction, focused green command,
   and proof that dirty/cancelled control connections are discarded rather than
   reused. The review must check control versus snapshot isolation and no
   unbounded waiter/buffer ownership.
2. **Storage worker:** test-only matrix diff and command/artifacts for one, two,
   and three independent client TCP connections with one gateway/three nodes,
   then the one-client/eight-worker regression. Each run must list the source
   commit, parameters, process IDs/cleanup result, and status.
3. **Coordinator:** compare the frozen source revision to every artifact and
   mark old logs as baseline, current pass, current fail, or not run.
4. **Advisor and verifier:** acknowledge the same scope, source revision,
   artifact list, semantics, and limits. They may approve only the reviewed
   scope; they cannot override a failing actual run.
5. **Judge:** receive the same packet even if the other reviewers agree, per
   current user instruction. The Judge records evidence-based disposition and
   may require a focused missing test. A failing run remains failing unless a
   new frozen source passes its stated replacement gate.

No worker should schedule broad timing/default changes, uncertain mutation
replay, or a performance claim to make c8 appear green. The next tests need to
discriminate the peer connection lifecycle issue from native storage/gate delay
while preserving mutation certainty and bounded resource ownership.
