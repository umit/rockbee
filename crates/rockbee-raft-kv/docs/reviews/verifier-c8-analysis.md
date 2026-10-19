# Independent c8 failure analysis

The concurrency-eight soak failed its availability gate twice. The failure is real; the initiating cause is not yet established. Unknown is the correct client certainty result, not proof of a lost write or a harmless rejection. This is a read-only review of current source and worker-run artifacts; verifier ran no build, soak, fault, or tuning experiment.

```text
8 workers -> shared Client -> in-process Gateway -> 3 node processes
                                                -> 4 data Raft groups -> shared RocksDB
                       <- unavailable read / uncertain mutation
```

## Directly verified observations

| Artifact | Observed result |
|---|---|
| `reports/soak/stable-c0OU7U/` | Configured 60 seconds; failed after 50.993009584 seconds. 119,908 successful operations, 19,984 complete cycles, three partial cycles; two PUTs and one DELETE returned Unknown with ForwardToLeader(2). |
| `reports/soak/stable-4xGY5g/` | Single diagnostic repetition; failed after 28.001665459 seconds. 74,017 successful operations, 12,333 complete cycles, five partial cycles; three quorum-read failures and two Unknown mutations. |

Both report zero wrong-value observations, no censored work and clean client/gateway/all-node shutdown. Unknown operations were not reconciled to determine their eventual application state. Therefore zero wrong-value observations is evidence for the checked successful prefixes, not proof of the fate of those Unknown writes or a complete concurrent-history safety proof. Neither failed run is a passing stability/capacity measurement.

I inspected the diagnostic CSV directly. Its microsecond times use the common measurement start:

1. At 27.410–27.412 seconds all eight final operations begin.
2. At 27.464 seconds three strong reads in groups 2, 3 and 4 report insufficient quorum, with acknowledgements only from node 1 (`got: {1}`). This establishes failed timely quorum confirmation, not physical loss or death of the other nodes.
3. Persistent-Link leader-hint probes remain responsive at 27.501–27.503 and 27.751–27.758 seconds, still reporting leader 1. This weighs against one continuous whole-process pause; these probes do not exercise fresh private peer connections or durable writes.
4. At 27.969–27.998 seconds three mutations succeed after roughly 558–587 ms; a group-2 update and group-3 DELETE return Unknown with ForwardToLeader(2).
5. At 28.001 seconds node 1's sampled hints first report leader 2 for groups 2 and 3. Hints are local observations, not quorum/term evidence or exact transition timestamps.

The manifest's `first_failure` is chosen by worker order, not chronological order. The CSV establishes that the quorum-read failures completed first. The diagnostic adds three backend Links and sampling traffic, so its timing/throughput is not a clean baseline. It retained 1,722 events with zero omitted events and joined its observer tasks/closed its Links.

Snapshot image-file changes were sampled around 15.003–15.255 seconds, with none near the diagnostic failure. This weakens a simple snapshot-publication-at-failure explanation. Directory sampling does not rule out background RocksDB or snapshot work. Original node logs contain only startup/shutdown lines and cannot reconstruct its initiating event.

## Why this error must remain Unknown

`cluster/service.rs::mutate` checks a local leader hint before calling OpenRaft `client_write`. Leadership can change between the check and core processing. Before that call an explicit NotLeader rejection is safe to reroute; after the call the error variant alone is insufficient.

The exact pinned OpenRaft 0.9.25 source has two relevant origins: `engine/engine_impl.rs::get_leader_handler_or_reject` can reject before leader work, while `core/raft_core.rs::Command::DeleteConflictLog` sends the same ForwardToLeader variant to already log-indexed client responders after truncating their suffix. The observed error does not identify which branch handled these particular writes. Current mapping to Unknown and absence of automatic replay preserve the stated contract. Changing this result to Rejected or automatically replaying it would invent evidence of safe rejection. Correct uncertainty handling does not satisfy the separate availability gate.

## Source-backed candidates, not confirmed triggers

- The adapter's peer TTL covers lane/connection admission, frame work, a fresh TCP connect, writes and reply. Upstream read-quorum/replication calls use the default 50-ms heartbeat interval; elections use 150–300 ms. The approximately 53-ms read failures are consistent with that deadline, but do not show which phase consumed it.
- Every private peer RPC opens a new connection. Socket churn/OS pressure is a candidate; no connect errno, socket-state or ephemeral-port evidence was captured. It is not a confirmed exhaustion bug.
- `rpc/network.rs::rpc_error` maps local errors, including Deadline, to Unreachable. Upstream replication enters backoff on Unreachable; its default is 500 ms. This is a concrete possible amplifier worth tracing against the roughly 580-ms mutation delays. No actual peer-error/backoff event was captured, and the outer/inner timeout race can select a different branch. The duration match alone is not causation.
- Per-group durable operations share a writer gate; all groups share native RocksDB/WAL and finite native lanes. Sync/stall/scheduling delay is possible despite bounded memory. No gate-wait, sync, compaction or stall timing identifies it here. Normal snapshot scanning is outside the group writer gate; describing the entire builder as holding that gate would be incorrect.

## What we had not planned or demonstrated sufficiently

The earlier storage, transport and process tests prove their named safety/ownership/recovery properties. The 60-second c1 pass qualifies that workload only. They did not establish sustained c8 progress across the co-hosted groups and shared native/network resources. Separate admission lanes and finite memory budgets do not by themselves prove timely Raft quorum progress.

We left causal diagnostics too late: the first real concurrent failure had no peer phase/error trace, native/gate timing, or actual Raft term/role transition history. Consequently we can explain why the application returned Unknown, but cannot yet honestly name the first event that caused availability to drop. Treating a low-concurrency pass as general stability would have overstated evidence. The strict soak correctly failed instead of hiding this gap with retries or treating Unknown as success.

## Next focused acceptance

Keep the same workload and defaults. On the next authorized diagnostic run, capture bounded slow/failure events for peer phase/elapsed/error and upstream error class/backoff selection, group/target/kind, gate/lane wait and native I/O duration, plus actual term/role transitions. Record the first departure from healthy progress; avoid keys/values and unlimited successful-request logging. This should distinguish connection failure from native/gate latency and a deadline/backoff amplification path.

Choose the smallest reproducible cause-specific test from that evidence, then fix one cause and repeat the same c8 gate. In particular, a confirmed transient-deadline/backoff path needs a deterministic peer regression proving its error classification and subsequent progress; a confirmed gate/native stall needs a controlled held-I/O progress test. Independently retain the accepted-proposal/leadership-change regression: Unknown, no second proposal, acknowledged-prefix reads correct. Recovery after unavailable/Unknown is not equivalent to proving uninterrupted availability.

Do not declare resolution from a longer timer, a single successful repeat, ignored Unknown outcomes, or mutation replay. We have not changed any default or proposed a source fix before identifying the initiating path.

Bilateral review: directly exchanged evidence with advisor and testci. All agree on the reproduced availability failure and unresolved initiating cause; there is no Judge disagreement. Advisor's `advisor-c8-analysis.md` and testci's `soak-c8-investigation.md` provide complementary architectural and experimental detail.
