# c8 soak investigation: reproduced quorum failures and leadership change

Status: failure reproduced once with approved bounded test-only diagnostics; **underlying trigger remains unresolved**. No production behavior, election/heartbeat defaults, mutation certainty classification or retry policy changed. Native/source ownership is returned to root. No further run was performed.

```text
shared Client (8 workers) -> Gateway -> 3 CLI nodes -> 4 Raft groups -> RocksDB
                               diagnostic backend Links -> local leader hints
                               owned snapshot directories -> file observations
```

## Original evidence

`reports/soak-60s-c8.log`, artifact `reports/soak/stable-c0OU7U/`:

- Configured 60 seconds, stopped after actual 50.993009584 seconds when validation failed.
- 119,908 successful operations; 19,984 complete cycles; three partial cycles.
- Two PUT and one DELETE returned `Unknown(Unavailable, ForwardToLeader(Some(2)))`. No wrong-value observations, censored attempts or local admission errors. All resources shut down cleanly.
- Maximum completed mutation latencies were approximately 576–606 ms. These are HDR maxima, not a timestamped failure timeline.
- Group completed-cycle counts `[4999,4999,4995,4991]` lie near 15,000 mutations per group. The default snapshot interval is 5,000 applied entries, so a snapshot-boundary hypothesis was reasonable to investigate but not established.
- Each preserved node log contains only the ready/stopped messages (75 bytes), with no leadership, storage-duration or RPC-phase history. The original run cannot retrospectively identify its trigger.

## Single approved diagnostic reproduction

`reports/soak-60s-c8-diagnostic.log`, artifact `reports/soak/stable-4xGY5g/`:

- Same 60-second cap, c8, 1,024-byte payload, four groups, seed 20261019, debug/aarch64/macOS. Stopped after actual **28.001665459 seconds** due to validation failure.
- **74,017 successful operations**, 12,333 complete cycles, five partial cycles. Failures: **three remote quorum-read failures and two Unknown mutations**. No wrong values or censored operations.
- Diagnostic opt-in `ROCKBEE_SOAK_DIAGNOSTICS=1` adds three owned backend Links. Every 250 ms it requests local leader hints for groups 0–4 from all three nodes, with a 200-ms request deadline. These hints are neither term observations nor quorum authority.
- Snapshot file-name changes are observed at the same cadence using an awaited blocking directory-read job. This does not timestamp exact snapshot publication.
- Only failed or >=100-ms operations add operation records. History is capped at 8,192 rows; this run recorded **1,722 rows and omitted zero**. All observer tasks were joined and their Links closed before application resources closed.
- `diagnostics.csv` timestamps are microseconds since the common measurement start. Diagnostic traffic perturbs the workload, so this run is not a clean throughput baseline.

| Time since measurement start | Observed event |
|---|---|
| 15.003–15.255 s | First snapshot image files observed across all four groups and all three nodes. No later snapshot file-name changes observed. |
| 27.410464–27.411611 s | All eight workers start their final in-flight operation. |
| 27.464274–27.464286 s | Three reads fail after approximately 52.7 ms: groups 2, 3 and 4 report insufficient quorum, with acknowledgements only from node 1 (`got: {1}`). |
| 27.501–27.503 s | All 15 leader-hint probes respond in 105–250 microseconds, still reporting leader 1. |
| 27.751–27.758 s | All 15 probes respond in 327–787 microseconds, still reporting leader 1. |
| 27.968978–27.969009 s | Two PUTs in groups 4 and 1 complete successfully after about 557.5 ms. |
| 27.992279 s | Group 2 update returns Unknown with ForwardToLeader(2), after 581.811 ms. |
| 27.996953 s | Group 3 delete returns Unknown with ForwardToLeader(2), after 585.425 ms. |
| 27.997714 s | Another group 3 update completes successfully after 587.250 ms. |
| 28.001054–28.001385 s | Node 1's group 2 and group 3 hints change from leader 1 to leader 2. Sampling stops as worker drain finishes. |

All client/gateway/node shutdown entries are `ok`. The first error printed in the aggregate manifest is selected by worker order, not time order; **the diagnostic operation timestamps establish that the quorum reads failed first**.

## What the evidence supports

1. The workload reproducibly exposes loss of timely quorum responses, then leadership changes affecting pending mutations. Unknown remains the correct conservative client result. It is not evidence that an acknowledged write was lost.
2. Node 1's service checked its observed leader before `client_write`; errors after invoking `client_write` become Unknown in `cluster/service.rs::mutate`. Gateway routing returns Unknown without replay. Upstream OpenRaft also sends ForwardToLeader to waiting client responders when conflicting log suffixes are deleted (`RaftCore::run_command`, `DeleteConflictLog`). The error text alone cannot distinguish that path from a role change before the queued request was processed.
3. Production OpenRaft defaults are heartbeat 50 ms and randomized election timeout 150–300 ms. `EngineConfig::new` adds a 300-ms leader lease to the committed-vote follower timeout, yielding roughly 450–600 ms; a greater-log observation can add another 600 ms. Replication and read-quorum heartbeats use 50-ms timeouts. These code timings are consistent with the observed delay but do not identify why timely quorum responses stopped.
4. The diagnostic failure occurred around 9,250 mutations per group, about 13 seconds after observed first snapshots and before the next expected 10,000-entry snapshot boundary. This **weakens snapshot-boundary coincidence as the cause**. It does not rule out general storage stalls, compaction or earlier snapshot side effects.
5. Persistent diagnostic backend connections stayed responsive while mutations waited. This argues against one continuous whole-process/whole-host pause covering the entire ~580-ms interval. It does not rule out shorter scheduling pauses, Raft-core stalls, storage waits or failures specific to new private peer connections.

## Remaining hypotheses and next evidence needed

- **Private peer connection failure or timeout:** `rpc/network.rs` opens a fresh TCP connection for every RPC. Unlike the diagnostic Links, these connections have no reuse. TCP connect/ephemeral-port/TIME_WAIT pressure is a plausible hypothesis, not a finding. Capture exact connect errors and phase durations before choosing pooling or another transport change. An unprivileged host sysctl inspection was blocked by the sandbox; no host port-range or TIME_WAIT claim is made.
- **Durable storage or admission stall:** each group serializes its writes; all groups share two log and two state native-I/O slots. Native writes use synchronous WAL durability and are awaited by the Raft core. Collect durations for group-gate wait, native-lane wait, RocksDB write/sync, and snapshot publication before changing limits or durability. No phase durations were captured in this run.
- **Scheduling/host contention:** no owned-PID CPU, scheduler or disk telemetry was captured. The responsive hint probes narrow this possibility but do not eliminate it.
- **Timeout/backoff interaction:** the adapter maps local peer errors, including its deadline error, to `RPCError::Unreachable`. OpenRaft replication enters backoff for Unreachable, and its default `RaftNetwork::backoff` is a repeated 500-ms delay. That interval is relevant to the observed ~580-ms recovery and 450–600-ms election window, but this run did not capture which concrete peer error, if any, started backoff. The outer and inner RPC deadlines can also race. Capture exact error/phase evidence before changing classification, backoff or timers.

A next diagnostic should correlate monotonic timestamps for private connect/write/response phases, concrete I/O errors, Raft role/term transitions, and storage wait/write durations. Keep it bounded and explicitly owned. No further run or production instrumentation is authorized by this report; root/advisor/verifier/Judge should select the smallest discriminating experiment.

## Planning gap and validation limits

The prior storage suite, RPC framing/ownership tests and c1 soak established specific correctness and cleanup properties. They did not establish sustained c8 liveness across five co-hosted Raft groups, native-I/O contention and repeated fresh peer connections. The c1 run also used fewer total mutation entries. Claiming general stability from that checkpoint would overstate its coverage. The original CLI's lack of term/RPC/storage phase history made the first failure harder to diagnose; the appropriate response is to gather causal evidence, not weaken the soak, replay Unknown mutations, or increase timers to obtain a green result.

Test-only changes: `tests/support/soak/diagnostics.rs` plus opt-in hooks in `tests/support/soak.rs`. Seven focused helpers pass (`reports/soak-diagnostics-tests.log`); strict scoped Clippy passes (`reports/soak-diagnostics-clippy.log`). The diagnostic process run intentionally fails stable validation and preserves all evidence. Advisor/verifier/Judge review is pending; no root-cause fix is claimed.
