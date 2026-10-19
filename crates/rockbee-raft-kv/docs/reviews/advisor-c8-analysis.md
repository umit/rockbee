# Advisor: c8 availability failure

Final checkpoint verdict: **APPROVED for the existing 60-second c8 scenario**, supported by `stable-YFt5NP`; see final section. Earlier failures below remain the investigation record. Native stalls and broader stability are not claimed resolved. Read-only review; no tuning, code change, build or replay performed by this reviewer.

```text
8 client workers -> gateway -> node leader hint -> OpenRaft client_write
                                      -> peer quorum RPC -> durable storage
                  <- Unknown after authority changed / proposal unresolved
```

## What the artifacts establish

The first run, `reports/soak/stable-c0OU7U`, stopped after 50.993 seconds with 119,908 successful operations and three mutation `Unknown` results (two PUT, one DELETE). The first error was OpenRaft `ForwardToLeader(Some(2))`, translated to `Unknown(Unavailable)`. No wrong value was reported. The three node logs contain only startup/shutdown messages; they cannot establish why leadership changed. This was a failed stability run, not a successful capacity measurement.

The instrumented repetition, `reports/soak/stable-4xGY5g`, also failed. Its common-start microsecond timestamps show:

| Time from measurement start | Observation |
|---|---|
| 27.410–27.412 s | Eight operations began across the four data groups |
| 27.464 s | Three GETs failed quorum confirmation for groups 2/3/4; reported acknowledgements were only `{1}` |
| 27.969–27.998 s | Three mutations succeeded after approximately 558–587 ms; two other mutations returned `ForwardToLeader(2)` as Unknown |
| 28.001 s | Node 1's sampled leader hints for groups 2/3 first showed node 2 |

Generation-one snapshot files appeared around 15.003–15.255 s in the second run, with no sampled file change near its failure. Therefore a snapshot publication at the failure instant is not established. File sampling also cannot prove absence of background RocksDB work. The diagnostic run records 74,017 successful operations, five errors, no wrong values, and clean joined-worker/resource shutdown. Local hints are sampled observations, not term or quorum evidence.

Persistent backend leader-hint probes remained responsive during the stalled operations: the 27.5 s and 27.75 s samples generally completed in fractions of a millisecond. This argues against a complete pause of every Tokio task on every node. Those probes bypass private peer connection setup and durable writes, so they do not distinguish a fresh-TCP failure from a storage/Raft progress stall.

## Why Unknown is correct here

`cluster/service.rs::mutate` checks `metrics().current_leader` before calling `client_write`. This check is a routing optimization: leadership can change immediately afterward. The call must remain owned until its actual response; it cannot atomically reserve authority with a cached hint.

After `client_write`, `ForwardToLeader` is not a proof that the write was never proposed. In the exact OpenRaft 0.9.25 source, `core/raft_core.rs`, `Command::DeleteConflictLog` can send that error to log-indexed pending client responders. The observed error alone does not identify whether this particular request took the pre-append rejection branch or the later indexed-proposal branch. Mapping either to a safe rejection or automatically replaying it would exceed the evidence. The existing conservative `Unknown` classification preserves correctness; it does not make the availability failure acceptable.

## Concrete source-level pressure paths

1. **Short peer deadline includes every phase.** Runtime config overrides payload and snapshot chunk sizes but retains OpenRaft defaults: heartbeat 50 ms, election range 150–300 ms. In 0.9.25, read-quorum probes (`core/raft_core.rs::handle_check_is_leader_request`) and replication append calls (`replication/mod.rs`) use the heartbeat interval for their RPC timeout. Our `rpc/network.rs::call` wraps lane acquisition, connection admission, frame work, a fresh `TcpStream::connect`, transmission and reply in the provided hard TTL. The roughly 53 ms read failures are consistent with this deadline path. This identifies sensitivity, not the cause of the delay.
2. **Network lanes are not complete progress isolation.** Control and snapshot RPC admission have separate pools, and snapshot capacity cannot consume every connection slot. Yet all requests still share OS sockets, scheduling, per-group Raft execution, and storage. Every private RPC opens a fresh TCP connection. Connection churn/OS resource pressure is a plausible alternative to disk delay; no connect error or socket-state evidence has yet established it.
3. **One group gate covers durable operations.** `storage/mod.rs::Group::reserve` is used by vote/log/committed writes, state apply and snapshot publication/install. Native closures retain gate ownership for cancellation safety. Separate process-wide lanes (default log 2, state 2, snapshot 1) bound concurrency but do not stop a group log/vote operation waiting behind that group's apply or snapshot work. Log readers also use the state-read lane. Large apply/append iterables retain a session across bounded chunks; memory boundedness is not a wall-clock hold-time bound.
4. **Shared native storage can couple groups.** All groups share one RocksDB DB, WAL and write-buffer manager; writes use synchronous WAL durability. The write-buffer manager enables stalls. Flush, compaction, WAL sync and ordinary OS scheduling may delay otherwise separate lanes. No captured RocksDB stall/sync/compaction timing yet identifies one of these as the trigger.
5. **Snapshot scan is deliberately outside the group gate.** Build reserves its durable sequence, releases the gate for streaming scan/file sync, then reacquires it for descriptor publication. Publication still includes cleanup, and installation retains the gate for its native lifecycle. It would be wrong to describe the entire normal snapshot build as holding the group gate. Neither path has been proven to initiate this failure.

The storage tester and verifier identified one additional source-level amplification candidate: our adapter maps local peer errors, including its deadline error, to OpenRaft `Unreachable`; OpenRaft's default network backoff is 500 ms and replication backs off for that variant. This may amplify a transient failed call beyond the normal election window. The recorded roughly 580 ms waits are compatible with this path, but neither the triggering peer error nor the actual backoff branch was captured; an outer/inner timeout race also matters. Capture the original peer error and resulting OpenRaft error category in the next experiment before changing the mapping.

## What our plan omitted

We planned durability, atomic state/snapshot publication, bounded ownership and uncertain-outcome behavior in detail. We had not yet demonstrated sustained multi-group progress under concurrent ordinary traffic. The c1 pass and storage/transport correctness tests do not establish that c8 will maintain quorum deadlines. In particular, separate admission lanes were not accompanied by measured end-to-end peer and storage wait budgets.

The first run lacked the evidence needed to distinguish a scheduling/disk stall from TCP failure: no peer RPC phase/reason counters, no gate/lane wait or native sync timings, no term/role transition trace. `rpc/server.rs` currently discards individual `serve_connection` errors, and the node executable does not provide useful Raft transition logs in these artifacts. Treating observability as a late diagnostics task made this first failure more expensive to explain. This is a planning gap, not evidence that a particular subsystem is broken.

The stable soak is correctly strict: unexpected Unknown or unavailable reads fail it. We must keep that gate and separately explain that a correct distributed client can still receive Unknown during an availability incident. We should not alter the test to turn these outcomes into successes.

## Next smallest discriminating experiment

Keep the same workload/defaults and add bounded diagnostics to the next authorized run: for failed/slow peer calls, record kind/group/target, phase reached, exact error/errno, and monotonic elapsed; record bounded gate/lane wait and native-write duration events plus actual Raft term/role changes. Fixed dimensions and capped slow-event rows suffice; do not log keys/values or every successful request. Sample relevant socket counts/OS errors and RocksDB stall properties only if available without intrusive work.

The outcome determines the fix: demonstrated connect/resource failure supports connection-lifecycle work; demonstrated gate/native delay supports a focused progress-preservation change; demonstrated scheduler delay supports examining blocking placement. Only measured evidence can justify changing timeouts or lane counts. A longer election timeout alone might hide a storage/transport stall, and replaying Unknown would corrupt the intended certainty contract. Add a regression for the observed cause, then rerun the same c8 gate before declaring the incident resolved.

Sources inspected: current `cluster/service.rs`, `cluster/mod.rs`, `storage/{mod,log,state,snapshot}.rs`, `rpc/{network,server,admission}.rs`; cached official OpenRaft 0.9.25 `src/config/config.rs`, `src/core/raft_core.rs`, `src/replication/mod.rs`; the two named soak artifact directories. No physical power-loss or maximum-throughput claim follows from these runs.

Bilateral discussion: advisor and verifier agree on the observed failure chain and evidence limits. Neither has identified a proven initiating defect. No Judge disagreement is pending.

## Follow-up: independent peer error-classification issue

The user requested an independent Judge review even though reviewers agree; the evidence packet has been sent to `/root/raft_judge`.

Exact pinned-source comparison establishes a narrower issue independently of incident causation:

| OpenRaft 0.9.25 category | Contract and actual replication handling |
|---|---|
| `Timeout` | Carries RPC action, source, target and timeout duration (`error.rs:546`). Replication does not install unreachable backoff for it (`replication/mod.rs:287`). |
| `Network` | Temporary network failure; API documentation permits prompt retry (`error.rs:366`). This replication loop does not immediately reattempt the same log action merely because of this variant; importantly, it does not install unreachable backoff (`replication/mod.rs:304`). |
| `Unreachable` | Explicitly requests delaying retries (`error.rs:386`). Replication installs `network.backoff()` (`replication/mod.rs:288–295`); the default is a constant 500 ms (`network/network.rs:160–162`). |

Our `rpc/network.rs:85–88` produces `PeerError::Deadline` when the whole call expires; `rpc_error` at lines 134–135 unconditionally converts it to `Unreachable`. The deadline can expire during local admission, before connecting to the target at all. This loses a known timeout category and prescribes a target-unreachable retry policy without target-unreachability evidence. Advisor position: this is an independently identifiable error-classification defect, with an effect on replication policy; it is not proof that this branch caused either soak failure. The outer OpenRaft timeout (`replication/mod.rs:455–468`) may instead win and already produce the correct `Timeout`.

If a mapping correction is authorized after the focused regression, preserve `PeerError::Deadline` as `RPCError::Timeout` with the actual RPC kind/source/target/hard TTL. Do not replace every error with `Network`: genuine unreachable endpoints, protocol failures and local size errors require separate deliberate handling. Do not promise instantaneous retry or change OpenRaft timer defaults.

The smallest useful diagnostic record has monotonic start/end, group, target, RPC kind, last phase (`lane`, `connection_admission`, `encode`, `connect`, `write`, `read_header`, `read_body`, `decode`), original error/OS errno, and final OpenRaft category. Count outer cancellation separately: the outer OpenRaft timeout can drop the adapter future before its normal error-return instrumentation runs. A cancellation record alone must not invent a socket error or identify which timeout won. Keep fixed counters plus a capped slow/failure-event ring; no payload/request-ID labels or per-request logging task. Correlate with bounded native gate/lane/sync timings and actual term transitions before attributing the incident to this policy issue.

## Third run: connection phase localized; bounded reuse decision

Read all three node logs, manifest and diagnostic tail for `reports/soak/stable-bBn2R9`. The run failed at 44.761 s with one group-1 GET quorum failure, 113,219 successful operations and no Unknown mutation. Other affected mutations eventually committed after roughly 560 ms. Recorded terms remained 1 and leader remained node 1 through the failure.

Node 1 recorded group-1 calls to both followers timing out in `connect`: phase durations 51.051, 51.358 and 51.647 ms, starting near Unix microsecond 1792438655012xxx. A preceding call to node 3 timed out in `read_header`. Nodes 2/3 recorded accepted connections followed by early EOF around 1792438655063xxx, consistent with callers abandoning timed-out connections. This localizes the observed quorum loss to private transport progress, especially connection establishment; it does not prove ephemeral-port exhaustion, TCP backlog exhaustion, or distinguish kernel handshake delay from delayed polling of connect completion. Connection-refused events later in node 1's log occur during node shutdown and must not be presented as the original cause.

No retained storage event at or above the diagnostic threshold appears at this failure. Each node reports one omitted event, and the collector has finite scope, so this is supporting negative evidence rather than proof that storage did nothing slow. Earlier native-queue delays occurred without this failure. Post-run socket snapshots do not establish transient socket exhaustion during the incident.

Recommended order is two separately reviewable checkpoints:

1. Prove and correct the narrow Deadline-to-Timeout classification independently. It removes the unjustified unreachable-backoff decision; it cannot make a connect that misses the read-quorum deadline succeed.
2. Introduce bounded reuse of **control RPC connections only** as a measured mitigation of an observed vulnerable phase. Keep snapshot transfer fresh and on its separate lane in this first change. Further OS diagnosis is useful, but not a prerequisite to removing repeated handshake work from a path now shown to miss its deadline. Do not call reuse a proven cure before the same c8 gate passes.

Minimal reuse architecture: a small idle pool shared by `PeerNetworkFactory` and all its clients, keyed by validated target endpoint/identity. A cache only inside one `PeerNetwork` misses short-lived read-probe clients. Lease a socket exclusively for one request/reply; do not introduce multiplexing or correlation IDs. Existing process-wide operation bounds still apply. A fully received, decoded and identity/kind-validated response may return the socket to the bounded idle pool. Timeout, cancellation, partial framing, EOF, decoding or identity failure destroys the socket; later bytes can never satisfy a different RPC. There is no hidden same-call retry after a possible send: subsequent OpenRaft calls retain their existing consensus retry semantics.

Hold physical connection permits for each actual socket's lifetime, including idle time. Bound idle sockets per endpoint and globally; when capacity is exhausted, evict an idle socket before waiting for a permit so an idle cache cannot deadlock new work. Never hold the pool mutex across network I/O or a permit await. Server connections become a sequential frame loop with idle timeout and pinned sender/receiver/cluster/control class; every frame still validates group and identity. Once a Raft call is dispatched, client timeout/disconnect cannot cancel that server-owned call or release its resource ownership prematurely. Shutdown drains accepted work and closes idle sockets. Do not allow a reused control connection to switch into the snapshot lane.

Required focused regressions: successive valid RPCs reuse one connection; different groups preserve header identity; canceled/partial reply is never returned to the pool; cancellation during dispatch still drains server work; an idle pool cannot monopolize connection capacity; snapshot traffic cannot consume reserved control admission; shutdown leaves no owned sockets/tasks. Compare the same c8 workload with connection-attempt counts and phase diagnostics retained. Keep timer, durability, public certainty and public no-replay behavior unchanged.

## Logged checkpoint Sc1F8R: narrow Timeout correction approved

`stable-Sc1F8R` still fails: 57.019 s, 159,862 successful operations, six mutation Unknown results, zero read errors/wrong values. The logs demonstrate the read-budget change working: group 4's failed barrier at 19:57:37.187 is followed by a successful fresh barrier at 19:57:37.381 within the same five-second budget. That does not establish complete stability.

Later, inner adapter deadlines are explicitly logged around 19:57:51.756, followed by 171–178 ms synchronous storage waits and group-2/group-4 leadership changes with Unknown outcomes around 19:57:52.282/52.418. These logged inner-deadline return paths are mapped to Unreachable by the current helper. Snapshot activity overlaps, but neither snapshot causation nor a particular underlying disk mechanism is proven.

Timing precision: the 150–300 ms values above are the configured random election component. For an established committed leader, `core/raft_core.rs:1476` adds the leader lease; `engine/engine_config.rs:56` sets that lease to `election_timeout_max` (300 ms). The nominal follower expiry window is therefore 450–600 ms, with tick/scheduling effects and possible smaller-log extension. The default 500 ms Unreachable backoff is still material; it is not correct to compare it only against the random component when describing the full follower expiry path.

Advisor approves the narrow semantic correction: map **only `PeerError::Deadline`** to `RPCError::Timeout(openraft::error::Timeout { action, id, target, timeout })`. Supply the correct `RPCTypes` action, local node ID, target ID, and the original `RPCOption::hard_ttl()` duration, captured before moving the option into the call. Cover append, vote, and install-snapshot; the snapshot implementation currently has its own direct Unreachable conversion and must not be missed. Preserve all other error mappings for this change, including payload splitting and explicit remote failures.

This removes an adapter-imposed unreachable retry policy from a known timeout. It does not shorten native synchronous writes, change election/heartbeat settings, guarantee an immediate upstream retry, or permit public mutation replay. Keep the current c8 workload as the validation target and report any remaining failure honestly. No additional scenario expansion, code edit or build was performed by this advisor.

## Final scoped acceptance: stable-YFt5NP

Advisor independently read the final manifest, all six summary rows, actual retry/storage/peer log entries, current read and error-mapping source, and workspace check artifacts. **Specification and implementation checkpoint approved**; direct concurrence was sent to verifier and Judge. No remaining blocker for this local checkpoint was found.

The existing scenario passed for 60.008801750 measured seconds: one client connection, eight workers, one gateway, three node processes, four exercised data groups, 28,373 complete cycles and exactly `6 × 28,373 = 170,238` successful operations. All six operation rows have the same cycle count; errors, wrong values, Unknowns, partial cycles, censored samples and histogram overflow are zero. The manifest records workers joined before resource close and successful client/gateway/all-three-node shutdown.

This includes direct behavioral evidence for the read-budget policy: node 1 log lines 68–69 show a group-2 qualifying quorum failure after 51,691 µs and a successful fresh second barrier in 474 µs, with about 4.89 seconds of the original budget remaining. The source retries only typed QuorumNotEnough, retains each submitted read until completion, and checks the deadline around the barrier and coherent read. The final peer helper maps only Deadline to Timeout with original action/source/target/hard TTL in all three RPC methods.

`reports/workspace-after-logging.log` totals 149 passed, zero failed and one intentionally ignored manual soak; this manual c8 run is separate successful evidence. `reports/clippy-after-logging.log` and `reports/default-build-after-logging.log` finish successfully. Root reports formatting passed; this advisor did not rerun formatting or builds.

Native synchronous writes around 186–190 ms and peer deadline events remain visible in this passing run. Therefore the supported result is that the corrected transport/read policy completed this workload despite those events. It is not proof of eliminated disk stalls, maximum throughput, long-run stability, physical power-loss durability, or completed movement/TTL work. The broader connection-count matrix remains unexecuted at this checkpoint. No new test scenario was added or run by this advisor.

## Subsequent independent-client matrix: approved

Independently checked `docs/tasks/client-matrix-report.md`, all three manifests and every summary row, client connection log counts, and the fixture's topology/connection construction. The sequential 60-second cases pass: `stable-Mxil47` has 1 client/1 worker and 73,344 operations (`6 × 12,224` cycles); `stable-1OH76d` has 2/2 and 118,320 (`6 × 19,720`); `stable-Q1eLrK` has 3/3 and 129,186 (`6 × 21,531`). All have zero admission/rejection/NotSent/Unknown/read failures/wrong values/censored samples/histogram overflow/partial cycles, exercise all four groups, join workers before close, and record successful shutdown of every client, gateway and all three owned nodes.

Topology is explicitly `N independent public TCP clients -> one gateway inside the test process -> three separate CLI node processes`; it is not N clones of one public connection. `connect_and_warm_clients` calls `RockBeeRaftClient::connect` N times; INFO connected-event counts are exactly 1/2/3 and workers select `clients[id % N]`. Source/report inspection supports **approval of this finite matrix on checkpoint 7d2ff9f**, without a capacity, long-duration or broader-goal completion claim. Advisor ran no tests/builds and changed no product source.
