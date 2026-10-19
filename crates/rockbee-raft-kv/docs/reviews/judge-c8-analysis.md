# Judge: c8 availability incident

**Decision:** c8 is a reproduced availability failure, not a demonstrated data-corruption failure and not yet a diagnosed transport, storage, or snapshot defect. Keep the strict availability gate, `Unknown`, no replay, and all defaults unchanged while collecting one narrow causal trace.

```text
Client (8 workers) -> Gateway -> writer/leader -> private Raft RPC -> SlateDB
                         |             |              |
                         |             |              +-- failed quorum progress
                         |             +-- later local hint moved 1 -> 2
                         +-- GET goes to a reader after its Raft barrier
```

## What is established

- The first c8 run contains two PUT and one DELETE `Unknown`; its summary has no checked wrong-value result ([`stable-c0OU7U/summary.csv:2-7`](../../reports/soak/stable-c0OU7U/summary.csv#L2-L7)). The diagnostic repetition contains three remote read failures and two `Unknown`s, again without a checked wrong value ([`stable-4xGY5g/summary.csv:2-7`](../../reports/soak/stable-4xGY5g/summary.csv#L2-L7)). Neither run qualifies stability or capacity.
- In the diagnostic timeline, two mutations return `ForwardToLeader(2)` at 27.992/27.997 s, after long attempts; node 1 first reports leader 2 for groups 2 and 3 at 28.001 s ([`diagnostics.csv:1715-1721`](../../reports/soak/stable-4xGY5g/diagnostics.csv#L1715-L1721)). A leader hint is neither a term record nor proof of the initial event.
- Snapshot filenames were first observed around 15.003/15.255 s, not at the 27.4--28.0 s failure window ([`diagnostics.csv:932-959`](../../reports/soak/stable-4xGY5g/diagnostics.csv#L932-L959)). This rejects the narrow claim that *observed publication at failure time* caused the incident; it does not exclude background native work.
- A public mutation is deliberately `Unknown` after `client_write()` returns any error: the pre-call hint is only an optimization, and the source explicitly warns that `ForwardToLeader` can name a formerly appended proposal ([`service.rs:221-234`](../../apps/rockbee-raft-node/src/cluster/service.rs#L221-L234)). The protocol contract also states that `Unknown` may have committed and must not be replayed ([`response.rs:11-24`](../../crates/rockbee-raft-protocol/src/kv/response.rs#L11-L24)); client regression coverage already asserts no automatic replay after a missing reply ([`client.rs:135-159`](../../crates/rockbee-raft-client/tests/client.rs#L135-L159)).

The last point makes the classification and no-replay rule correct. It does **not** establish what happened to these two particular mutations: their eventual fate was not reconciled. “No wrong value” covers the completed, checked operations only. The soak retains aggregate metrics and an in-flight step, rather than a final per-key acknowledged state or unresolved intent ([`soak.rs:270-277`](../../apps/rockbee-raft-node/tests/support/soak.rs#L270-L277)); workers stop the dependent cycle on failure ([`soak.rs:389-419`](../../apps/rockbee-raft-node/tests/support/soak.rs#L389-L419)) and the owner closes resources after measurement ([`soak.rs:720-747`](../../apps/rockbee-raft-node/tests/support/soak.rs#L720-L747)).

## Candidates, not findings

- Each peer RPC spends its one hard TTL on lane admission, connection admission, fresh TCP connect, send, and receive ([`network.rs:80-124`](../../apps/rockbee-raft-node/src/rpc/network.rs#L80-L124)). Fresh-TCP churn can therefore be a cause, but no phase, socket errno, or connection count was captured.
- Every local `PeerError`, including the local deadline, is currently represented as `RPCError::Unreachable` ([`network.rs:134-136`](../../apps/rockbee-raft-node/src/rpc/network.rs#L134-L136)). This is a source-established error-classification pressure point; it is not evidence that its upstream backoff branch occurred in c8. An outer OpenRaft timeout/cancellation may win first.
- Durable operations use a per-group gate and finite process-wide lanes, and native work retains both through `spawn_blocking` ([`storage/mod.rs:395-499`](../../apps/rockbee-raft-node/src/storage/mod.rs#L395-L499)). The database also has one shared write-buffer manager and synchronous writes ([`storage/mod.rs:261-328`](../../apps/rockbee-raft-node/src/storage/mod.rs#L261-L328)). This supports a native/gate-delay hypothesis, not its confirmation.
- Snapshot scan releases the group gate before publication; only reservation/publication and installation take it ([`snapshot.rs:99-134`](../../apps/rockbee-raft-node/src/storage/snapshot.rs#L99-L134), [`snapshot.rs:187-203`](../../apps/rockbee-raft-node/src/storage/snapshot.rs#L187-L203)). Do not attribute the full snapshot build to the gate.

## Missing qualification

The plan qualified safety, bounded ownership, and c1 behavior but did not qualify sustained c8 quorum progress with co-hosted groups, fresh private connections, and shared native storage. In particular it omitted: (1) peer phase/error and mapped-error evidence; (2) term/role transition history; (3) gate/lane/native-I/O wait durations; and (4) an explicit reconciliation plan for uncertain mutations. Separate lanes and clean shutdown do not supply these facts. The peer server also discards individual connection errors ([`server.rs:94-106`](../../apps/rockbee-raft-node/src/rpc/server.rs#L94-L106)), so existing node logs cannot fill the gap.

## Smallest discriminating next experiment

Repeat the same c8 workload and defaults once with a nonblocking, bounded **failure/cancellation-only** diagnostic sink:

1. Record outgoing peer group/target/kind, last completed phase (lane, connection permit, connect, write, read), elapsed time, raw `PeerError`/OS errno, and mapped RPC error. A cancellation guard may say only `caller_cancelled`; it cannot identify an outer timeout, leader shutdown, or OpenRaft backoff branch without corresponding upstream trace.
2. Record actual Raft term/role changes and thresholded slow gate wait, lane wait, and native-I/O events. Use fixed-size records and a drop/saturation counter; no synchronous logging, keys/values, or success-latency stream in the critical path.
3. Capture upstream tracing only if a preflight run confirms that the pinned binary emits the required term/RPC/backoff fields. `RUST_LOG` is supplementary: it cannot supply local phase or native-wait measurements and unbounded logs could perturb c8.

Acceptance before a fix is chosen: the trace must identify the first unhealthy event and distinguish a peer phase/error from a storage/gate delay; otherwise the result remains inconclusive. A cause-specific deterministic regression must then reproduce that event, retain `Unknown`/no replay, and demonstrate recovery/progress. Resolution additionally requires the unchanged 60-second c8 gate to finish with no unexpected read failure or `Unknown`; a longer timer, a single green rerun, or a timer/lane tuning change alone fails this standard.

Separately, qualify the observed `Unknown`s without changing the availability result: retain at most one unresolved intent plus the latest acknowledged state for each of the four keys per worker, then after worker join perform bounded, fresh strong GETs before shutdown. For an unresolved initial PUT allow `None -> v1`, update `v1 -> v2`, and delete `v2 -> None`; a later successful strong read must never regress from the newer state to the older state. Old-only/unavailable remains unresolved, foreign data and acknowledged-state mismatch fail safety, and the original c8 result stays **FAIL**. Recovery canaries use separate keys and are new work, never replays.

## Immediate regression scope

No source change is justified as an incident fix today. The one independent, focused regression worth scheduling is characterization of the local-deadline-to-`Unreachable` mapping: induce a controlled delayed peer call, assert the emitted adapter error class and subsequent progress/backoff behavior, and make the outer-timeout race explicit. It must not claim that c8 took that branch. Retain the existing accepted-proposal/leadership-change rule as a safety regression: post-`client_write` `ForwardToLeader` remains `Unknown`, exactly one proposal is observed, and later reads do not manufacture a replay.

Direct discussion: advisor, verifier, and Judge agree that the incident is real and the initiating phase is unproven. The Judge's additional constraint is evidence discipline around cancellation: a dropped local future does not prove which upstream timeout/backoff mechanism fired.
