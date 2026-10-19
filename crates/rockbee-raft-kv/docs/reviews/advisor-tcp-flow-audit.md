# TCP flow audit: current c8 checkpoint

Final checkpoint: **approved for the existing 60-second c8 scenario**, after the subsequent read-budget and Deadline-to-Timeout fixes; `stable-YFt5NP` passed with 170,238 successful operations and zero errors. See `advisor-c8-analysis.md` final acceptance for inspected evidence and limitations. The findings below preserve the intermediate review: reuse alone failed in `stable-b6lFue`. No tests, builds, timer changes or production edits by advisor; line anchors describe the reviewed source checkpoints.

```text
Client --persistent multiplexed Link--> Gateway
       Gateway --per-node persistent BackendLink--> Node service
       Node --exclusive reused control TCP / fresh snapshot TCP--> peer Node
```

## Immediate decision: read deadline versus one Raft probe

**P1, confirmed source behavior:** `cluster/service.rs:250` calls `ensure_linearizable()` once, without accepting the request deadline. A typed `QuorumNotEnough` becomes `Unavailable` immediately. Gateway `server/mod.rs:249` retries remote reads only for `NotLeader`, so one failed internal 50 ms quorum probe terminates a public read even when its original request budget has time remaining.

`stable-b6lFue/diagnostics.csv:2378` onward records four group-2 GET failures at about 38.368 s. At the matching absolute time, all three node logs show roughly 97 ms synchronous RocksDB writes starting near 1792439297854xxx; node 1 records 51–52 ms peer `read_header` timeouts. Reusing connections did not remove this source of delayed replies. This is stronger evidence for storage-related peer progress delay than the earlier connect-only checkpoint, but does not identify why the OS/native write took 97 ms.

Recommended policy for Judge review: pass the **same original absolute deadline** into `strong_read`, including directory/bootstrap reads. Retry only `RaftError::APIError(CheckIsLeaderError::QuorumNotEnough(_))`, sequentially, obtaining a fresh barrier each time. Return `ForwardToLeader` promptly as `NotLeader`; do not retry fatal/storage/coherence failures. Pace retries using the already configured heartbeat interval, capped by remaining budget, rather than spinning or launching parallel probes. Exact available API in OpenRaft 0.9.25: `Raft::config(&self) -> &Arc<Config>` (`src/raft/mod.rs:341`); use `Duration::from_millis(group.raft.config().heartbeat_interval)`. This does not change OpenRaft configuration.

Check deadline before each attempt, after a barrier completes, and after the coherent local read. Never read/return a value after a failed barrier. Retain the accepted handler and permits while each actual Raft future resolves; wrapping it in an eager timeout and freeing the handler can leave internal work running beyond the accounting boundary. `ensure_linearizable` may wait for local apply with no internal timeout (`raft/mod.rs:577`), so its completion can outlast the caller: return deadline failure afterward while preserving ownership, rather than claiming a hard server cancellation bound. The client retains its own deadline and can stop observation. Subsequent conditional/mutation work must never be retried under this read-only rule. Keep failed-attempt logs so a successful later barrier does not erase the underlying latency problem.

## Public and backend boundaries

| Boundary | Source-backed behavior |
|---|---|
| Public client | `client/src/api/client.rs`: PUT/DELETE submit eagerly; GET starts on poll. One Link per connected client, shared by clones. Dropping a handle discards observation, not work. |
| Link admission/handoff | `protocol/src/transport/mod.rs:134`, `workers.rs:61`: request/count/byte reservations precede queue insertion. Monotonic IDs never wrap/reuse. One locked handoff competes with expiry before first socket-write poll. Pre-handoff is NotSent; post-handoff may be uncertain. |
| Link framing and retirement | `workers.rs:149` reads exact 4-byte length, validates cap, reserves encoded/decoded capacity before allocation, then reads exact payload. Partial read/write cancellation only occurs on generation retirement; the stream is discarded. Response version, ID, phase and expected kind are checked before deadline-sensitive delivery (`workers.rs:184`). Unknown/late response retires rather than growing tombstones. |
| Gateway/backend | `gateway/src/server/mod.rs:147,177,223,298`: per-node slot serializes connection replacement; unrelated node slots do not share a dialing lock. Closed-before-admission allows a **new** request on a replacement generation. NotSent and explicit pre-consensus NotLeader can try another candidate; handed mutation failures return Unknown immediately. No uncertain mutation replay. |
| Accepted server work | `protocol/src/transport/server/runtime.rs:155,258`: connection permits are acquired at accept; request bytes, operation and maximum-response capacity precede handler materialization. The pinned receive future survives completed-handler notifications. Writer completion now stops intake. Socket retirement aborts the network writer but joins accepted handlers, preserving Raft ownership. |
| Shutdown | `transport/mod.rs:190`, server `runtime.rs:84`: explicit close retains join handles while awaiting, so canceled close can be resumed. Gateway closes owned backend generations; node stops admissions and Raft work, then joins data/peer trees. |

## Framing findings

The 4-byte length prefix is compatible with TCP fragmentation/coalescing: `read_exact` consumes exactly the requested bytes, without assuming a TCP packet equals an application frame. Public/backend `codec/mod.rs:64` checks exact frame length and rejects trailing postcard bytes. Encoding uses a capped counting pass and a fixed-size buffer (`codec/mod.rs:20`). These mechanisms do not explain a timeout before connection establishment.

Private transport has a 42-byte fixed routing prelude (`rpc/message.rs:42`) inside its capped length. `network.rs::read_peer_header/read_peer_body` reads exact prelude/body lengths. DTO sequence visitors cap declared counts before allocation (`rpc/dto.rs:38`); append fitting is checked before DTO payload copies. Encoded DTO, postcard body and final frame coexist during private encoding, so physical/operation admission must precede encoding and account for that overlap. This is bounded work, not zero-copy.

**P2, confirmed strict-validation gap, deferred until c8 fix:** private `rpc/message.rs:287` uses `postcard::from_bytes`. In pinned postcard 1.1.3 `src/de/mod.rs:12`, that function accepts a valid object while ignoring unused suffix bytes. Unlike the public codec, the private body therefore does not reject a trailing suffix inside its declared frame. This does not itself desynchronize TCP: the whole declared body has already been consumed. It is not evidence for the observed deadline failures. A later narrow change can require `take_from_bytes` plus empty remainder; no new scenario or change requested during the current failure fix.

Private response correlation relies on one exclusive request/reply per socket, with exact cluster/source/target/group/kind checks (`rpc/message.rs:92`). It has no request ID. Reuse therefore depends on never returning an incompletely consumed/canceled stream to idle. A fully valid current reply plus arbitrary later unsolicited same-kind replies is not authenticated away by these headers; peers are trusted cluster software and the protocol does not claim Byzantine protection. Idle surplus/EOF detection is useful, not a proof against a malicious timed sender.

## Peer reuse findings sent directly to implementer/verifier

**P1 integration requirements, found in incremental code:** control idle sockets can retain all outgoing physical permits while a fresh snapshot waits forever unless snapshot acquisition also evicts idle control sockets and observes return notifications. Incoming idle control sockets need a physical class reservation (`max_connections - max_snapshot_operations`) held for socket lifetime, so they cannot consume snapshot capacity for the full idle timeout. The first validated frame must pin control-versus-snapshot class. The idle timestamp must refresh on return, and return must wake waiters even though the socket still owns its physical permit. These were sent to the worker; verifier subsequently reports fixes present. Final frozen code acceptance remains separate.

The checkout notification must register before inspecting the pool to avoid a lost wake. No pool mutex may span network I/O. The leased socket and its physical permit stay owned by the call; every cancellation/error drops them. Only a fully read, decoded, validated reply may return the stream. Snapshots remain fresh and never borrow a control socket. Encoding before physical admission was separately flagged by root/verifier and must remain charged at the corrected boundary.

**P1 independent error policy issue:** Deadline is still mapped through the generic `Unreachable` helper at the inspected checkpoint. Exact upstream semantics and the narrow Deadline-to-Timeout recommendation are recorded in `advisor-c8-analysis.md`. Fixing this can remove unjustified 500 ms backoff; it cannot make one 50 ms read probe survive a measured 97 ms peer storage delay.

No confirmed public/backend framing defect or automatic uncertain-write replay was found in this finite review. This is not an exhaustive proof. The immediate decision is bounded fresh-barrier retry plus the independent error classification correction, reviewed by Judge before implementation; existing c8 remains the validation target. Additional fault/scenario expansion stays deferred per user instruction.
