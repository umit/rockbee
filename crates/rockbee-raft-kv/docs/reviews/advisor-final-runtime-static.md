# Final runtime static review checkpoint

## Final follow-up: peer/gateway scope approved

The two gates recorded below are now fixed in the reviewed source. Peer limits reject snapshot concurrency greater than or equal to connection capacity, with the 1/1 boundary regression. Gateway advances to the next voter only on proven `Delivery::NotSent`; `MayHaveBeenSent` still returns Unknown immediately. The new gateway regressions cover a dead first candidate and a timed-out mutation followed by a distinct GET using a replacement connection. The delayed fixture counts one backend handler invocation; it does not itself count real Raft proposals or establish durability.

Advisor final vote: **specification approved / code quality approved for the peer and gateway checkpoint**. The exact source changes and acceptance position were exchanged directly with verifier. No remaining counterexample or disagreement was found in this scope. `reports/runtime-peer-final.log` was inspected: 21 tests pass across node library, CLI, peer, and real runtime targets. `reports/runtime-workspace-clippy.log` records successful completion; root reports the all-target strict invocation. Gateway's four-test execution result is worker-reported, with the regression source inspected here; no new tests were run by advisor.

Verifier independently returned specification approved / code quality approved for the same scope after direct discussion. No Judge was needed. The subsequently inspected `reports/three-process-recovery.log` also records one passing real-process recovery test; root describes three node binaries, all-node SIGKILL, same-DB reopen, preserved successful PUT, DELETE/GET absence, and clean SIGTERM. Gateway runs inside the test process. This is process-crash recovery evidence, not physical power-loss validation.

Snapshot implementation/recovery, replica movement and fencing, broader fault schedules, sustained resource measurements, and physical power-loss claims remain outside this approval. Earlier findings below are retained as review history, not current unresolved blockers.

Read-only inspection of the frozen peer, generic server, gateway, node bootstrap and new CLI. No Cargo/build commands were run by this reviewer. `reports/public-path-and-cli.log` records two CLI parser tests and three runtime tests passing, including real public Client → Gateway → Raft PUT/GET/DELETE. This is useful integration evidence; it does not cover every failure interleaving below.

```text
CLI -> NodeRuntime -> OpenRaft groups -> RocksDB
Client -> Gateway -> NodeService -----> client_write / read barrier
                     PeerServer -----> native OpenRaft RPC methods
```

## Accepted changes

- Full LogId identity survives peer DTO conversion. Outbound append preflight happens before large payload copies; a zero-fitting-prefix request no longer receives an ineffective hint of one.
- Outbound peer work takes its classified lane before common connection capacity. Inbound saturated snapshot work rejects immediately after the fixed prelude instead of queueing while holding connection capacity. Pre-dispatch waits terminate on timeout/shutdown; accepted consensus work drains.
- The receiving server keeps one pinned frame receive future while completed handlers are joined. This fixes partial-frame loss. Any `write_all` I/O error or timeout stops the response writer. Handler/request/response reservations retain their intended lifetime after disconnect.
- GET now tries another voter after explicit NotLeader. Backend connection establishment is serialized per node under the request deadline, without holding the global link-map lock. A closed generation can be replaced for a newly unadmitted request; an already handed request is not replayed. Generation comparison prevents an old observer from replacing a newer published connection.
- The executable is now a thin lifecycle: parse validated explicit open/create intent, install signal handlers, start, optionally initialize/bootstrap, await signal, and close. The common cleanup path runs after bootstrap or signal failure. No second Raft algorithm was introduced.
- Bootstrap remains within the agreed immutable common-configuration contract: actual linearizable reads, no caller directory payload, existing-value conflict rejection, and post-submission uncertainty. Task-7 fencing is still separate unfinished work.

## Two remaining concrete gates

1. **Peer configuration must preserve control headroom.** `PeerLimits` still accepts `max_connections = 1` with `max_snapshot_operations = 1`. One active snapshot then holds all shared connection capacity, despite the new correct lane ordering. Require `max_snapshot_operations < max_connections` (defaults already satisfy it), and add a rejection/boundary test. Advisor and verifier agree this is a completion blocker for the advertised control/bulk isolation. Hostile clients holding incomplete preludes are a separate bounded-timeout limitation, not solved by this relation.
2. **Gateway must advance past a proven-unsent unavailable candidate.** Its mutation branch still returns immediately on any transport failure. If the first sorted voter is down, every fresh mutation tries that node and ends Rejected(NotSent), never reaching the surviving quorum. Continue to the next candidate only for `Delivery::NotSent`, within the same deadline. Keep `MayHaveBeenSent` → immediate Unknown with no replay. Test a stopped first candidate with a later accepting node, and separately response loss → Unknown → a distinct new operation reconnects while the old mutation submission count stays one. The two current gateway fixtures do not cover the latter lifecycle.

The partial-frame regression checks the right byte interleaving, but its repeated `yield_now` scheduling is not an acknowledged reader-phase barrier. The pinned-future source fix is sound; avoid calling the existing scheduling fixture fully deterministic without a phase hook. Likewise the peer isolation unit test exercises semaphores rather than actual network dispatch ordering; later bulk-transfer integration remains required.

These gates and acceptance positions were discussed directly with verifier and sent to root. No substantive disagreement currently requires a Judge. Approval of CLI/bootstrap static structure is not approval of unfinished snapshot/movement/resource-soak tasks.
