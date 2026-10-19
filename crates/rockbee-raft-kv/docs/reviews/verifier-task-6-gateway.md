# Task 6 gateway/server review checkpoint

Final spec verdict: APPROVED for the Task 6 gateway/server scope. Final quality verdict: APPROVED for that scope. Earlier checkpoint findings below are resolved by the frozen fixes and final evidence.

P1: the generic server originally recreated `receive_request` whenever a handler join won `select!`. A partly consumed TCP header/body was discarded and its remainder became the next frame. Keep the same pinned receive future alive while draining completed handlers. Regression: hold handler A, send a partial B frame, release A, then send the rest of B; B dispatches exactly once with intact binary bytes and correlation. A source correction is now visible; test evidence is pending.

P1: `write_responses` originally checked only the outer timeout error and ignored `Ok(Err(io))` from `write_all`. Retire the writer on either error. The source now checks `Ok(Ok(()))`; regression/evidence is pending. The complete write/flush phase should remain bounded.

P1: gateway `route` returned `ReadError(NotLeader)` immediately instead of trying another group voter. A first configured follower therefore prevents a valid strong GET even when the leader is available. Test a follower first and a leader second returning `Some(empty)`.

P1: `backend_link` returned a cached retired Link forever. A transient failed generation permanently disabled that endpoint. Replace retired generations only for new requests. Never resubmit a handed mutation; a response-loss test must observe Unknown and exactly one backend proposal, followed by a separate request that reconnects successfully. Avoid holding the process-wide link-map mutex across a potentially stalled network connect.

P2: `ServerConfig::validate` rejects unsupported connection/operation counts but does not reject `connection_response_queue > Semaphore::MAX_PERMITS`. Passing that accepted capacity to `mpsc::channel` panics when a connection is accepted. Apply the same supported-counter bound and add a construction-boundary test. Sent directly to the worker.

Positive ownership findings: global work and maximum response bytes are reserved before handler materialization; request decode reserves bytes before allocation; socket loss aborts only the writer and drains accepted handlers with their permits retained. Mutation transport uncertainty is mapped to Unknown, and explicit matching NotLeader rejection is the only existing mutation retry branch. Reconnection fixes must preserve those properties.

Advisor and verifier directly exchanged the framing schedule, routing findings and acceptance cases, and agree on these gates. No Judge disagreement. No Cargo or production edits by verifier; root owns native integration.

Review handoff: source corrections are being implemented concurrently by the assigned worker; this report records the inspected checkpoint, not final approval of those edits. Root will reactivate verifier after the focused fixes freeze.

Frozen-fix review: pinned per-frame receive, correct inner write-error handling, supported queue-count validation, GET NotLeader retry, and per-node generation slots are present. The worker reports server 6/6, gateway 2/2 and strict Clippy passing. The partial-frame test uses scheduler yields rather than an acknowledged internal read phase, so describe it as a regression plus source evidence, not a fully controlled phase proof.

Remaining actionable gateway gate: `route` still returns immediately on mutation `Delivery::NotSent`. If the first sorted voter is stopped, every new PUT ends at that voter and never reaches the live quorum. Continue candidates only for proven NotSent; MayHaveBeenSent must immediately yield Unknown. The two current gateway tests do not cover backend generation replacement or response-loss Unknown/no replay. Add a focused fixture proving stopped-first-voter progress and a handed response-loss mutation observed once, followed by a separate request that reconnects. Advisor and verifier agree; worker and root notified. No Judge disagreement.

```text
Client -> gateway Server -> backend Link -> Raft -> RocksDB
          gateway/server scope approved; snapshots unfinished
```

## Final acceptance

The final source continues mutation routing only for proven `Delivery::NotSent`; MayHaveBeenSent immediately returns Unknown. A Closed cached generation is replaced only for a new unsent request, with per-node generation identity protecting newer links from late old failures. No awaited uncertain mutation is resubmitted. The two added fixtures cover a refused first-voter connection followed by successful mutation routing, and a delayed handed mutation producing Unknown followed by a separate successful GET through a replacement connection. Worker reports all four gateway tests and scoped strict Clippy passing; the server's six-test checkpoint already covers partial-frame preservation, queue validation and disconnect-held ownership.

Precision: the delayed fixture counts one backend handler dispatch, not a real Raft proposal or durable commit. Its later GET returns the fixture's None, which does not imply an Unknown mutation was absent in a real replicated history. Source review proves the immediate uncertainty branch; the actual OpenRaft public path has separate root-run evidence.

Verifier independently read `reports/runtime-peer-final.log` (21 passing checks including real public Client/Gateway/Raft integration), `reports/runtime-workspace-clippy.log` (success), and `reports/three-process-recovery.log` (one passing real-process recovery test). The last test runs three independent node OS processes and an in-test gateway, acknowledges a public PUT, SIGKILLs all nodes, reopens the same disks, checks the value, deletes it and verifies None, then requires SIGTERM exit success. This is process-crash/reopen evidence, not physical power-loss proof. Verifier ran no Cargo.

Direct final advisor/verifier exchange reached independent spec and quality approval. No Judge disagreement. Snapshot transfer, fourth-node membership and full bounded-soak qualification remain outside this scope.
