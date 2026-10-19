# Server writer termination follow-up

Spec verdict: APPROVED. Quality verdict: APPROVED for this narrow lifecycle correction. No Cargo/source edits by verifier.

The earlier server loop could keep admitting input after its response writer exited. The frozen `transport/server/runtime.rs` now selects the owned mutable writer JoinHandle before handler joins and the pinned receive future. Writer completion retires intake; dropping an incomplete receive is safe because that entire connection is retiring. Ordinary handler completion still preserves the same receive future. A completion flag prevents awaiting an already completed JoinHandle twice. Accepted handlers retain their permits and are drained after response sender teardown.

The real-socket regression waits for EOF on the exact output half while held-request heartbeats keep input active, then sends a unique late frame. It verifies no later admission and, after releasing already accepted handlers, requires completed count equal admitted count. The private writer checkpoint separately awaits real writer termination under unread output; successful client write_all is not confused with application admission.

Verifier read `reports/workspace-after-writer-fix.log`: 143 tests pass and the manual soak is the sole intentional ignore. Worker additionally reports scoped strict Clippy passing. Advisor and verifier directly agreed on the root cause, exact-EOF regression and minimal JoinHandle fix; no Judge disagreement. No reconnect, mutation replay, cancellation of accepted Raft work, or resource default change is introduced.

```text
writer exits -> stop this connection's intake -> drain accepted handlers
```
