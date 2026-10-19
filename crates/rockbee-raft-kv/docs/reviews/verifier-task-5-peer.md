# Task 5 peer review checkpoint

Final spec verdict: APPROVED for the Task 5 peer scope. Final quality verdict: APPROVED for that scope. Earlier checkpoint findings below are resolved as recorded in final acceptance; snapshot implementation remains separate.

Reviewed `rpc/{admission,dto,message,network,server}.rs` and the Task 5 report. The previous full-LogId defect is corrected: wire values preserve term, committed leader node ID and index. Bounded collection decode, fixed prelude validation before body allocation, outbound append size preflight, hard RPC deadline, server phase timeout/shutdown, and ownership of dispatched Raft operations are present. The reported 15 tests and Clippy success are implementer evidence; verifier has not rerun Cargo.

P1: `network.rs::call_before_deadline` acquires the shared connection before the snapshot lane; `server.rs::serve_connection` waits for the snapshot lane while holding an accepted connection. With eight active snapshots and 120 queued snapshots, all 128 connection permits can be held while control permits remain free. Votes/heartbeats cannot enter despite the advertised independent lanes. Acquire outbound kind lane before the shared connection. Reject inbound excess snapshot work immediately after the fixed prelude rather than queueing it while holding connection slots; ensure configured active bulk capacity leaves room for control.

Acceptance: hold bulk work at its configured limit, submit more bulk work, and complete a valid control request in each direction before releasing the held bulk work. Test the actual admission order, not merely that two semaphores exist. Root and peer worker have this counterexample. Advisor independently agrees it blocks the advertised isolation contract; no Judge disagreement.

Frozen-fix review: outbound kind-lane-before-connection and inbound immediate bulk rejection are now present. The worker reports 16 tests and strict Clippy passing. The new semaphore-level test supports the admission design but does not itself exercise real socket acquisition order. One accepted-configuration gap remains: `max_snapshot_operations >= max_connections` allows active bulk to consume every connection. Reject that relation (defaults remain unchanged), or provide an equivalent reserved-control scheme. Advisor and verifier independently agree on this precise constructor gate; worker notified.

Follow-up hardening, not a demonstrated legitimate-path blocker: membership DTO conversion silently normalizes duplicate IDs and lets upstream `Membership::new` fill missing voter endpoints. Strict rejection would make malformed private traffic easier to diagnose.

```text
OpenRaft -> bounded private RPC -> remote OpenRaft
            peer scope approved; snapshot storage unfinished
```

## Final acceptance

The frozen source now rejects `max_snapshot_operations >= max_connections`; its constructor regression covers the former one-connection/one-snapshot counterexample. Together with outbound lane-first admission and inbound immediate bulk rejection, this closes the identified bulk-waiter/control-slot gap. Arbitrary incomplete-header floods are still bounded by connection admission and phase timeout; no authentication or universal DoS-proof progress is claimed.

Verifier inspected `reports/runtime-peer-final.log`: 14 library tests, 2 peer tests, 2 CLI tests and 3 runtime tests pass (21 total). `reports/runtime-workspace-clippy.log` finishes successfully. These are root-run command results, independently read; verifier did not rerun Cargo. Static review confirms the fixed source and useful tests, with no remaining actionable blocker in the reviewed scope.

Direct final advisor/verifier discussion reached independent spec and quality approval. No Judge disagreement. Task 4 snapshots, placement changes, physical power loss and resource soak qualification are outside this approval.
