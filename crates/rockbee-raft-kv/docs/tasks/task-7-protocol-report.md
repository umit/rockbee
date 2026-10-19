# Task 7 protocol DTO report

## Implemented boundary

`rockbee-raft-protocol::control` now provides the engine-independent values for
the restricted first movement milestone:

- `MoveJobId` is exactly sixteen bytes and rejects the all-zero sentinel.
- `MoveReplica` accepts one replacement request and derives `MoveIntent` from
  a validated directory. It never accepts an arbitrary voter list.
- `MoveIntent` stores canonically sorted `[NodeId; 3]` source and target
  placements. Decoding verifies that the target is exactly the source with one
  removed voter replaced by the requested endpoint node.
- `MoveRecord` stores only `Pending` or `Completed`. Completion carries the
  complete `(term, leader_node_id, index)` identity observed by trusted runtime
  reconciliation.
- `AdminOperation` rejects metadata group zero and groups over 256 at the
  decoding boundary. `AdminResult` rejects diagnostics over the existing
  protocol message limit.

The values use private wire forms and `TryFrom` validation during serde. A
directly constructed `MoveIntent`, `MoveRecord`, or `AdminResult` should still
be validated before it becomes durable; an operator request derives its intent
against the current directory. Decoding performs its applicable validation.

## Metadata-apply responsibility

This is a DTO layer only. Future group-zero metadata application owns durable
acceptance, same-job idempotency, permanent rejection of a later target, and
the prospective catalogue check. That check must count the union of published
nodes and all stored jobs. Each conditional transition applies in its own
durable metadata chunk. The protocol values do not claim to fence a later
target or prove that a supplied log position was committed.

## Evidence

```text
CARGO_TARGET_DIR=target/control-task cargo test -p rockbee-raft-protocol --test control --locked --offline
6 passed; 0 failed

CARGO_TARGET_DIR=target/control-task cargo clippy -p rockbee-raft-protocol --all-targets --locked --offline -- -D warnings
finished successfully
```

The focused tests cover all-zero IDs, malformed operator groups, existing-node
targets, exact ABC-to-ABD derivation, malformed persisted target arrays,
oversized result diagnostics, and full committed-log-position framing.

The verifier found two initially red regressions: a `Completed` result could
contain `Pending` progress, and a decoded request could name the removed node
as its replacement. The contradictory result fails during decode and direct
`AdminResult::validate()`; the malformed request fails during decode and
`MoveReplica::derive_intent()`. The focused suite is now 8 passing tests.

## Unfinished

No metadata command, RocksDB persistence, OpenRaft membership call, learner
catch-up proof, gateway refresh, process test, or live ABC-to-ABD movement is
implemented by this protocol-only task. General repeated movement remains out
of scope.
