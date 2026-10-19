# Independent verifier: initial acceptance review

Reviewed 2026-10-19. Scope: design, source placeholders, and read-only Cargo
metadata after relocation. No builds, production changes, services, commits, or
runtime correctness claims. `docs/implementation-plan.md` was not yet present.

```text
Existing: Client -> Gateway -> Writer/Readers -> SlateDB -> Object storage
Planned:  Client -> Gateway -> 4 data Raft groups -> RocksDB on A/B/C
                          metadata group -> placement controller -> D learner
```

## Verified boundary and current status

- `cargo metadata --no-deps --format-version 1` succeeds from both workspaces.
  Parent: 12 legacy packages and parent `target/`. Nested: exactly four
  `rockbee-raft-*` packages and module-local `target/`.
- Nested package dependencies are protocol-only; neither existing Rockbee
  packages nor OpenRaft/RocksDB are dependencies yet. Source libraries contain
  documentation only; gateway/node entry points explicitly return failure.
- This verifies scaffolding, not Raft, networking, persistence, resource bounds,
  or a working database. Earlier compilation records were not rerun here.
- Relocation follow-up: `design.md` and `workspace-plan.md` still describe the
  module as repository-root `rockbee-raft-kv/`; actual location is
  `crates/rockbee-raft-kv/`.

## Highest-value acceptance tests to require

| Priority | Failure or adversarial schedule | Required observable result |
| --- | --- | --- |
| P0 | Hold stable-log persistence callbacks on a quorum, or hold leader durable apply, while mutation runs | No success before both durable quorum and durable leader apply; release barriers and then success becomes possible |
| P0 | Acknowledge PUT and DELETE, kill current leader, elect another, restart failed node | A strong GET preserves acknowledged value/deletion or a later mutation; no deleted value resurrection |
| P0 | Restart a node immediately after persisting a granted vote; request a conflicting vote in the same term | Persistent voting state prevents a second vote; log append/conflict truncation also remains coherent across restart |
| P0 | Commit mutation, drop its response at node/gateway/public connection independently | Client observes Unknown; proposal observer proves no automatic replay, including routing refresh and reconnect |
| P0 | Cancel/expire before transport handoff, during partial write, and after backend submission | Only proven unsent is NotSent; only explicit pre-proposal rejection is Rejected; other missing outcomes are Unknown; dropped completion handle does not undo submitted work |
| P0 | Isolate old leader, complete newer mutation on majority, direct GET to old leader | Old leader cannot return an unauthorized successful strong read; new leader read includes prior completed mutation |
| P0 | Stall a group's quorum; repeatedly submit then time out/disconnect | Pending proposals, owned tasks, and retained bytes stay bounded; already-submitted work keeps its reservation until underlying processing finishes; other groups and Raft control retain capacity |
| P0 | Crash snapshot install before/after file sync, publication rename/directory sync, active pointer publication, and old-generation cleanup | Recovery exposes a complete old or complete new generation with matching applied log identity and membership; no mixed state or missing committed tail |
| P0 | Run strong GET concurrently with snapshot installation and generation cleanup | GET pins one coherent generation/applied identity/data view and checks it satisfies its read barrier; cleanup cannot invalidate a live read |
| P0 | Interrupt learner catch-up and membership/controller actions before and after committed `ABC -> ABD`, including response loss | Controller inspects durable job state and actual membership, resumes safely, and never treats timeout as no change; C cleanup occurs only after final membership/removal are durably known |
| P1 | Snapshot larger than configured memory allowance; slow/truncate/corrupt/reorder transfer; restart receiver | File streaming uses bounded chunks/queues and validates identity, length, order, and integrity; incomplete files stay invisible; abort releases resources |
| P1 | Oversize/malformed frame, slow input, slow output, duplicate response, reused request ID across connections | Limits apply before allocation, reservations cover encoded bytes/output, correlation remains connection-scoped, and pending maps drain on disconnect |
| P1 | Remove/recover one group while shared RocksDB holds others; use identical byte keys in every group | Snapshot, log purge, range cleanup, membership, and data changes remain group-isolated; healthy groups progress without global data serialization |
| P1 | Start gateway with stale persisted metadata and no metadata quorum; lose metadata after valid startup | Startup fails closed; known data groups may continue only through their own Raft authority; discovery/moves pause |
| P1 | Wrong cluster/group/node ID or stale incarnation on peer/snapshot/public routing paths | Identity mismatch is rejected before storage or authority changes; a stale map cannot authorize a removed replica |

Use one real gateway, public client, three independently owned node processes,
four data groups plus metadata, and then D for movement. Exercise PUT, GET,
DELETE and both success/Unknown histories through that complete path. Record
invocation/response intervals and verify per-shard single-key linearizability;
Unknown mutations may have happened and cannot simply be discarded from the
history. Process-kill tests alone do not establish device power-loss durability.

## Questions the implementation plan must settle

1. Name exact storage flush/callback boundaries, startup validation, log
   conflict/truncation/purge order, and data/applied/membership atomic batches.
2. Define group/generation-prefixed key encoding, including arbitrary byte keys,
   and how read snapshots pin active state while old generations are reclaimed.
3. Make bootstrap explicit and one-time; refuse conflicting cluster/config
   identity and never reinitialize a group merely because a peer is unavailable.
4. Specify numerical limits at client, gateway, node, replication, and snapshot
   layers, with reservation ownership after deadlines and cancellation. Shared
   block caches are not a process RSS cap.
5. Give placement jobs durable reconciliation states around external Raft
   membership effects; publication of metadata cannot substitute for committed
   data-group membership. Placement and leader transfer remain distinct.
6. Identify minimal milestones honestly: static four-group gateway path is a
   useful intermediate gate; it does not complete fourth-node membership,
   crash/snapshot, overload, or first-version acceptance.

## Advisor dialogue

The advisor and verifier agreed that generation publication requires a coherent
read view, durable snapshot publication, and deferred old-generation reclamation.
The advisor identified the OpenRaft pending-work bound as a specific risk: caller
timeouts must not release node admission permits while accepted Raft work is
still outstanding. The verifier requested an explicit stalled-quorum storm test
and separate byte/replication accounting. Version-specific API assertions remain
the advisor's sourced responsibility; this report makes no runtime verification
claim from them.

## Implementation-plan review

Reviewed the now-present `implementation-plan.md` and `progress.md` on
2026-10-19. The stale root-location descriptions are corrected. Root is adding
explicit retained-proposal reservations and generation/read-barrier coherence.

**Spec verdict: Task 1 approved. Quality verdict: Task 1 approved to implement.**
There is no actionable protocol blocker. The plan distinguishes an independent
workspace, mandatory gateway/Multi-Raft path, durable ACK, Unknown outcomes,
linearizable reads, streaming snapshots, and fourth-node movement. Approval is
of these contracts and test sequencing; all runtime acceptance remains pending.

Task 1 review checks to carry into its implementation:

- Test malicious postcard inner sequence/byte lengths inside a small legal outer
  frame, including enormous declared lengths and truncated payloads. Outer frame
  limits alone do not prove bounded decoder allocation.
- Exercise invalid identities through deserialization, not only typed
  constructors. Test exact key/value/frame maxima and one-byte excess, zero
  shard count, metadata group exclusion, and fixed seed/hash vectors.
- Interpret mapping stability as unchanged keys under the same hash/seed/shard
  count while node membership changes; changing the shard count is prohibited by
  the immutable bootstrap contract.
- Keep response correlation mismatch validation at the pending-request owner;
  generic syntactic frame decode cannot know the expected request ID or kind.

Concrete later-task acceptance additions:

1. Task 3: explicitly persist vote, crash/reopen, and reject a conflicting
   same-term vote. The phrase vote recovery should include this failure schedule.
2. Tasks 2/5: caller deadline only detaches its waiter. Retain count/byte
   reservations until underlying accepted work ends; a dropped Raft responder or
   Fatal result must close group admission and finish owned shutdown rather than
   permit another wave of unbounded proposals. Exercise stalled-quorum storms.
3. Task 4: state larger than an I/O chunk is insufficient evidence against a
   hidden complete-state buffer. Transfer data larger than the configured
   streaming-memory allowance and instrument retained chunks/queued bytes;
   document engine memory separately.
4. Tasks 4/5: capture generation/applied identity/KV in one read snapshot and
   verify its applied identity satisfies the barrier. Concurrent install/GET/GC
   must be a focused regression schedule.
5. Tasks 7/8: after final movement, reconnect removed C with retained old storage;
   it must not regain read/write authority. Verify selected and unrelated groups,
   durable DELETE, snapshot catch-up, and lost membership responses.

These are specific test/implementation obligations, not a request to redesign
the protocol or delay Task 1. Advisor/verifier dialogue remains direct; root
owns incorporation and evidence tracking.
