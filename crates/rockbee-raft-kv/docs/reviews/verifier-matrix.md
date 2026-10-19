# Independent review of the failure matrix

Status: design review only, 2026-10-19. Reviewed `failure-matrix.md` against
Tasks 1–14 and exchanged corrections directly with the advisor. No Cargo,
build, test, runtime fault, or production edit was performed. All scenarios
remain Pending.

```text
Planned: Client -> Gateway -> Raft groups -> RocksDB
                         faults -> outcomes + recovered state + bounded counters
```

**Verdict: coverage approved; no remaining design blocker for Task 1.**
The matrix gives concrete observations, separates synthetic/restart/measurement
evidence, and keeps physical power-loss qualification outside prototype
completion. No protocol redesign or additional product feature is required.

## Accepted corrections and additions

- F45 and completion rules: Unknown notification does not close a mutation's
  execution interval. `PUT(v) -> Unknown -> GET(None) -> GET(v)` can be legal.
  The checker keeps optional incomplete mutations through the observation
  horizon unless server evidence narrows them. Search exhaustion is inconclusive,
  never pass. Distinct write values and invocation/definitive-completion partial
  order avoid modeling concurrent writes by response order.
- F38: count actual node Raft proposals separately from transport transmissions.
  Identical PUT/DELETE replay may be invisible in final KV state. Safe explicit
  routing rejections may cause several transmissions; uncertain attempts must
  not cause automatic replay. The response-loss schedule requires one proposal.
- F27: a pinned old generation is legal if its coherent view covers the read
  barrier and overlaps a newer installation. Forbid mixed views or a view below
  the barrier; do not reject every old-generation read.
- F34/F59/F60: confirm fault and membership phase before fresh invocation and
  account for earlier work. Learner D cannot replace voter quorum. While joint
  ABC/ABD is active, BC versus AD supplies neither side both required majorities.
- F42: pause/resume stale controller work after replacement to test fencing,
  alongside duplicate and conflicting jobs. Job status alone is insufficient;
  observe actual committed membership and durable removal evidence.
- F01: present empty bytes differ from a missing value. F23 permits node-wide
  fail-closed on shared-engine corruption while forbidding sibling-prefix cleanup.

## Added whole-path gate: F57

Hold actual persistence completion at two independently exercised boundaries:

1. Fewer than a quorum of durable-log completions have been released.
2. Quorum log durability is established, but the leader's synchronous apply
   batch has not completed.

In each case, a mutation submitted through the public client and gateway cannot
produce Committed. Observe phase markers and a retained pending response;
release the boundary and require eventual successful completion plus a matching
strong GET. An ordinary short sleep without a known held boundary is inadequate.

F17 establishes adapter callback ordering, F19 atomicity, and F35 successful-write
survival, but they do not individually prove that the complete request path
waits for both durability boundaries. The advisor added this focused P0
process case as F57; it should not be replaced by a duplicated final-state-only
test. The administrative final-scope review remains in the Task 14 completion
checklist rather than consuming a behavioral test row.

## Scope and evidence discipline

Keep short deterministic cases for all boundary inputs and publication cuts,
then selected real-process cases for the complete path. Avoid multiplying every
fault by every operation/group when no new invariant is exercised. Tables may
parameterize related inputs, but distinct publication phases need separate
assertions and records.

A fixed soak seed reproduces a fault schedule, not OS thread scheduling; retain
actual event timelines and smallest failing histories. Hard memory assertions
come from owned buffer/queue counters and allocation/reservation checks. RSS,
CPU, disk growth, and latency are measured, with workload and host conditions.
No throughput target, automatic balancing, authentication, cross-shard
transaction, or independent-host resilience is introduced by this matrix.

Physical power-loss remains explicitly unvalidated. Process SIGKILL, real
RocksDB reopen, mocked fsync, and synthetic ENOSPC exercise useful failure logic
without proving a storage device's power-loss persistence behavior.
