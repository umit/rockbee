# Task 4 pre-implementation review

Design verdict: approved with the explicit lock order now added to `task-4-brief.md`. Implementation and runtime acceptance remain pending.

Advisor and verifier independently identified the same deadlock: a builder retaining the sole snapshot lane waits for the group publication gate while an installer owns that gate and waits for the lane. Finish the builder's scan job and release its lane before acquiring the publication WriteSession. The immutable completed image can survive this gap. Installer gate-then-lane ordering is then safe. Keep the short descriptor/open/unlink mutex entirely inside admitted blocking work.

The brief otherwise covers the necessary boundaries: actual passed File rather than remembered receive path; bounded streaming and metadata; file sync, rename and directory sync before descriptor publication; atomic generation/descriptor activation; stale-builder recheck; full LogId identity; coherent pinned reads; cleanup that cannot cross group prefixes; cancellation retaining native ownership; and the upstream suite including snapshot-dependent purged recovery. These are requirements, not evidence that snapshots work.

Required regression for the ordering clarification: pause a builder after its scan while an installer owns the group gate, let the install obtain the snapshot lane and finish, then release the builder. Both complete, and the stale builder cannot replace the newer installed descriptor.

Direct bilateral discussion reached agreement; no Judge escalation needed. No source edits or builds by verifier.

```text
Raft state -> bounded snapshot file -> inactive generation -> atomic activation
              implementation and failure tests pending
```
