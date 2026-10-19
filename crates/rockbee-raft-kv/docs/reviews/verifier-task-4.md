# Task 4 independent verification

Final verifier spec verdict: APPROVED for Task 4's storage snapshot scope. Final verifier quality verdict: APPROVED for that scope. Frozen fixes and refreshed evidence resolve the concrete counterexamples below; no remaining actionable blocker was found. Verifier ran no Cargo and made no storage edits.

Reviewed the Task 4 brief/report, advisor review, `storage/snapshot.rs`, `snapshot/format.rs`, relevant tests, and shared storage/log/state ownership paths. Direct advisor/verifier discussion agreed on each coherence gate; no Judge disagreement.

Advisor independently reread the final frozen fixes and refreshed logs and concurs with final spec and quality approval. This closes the bilateral review gate.

## Concrete findings and fixes

P1, snapshot metadata coherence: a CRC-valid image could claim equal applied/membership indexes with different full LogIds, or advance applied progress while regressing membership or changing configuration under the same membership LogId. The worker reproduced the first two classes in `task-4-membership-red.log` and `task-4-membership-regression-red.log`; verifier inspected those failures and the focused green. The frozen code now validates header identity, nondecreasing membership progress and exact membership at equal progress before import. Recovery also retains the explicit same-applied membership equality check, whose absence was reproduced in `task-4-recovery-membership-red.log`.

P1, opposite index/leader ordering: verifier independently found that active applied `(term 5, index 10)` could be replaced by `(term 1, index 11)`, and a header could put membership leader term 9 beneath applied term 6 merely because the membership index was smaller. The worker reproduced both CRC-valid cases in `task-4-leader-order-red.log` (accepted variants `[0, 1]`). Frozen `compare_logs` now rejects opposing index versus full committed-leader `(term,node)` order while preserving index-based progress and equal-index full identity. Header parsing also requires membership leader identity not to exceed applied leader identity. Internal node zero remains legal. This is the same bounded history-coherence gate, not a new snapshot protocol.

## Accepted source properties

- Build reserves a durable unique sequence, retains its live-image lease, and captures state record and KV prefix through one RocksDB MVCC view. The scan lane is released before waiting for the publication write gate; installer gate-then-lane cannot form the earlier cycle. Publication rechecks generation/current descriptor and a stale builder opens the superseding image.
- Actual passed File is sought to zero and copied through bounded validation. The file flush/sync, unique rename and directory sync precede atomic state-record/descriptor activation. No install fabricates log, vote, committed or purge metadata. Inactive imported chunks remain invisible until the final synchronous batch.
- Equal applied progress requires identical logical KV contents as well as membership. Current selection/open and unlink share a short publication mutex; existing Unix transfer handles and pinned state views survive cleanup. Cleanup stays under the exact group/generation prefix, uses bounded batches, and protects active/current/live-build owners.
- Cancellation retains lane/group/write-gate ownership inside native closures; abandoned writers retire admission. Caller disappearance cannot allow a replacement group to overtake native I/O. Startup validates authoritative descriptors, images and allocator reservations before garbage deletion.
- Record lengths are checked before allocation, keys are strictly ordered, count/checksum and exact EOF are required. Header collection sizes are bounded by remaining header bytes, but heap collection overhead is additional. Parser/native batch counters are useful concrete owners; they do not measure whole-process RSS or RocksDB internals.
- The upstream-suite-triggered stale append compatibility fix skips only entries covered by a durable purge marker, rejects a conflicting full boundary identity and preserves live contiguity under the existing write session. It does not resurrect purged log bytes or adapt away an upstream test.

## Evidence boundary

Verifier read the final `task-4-tests.log`: 27 library plus 34 storage integration tests pass with zero ignores, including every new coherence regression, the unmodified complete upstream suite and formerly ignored purged-history recovery. `task-4-runtime-tests-loopback.log` adds eight passing CLI/peer/runtime/process tests, including the actual public path and three-node process recovery. Final `task-4-clippy.log` finishes successfully (3.16 seconds), and the worker records formatting exit zero. These refreshed results follow the frozen fixes, rather than reusing the earlier 24+34 green. Total: 69 passing tests. The initial listener EPERM was an environment prerequisite; the authorized loopback retry passed.

The existing tests cover actual file streaming/handle substitution, cross-physical-node transfer, maximum records and a multi-megabyte image with bounded buffers/batches, malformed files, five failure/reopen boundaries, stale builder, pinned MVCC read, native cancellation, published-file corruption, allocator corruption and tail DELETE. CRC-valid metadata regressions complement byte-corruption tests. These establish application ordering at tested failure points, not physical power-loss behavior. Unix unlink semantics are the supported scope; network snapshot catch-up and fourth-node movement still need their runtime gates.

The report also accurately scopes oversized synthetic raw storage memberships: a near-log-limit raw membership can exceed the separately capped snapshot header after framing and fail build. Bounded production ingress (16 nodes and bounded endpoints) stays far below that default header budget. No universal claim that every arbitrary raw adapter membership is snapshotable is made. Do not widen that limitation into a claimed successful runtime path.

```text
coherent Raft state -> bounded durable image
received File -> invisible generation -> atomic state + descriptor
storage snapshot scope approved; physical power loss unvalidated
```
