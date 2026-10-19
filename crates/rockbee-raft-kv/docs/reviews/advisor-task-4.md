# Task 4 independent storage review

Status: final frozen checkpoint APPROVED for specification and code quality. Advisor and verifier directly concurred after independently inspecting the corrections and final evidence; no Judge disagreement. No Cargo commands or production edits by advisor.

```text
Client -> Gateway -> OpenRaft -> synchronous RocksDB batches
                           -> coherent view -> durable snapshot image
received File -> validate/import inactive generation -> atomic activation
```

## Corrected review findings

`snapshot/format.rs::decode_header` checks membership index is not ahead of the applied index, but initially accepted different full LogIds at equal indices. `snapshot.rs::validate_and_import` initially compared membership only when incoming and active applied progress were equal. A snapshot with newer applied progress could therefore install an older membership, or alter its configuration while retaining the same membership LogId. These are impossible transitions in one committed history, even when framing and checksum are valid.

The final source requires membership progress to be nondecreasing relative to active state; equal membership progress retains the full identity and configuration. Inside a header, equal applied/membership indices have equal full LogIds. Valid older membership beneath a later ordinary applied entry remains accepted. CRC-valid regressions reproduced the gaps before the fixes. The final shared checks also reject committed-leader ordering that contradicts index ordering, and recovery requires exact membership equality at equal applied progress. Advisor reread these changes after the final freeze.

## Findings accepted from source

- Build captures the active generation, applied identity and membership from one RocksDB snapshot. The image lease protects unpublished output while the scan lane is released before waiting for the group publication gate. Install takes the gate before its native snapshot lane. The previously identified lock-order cycle is absent.
- The actual supplied `tokio::fs::File` is sought to zero and parsed/copied. File flush and sync, unique rename, and directory sync precede descriptor publication. Installation synchronously writes the active generation, applied identity, membership and descriptor together; imported chunks remain invisible before that batch. Log, vote and purge metadata are untouched.
- Builder publication rechecks current generation and progress. Superseded builders return the current image. Current-image selection/open and unlink share a short publication mutex. Open Unix handles and pinned RocksDB views remain usable after obsolete image/generation cleanup.
- Native jobs retain group, lane and write-session ownership through actual completion. Canceled installation retires the group; dropping async handles cannot reopen it while the native closure still owns it.
- Recovery validates authoritative images, descriptors and sequence reservations before garbage collection. Missing referenced files, corrupt bytes and rewound allocators fail closed. Cleanup streams keys and files and protects active/current/live-build owners.
- Record parsing checks bounded lengths before allocation, strict key ordering, count, checksum and exact EOF. Metadata counts are bounded by remaining header bytes; collection overhead is additional to the encoded header budget. This is a finite allocation bound, not a claim that a 2 MiB header costs at most 2 MiB of heap. The report states this limitation accurately.
- The purge compatibility correction runs under the existing owned writer gate and a coherent log snapshot. Entries covered by the durable purge marker stay absent; a conflicting full identity at the boundary fails. The regression crosses chunk boundaries and still rejects a live log hole. No upstream suite adapter hides the behavior.

## Evidence inspected

The final `reports/task-4-tests.log` contains 27 passing library tests and 34 passing storage integration tests, with zero ignored tests. The latter includes the complete upstream `Suite::test_all` and formerly ignored purged-history recovery. `reports/task-4-runtime-tests-loopback.log` adds eight passing CLI/peer/runtime/process checks. `reports/task-4-clippy.log` contains the successful strict final check. Advisor inspected these post-correction logs; verification was run by the source owner, not rerun by advisor.

The focused tests exercise five persistence/cleanup failure boundaries with reopen, malformed images, actual file transfer, different physical node identities, a held MVCC view and stale builder, native cancellation ownership, missing/corrupt published files, allocator recovery, and same-applied logical-state conflict. The large-image test records a 7,258,376-byte image, bounded import batches and parser record buffers. Those counters exclude metadata collection overhead, RocksDB internal memory and whole-process RSS.

Synthetic failure/reopen tests establish the tested application ordering. They do not prove physical power-loss behavior. Unix receive-file unlink semantics are explicitly scoped to macOS/Linux. Network snapshot catch-up and membership movement remain separate runtime gates.

The raw storage adapter accepts some synthetic near-limit membership entries whose fixed-width snapshot header can exceed the configured header cap and fail honestly. Current production membership ingress has much smaller node/endpoint bounds; this raw-adapter caveat is not evidence that a permitted production membership cannot be snapshotted. No claim is made that every arbitrary raw adapter membership fits the snapshot header budget.

## Separate process-fixture concurrence

Advisor agrees with `verifier-process-cancellation.md`: handled parent signals are registered before spawn; direct scenario cancellation drops exact owned children and waits before removing their temporary storage. The recorded SIGTERM red/green evidence is three surviving children before the fix and zero afterward. SIGINT uses the inspected equivalent path but was not separately injected; parent SIGKILL/abort cannot run destructors. No Judge disagreement.
