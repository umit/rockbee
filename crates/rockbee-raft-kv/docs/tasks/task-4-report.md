# Task 4 — durable streamed snapshots

Status: implementation and final verification complete. All affected tests, the complete upstream storage suite, formatting, and strict Clippy pass. Advisor and verifier both approved the final scope after the coherence fixes; see [advisor review](../reviews/advisor-task-4.md) and [verifier review](../reviews/verifier-task-4.md).

```text
Client -> Gateway -> Raft group -> RocksDB
                         |
                         +-> consistent view -> streamed immutable image
Incoming image -> bounded validation/import -> inactive generation
               -> synced file + directory -> atomic state/descriptor activation
```

The new workspace now implements OpenRaft's real file-backed build, receive, install and current-snapshot capabilities. `SnapshotData` remains `tokio::fs::File`; installation seeks and copies the actual supplied handle. Both the upstream handle-substitution case and an actual `tokio::io::copy` transfer are covered. Existing Rockbee and `.herdr/` were not touched. No dependency or engine settings changed.

## Durable flow and ownership

`storage/snapshot.rs` owns named domain steps; its private `snapshot/format.rs` owns bounded framing and checksum parsing. Minimal storage/state hooks retain the existing API. One shared snapshot blocking lane defaults to one slot and is separate from the log/state lanes. All scan/file/checksum/sync work executes in admitted blocking jobs.

A durable checked per-group sequence reserves every local image/generation ID before import. Build captures one RocksDB MVCC view and streams only its active generation. A live-image lease protects the finished file while the builder waits to publish. The scan closure releases its snapshot-lane permit **before** the builder takes the write session. Install owns the group write session first, then acquires the snapshot lane. No builder holds the lane while waiting for an installer-held gate.

Images are created with `create_new` in the storage directory, flushed, file-synced, renamed to unique immutable names and directory-synced before publication. Newly created directory ancestry is synced. Build publishes only a descriptor. Install validates full framing, exact supplied metadata, checksum/count and EOF, imports bounded synchronous batches under a fresh invisible generation, then synchronously writes the active `StateRecord` and current descriptor in one batch. Log/vote/committed/purge metadata is untouched by installation. A stale build returns the newer current image instead of regressing it. Equal-index conflicting full LogIds/membership are errors; same-applied installation also compares logical KV state, so a valid checksum alone cannot rewrite that history.

The native closure owns its group, write gate and snapshot lane through actual completion. Cancellation retires write admission; dropping caller handles cannot replace the group while native work still owns it. Current descriptor selection plus file open and obsolete-file unlink use a short per-group publication mutex. Already opened Unix handles and pinned RocksDB views remain valid after cleanup.

Startup validates every authoritative descriptor/image, exact framing/checksum, active generation, applied/membership compatibility, and reservation sequence before deleting garbage. A snapshot may legitimately lag later applied entries. When progress is equal, the image's complete KV set must match the active generation. Missing/corrupt referenced files and missing/rewound reservations fail closed and retain evidence. Cleanup streams inactive keys in bounded batches and enumerates owned files without collecting the dataset; active generations/current/live-build images are protected. Generations need not be contiguous.

## File format and bounds

Version one uses `RBRSNAP1`, a big-endian u32 header length, explicit cluster/group/hash settings and exact full snapshot metadata, then ordered `key_len:u32, value_len:u32, key, value` records. `key_len=0` ends the records, followed by `count:u64` and CRC-32/ISO-HDLC `u32`. The checksum covers all preceding bytes, including count; `123456789 -> 0xcbf43926` is tested. Remote snapshot IDs never form filesystem paths.

Header bytes are limited by `max_entry_bytes`; nested membership counts and string lengths are checked against remaining header bytes before allocation. Parsed metadata and temporary header encoding are therefore bounded by the configured header budget, with collection overhead rather than a claimed one-byte-per-byte heap bound. Key/value lengths are checked against the public limits before allocation. Streaming retains a current record plus previous key, a bounded native import batch and two 64 KiB I/O buffers. Same-progress validation may briefly retain one existing KV record for comparison. Header/state metadata are additional bounded owners. No whole-file/state collection occurs in production. Test-only instrumentation records actual peak native batch bytes/count and parser record buffer capacities; these are not process RSS measurements or measurements of RocksDB's internal memory.

## Test evidence

All Cargo commands use the nested workspace's existing target and:

```sh
CXXFLAGS='-isystem /Library/Developer/CommandLineTools/SDKs/MacOSX.sdk/usr/include/c++/v1' CARGO_BUILD_JOBS=2 cargo ... --locked --offline
```

- `reports/task-4-red.log`: real streamed-round-trip regression initially failed at Task 3's Unsupported build operation.
- `reports/task-4-first-green-attempt.log`: actual stream transfer with binary key, empty value, deleted key and reopen passed after implementation.
- `reports/task-4-upstream-first.log`, `task-4-upstream-membership-red.log`, `task-4-stale-append-red.log`: full upstream suite and isolated regression reproduced stale append below the durable purge marker.
- `reports/task-4-upstream-second.log`: the complete unmodified synchronous `Suite::test_all` passed, including passed-File substitution, after the narrowly scoped log fix.
- `reports/task-4-snapshot-gates-first.log`: 8 snapshot tests passed; the new missing/rewound reservation regression failed before the recovery fix.
- `reports/task-4-equal-state-red.log`: same-LogId, different-KV snapshot was initially accepted; the focused test failed before adding logical-state comparison.
- `reports/task-4-snapshot-gates.log`: 10 snapshot tests passed after both fixes. Coverage includes 19 malformed-image variants, all five injected persistence/cleanup boundaries and reopen, stale builder and a pinned read across install/GC, canceled native install ownership, maximum legal record and an image larger than the work budget, authoritative-file corruption, replayed tail DELETE and retained old open handles.

The Task 3 purged-history recovery ignore has been removed. The round-trip integration now additionally preserves full membership addresses/internal node zero while transferring between different physical node IDs. Final rerun evidence for these strengthenings and measured buffer bounds follows below.

The log scope extension was authorized by root after actual upstream failure. `append_chunk` discards only entries already covered by its durable purge marker, rejects a conflicting full LogId at that boundary, and still checks the first live entry against the actual current tail. This defensive stale-call handling does not resurrect purged history; normal submitted live entries remain readable and callbacks still follow completed durable work. The focused regression spans chunk boundaries and checks a real hole and boundary identity conflict. No test-only adapter or upstream suite modification was used.

## Final verification

All commands below exited zero on the final source. The shared native target was used sequentially, with the SDK include flag and two-job limit shown above.

- `cargo test -p rockbee-raft-node --lib --test storage --locked --offline -- --nocapture`: **27 node unit tests + 34 storage integration tests passed**, zero ignored. This includes all 13 snapshot unit tests, the full upstream suite, purged-history recovery, exact membership addresses, cross-node transfer, stale-prefix/chunk/boundary regressions, and the reviewed metadata-coherence cases. Log: `reports/task-4-tests.log`.
- `cargo test -p rockbee-raft-node --bins --test peer --test runtime --test processes --locked --offline`: **8 additional tests passed** (2 CLI, 2 peer, 3 runtime, 1 actual three-process recovery). Log: `reports/task-4-runtime-tests-loopback.log`. The first sandboxed attempt was denied at listener bind with EPERM before peer behavior ran; that environmental prerequisite is retained in `reports/task-4-runtime-tests.log`. The authorized loopback retry passed the public Client→Gateway→real Raft path, four groups, restart durability and process shutdown.
- `cargo clippy -p rockbee-raft-node --all-targets --locked --offline -- -D warnings`: passed. Log: `reports/task-4-clippy.log`. The initial five `collapsible_if` findings are retained in `task-4-clippy-first.log`; they were fixed without changing behavior.
- `cargo fmt --all -- --check`: passed. Log: `reports/task-4-fmt.log`.

Final streaming measurement: **7,258,376-byte image**, observed native import batch peak **1,228,872 bytes / 2 records**, parser current-record-plus-previous-key peak **1,114,119 bytes** against a **1,179,648-byte** public-record bound, with **131,072 bytes** of fixed I/O buffers. Header/parsed metadata, optional same-progress comparison row and RocksDB internals are separate owners as described above. No process-RSS bound is claimed.

Advisor/verifier review exposed three metadata-coherence issues, each reproduced before its fix: `task-4-membership-red.log` (same index, differing full membership/applied identity), `task-4-membership-regression-red.log` (newer applied image regresses membership), and `task-4-leader-order-red.log` (both newer index/older leader and membership leader newer than applied were accepted). `task-4-recovery-membership-red.log` additionally reproduced altered active membership at exact snapshot progress. The final test log passes every corresponding regression. Installation now checks membership progress before importing records; recovery requires exact membership at exact applied progress; and index order cannot oppose committed leader `(term,node)` order. Internal node zero remains supported.

## Limits

The raw storage adapter accepts synthetic membership entries larger than the bounded runtime ingress (16 nodes, 256-byte endpoints). A near-limit raw membership may fit the log entry budget yet exceed the independently capped snapshot metadata header once framing is added; build returns an error rather than publishing success. This report does not claim that every oversized raw adapter membership is snapshotable. The actual runtime membership ingress remains far below the default 2 MiB header cap.

Receiving uses an immediately unlinked owned file: macOS/Linux Unix file-handle semantics are the supported scope here; Windows behavior is not claimed. CRC detects accidental corruption, not forgery. Deterministic injected failures and reopen establish application ordering at the tested boundaries; they are not physical power-loss or filesystem hardware guarantees. Snapshot network catch-up and later learner/membership integration remain separate runtime tasks. No throughput or whole-process memory improvement is claimed.
