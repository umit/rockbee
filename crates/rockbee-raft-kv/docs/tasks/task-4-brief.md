# Task 4 — durable streamed snapshots

Implement only node `storage/snapshot.rs`, minimal hooks in `storage/{mod,state,keys}.rs`, and storage tests/support. Keep runtime, peer, gateway, protocol, and existing Rockbee untouched. Preserve Task-3 APIs and tests. This brief is a handoff, not evidence of implementation or passing tests.

## Existing contracts to retain

`SnapshotData = tokio::fs::File` is already configured. Implement these existing methods without changing their signatures:

```rust
// RaftSnapshotBuilder<TypeConfig>
async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StoreError>;
// RaftStateMachine<TypeConfig>
async fn begin_receiving_snapshot(&mut self) -> Result<Box<tokio::fs::File>, StoreError>;
async fn install_snapshot(
    &mut self,
    meta: &SnapshotMeta<u64, BasicNode>,
    snapshot: Box<tokio::fs::File>,
) -> Result<(), StoreError>;
async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, StoreError>;
```

OpenRaft's suite replaces the receiver's File with the sender's File. Always seek and read the **actual passed handle**; never install by renaming a remembered receive path. Copy it into a newly owned local staged image while validating/importing. Return readable handles positioned at zero. `Snapshot` contains `meta` plus `snapshot: Box<File>`.

`StateRecord { generation, applied, membership }`, the state CF layout, shared DB, synchronous `Group::persist`, and retired `WriteSession` semantics already exist. `StateReader::read_local` reads record and value through one RocksDB snapshot; preserve that coherence. Add stored DB-root/cluster/hash identity to `Shared` so files live below that storage directory. Snapshot compatibility checks compare cluster/group/format/hash configuration, **not the source node ID**: snapshots transfer between nodes.

## File format and bounded work

Use a versioned binary file: fixed magic/version; bounded metadata header with cluster/group/hash configuration, full applied LogId, full StoredMembership, and snapshot ID; ordered length-prefixed key/value records; explicit end marker/count and checksum footer. Empty state is valid. Absent keys represent deletes; no resurrection from a previous generation. Header metadata must exactly agree with the `SnapshotMeta` supplied to install. Do not derive a filesystem path from a remote snapshot ID.

Choose a documented stable streaming checksum, such as CRC-32/ISO-HDLC, and test its known vector; a small checksum dependency is justified only if needed to avoid maintaining an error-prone implementation. Do not use Rust's unspecified `DefaultHasher` as the disk format. The checksum detects accidental corruption, not hostile forgery. Reject truncated header/record/footer, invalid lengths before allocation, trailing bytes, duplicate/non-increasing keys, checksum/count mismatch, and wrong identity. Bound nested header membership/string allocation too; an outer byte cap alone does not prove arbitrary serde allocation safety.

Reuse `max_entry_bytes` for a bounded metadata header, public key/value limits for each record, and `io_chunk_bytes`/`io_chunk_entries` for import WriteBatches. Keep one record/lookahead plus one bounded batch and a small I/O buffer (e.g. 64 KiB); never collect all records, file bytes, or generation keys. A maximum legal key/value must fit. Check arithmetic and generation/sequence exhaustion explicitly. Instrument peak buffered records/bytes in tests rather than inferring boundedness from file size.

Add one process-shared snapshot blocking lane (default one slot), separate from the existing log/state lanes, so a complete streaming scan does not occupy all read/apply slots. Acquire before spawning and retain its owned permit inside the native closure. Build a RocksDB read snapshot and iterator entirely inside that closure, avoiding self-referential borrowed snapshots. File I/O, scans, directory sync, and checksums stay off executor threads.

## Build: capture, stream, publish

1. Allocate a unique owned partial filename with `create_new`; keep all temporary/final images on the same filesystem. A checked durable per-group sequence in `snapshot_meta` can supply local image IDs and generation IDs without reusing orphan generations after a crash. Persist reservation before importing any generation data. Do not use `active_generation + 1` when an abandoned generation may already exist.
2. In the admitted blocking job, capture one RocksDB snapshot and read `StateRecord` from it. Iterate **only** that captured group/generation prefix, streaming records and metadata into the partial file. Ordinary apply may continue. Hold the RocksDB snapshot until the scan finishes.
3. Complete footer, flush buffered output, `sync_all` the file, rename it to its unique immutable final name, then sync the containing directory. Ensure newly created directory ancestry is durably established too. Never overwrite an existing published image in place.
4. Finish the scan job and release its snapshot-lane permit **before** acquiring the existing per-group write session for publication. An installer holds the group gate while waiting for the snapshot lane; a builder must never hold that lane while waiting for the same gate. The completed immutable image survives release of the scan lane. In the publication write closure re-read active generation and current image descriptor. A builder whose captured generation was replaced, or whose image is older than the already published descriptor, must not regress current snapshot metadata. Return an opened current image when it supersedes this build; equal-index conflicting log identities are corruption, not an ordering shortcut. Normal apply advancing within the same generation does not invalidate a consistent older image.
5. Publish the descriptor through one synchronous RocksDB batch, only after the file and directory are durable. This does **not** change active state/applied progress. Return the selected descriptor's metadata and an opened file at zero. An abandoned build file is an orphan, never authoritative recovery state.

## Install: stage, validate, atomically activate

Reserve a `WriteSession` for the install lifecycle, preserving sole group mutation ownership. Import can serialize this group's writes; it must not hold a process-global lock. Move/retain the shared gate, Group owner, and snapshot-lane permit inside every running blocking closure. Cancellation retires group write admission exactly as Task 3 does, and cannot permit a replacement group while native work still owns it.

Allocate a fresh inactive generation durably. Seek the actual passed file to zero. Stream it into an owned staged image and bounded synchronous batches under that generation. Validate all records, metadata, footer, and exact EOF before publication. Intermediate imported batches are intentionally visible only under the inactive generation; failure leaves the current state and current descriptor intact.

After validation, sync the owned file, rename, and directory-sync as above. Then execute **one synchronous WriteBatch** containing both:

- `state[group || 0x00] = StateRecord { generation: imported, applied: meta.last_log_id, membership: meta.last_membership }`;
- `snapshot_meta[current] = descriptor` naming that durable local immutable file and its exact metadata/checksum/length.

This is the activation point. Never publish one pointer without the other. Return success only after it completes. Keep vote/log/committed/purge handling with their existing OpenRaft owners; snapshot installation must not invent a purge marker from applied state. Reject unexpected rollback/conflicting identity defensively; a duplicate same snapshot may return success only after proving its already durable matching state/descriptor. Do not reject ordinary valid suite recovery merely because its synthetic membership uses internal node ID zero.

## Read, recovery, and cleanup

`get_current_snapshot` reads the authoritative descriptor and opens its file at zero. `None` means genuinely absent; a missing/corrupt referenced image is a contextual storage error. Serialize descriptor selection+file-open with image publication/unlink using a short per-group publication mutex inside blocking work (no mutex guard across await). This prevents a current-image read racing cleanup before opening the file. On Unix, already opened File handles remain usable after unlink; state readers retain their RocksDB MVCC view while old-generation keys are deleted.

On reopen, validate published descriptor/file identity, complete framing/checksum, and active-state consistency before treating recovery as usable. Current snapshot may legitimately lag applied state after later writes; require compatible metadata, not blanket equality. Active generation must exist coherently; never restore an empty replacement after corruption. Retain evidence on validation failure.

Reclaim only proven inactive generation prefixes and unreferenced owned partial/final files, in bounded batches/directory iteration. Protect current/being-built images and all active import work. Garbage collection must never scan into another group or reuse a generation. After successful activation, old named snapshots are obsolete; unlink under the publication lock while existing open transfer handles remain valid. Sync directory changes when their durability matters. A crash during cleanup may leave garbage but cannot remove authoritative state. Startup cleanup runs only after published state validates and before live jobs start. Do not require image IDs or active generations to be contiguous.

## Acceptance gates and handoff report

Use deterministic barriers/hooks around actual persistence boundaries, unique temp directories, and only owned processes. No sleep-only assertions or real disk filling.

1. Empty and populated round trips, arbitrary binary keys, Some(empty), DELETE before snapshot and DELETE in replayed tail; full applied LogId and membership preserved; adjacent groups untouched.
2. A state larger than the configured work-buffer budget streams successfully, with observed maximum retained chunk/queue below the declared bound; a maximum legal record passes.
3. Truncation/corruption/wrong cluster or group/metadata mismatch/duplicate or out-of-order key/hostile length/trailing bytes reject without active-pointer change. Later reopen remains coherent or explicitly fails closed if authoritative metadata itself was corrupted.
4. Pause before file sync, after rename/directory sync, before and after activation batch, and during old-generation cleanup. Reopen verifies old-or-new complete state and exact descriptor; no mixed generation. Synthetic interruption/process kill is not proof of physical power-loss behavior.
5. Hold a builder after capture, install a newer snapshot, then release builder: current descriptor and active generation never regress. Hold a read snapshot across install+GC: it remains coherent; later reads observe the new generation. A pinned older read is legal when it covers its caller's authority barrier.
6. Cancel an install waiter while actual blocking work is held: group remains retired, another same-group write/reopen cannot overtake it, and existing owner/permit lifetimes are preserved. Validating/receiving an image must not cause unbounded detached work.
7. Run complete `openraft::testing::Suite::<TypeConfig, LogStore, StateMachine, support::Builder, tempfile::TempDir>::test_all(support::Builder)` in an ordinary `#[test]` (the suite owns its runtime). Remove Task-4 ignore from `get_initial_state_re_apply_committed`, covering purged-log recovery. Keep the suite's passed-File substitution case and add actual streamed file transfer; neither replaces later network tests.
8. Run affected storage tests, formatting, and strict Clippy in coordination with root's Cargo slot. Report exact commands/results and limitations; no snapshot success claims until these gates pass.

Keep named functions aligned with this flow (`write_image`, `validate_and_import`, `publish_image`, `activate_generation`, `open_current`, `collect_obsolete`). These are private domain steps, not a new trait framework. Add short ownership/durability comments where behavior depends on Drop, file sync, and the atomic batch.
