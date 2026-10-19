# Task 3: durable basic storage

Status: implementation brief; all acceptance below is pending. Implement only
inside `crates/rockbee-raft-kv`. Existing Rockbee remains untouched. Do not change
the engine versions, runtime/TCP settings, or native compiler environment; the
root agent owns the evidenced command-scoped native-build workaround.

```text
Existing: Client -> Gateway -> Writer/Readers -> SlateDB
This task: group LogStore / StateMachine -> one process RocksDB
                           |
                    local read view
Snapshots and real Raft/network service: unfinished, Tasks 4 and 5
```

## Deliverable and limits

Implement real durable log/vote/committed storage, atomic bounded-chunk state apply,
coherent local reads, group isolation, and the actual OpenRaft 0.9.25 storage-v2
traits. Add focused restart/failure tests and selected upstream suite cases.
Do not start a Raft service, implement peer transport, build full snapshots, or
claim the complete OpenRaft suite passes. No fake successful snapshot methods.

Read [the exact API notes](../reviews/advisor-storage-api.md) for upstream
signatures. Engine dependencies already exist in the node package. Main files:
`src/raft/types.rs`, `src/storage/{mod,keys,log,state,snapshot}.rs`,
`tests/storage.rs`, and a small shared test fixture. Split by these responsibilities,
not one file/trait per helper. All snapshot logic in this task is explicit
unavailability as specified below.

## Constructors and application-owned types

Use internal Raft `NodeId = u64`; the public nonzero NodeId remains validated at
configuration/wire boundaries. The upstream suite legitimately uses internal 0.
Use `Entry = openraft::Entry<TypeConfig>`,
`Node = openraft::BasicNode`, `SnapshotData = tokio::fs::File`,
`AsyncRuntime = openraft::TokioRuntime`, and the default OneshotResponder.

Node-private `Command` initially has only `Put { key: Bytes, value: Bytes }` and
`Delete { key: Bytes }`. `ApplyResult` can be a small `Applied` enum variant.
They derive the required Debug/Clone/serde traits. Keep GET, deadlines and public
transport certainty outside the log. Blank and membership entries return an
ApplyResult too; they are not silently omitted from the response vector.

Provide these concrete constructors; names may follow existing module naming,
but preserve the semantic boundaries:

```rust
pub struct StorageIdentity {
    pub cluster_id: ClusterId,
    pub node_id: NodeId,             // validated public ID
    pub shard_config: ShardConfig,   // count, hash version, seed
}

pub enum OpenMode { CreateNew, OpenExisting }

impl Storage {
    pub async fn open(
        path: PathBuf,
        identity: StorageIdentity,
        options: StorageOptions,
        mode: OpenMode,
    ) -> Result<Self, StorageOpenError>;

    pub fn open_group(&self, group: GroupId) -> Result<GroupStores, StorageOpenError>;
}

pub struct GroupStores {
    pub log_store: LogStore,
    pub state_machine: StateMachine,
}

impl StateMachine {
    pub fn reader(&self) -> StateReader;
}

impl StateReader {
    pub async fn read_local(&self, key: Bytes) -> Result<StateRead, StorageError<u64>>;
}

pub struct StateRead {
    pub generation: u64,
    pub applied: Option<LogId<u64>>,
    pub value: Option<Bytes>,
}
```

`read_local` is explicitly not a strong read; Task 5 supplies the authority
barrier and checks applied progress. It obtains generation/applied/value from
one RocksDB read snapshot. Keep these storage types node-internal; visibility can
be public in the node library only as needed by integration tests, not reexported
by client/protocol.

Open validates options before spawning/allocating native resources. CreateNew
refuses any existing database/nonempty storage directory; OpenExisting refuses
missing databases, unexpected format/CF layout, or identity/shard mismatch.
Persist versioned identity synchronously before returning a new store. Use an
explicit storage format constant 1; fail incompatible formats rather than migrate
or silently create missing CFs. The OS DB lock remains authoritative against
concurrent opens. A failed partial creation stays diagnosable, not auto-reset.

## Local defaults and ownership

`StorageOptions` has named fields and a validated Default implementation:

| Field | Initial default | Meaning |
|---|---:|---|
| `block_cache_bytes` | 64 MiB | One shared block cache for the process DB/CFs |
| `write_buffer_budget_bytes` | 32 MiB | One shared WriteBufferManager, soft budget with write stalls enabled |
| `log_io_slots` | 2 | Process-wide blocking work for log/vote/committed/purge/truncate |
| `state_io_slots` | 2 | Process-wide blocking state apply/local-read work |
| `io_chunk_bytes` | 4 MiB | Maximum encoded work chunk; no whole-iterator collection |
| `io_chunk_entries` | 128 | Maximum entries per prepared chunk |
| `max_entry_bytes` | 2 MiB | Bound for encoded single persisted Raft entry in this prototype |

Validate nonzero fields, semaphore-compatible slot counts, checked byte
arithmetic, and `io_chunk_bytes >= max_entry_bytes`; do not silently clamp.
The accepted chunk budget must also cover one legal entry and durable metadata:
`max_entry_bytes + 256 + min(max_entry_bytes, MAX_KEY_BYTES + MAX_VALUE_BYTES + 128)
+ 64 + 128`. This conservative minimum preserves the defaults. State admission
removes the old record's native-write reservation when a membership entry
replaces it; its owned memory is still accounted separately. Tight configurations
that cannot fit this minimum fail before the database is created.
Validate public KV limits inside Command validation as well. Raft membership
entries must fit the entry bound before network/log admission in Task 5; this
task reports oversized/corrupt entries as storage errors, never drops them.

Create `Cache::new_lru_cache(block_cache_bytes)` and
`WriteBufferManager::new_write_buffer_manager(write_buffer_budget_bytes, true)`
once. Apply the same cache to every CF's BlockBasedOptions and the same manager
to DB/CF Options; keep owners in Storage. Preserve RocksDB's per-CF write-buffer
size and count defaults; the shared WBM provides the explicit process memtable
budget without unrelated per-CF tuning. Do not create one cache/manager per
group. These are initial explicit memory budgets, not
performance claims, and do not establish a hard RSS cap. WBM can stall the shared
engine; independent application lanes cannot promise independent disk latency.

Use one `Arc<DBWithThreadMode<SingleThreaded>>`; fixed CFs need no dynamic CF
creation/deletion lock. LogStore and StateMachine are non-Clone. A per-group
shared ownership token prevents duplicate active writer handles. Read handles
and dispatched blocking jobs hold that token too; reopening a group is refused
until old handles **and actual blocking jobs** finish. A dropped async waiter
does not prove its blocking job stopped. Reopening the DB requires all its
handles/jobs to close.

## Persisted key layout

Use bytewise ordering and big-endian u64 components. Do not use textual prefixes.

| CF | Key | Value |
|---|---|---|
| `raft_log` | `group || log_index` | Full versioned Entry including full LogId |
| `raft_meta` | `group || tag` | Separate vote, committed LogId, last-purged LogId |
| `state` | `group || 0x00` | One versioned state record: active generation, applied LogId, StoredMembership |
| `state` | `group || 0x01 || generation || key` | Opaque value, including empty bytes |
| `snapshot_meta` | `group || tag` | Reserved for Task 4; not populated with fake descriptors |

Store the process identity at reserved `raft_meta` key
`group_zero || 0x80`; log/vote tags use a distinct documented tag range. Never
include this identity key in group-log maintenance. Default CF stays empty.
Fresh state uses generation 0, no applied LogId, and default empty membership;
generation allocation belongs to Task 4. A missing state record is only pristine
if no generation data exists; orphan state data is corruption, not an empty DB.

Construct checked range boundaries once. For fixed 16-byte log keys,
`group || [0xff; 8] || [0x00]` is an exclusive end after every index in that group,
including u64::MAX. Avoid wrapping index+1 or group+1. Range conversion handles
inclusive/exclusive/unbounded bounds; excluded MAX start is empty. Iteration
always confirms group prefix and reports iterator/deserialization errors.

## Bounded I/O and durable operations

For mutating jobs, acquire the per-group writer gate, then the relevant process
lane permit before dispatching `spawn_blocking`. Read-only read_local,
get_log_state, and log-reader jobs take a state/read lane permit and coherent
RocksDB snapshot without taking the log writer gate. The actual closure owns
the lane permit and a
shared owner of the group gate/token until its native operation ends. When a
multi-chunk call retains the gate between jobs, each active job must also own it;
cancellation cannot release ordering while a write is still running. No global
DB mutex and no blocking call on an async executor worker.
For example, the shared gate owner is `Arc<OwnedSemaphorePermit>`;
OwnedSemaphorePermit itself is not Clone. An abandoned multi-chunk operation
retires that group's write admission until its actual jobs drain and recovery
reconciles the durable prefix; cancellation is never a safe-retry rejection.

Prepare only bounded owned chunks before passing them into `'static` closures.
Do not `collect()` arbitrary Raft input into an unbounded Vec first. Account for
the owned input entries, encoded buffers, native WriteBatch and at most one
lookahead entry while simultaneously alive. A chunk boundary does not authorize
rejecting a valid large iterable: process its next chunk in order. Compact
response vectors and the full `try_get_log_entries` result are required by the
upstream API; report those API-level allocations honestly rather than calling
this a total-memory proof.

- **Vote/committed:** sync WAL write before returning. All log-side mutations
  use the same group gate; no later write overtakes an unfinished earlier write.
- **Append:** write bounded consecutive chunks durably in order. No hole may be
  created; partial failure leaves at most a persisted prefix and fails the group.
  The callback covers the entire append, not one chunk. Its final owner calls
  `log_io_completed(Ok(()))` only after every chunk succeeded; on error it reports
  I/O failure and never success. The final blocking operation owns callback
  completion where possible so waiter cancellation cannot create early success.
  An abandoned append must not let further group admission continue unchecked.
- **Apply:** use one synchronous WriteBatch per bounded contiguous input chunk:
  every data effect in log order plus the chunk's final applied identity and
  resulting membership record. Respect io_chunk_entries/io_chunk_bytes, including
  conservative space for metadata and native batch representation. An entry is
  never split across chunks. PUT/DELETE/blank/membership all contribute to final
  applied progress; membership records retain their own membership-entry LogId.
  Return one ordered ApplyResult per input entry only after the chunk is durable.
  Whole `apply(iterator)` is not a transaction: failed later chunks may leave a
  coherent prefix of fully applied chunks. Do not fsync unconditionally per entry,
  invent all-iterator atomicity, or replay/roll back uncertain operations.
- **Truncate/purge:** truncate deletes inclusive suffix; purge deletes inclusive
  prefix and writes last-purged LogId in one sync batch. Marker and surviving log
  state are coherent after restart. The low-level suite may request purge beyond
  the present tail; store that requested marker. Raft decides safe production
  purge points; storage tests do not fabricate snapshot safety.
- **Read:** `get_log_state` reads purge marker and last present entry from one
  RocksDB snapshot; applied state does not supply the log tail. Empty log returns
  last-purged as last-log. `try_get_log_entries` preserves full requested-range
  semantics; `limited_get_log_entries` returns bounded prefix chunks for
  replication, with at least one legal entry for a nonempty available range.

Always leave WAL enabled and set WriteOptions sync=true. Corruption, JoinError,
CF mismatch, or native I/O failure becomes contextual StorageError, not None,
Committed, or pre-submission Rejected. No public fault toggles: test hooks must
exercise the actual adapter result path and remain test-only.

## Snapshot methods until Task 4

All required trait methods exist with honest behavior:

- `get_snapshot_builder()` returns the concrete builder type; its
  `build_snapshot()` returns StorageError wrapping Unsupported.
- `begin_receiving_snapshot()` and `install_snapshot()` return Unsupported
  storage errors; they never mutate active state.
- `get_current_snapshot()` returns Ok(None) only when no descriptor exists.
  If it encounters any descriptor/image version it cannot support, return error
  instead of pretending the snapshot is absent.

These errors intentionally prevent claiming a functional Raft cluster before
Task 4. Do not run or mark complete upstream snapshot cases/full Suite yet.

## Focused acceptance

1. **Open/reopen:** unique new DB, durable identity, repeated create refusal,
   missing existing refusal, same-config recovery, identity/format/CF mismatch,
   and lock failure. Never delete failed-open evidence to make a test pass.
2. **Ordering/ownership:** two same-group writer acquisitions cannot coexist;
   reader clones work; cancel an async waiter while native write is held, then
   prove later same-group write and writer reacquisition cannot overtake it.
   Different groups still use separate logical gates.
3. **Real apply recovery:** PUT, overwrite, DELETE, empty value, blank and
   membership entries; reopen and inspect effects/applied/membership. Inject a
   chunk failure and verify no partial chunk effects or mismatched applied/
   membership metadata. Exercise long input
   across chunk boundaries without whole-input collection or false batch success.
4. **Log recovery:** vote/committed persistence, append/truncate/purge, range
   boundaries including MAX and group isolation; concurrent purge/get_log_state
   cannot combine marker and tail from incompatible views.
5. **Durability failure:** held/failing actual native write does not emit a
   successful append callback; API returns failure and state is recoverable.
   Since LogFlushed::new is crate-private, use the supported upstream suite or
   minimal real Raft harness only if needed to obtain the genuine callback.
6. **Selected upstream cases:** fresh StoreBuilder per case; save_vote,
   initial_logs, get_log_entries, limited_get_log_entries, get_log_state,
   last_applied_state, truncate/purge variants, apply_single/apply_multiple, and
   committed-reapply recovery. A guard is first in `(guard, log_store, sm)`.
   Named async suite methods may run in a test runtime; synchronous test_all
   cannot run inside a Tokio test and remains deferred to Task 4.
7. **Explicit missing feature:** all unavailable snapshot methods return the
   promised result and leave state unchanged. Record full-suite omission as
   Task 4 work, not a skipped failure counted as success.

Use targeted tests, formatting, node check and strict Clippy with the root-approved
build command/environment. Coordinate Cargo execution with root; no parallel
native builds. Preserve the full failing test/error before implementing a fix.
Reviewer must inspect persistence ordering and cancellation ownership, not only
test totals. Later F57 public ACK dual-barrier coverage remains mandatory.

## Readability and handoff

Keep each trait method a short sequence of domain steps: normalize/validate,
reserve/own, read or persist, map result. Give nontrivial encoding/range logic
named helpers; comment the cancellation/durability invariant at its ownership
transfer. Explain OpenRaft traits as real capability interfaces. Do not create
mock Java inheritance, generic storage backends, or extra cloning/locks/tasks for
style. Report exactly which focused cases passed and list snapshots/full suite/
real cluster as unfinished. Do not commit or push.

Review decision: root requires natural bounded Raft apply batches, with atomic
effects and final metadata per chunk. This supersedes the advisor's initial
per-entry-fsync suggestion. RocksDB per-CF memtable defaults are preserved;
shared cache/WBM budgets are the explicit resource configuration. Both choices
retain the same cancellation, coherent-prefix recovery, and no-hard-RSS claims.
The verifier accepted bounded-chunk atomicity and requested the explicit
separation of read admission from the per-group log writer gate above.
