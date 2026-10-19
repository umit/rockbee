# Task 3 storage integration notes

Status: signatures checked against official OpenRaft **v0.9.25** and RocksDB
**v0.25.0** source; not compiled locally. No production code or Cargo changes.
After dependency download, the StoreBuilder/Suite, storage-v2, callback, and
SnapshotData signatures were also checked in the cached `openraft-0.9.25` source.

```text
Existing: Client -> Gateway -> Writer/Readers -> SlateDB
Planned:  Gateway -> group Raft -> LogStore / StateMachine -> shared RocksDB
                                     |            |
                                 LogReader   file SnapshotBuilder
```

## Types and trait boundaries

Use internal Raft node IDs as `u64`. Keep nonzero public `NodeId` validation at
config/network boundaries; explicit conversions preserve that contract. Do not
make public `NodeId: From<u64>` merely for tests: the upstream suite constructs
node ID **0**. Internal default/test values must remain representable.

The node-private configuration can start with:

```rust
openraft::declare_raft_types!(
    pub TypeConfig:
        D = Command,
        R = ApplyResult,
        NodeId = u64,
        Node = openraft::BasicNode,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = tokio::fs::File,
        AsyncRuntime = openraft::TokioRuntime,
        Responder = openraft::impls::OneshotResponder<TypeConfig>,
);

type Entry = openraft::Entry<TypeConfig>;
type LogId = openraft::LogId<u64>;
type StoredMembership = openraft::StoredMembership<u64, openraft::BasicNode>;
type StoreError = openraft::StorageError<u64>;
```

`Command` and `ApplyResult` are node-owned, serde-capable application types.
Do not log public deadlines, transport outcome variants, or GET requests as
ordinary KV mutations. BasicNode's address is the peer address; public/backend
addresses remain explicit routing configuration. The declaration defaults and
file bounds are in the [versioned type configuration](https://raw.githubusercontent.com/databendlabs/openraft/v0.9.25/openraft/src/type_config.rs).

Implement these four types, not the deprecated monolithic `RaftStorage` or an
Adaptor around it:

| Type | Trait | Associated type |
|---|---|---|
| `LogStore` | `RaftLogReader<TypeConfig>` and `storage::RaftLogStorage<TypeConfig>` | `LogReader = LogReader` |
| `LogReader` | `RaftLogReader<TypeConfig>` | none |
| `StateMachine` | `storage::RaftStateMachine<TypeConfig>` | `SnapshotBuilder = SnapshotBuilder` |
| `SnapshotBuilder` | `RaftSnapshotBuilder<TypeConfig>` | none |

Key implementation signatures, using the aliases above:

```rust
// RaftLogReader: implement on LogStore and delegating read-only LogReader.
async fn try_get_log_entries<RB>(
    &mut self, range: RB,
) -> Result<Vec<Entry>, StoreError>
where RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + openraft::OptionalSend;

// RaftLogStorage:
async fn append<I>(
    &mut self, entries: I, callback: openraft::storage::LogFlushed<TypeConfig>,
) -> Result<(), StoreError>
where
    I: IntoIterator<Item = Entry> + openraft::OptionalSend,
    I::IntoIter: openraft::OptionalSend;

async fn truncate(&mut self, log_id: LogId) -> Result<(), StoreError>;
async fn purge(&mut self, log_id: LogId) -> Result<(), StoreError>;
async fn get_log_state(&mut self) -> Result<openraft::LogState<TypeConfig>, StoreError>;
async fn get_log_reader(&mut self) -> LogReader;
async fn save_vote(&mut self, vote: &openraft::Vote<u64>) -> Result<(), StoreError>;
async fn read_vote(&mut self) -> Result<Option<openraft::Vote<u64>>, StoreError>;
async fn save_committed(&mut self, committed: Option<LogId>) -> Result<(), StoreError>;
async fn read_committed(&mut self) -> Result<Option<LogId>, StoreError>;

// RaftStateMachine:
async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMembership), StoreError>;
async fn apply<I>(&mut self, entries: I) -> Result<Vec<ApplyResult>, StoreError>
where
    I: IntoIterator<Item = Entry> + openraft::OptionalSend,
    I::IntoIter: openraft::OptionalSend;
async fn get_snapshot_builder(&mut self) -> SnapshotBuilder;
async fn begin_receiving_snapshot(&mut self) -> Result<Box<tokio::fs::File>, StoreError>;
async fn install_snapshot(
    &mut self,
    meta: &openraft::SnapshotMeta<u64, openraft::BasicNode>,
    snapshot: Box<tokio::fs::File>,
) -> Result<(), StoreError>;
async fn get_current_snapshot(&mut self) -> Result<Option<openraft::Snapshot<TypeConfig>>, StoreError>;

// RaftSnapshotBuilder:
async fn build_snapshot(&mut self) -> Result<openraft::Snapshot<TypeConfig>, StoreError>;
```

These are signature sketches, not a standalone compilable file. Ordinary native
`async fn` implementations suffice; avoid adding async-trait just for this.
The [reader/builder contracts](https://raw.githubusercontent.com/databendlabs/openraft/v0.9.25/openraft/src/storage/mod.rs)
and [storage-v2 contracts](https://raw.githubusercontent.com/databendlabs/openraft/v0.9.25/openraft/src/storage/v2.rs)
are the authority. Keep OptionalSend iterator bounds exactly; borrowed generic
iterators/range objects cannot simply be captured by a `'static` blocking closure.
Normalize range bounds and prepare owned bounded write work first.

Override `limited_get_log_entries(start, end)` for replication chunk budgeting.
Do not silently cap `try_get_log_entries`, whose complete range is needed by
callers/tests. Distinguish inclusive/exclusive/unbounded range limits with
checked arithmetic, including excluded `u64::MAX` and inclusive maximum ends.

## Builder and upstream test invocation

```rust
struct TestStoreBuilder { /* test root and budgets */ }

impl openraft::testing::StoreBuilder<
    TypeConfig, LogStore, StateMachine, tempfile::TempDir,
> for TestStoreBuilder {
    async fn build(&self) -> Result<
        (tempfile::TempDir, LogStore, StateMachine), StoreError,
    > {
        // Fresh unique directory and DB on EVERY call, including two calls
        // from the same snapshot-transfer test. Return guard FIRST.
        self.build_fresh_group().await
    }
}

#[test] // NOT #[tokio::test]: Suite creates its own runtime.
fn openraft_storage_suite() -> Result<(), StoreError> {
    openraft::testing::Suite::<
        TypeConfig, LogStore, StateMachine, TestStoreBuilder, tempfile::TempDir,
    >::test_all(TestStoreBuilder::new())
}
```

This proposed helper constructor/build method must be supplied locally.
Place unique test directories under the module's test root. Keep guards alive
until all DB/file handles close. No storage-v2 closure shortcut substitutes for
StoreBuilder. Task 3 can run named suite cases with fresh stores; the complete
suite requires real Task 4 snapshot behavior. No passing snapshot stubs.
[Builder signature](https://raw.githubusercontent.com/databendlabs/openraft/v0.9.25/openraft/src/testing/store_builder.rs),
[Suite invocation and runtime](https://raw.githubusercontent.com/databendlabs/openraft/v0.9.25/openraft/src/testing/suite.rs).

Two suite-specific traps: it purges beyond the last existing entry and expects
`last_log_id = last_purged_log_id` after emptying the log. Also, snapshot transfer
replaces the receiver's returned File with the sender's File directly. Therefore
`install_snapshot` must seek/read the **actual passed handle**, copying into its
own staged image as needed; never rename an assumed previously recorded receive
path. Fresh readable snapshot handles start at offset zero. Test real chunked
network transfer separately from this adapter suite.

## Shared database layout

Use `Arc<rocksdb::DBWithThreadMode<rocksdb::SingleThreaded>>` with fixed CFs.
SingleThreaded here restricts **CF creation/deletion**, not concurrent database
reads/writes; fixed CFs need no runtime CF lock/multi-threaded-cf feature. Open
CF descriptors once, then clone the DB Arc. Obtain borrowed CF handles inside
the blocking closure rather than storing references beside their owning Arc.
The unavoidable `default` CF remains empty; the four application CFs are:

| CF | Key structure / contents |
|---|---|
| `raft_log` | `group:u64be || index:u64be` -> serialized full Entry |
| `raft_meta` | `group:u64be || tag` -> vote, last-purged LogId, committed LogId |
| `state` | `group:u64be || 0x00` -> active-generation/applied/membership record; `group:u64be || 0x01 || generation:u64be || raw_key` -> value |
| `snapshot_meta` | `group:u64be || tag` -> current image descriptor and checked generation-allocation metadata |

Persist full LogId, not index alone. The state tag separates arbitrary byte keys
from application metadata. DELETE targets exactly the active-generation data
key. Use one sync WriteBatch for effects plus updated applied/membership record;
return one response per input entry, including blank and membership entries.
Persist committed identity too, as planned; keep that distinct from applied.

`get_log_state` derives the last log only from present log entries/purge marker,
never state-machine applied identity. Read the marker and log end coherently.
Purge batches combine range deletion and marker update; truncate does not advance
the purge marker. Upper prefix bounds must handle all-FF group IDs. For fixed
16-byte log keys, `group || [0xff; 8] || [0x00]` is a finite exclusive group end,
including maximum index without overflowing `index + 1`. Generic generation
prefix successor returns an Option and must never wrap into another group.
[RocksDB thread modes/CF handles](https://raw.githubusercontent.com/rust-rocksdb/rust-rocksdb/v0.25.0/src/db.rs).

## Ownership and blocking work

Keep LogStore and StateMachine non-Clone; group startup hands their unique writer
handles to one Raft instance. Only LogReader/SnapshotBuilder are clonable access
handles. A process group registry refuses duplicate active writer ownership.

The smallest implementation can await bounded `spawn_blocking` jobs rather than
introducing a second storage actor framework. Acquire a per-group log-write gate,
then a process I/O-lane permit **before spawning**. Move both owned permits into
the blocking closure, so cancellation of the async waiter cannot release them
while an uncancellable blocking write still runs. The same per-group gate covers
vote, append, committed, truncate and purge writes. Never acquire a global DB
mutex; readers do not take the log writer gate. Use bounded lanes that preserve
replication/control capacity when bulk snapshot jobs are active.

For append, move its callback into the write closure. After sync WAL completion,
call `callback.log_io_completed(Ok(()))`; errors use an I/O error callback and
return storage failure, never a success notification. Callback construction is
crate-private, so focused tests should use the suite or an actual Raft path,
not attempt `LogFlushed::new`. Callback failure after receiver shutdown does not
undo a persisted write.
[Exact callback method](https://raw.githubusercontent.com/databendlabs/openraft/v0.9.25/openraft/src/storage/callback.rs).

Snapshot scans and RocksDB read snapshots borrow the DB: create and finish them
inside an owning blocking closure. Avoid a self-referential builder holding both
an Arc and a snapshot borrowing that Arc. Capture the consistent database view
when build starts, and read its data/applied/membership together. Task 4's
publication gate then rejects stale builders without locking the whole scan.

Treat blocking JoinError, decoding corruption, and missing expected CFs as
contextual StorageError, not absent entries or successful no-ops. A direct
conversion without another error dependency is
`StorageError::from_io_error(subject, verb, std::io::Error::other(error))`;
choose the real vote/log/apply/snapshot subject. Storage errors fail the Raft
group closed and must not be translated to pre-submission Rejected.
[Storage error API](https://raw.githubusercontent.com/databendlabs/openraft/v0.9.25/openraft/src/storage_error.rs).

Compile checkpoint: pin the two engine versions, implement signatures with real
durable basic operations, run focused storage cases, then complete snapshot
methods and run the full suite. Adapter success is followed by the matrix's
public dual-durability-barrier test; it does not substitute for that test.

## Readability standard for this new module

Keep orchestration functions readable in execution order: validate/normalize,
reserve work, perform durable operation, publish outcome. Extract named domain
steps where a reader otherwise has to decode a long closure; retain local code
when extracting it only hides straightforward operations. Document ownership
transfers where they determine cancellation, durability, or cleanup behavior.

Explain existing traits as capability contracts: LogReader can read a group's
log, LogStore can persist its log/vote, StateMachine applies committed changes,
and SnapshotBuilder captures a consistent image. These are meaningful shared
interfaces. Do not add one trait per struct, inheritance-style hierarchies, or
generic backends merely to resemble Java. Readability changes introduce no extra
cloning, locks, tasks, or allocations. This standard applies only to
`crates/rockbee-raft-kv`; existing Rockbee remains untouched.
