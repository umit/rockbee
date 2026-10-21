# Storage: the durable side of a Rockbee Raft node

This directory implements the durable storage that OpenRaft needs for one node.
It is not a separate Cargo crate or a second service. It is a Rust module tree
inside `rockbee-raft-node`; the public entry point is
[`storage/mod.rs`](mod.rs). Rust `mod` files are namespace and visibility
boundaries, roughly comparable to Java packages plus package-private access.

The implementation currently uses RocksDB. The public API deliberately does
not expose RocksDB types: callers open a `Storage`, open one `GroupStores` pair
per Raft group, and pass its `LogStore` and `StateMachine` to OpenRaft.

```text
public client PUT / DELETE
        |
        v
cluster service -> OpenRaft leader -> private peer RPC -> follower OpenRaft
        |                   |                                 |
        |                   +------ LogStore ------------------+
        |                               |
        |                               v
        |                        StateMachine apply
        |                               |
        v                               v
response after Raft commit       Group runtime -> RocksDB
                                                + snapshot files
```

For a Java mental model, `Storage` is the node-level resource owner,
`GroupStores` is the pair of adapters injected into one Raft instance, and a
`Group` is the private, shared owner behind that pair. `Arc` is shared,
thread-safe ownership; it keeps the database and group resources alive while
asynchronous work still owns them.

## Start here

1. [`mod.rs`](mod.rs) is the small public facade and its stable exports.
2. [`rocksdb/mod.rs`](rocksdb/mod.rs) explains the backend packages and owns
   `Storage` and `GroupStores`.
3. [`../cluster/runtime/mod.rs`](../cluster/runtime/mod.rs) opens a group and
   supplies the two stores to `Raft::new`.
4. [`../cluster/service/mod.rs`](../cluster/service/mod.rs) is the public
   operation layer: it chooses a group, proposes mutations, and establishes
   authority for strong reads.

The most important separation is this:

| Question | Owner |
| --- | --- |
| Is this mutation part of the replicated Raft history? | OpenRaft and `raft/log.rs` |
| Is it committed by a voting quorum? | OpenRaft and the cluster service |
| Which local RocksDB records are durable? | `engine/group.rs` |
| What does this node's applied state currently contain? | `raft/state.rs` |

Local storage does not independently make a distributed operation linearizable.

## Package map

`storage/` has two public-facing pieces and one private backend package.

| Location | Responsibility |
| --- | --- |
| [`options.rs`](options.rs) | `StorageIdentity`, open mode, cache/buffer/I/O limits, and validation. |
| [`rocksdb/engine`](rocksdb/engine/mod.rs) | Actual RocksDB handle, startup checks, shared budgets, blocking execution, and synchronous batch persistence. |
| [`rocksdb/raft`](rocksdb/raft/mod.rs) | OpenRaft log-reader, log-storage, and state-machine adapters. |
| [`rocksdb/format`](rocksdb/format/mod.rs) | Versioned postcard envelope, physical key layout, and bounded entry batching. |
| [`rocksdb/control`](rocksdb/control/mod.rs) | Group-zero metadata state and durable local movement seals. |
| [`rocksdb/snapshot`](rocksdb/snapshot/mod.rs) | Snapshot image files, descriptors, installation, generation activation, and startup recovery. |
| [`rocksdb/test_support`](rocksdb/test_support/mod.rs) | Test-only fault and scheduling hooks. |
| [`rocksdb/tests`](rocksdb/tests/mod.rs) | Backend regression scenarios; production code does not depend on them. |

The `rocksdb` module is private implementation detail. Reexports from
[`mod.rs`](mod.rs) keep callers on `crate::storage::{Storage, LogStore,
StateMachine, ...}` rather than coupling them to the file layout.

## Opening one node database

`Storage::open` validates [`StorageOptions`](options.rs), then moves the native
open work to a blocking worker. [`engine::open_database`](rocksdb/engine/mod.rs)
creates one `Arc<Shared>` containing the actual RocksDB `Db`, the path and
identity, three I/O semaphores, the block cache, and the write-buffer manager.
All Raft groups on this node share those resources.

There are two explicit modes:

- `CreateNew` accepts only a missing or empty directory, creates the database,
  and writes the node identity.
- `OpenExisting` first requires exactly these column families:
  `default`, `raft_log`, `raft_meta`, `state`, and `snapshot_meta`; it then
  reads and compares the stored cluster, node, and shard identity.

The default column family is required to remain empty. The named column
families separate Raft history and metadata, state-machine records, and
snapshot descriptors. Physical keys are defined in
[`format/keys.rs`](rocksdb/format/keys.rs); group IDs prefix the relevant keys,
so one node database can host multiple groups.

The default node-wide budgets are 64 MiB block cache, 32 MiB RocksDB write
buffer budget, two log lanes, two state lanes, one snapshot lane, 4 MiB chunk
budget, 128 entries per chunk, and a 2 MiB maximum encoded entry. They are
defaults, not promises of total process memory use. `options.rs` rejects unsafe
or arithmetic-overflowing configurations before opening the database.

## What a PUT acknowledges

The public service proposes a `Command::Put` to the selected group's Raft
leader. OpenRaft appends the entry through
[`raft/log.rs`](rocksdb/raft/log.rs), where the adapter durably writes the local
Raft log batch. This is a local log-I/O acknowledgement; it is not yet proof
that the value is committed or visible in the state machine.

Raft replication and commit are cluster work. In a normal three-voter group,
a majority is two voters, so a committed entry needs the leader and one follower
and does not wait for every node. Membership changes can alter that quorum; the
storage layer does not calculate it. After OpenRaft commits and applies the
entry, [`raft/state.rs`](rocksdb/raft/state.rs) writes the key/value and the
new applied-state record together in one RocksDB batch. Only then does the
local state-machine read path observe the new value.

`DELETE` follows the same Raft path. Its state-machine batch deletes the
generation-qualified key and persists the advanced applied record atomically.
There is no gateway key/value cache in this path.

## Reads and consistency

`StateReader::read_local` takes a RocksDB read snapshot, reads the state record
and key from that one snapshot, and returns a coherent local view containing
the applied LogId and generation. It is useful after an authority decision or
for explicitly local inspection.

It is not a read-index or quorum barrier. A reader can lag the writer, and a
local read alone cannot claim current cluster state. The service's strong-read
path calls `ensure_linearizable` before using that local view; see
[`cluster/service/mod.rs`](../cluster/service/mod.rs). Keep the two questions
separate: storage provides a coherent local snapshot, while Raft establishes
cluster authority.

## Blocking work, admission, and cancellation

[`engine/group.rs`](rocksdb/engine/group.rs) is the concurrency-critical file.
`reserve` obtains the per-group write gate and returns a `WriteSession`.
`read` and `write` acquire a bounded lane before moving native RocksDB work into
`spawn_blocking`. A write closure retains its lane permit, group `Arc`, and gate
permit until the native operation ends, even if the async caller is cancelled.

Dropping an incomplete `WriteSession` marks the group retired. Later writes are
rejected until the group is reopened and recovered; Rockbee intentionally does
not automatically replay an uncertain mutation. `persist` also rejects batches
over the configured chunk budget before issuing the native write.

Every adapter mutation reaches `Group::persist`, which uses RocksDB
`WriteOptions::set_sync(true)`. Identity initialization uses the same policy.
This requests a synchronized WAL write before RocksDB reports that write
successful. It is not the same operation as waiting for SST compaction, and it
does not by itself prove distributed Raft commit or cover a process crash beyond
the guarantees of RocksDB and the surrounding filesystem.

## Snapshots are files plus database metadata

A RocksDB read snapshot is an in-process, consistent read view used while
scanning state. A Rockbee Raft snapshot is different: it is a streamed image
file under the storage directory plus a descriptor stored in `snapshot_meta`.

Snapshot construction reserves an image sequence, scans a stable state view,
syncs the image file, and only then publishes its descriptor. Installation
validates the supplied image and identity, imports into a new state generation,
and atomically activates that generation. Startup recovery validates
authoritative descriptors and cleans only orphaned artifacts after validation.
See [`snapshot/mod.rs`](rocksdb/snapshot/mod.rs) and
[`snapshot/format.rs`](rocksdb/snapshot/format.rs).

## Control metadata and wire bounds

Group zero is the control/metadata Raft group. Its metadata commands use the
same replicated log and state-machine mechanism as opaque user values, but
[`control/metadata.rs`](rocksdb/control/metadata.rs) applies their related
records as one bounded, synchronous RocksDB batch. Local movement seals live in
`raft_meta`; they are durable local admission evidence, not a distributed fence
or permission to accept a successor job.

Before a metadata command is sent to a peer, the bounded wire layer also sizes
its possible Raft append envelope; see
[`rpc/wire/message.rs`](../rpc/wire/message.rs). Storage validates the encoded
entry and batch independently. These are complementary size checks at different
boundaries, not duplicated consistency protocols.

## Tests and safe claims

The storage backend has focused tests for open modes, group ownership, bounded
batches, failed and abandoned writes, log callbacks, state application,
snapshots, metadata, and movement seals. Existing verification before the wider
package reorganization included 54 storage tests and 427 node/workspace tests;
those results are historical evidence, not a substitute for rerunning checks
after later changes.

When changing this directory, preserve the paired `LogStore`/`StateMachine`
ownership, `sync=true`, bounded lanes, `WriteSession` retirement, snapshot
file-before-descriptor ordering, and the distinction between local state and
Raft-backed cluster authority.
