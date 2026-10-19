# Initial architecture advice

Status: design review only, 2026-10-19. No implementation or builds performed.

```text
Existing: Client -> Gateway -> Writer/Readers -> SlateDB -> object storage
Planned:  Client -> Gateway -> data-group leader <-> voters -> RocksDB
                           ^
                     metadata group
```

## Decisions to carry into implementation

1. Keep `openraft = "=0.9.25"` with `serde` and `storage-v2`, and
   `rocksdb = "=0.25.0"`, confined to the node package. Check the actual locked
   features in the first compile spike. The RocksDB wrapper declares Rust 1.88
   minimum and native build dependencies; no claim of a successful local build
   follows from selecting its version.
   [Versioned manifests](https://raw.githubusercontent.com/databendlabs/openraft/v0.9.25/openraft/Cargo.toml),
   [RocksDB manifest](https://raw.githubusercontent.com/rust-rocksdb/rust-rocksdb/v0.25.0/Cargo.toml).

2. Use one RocksDB database per process with fixed `raft_log`, `raft_meta`,
   `state_meta`, and `data` column families. Prefix keys with fixed-width group
   IDs; log indexes use big-endian encoding. Data keys additionally include an
   active generation. Encode prefix upper bounds explicitly and test the maximum
   group ID. Persist cluster ID, stable node ID, storage format, shard count,
   and hash version; reject mismatched restarts before starting Raft.

3. Every durable write uses WAL and synchronous write options. Apply each
   bounded batch of committed entries as one WriteBatch containing data effects,
   applied LogId, and membership changes. Blank entries advance applied identity
   too. Do not return application success until this batch succeeds. Application
   batches may include multiple committed entries, but admission responses and
   order remain per operation. RocksDB sync is disabled by default, so this is
   an explicit correctness setting.
   [Versioned write options](https://raw.githubusercontent.com/rust-rocksdb/rust-rocksdb/v0.25.0/src/db_options.rs).

4. Serialize log/vote writes within each group, including append, truncate, and
   purge; do not introduce a cross-group application lock around RocksDB. Save
   votes durably before returning; invoke append's LogFlushed callback only
   after durable completion. Persist purge boundary and deletion together.
   Execute synchronous RocksDB calls on bounded blocking work, with capacity
   available for replication/control. The API permits an append callback before
   method return, allowing a simple synchronous-durability-first implementation.
   [OpenRaft storage contracts](https://raw.githubusercontent.com/databendlabs/openraft/v0.9.25/openraft/src/storage/v2.rs).

5. GET calls `ensure_linearizable()` under the request deadline. This version
   already performs the quorum check and applied-index wait. Afterward capture
   one RocksDB snapshot; read generation pointer, applied identity, and value
   from that same view. Require its applied identity to cover the returned read
   barrier. Never select a generation before the barrier and then read it after
   an installation. Use the same read rule for metadata bootstrap.
   [Versioned read implementation](https://raw.githubusercontent.com/databendlabs/openraft/v0.9.25/openraft/src/raft/mod.rs).

6. Keep snapshots file-backed and streaming. The default SnapshotData contract
   accepts async readable/writable/seekable data; a Tokio file fits without the
   generic-snapshot-data feature. A memory Cursor containing the entire shard
   does not fit the larger-than-memory requirement.
   [Versioned type contract](https://raw.githubusercontent.com/databendlabs/openraft/v0.9.25/openraft/src/type_config.rs).

## Snapshot publication protocol

Use separate identities for an immutable snapshot image and an active state
generation. A snapshot builder scans only its group's prefix through one
consistent RocksDB snapshot, including applied identity and membership from that
same view. Building an image never changes the active data generation.

For installation: validate cluster/group/format and embedded metadata, lengths,
and checksum; fsync the complete image, rename it, and fsync its parent
directory. Import its data into a fresh, hidden generation through bounded,
durable chunks. Then synchronously publish one metadata batch containing active
generation, applied identity, membership, and current snapshot descriptor.
Only this final batch exposes the replacement. Return install success afterward.
An older concurrent builder cannot replace a newer snapshot descriptor: serialize
publication per group and compare covered log identity before publishing.

GETs and builders must pin a coherent view while old generations are collected.
Snapshot transfer handles must remain valid through replacement. Orphan images
and inactive staged generations are cleanup candidates, never inferred active
state. Validate recovery at the image-fsync, rename/directory-fsync, data-import,
metadata-publication, and old-generation-cleanup boundaries. These are proposed
application invariants; ordinary process-kill tests do not establish actual
power-loss behavior.

## Transport and admission details that cannot be deferred

- A length-prefixed binary public protocol is suitable. Freeze version,
  maximum frame/key/value lengths, request-ID scope, and decode-error behavior
  before implementation. Reject oversize lengths before payload allocation.
  Preserve empty and non-UTF-8 keys/values. Client IDs must not collide in the
  gateway's multiplexed backend space: use a connection-scoped correlation key
  or a distinct backend ID with explicit mapping.
- Keep peer RPCs on a separate listener and independently bounded resources.
  Validate cluster, group, sender, recipient, and configured identity before
  dispatch. A single TCP stream carrying large snapshot chunks can still block
  heartbeats; give bulk transfers separate bounded connections or scheduling
  capacity. Trusted local deployment does not make these checks authentication.
- The eager PUT method must synchronously attempt bounded admission and return
  a completion handle; polling that handle must not initiate submission.
  Admission must bound count and bytes. A full queue completes as NotSent.
  NotSent belongs to the originating transport boundary; a gateway that received
  a request but did not forward it returns an explicit pre-submission Rejected.
  After possible Raft submission, lost responses and generic leadership errors
  remain Unknown. Do not retry Unknown.
- Node admission is required in addition to gateway/client limits. OpenRaft has
  unbounded internal API channels. An owned node worker must retain its request
  and byte reservation while an external waiter times out or disconnects, until
  the actual Raft result arrives. Never free the permit because an outer timeout
  synthesized Unknown. Fatal errors stop group admission and require shutdown
  reconciliation; a dropped responder can report Fatal even while its core is
  still running. Separately bound replication payload sizes and peer inflight
  work; admission alone does not prove a process RSS cap.
  [Core request lifecycle](https://raw.githubusercontent.com/databendlabs/openraft/v0.9.25/openraft/src/raft/raft_inner.rs).

## Smallest correct milestone

First prove the storage adapter with two group prefixes, sync apply/reopen,
snapshot install/reopen, and OpenRaft's adapter suite. This is an internal spike,
not the first working database.

The first working end-to-end milestone retains the requested shape: one public
client, real gateway, three node processes, four three-voter data groups, and a
metadata group. Placement is initially static. Use a designated explicit
bootstrap operation with a fixed initial membership; ordinary restart never
initializes a replacement group. The metadata group serves a linearizable
cluster/shard-map record so gateway startup follows the existing design.

Acceptance for this milestone: PUT/GET/DELETE through the public path; restart
and leader failover preserve successes; minority rejects strong operations;
response loss reports Unknown without replay; a stalled group cannot consume
all other groups' admission capacity; snapshot installation remains invisible
until publication; deadline storms and slow consumers stay within configured
request/byte limits. Exercise multiple groups concurrently, not only sequentially.

The fourth-node movement controller and interrupted-move recovery remain a later
milestone, explicitly unfinished. Therefore this first milestone must not be
called completion of all acceptance criteria in design.md.

## Review exchange

The verifier agreed with generation publication and required one coherent GET
view plus deterministic install/GET concurrency tests. We agreed that timed-out
requests must not release node admission while their actual Raft operation is
still pending, and that independent replication buffering needs its own limits.
Shared RocksDB intentionally shares WAL/compaction/device contention: interpret
group independence as no application-wide lock or exhausted admission lane, not
hard disk-failure or latency isolation.
