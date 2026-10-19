# Rockbee Raft KV: Multi-Raft and gateway design

Status: package scaffolding; database behavior is design only.
No Raft cluster, storage engine integration, or network service has been
implemented. This document records the agreed direction and proposed
implementation defaults; it is not a completed database implementation plan.

## Goal and agreed scope

Build an independent Rust KV system whose data can exceed its memory budget,
whose successful mutations survive a replica failure, and whose reads are
linearizable within the owning shard. Use opaque byte keys and values with PUT,
GET, and DELETE.

The user selected these requirements:

- A module named `rockbee-raft-kv`, independent of existing Rockbee components.
- OpenRaft for consensus and RocksDB for local persistent storage.
- Multiple Raft groups from the first working version.
- A ZGateway-like gateway as part of that first version.

Local SSD replicas are the primary storage in this design. Object storage may
later hold backups; it is not on the initial read/write path. This deliberately
differs from the existing SlateDB/object-storage architecture. No performance
parity with Redis, Couchbase, TiKV, or ZippyDB has been established.

## Existing application and isolation

The existing application remains a separate system:

```text
Client -> Gateway -- PUT/DELETE --> Writer  -> SlateDB -> Object storage
                  `-- GET ------> Readers -> SlateDB cache/view
```

User-selected location: `crates/rockbee-raft-kv/` inside the existing repository,
with its own Cargo workspace,
lockfile, tests, and build directory. Do not add it to the parent workspace or
depend on existing Rockbee client, protocol, runtime, gateway, reader, or writer
crates. Parent Cargo commands must not start building this experiment.

Use four Cargo packages inside that workspace. This is the proposed package
layout following the user's request to explain the new submodules:

```text
rockbee-raft-kv/
  Cargo.toml                  # independent workspace, four members
  Cargo.lock
  crates/
    rockbee-raft-client/       # public byte-KV API and bounded client transport
    rockbee-raft-protocol/     # public wire messages, framing, error outcomes
  apps/
    rockbee-raft-gateway/      # public listener, routing, pools, admission
    rockbee-raft-node/         # Raft groups, storage, replication, cluster control
  configs/                    # local gateway and storage-node configurations
  docs/                       # operation and protocol documentation
```

The client and protocol are library packages. Gateway and node each have a small
binary entry point and an internal library to support focused integration tests.
Client examples demonstrate the full public API; end-to-end tests live with the
gateway package and launch uniquely configured, owned node processes.

Dependencies point from client, gateway, and node to the small public protocol
package. The gateway uses its own bounded backend transport; it does not depend
on the public client or import the node implementation. OpenRaft and RocksDB are
node-package dependencies. Raft-internal network messages and snapshot transfer
belong to the node's private peer protocol, so compiling the client or gateway
does not compile either engine. Public protocol metadata may carry shard IDs and
validated leader hints without exposing library-specific Raft types.

Inside the gateway, use ordinary Rust modules for routing, backend connections,
admission, and metrics. Inside the node, use modules for group lifecycle,
Raft storage adapters, KV application, snapshots, and cluster placement control.
The metadata leader runs the controller inside the node process; there is no
separate controller service or crate initially. Nodes assume leader/follower roles
per group, so separate permanent writer and reader applications are unnecessary
for this experiment. The storage and Raft modules are not independently swappable
plugin frameworks. Public naming and transport encoding remain plan decisions.

Preserve the useful API contracts: eager PUT submission with a completion handle,
bounded multiplexing, durable mutation acknowledgements, and explicit
NotSent/Rejected/Unknown outcomes. A request ID correlates responses; it does not
imply persisted deduplication or exactly-once execution.

## Initial topology and responsibilities

Proposed local acceptance topology: one client, one gateway, three storage-node
processes, four data Raft groups, and one small metadata Raft group. Each group
initially has three voters. These are logical groups sharing the same three
processes, not fifteen separate servers. Four data groups exercise Multi-Raft
without assuming that 256 is an appropriate default.

```text
Client
  |
  v
Gateway: admission -> shard lookup -> bounded backend connections
  |
  +-> Group 0 leader <----> its voting replicas
  +-> Group 1 leader <----> its voting replicas
  +-> Group 2 leader <----> its voting replicas
  `-> Group 3 leader <----> its voting replicas
          |
          v
     RocksDB on each hosting node

Metadata group -> cluster identity, shard map, placement jobs
                      |
                      `-> gateway routing updates and placement controller
```

The metadata group is a control-plane proposal, not an extra consensus round on
each user operation. Gateways keep bounded routing metadata and refresh it on
version changes or routing errors. Group membership and read authority are
verified by the destination Raft group, never granted by a gateway's cached map.

At startup, a gateway must obtain a linearizable metadata read from configured
seed nodes and validate its configured cluster identity. If that is unavailable,
startup fails closed; a persisted map alone is not sufficient. After startup,
metadata unavailability can leave the gateway serving already-known groups from
its in-memory map, subject to each data group's own authority checks. New group
discovery and placement changes pause until metadata quorum is available.

A key maps to a stable logical shard independent of the number of nodes. Each
logical data shard maps to exactly one data Raft group in this version; the
metadata group uses a separate reserved identity. There is no microshard-to-group
remapping layer. The
hash algorithm, seed, and shard count are versioned cluster configuration. A
single-key request visits only its owning data group. Nodes host many groups
and may lead some while following others. Cross-shard transactions, arbitrary
resharding, JSON tables, SQL, and vector search are outside the first version.

## Gateway from the first version

The gateway is a mandatory executable in the initial end-to-end path. It owns:

- Public request framing, validation, deadlines, and bounded client admission.
- Key-to-shard lookup and routing to the group's current leader.
- Reusable, multiplexed node connections with bounded pending requests.
- Queue limits in both operation count and bytes, including slow-client output.
- Separate client-data and Raft/control traffic resource accounting. A client
  flood must not consume every slot needed for elections or replication.
- Metrics for queue wait, active requests, rejection, routing refresh, backend
  latency, and response delivery. Preserve request IDs through this path.

The gateway stores no durable user data and does not acknowledge a mutation
before the node's durable result. Its loss may leave a client's mutation outcome
Unknown even if that mutation committed. It must not automatically replay that
mutation. Reads and explicitly pre-submission routing rejections may be retried
within one deadline; generic transport failures are not proof of rejection.

The end-to-end deadline includes admission, connection, queueing, and response.
Before any request bytes are handed to the transport, a locally cancelled or
expired mutation may be NotSent. After that boundary, a missing final response
is Unknown unless a matching explicit rejection proves the server did not submit
it to Raft. A generic NotLeader/timeout observed after possible submission is
insufficient evidence. Cancellation or dropping a completion handle after
submission does not undo a mutation. A manually repeated Unknown mutation is a
new attempt and may be applied again, even with the same request ID; the initial
protocol provides correlation, not durable deduplication or exactly-once retry.

There is no gateway key/value cache in the first version. RocksDB owns data
caching. Routing metadata is distinct from cached user values.

The first deployment is a trusted local prototype. Identity checks must bind
cluster, group, and stable node IDs to internal messages; do not expose the
prototype publicly or claim tenant isolation before authenticated identity and
authorization exist. TLS, authentication, and tenant quotas need a separate
deployment design before use across an untrusted network.

## ZGateway-inspired mechanisms and their order

Meta's ZGateway article describes a managed proxy, connection reuse, bounded
batching, concurrent-read coalescing, admission control, and an optional cache
with an explicit bounded-staleness contract. Its reported throughput is across
a fleet; the connection-reduction percentages come from a model, and the 6%
overhead figure is computational overhead rather than request latency.
[Source](https://engineering.fb.com/2026/09/03/core-infra/zgateway-proxy-zippydb-meta/)

Take connection sharing, routing, bounded queues, and observability into the
first gateway. Measure optional batching and coalescing after the plain path
works; Multi-Raft and the gateway themselves are required from day one.

For a later measured batching experiment, keep per-operation order, deadlines,
outcomes, and acknowledgement guarantees. Set limits on count, bytes, wait time,
and total queued memory. Do not silently turn a batch into a transaction or
discard intermediate PUTs.

For read coalescing, group only compatible requests with the same key, namespace,
authorization context, and consistency requirement. Close the shared-read cohort
before acquiring its read barrier; later arrivals need a later read. This avoids
returning a result linearized before a new caller's request. Coalescing is local
to the serving gateway and does not remove the ordering limit on one hot write
key. These are our correctness requirements, not guarantees established by
Meta's article for arbitrary workloads.

## Durable writes and strong reads

PUT and DELETE go to the owning group's leader. The mutation is ordered in its
Raft log, durably replicated to a voter quorum, and applied on the leader before
success is returned. Network receipt or a buffered local append is insufficient.
The log/vote adapter must fulfill OpenRaft's persistence contract.

Durable means flushed to stable storage, including the required fsync-equivalent
barriers and crash-safe file publication; receipt into memory or the OS page
cache is insufficient. Before success, a quorum must have durable Raft log
entries and the leader's data effect plus applied identity must also be durably
published. Correctness must cover process crashes and host restart/power loss
under the storage device's persistence guarantees. The design tolerates one
replica's loss in a three-voter group; it does not survive destruction of every
durable copy. Actual machine power-loss validation is separate from local
process-kill tests.

Apply the data effect and the group's applied log identity atomically. Recovery
must produce a coherent combination of durable log, snapshot, membership, and
applied state. Keep deletion and log truncation safe across restart and replica
catch-up. TTL is deferred until its clock and expiry semantics are separately
specified; DELETE is included from the start.

GET initially goes to the leader and uses OpenRaft's linearizable read barrier,
then waits for the required applied state before reading RocksDB. A cached leader
hint, a local leader flag, or session affinity alone does not establish authority.
Lease optimizations and follower reads are later design decisions.

Snapshots must capture one group's consistent state and corresponding log and
membership metadata. Installation must be atomic to readers and recovery; partial
snapshots must not become visible. Truncate logs only once the recovery contract
permits it. The RocksDB layout and streaming snapshot format are left to the
implementation plan, subject to these invariants and group isolation.

## Adding a node and moving replicas

Keep replication factor three while adding a fourth storage node. An initial
operator-directed placement controller runs under the metadata-group leader;
automatic load-based balancing is deferred. Persist each movement's intended
old/new voter set and progress so a controller restart can reconcile it.

For each selected group, add the destination as a learner, bring it up to date
with a snapshot and log tail as necessary, and change membership through
OpenRaft's joint-consensus transition. Remove the old replica's local data only
after the final new membership is committed and removal is durably known. For
`A,B,C -> A,B,D`, the removed replica is C. Never bootstrap a replacement group
with the same identity to bypass this protocol.

Publish new placement after observing the committed membership. If the controller
loses an RPC response, inspect actual membership and job progress before resuming;
do not treat timeout as proof that nothing changed. Gateways tolerate stale maps
using validated routing hints; Raft membership remains authoritative. The new
node may host data groups without becoming a metadata-group voter.

Bound concurrent transfers and bytes in flight. Replica placement and leader
placement are separate concerns. Membership change alone does not guarantee that
the destination becomes leader or serves writes. Verify the selected OpenRaft
version's supported leadership controls before planning explicit transfers; do
not emulate transfer by bypassing consensus. Network partitions or leader changes
can produce temporary unavailability or Unknown mutation outcomes.

## Resource and performance requirements

Use process-level budgets for block cache, write buffers, request queues, and
snapshot traffic. Do not multiply a default cache allocation by the shard count.
Shared engine caches are not a hard process RSS cap: account separately for log
buffers, metadata, tasks, pinned blocks, allocator overhead, and other memory.
Storage sharing must keep group IDs, log ranges, snapshots, and cleanup isolated.

Retain normal runtime and TCP defaults until measurements justify tuning. Do not
add generic backend abstractions, custom allocators, or bespoke lock-free queues
for hypothetical speed. No gateway hop or batching benefit is assumed free.

Measure client-to-gateway-to-group latency (p50/p95/p99), successful throughput,
errors and Unknown outcomes, CPU, RSS, disk I/O, replication lag, queueing, and
compaction. Compare group counts under equal hardware, memory budgets, payloads,
durability, and read guarantees. Include uniform and hot-key workloads, warm and
cold reads, mixed mutations, overload, and a replica move. The M1 multi-process
setup establishes local behavior, not independent-machine failure resilience or
500 GB production capacity.

## Acceptance before calling the first version working

- Public client -> real gateway -> at least four data groups -> RocksDB works
  across three independent processes. All three KV operations cross that path.
- An unavailable group does not block requests to healthy groups with available
  quorum. No hidden global data-write serialization exists.
- Success followed by a strong GET observes that mutation or a subsequent one.
  Leader failover preserves successful writes and deletes.
- A minority partition cannot acknowledge new mutations or serve an unauthorized
  strong read. No response is sent before the declared durable boundary.
- Response loss after commit produces Unknown without an automatic mutation
  replay. Routing refresh must preserve this distinction.
- Restart and snapshot installation recover the correct data, membership, and
  applied state for every group; partial snapshot installation stays invisible.
- Adding a fourth node moves selected replicas safely while other traffic runs.
  Resume a move interrupted before or after membership commit.
- Saturation and a slow client keep pending operations and bytes bounded. Gateway
  disconnection and shutdown clean up only owned tasks/processes.
- Run OpenRaft's storage-adapter tests plus targeted integration and failure tests;
  passing library tests alone does not validate the whole system.

## Implementation decisions still to resolve

Pin a non-prerelease OpenRaft version after checking its APIs. The source reviewed
on 2026-10-19 distinguishes the 0.9.25 release from the 0.10 alpha line; do not mix
their examples. Final dependency versions, transport encoding, storage layout,
snapshot streaming, safe bootstrap procedure, and numerical resource limits belong
in the implementation plan and focused spikes. Any spike remains isolated and
must not mutate the existing Rockbee workload or storage.

Workspace scaffolding follows the [workspace plan](workspace-plan.md).
Implementation follows the [implementation plan](implementation-plan.md), with
verified milestones recorded in [progress](progress.md). No commit or push is
authorized.

## References

- [Meta ZGateway](https://engineering.fb.com/2026/09/03/core-infra/zgateway-proxy-zippydb-meta/)
- [Meta ZippyDB architecture](https://engineering.fb.com/2021/08/06/core-infra/zippydb/)
- [OpenRaft integration](https://docs.rs/openraft/latest/openraft/docs/getting_started/index.html)
- [OpenRaft read protocol](https://docs.rs/openraft/latest/openraft/docs/protocol/read/index.html)
- [OpenRaft membership changes](https://docs.rs/openraft/latest/openraft/docs/cluster_control/dynamic_membership/index.html)
- [OpenRaft versions](https://github.com/databendlabs/openraft#versions)
