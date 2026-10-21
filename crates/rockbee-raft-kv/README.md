# Rockbee Raft KV

An independent Rust workspace inside the Rockbee repository for a developing
Multi-Raft key-value database using OpenRaft, RocksDB, and a dedicated gateway.

**Inspired by Meta's ZippyDB architecture.** Rockbee explores this direction with
shard-aware gateway routing, independent OpenRaft groups, and RocksDB-backed
storage nodes.

Location in the repository: `crates/rockbee-raft-kv/`.

**Status: a working distributed KV foundation, verified locally.** The public
client, gateway, OpenRaft groups, and RocksDB storage run together. The following
records implemented capabilities and the scope of the checks already completed.

## Implemented and locally verified

```text
RockBeeRaftClient -> Gateway -> key's Raft group
                              -> leader + two replicas -> RocksDB on each node
```

| Capability | Implemented behavior |
| --- | --- |
| Public KV API | The Rust client performs opaque byte PUT, GET, and DELETE through the gateway into real nodes. JSON can be stored as bytes; the KV path does not interpret its fields. |
| Multi-Raft routing | Nodes host multiple independent Raft groups; leadership belongs to each group. The gateway routes keys to their group and leader. The local complete-path fixture uses three nodes and four data groups. |
| Durable writes and strong reads | `Committed` means a durable consensus acknowledgement. GET uses an OpenRaft read barrier: after an acknowledged write, a later successful read observes that write or a later committed change. Quorum loss may return an error. |
| Bounded asynchronous client | PUT and DELETE return eager completion handles. Dropping a handle does not cancel admitted work. Request IDs correlate out-of-order replies; request-count and byte limits bound pipelining across client clones. |
| Explicit write uncertainty | Outcomes distinguish `Committed`, `NotSent`, `Rejected`, and `Unknown`. A lost response may leave a committed mutation `Unknown`; the client does not automatically reconnect or replay it. Request IDs are correlation IDs, not exactly-once tokens. |
| Recovery and replica catch-up | Local tests cover leader shutdown, surviving-quorum reads/deletes, reopening all three killed node processes with acknowledged data intact, and snapshot plus log catch-up to a blank fourth node. |
| First replica movement | A finite `ABC -> ABD` move is verified. D becomes the actual leader, and the same running client and gateway refresh routing and complete GET, PUT, and DELETE. |
| Logs and client statistics | Client, transport, gateway, Raft adapters, and storage emit `log` messages without key/value payloads. `stats()` and `stats_handle()` expose fixed operation lifecycle counters, including uncertain outcomes. |
| Foundations for a second move | Durable successor preparation, predecessor sealing, activation validation, and actual data-log fence receivers are implemented and tested. These are building blocks; an actual second move is still unfinished as described below. |

The [client guide](crates/rockbee-raft-client/README.md) explains API guarantees.
The [runtime tests](apps/rockbee-raft-node/tests/runtime.rs) and
[process tests](apps/rockbee-raft-node/tests/processes.rs) exercise the public
path and owned node processes.

### Recorded verification checkpoint

At local commit `84a40d7` on **2026-10-21**, the recorded full workspace run had
**402 passed, 0 failed, and 1 manual soak test ignored**. Formatting, strict
Clippy in both checked feature configurations, and the diff check passed.
The local evidence files are `reports/m4b-receiver-evidence.json` and
`reports/m4b-receiver-workspace-final.log`.

Completed checks also include real TCP reply loss, selected process-crash and
disk-reopen scenarios, and a bounded test-only history checker for detecting
inconsistent reads and missing committed effects in recorded histories.
Receiver tests cover a disconnected caller while admitted sealing work drains
and completes, plus rejection of stale predecessor work.

An earlier **600-second local soak** at commit `3868e7f` completed **1,314,816
successful operations with zero reported errors or wrong values**, using three
independent clients, one gateway, and three node processes. It used a debug
build, four data groups, 12 fixed keys, and 1 KiB values in a warm, closed-loop
workload. The local report is `reports/soak/stable-SocGPQ/report.md`. This records
that workload's correctness; maximum capacity and long-running fault coverage
remain open below.

## Current limitations and open work

This is a locally verified development workspace. The following limits apply
specifically to `rockbee-raft-kv`; the separate SlateDB/DuckDB-based Rockbee
workspace has its own features and consistency guarantees.

| Area | Current limitation |
| --- | --- |
| Repeated replica movement | The public movement API supports one immutable lifetime target per data group, with `ABC -> ABD` verified. Durable successor preparation, predecessor sealing, data-log fences, and activation validation are implemented, but the coordinator for an actual second move (`ABD -> ACD`) is not connected end to end. Metadata voters and sibling groups remain unchanged. |
| Automatic cluster management | Adding a node does not automatically redistribute data. Automatic shard splitting, placement, and rebalancing are not implemented. |
| Data API | The Raft client supports opaque byte PUT, GET, and DELETE. TTL, SQL, a JSON table API, and multi-key transactions are not implemented in this workspace. |
| Storage | RocksDB requires local disk on the storage nodes. This workspace does not provide a diskless or S3-backed durable storage engine. |
| Production readiness | TLS/authentication and complete operational tooling are not implemented. Broader network-partition and disk-failure coverage, plus long-running soak and resource validation, remain incomplete. Existing logs, client counters, and local recovery tests cover only part of this work. |
| Capacity and performance | A 500 GB deployment, maximum throughput, and latency under representative production load have not been validated. Local functional tests and the completed local soak runs do not establish those capacity or performance limits. |
| Shared admission | Native write capacity is no longer divided by the hosted-group count. Existing server budgets still apply across connections. A group waiting for quorum can fill that shared capacity and prevent sibling requests from completing; group fairness is not implemented. A disconnected mutation may return `Unknown` and must not be automatically replayed. |

## Packages

| Package | Responsibility | Kind |
| --- | --- | --- |
| `rockbee-raft-client` | Public byte-key/value API and bounded client connection | Library |
| `rockbee-raft-protocol` | Wire messages, framing, and explicit mutation outcomes | Library |
| `rockbee-raft-gateway` | Client admission, shard routing, shared node connections | Application |
| `rockbee-raft-node` | Multiple Raft groups, RocksDB, snapshots, cluster control | Application |

```text
rockbee-raft-kv/
  Cargo.toml
  Cargo.lock
  crates/
    rockbee-raft-client/
    rockbee-raft-protocol/
  apps/
    rockbee-raft-gateway/
    rockbee-raft-node/
  configs/
```

The implemented request path is:

```text
Client -> Gateway -> Shard leader -> OpenRaft replicas -> RocksDB
```

Each node hosts several independent Raft groups. Leader and follower are roles
within each group. The gateway routes to the owning group and acknowledges a
mutation only after the node has met its durable completion contract.

The module has its own Cargo workspace, lockfile, and `target/` directory. It
does not import existing Rockbee packages. OpenRaft and RocksDB are node-only
dependencies. The repository's Rust toolchain applies to this directory as well.

## Reading the protocol

Start with the [protocol guide](crates/rockbee-raft-protocol/README.md). It explains
the package structure, Rust concepts through a Java lens, macros, a request's
lifecycle, mutation certainty, and the behavior tests. The runnable example is
in `crates/rockbee-raft-protocol/src/lib.rs`.

For the database application, start with the [node reading guide](apps/rockbee-raft-node/README.md).
It maps the process entry point, service, OpenRaft engine, storage and peer
adapters to their responsibilities, with Java comparisons where useful.
The [client guide](crates/rockbee-raft-client/README.md) explains eager mutation
handles, strong reads, outcome certainty and connection ownership.

`identity/` keeps identifiers distinct; `kv/` describes PUT/GET/DELETE and results;
`message/` adds IDs, versions, time budgets and failures; `codec/` handles frames;
`routing/` maps keys to logical groups and describes the initial directory;
`backend/` keeps node operations separate from public KV; `transport/` owns the
bounded connections and server.

```text
KV request -> request envelope -> frame bytes
frame bytes -> decoded envelope -> validation -> service -> OpenRaft
```

## Logs

Client, gateway, transport, Raft adapters and storage emit messages through the
`log` facade. Node and gateway executables initialize `env_logger` once, with
`info` as the default level. Library users choose and initialize their own logger.
There is no tracing subscriber or OpenTelemetry setup.

Set `RUST_LOG` before starting the process:

```sh
# Lifecycle and failures for the Raft application.
RUST_LOG=info,rockbee_raft_node=debug,rockbee_raft_gateway=debug

# Frame/request details for a focused transport investigation.
RUST_LOG=info,rockbee_raft_client=trace,rockbee_raft_protocol::transport=trace
```

DEBUG records connection and failure context. TRACE adds request IDs, frame
lengths and operation stages. IDs belong to individual connections; they are not
distributed trace IDs. Keys, values and snapshot contents are not logged.
Detailed logging writes to stderr and can affect timings, so use the narrowest
module filter needed for an investigation. The manual soak initializes the
in-process client/gateway logger and its child nodes inherit `RUST_LOG`.

For a client application, add `env_logger` to that application's dependencies
and initialize it before connecting:

```rust
env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
    .format_timestamp_micros()
    .init();
```

## Local checks

For a local cluster, use the [TOML configuration guide](configs/README.md).
It includes files for three nodes and a gateway, plus explicit create, restart,
and fourth-node join commands. Existing inline CLI settings remain supported.

Run from this directory:

```sh
cargo fmt --all -- --check
cargo test --locked --offline -p rockbee-raft-protocol
cargo clippy --locked --offline -p rockbee-raft-protocol --all-targets -- -D warnings
```

These checks exercise the implemented protocol without compiling the native
storage engine. The parent workspace continues to build its own packages
independently.

The first complete-path test is
`apps/rockbee-raft-node/tests/runtime.rs`. Run it from this workspace with
`cargo test --locked --offline -p rockbee-raft-node --test runtime`.
It creates only its own temporary database directories and localhost listeners.

On macOS, RocksDB's native C++ build may need the active Apple SDK's standard
library headers. If compilation reports missing `cstdint`, the verified local
invocation is:

```sh
CXXFLAGS="-isystem $(xcrun --sdk macosx --show-sdk-path)/usr/include/c++/v1" \
  cargo check --locked --offline -p rockbee-raft-node --all-targets
```

This supplies build headers; it does not change runtime or durability settings.

Development plans and detailed verification notes are kept locally outside
version control. The capability, checkpoint, and limitation sections above
record the published scope of this workspace.
