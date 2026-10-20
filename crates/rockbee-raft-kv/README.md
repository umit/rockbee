# Rockbee Raft KV

An independent Rust workspace inside the Rockbee repository for a developing
Multi-Raft key-value database using OpenRaft, RocksDB, and a dedicated gateway.

Location in the repository: `crates/rockbee-raft-kv/`.

**Status: first real client-to-database path verified locally.** The public client
can PUT, GET and DELETE through a gateway into three OpenRaft/RocksDB nodes with
four data groups. Runtime tests also cover leader shutdown, surviving-quorum
reads/deletes, disk reopen and replica catch-up. Node and gateway entry points
now start their actual runtimes. A three-process test also preserves an
acknowledged value after killing all node processes and reopening their existing
databases. A blank fourth node now receives a streamed snapshot plus later logs
and preserves them on offline reopen. A finite ABC-to-ABD replica move is also
verified: after D becomes the actual leader, the same running gateway and client
refresh routing and complete GET, PUT and DELETE.

Movement currently permits **one immutable lifetime target per data group**;
metadata voters and sibling groups stay unchanged. General repeated moves and
the remaining partition/crash schedules are still in progress.

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
  docs/
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

Run from this directory:

```sh
cargo fmt --all -- --check
cargo test --locked --offline -p rockbee-raft-protocol
cargo clippy --locked --offline -p rockbee-raft-protocol --all-targets -- -D warnings
```

These checks exercise the implemented protocol without compiling the native
storage engine. Node integration checks are tracked in the progress record.
The parent workspace continues to build its own packages independently.

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

The agreed direction and correctness requirements are recorded in
[the design](docs/design.md). The work completed in this stage is tracked in
[the workspace plan](docs/workspace-plan.md). The next stages and their acceptance
tests are in [the implementation plan](docs/implementation-plan.md);
[progress](docs/progress.md) distinguishes implemented and verified behavior.
