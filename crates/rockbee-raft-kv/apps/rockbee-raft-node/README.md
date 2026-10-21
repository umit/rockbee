# Reading the storage node

OpenRaft is the consensus engine. This application connects it to Rockbee's
requests, TCP transport, and durable RocksDB storage. It does not implement a
second election, voting, quorum, or log-replication algorithm.

```text
main.rs
  -> parse settings
  -> NodeRuntime: open one RocksDB, start multiple OpenRaft groups
  -> optional cluster initialization
  -> wait for shutdown, close owned tasks

Client -> Gateway -> NodeService -> OpenRaft group -> RocksDB adapter
                                        |
                                        +-> peer adapter -> remote OpenRaft
```

## Where to start

| File | Responsibility | Familiar Java comparison |
| --- | --- | --- |
| [main.rs](src/main.rs) | Process startup and shutdown | Application entry point |
| [cli/mod.rs](src/cli/mod.rs) | Parse process arguments and load TOML files | Command-line options |
| [config/mod.rs](src/config/mod.rs) | Validate one node's settings | Configuration object |
| [cluster/runtime/mod.rs](src/cluster/runtime/mod.rs) | Own the database, listeners and Raft groups | Lifecycle/composition root |
| [cluster/service/mod.rs](src/cluster/service/mod.rs) | Route KV operations through consensus/read authority | Application service |
| [cluster/membership/](src/cluster/membership/mod.rs) | Bootstrap the directory and discover an existing cluster | Startup orchestration |
| [cluster/movement/](src/cluster/movement/mod.rs) | Execute replica-movement steps and observations | Workflow service |
| [control/mod.rs](src/control/mod.rs) | Define metadata commands and deterministic state transitions | Domain model |
| [raft/types.rs](src/raft/types.rs) | Define commands and OpenRaft associated types | Request types and interface type parameters |
| [storage/rocksdb/engine/](src/storage/rocksdb/engine/mod.rs) | Open RocksDB and schedule bounded native I/O | Database connection and I/O executor |
| [storage/rocksdb/raft/log.rs](src/storage/rocksdb/raft/log.rs) | Persist votes and Raft log entries | Durable log repository |
| [storage/rocksdb/raft/state.rs](src/storage/rocksdb/raft/state.rs) | Apply committed commands to RocksDB | State-machine persistence |
| [rpc/transport/client.rs](src/rpc/transport/client.rs) | Implement OpenRaft's network traits | Outbound transport adapter |
| [rpc/transport/server.rs](src/rpc/transport/server.rs) | Deliver peer messages to OpenRaft | Inbound transport adapter |
| [rpc/wire/](src/rpc/wire/mod.rs) | Bound and encode peer frames and DTOs | Network message format |

The [storage walkthrough](src/storage/README.md) follows database creation,
group ownership, PUT/GET/DELETE, durable writes and snapshots step by step.

## Package layout

```text
src/
  main.rs                 process entry point and shutdown
  cli/                    arguments, TOML input and parser tests
  config/                 node, join, resolved startup and validation
  cluster/
    runtime/              resource ownership and lifecycle
    service/              gateway-facing request handling and admission
    membership/           bootstrap and join discovery
    movement/             replica-movement execution and its tests
    tests/                group isolation and public response-loss tests
  control/                metadata commands, state and validation
  raft/                   OpenRaft type configuration and payloads
  rpc/
    wire/                 peer encoding and framing
    transport/            outgoing client and incoming server
    test_support/         owned relays and dispatch observations
  storage/
    rocksdb/              engine, Raft adapters, control, snapshot, format
  diagnostics/            opt-in incident evidence
tests/                    Cargo integration tests and process support
```

These are Rust modules inside one Cargo package. The `mod.rs` files introduce
their module and expose its API; they do not create extra objects, threads or
network hops. Public paths such as `rockbee_raft_node::storage::Storage` stay
stable while implementation files move underneath them.

These comparisons explain responsibilities, not inheritance. Rust `impl` blocks
define methods; traits describe capabilities required by OpenRaft. The
`declare_raft_types!` macro selects command, response, node, runtime, and snapshot
types. The macro does not hide application persistence or request handling.

## Follow one request

For **PUT**, read `NodeService::execute_kv`, then `mutate`. The service checks the
destination and whether it can reject before submission, then calls
`group.raft.client_write(command)`. OpenRaft coordinates replication and applies
committed entries through our `RaftStateMachine` implementation. Storage writes
data and applied progress durably before the successful response.

For **GET**, read `strong_read`. It calls OpenRaft's `ensure_linearizable`, then
reads the local RocksDB state and its applied position together. A follower's
local value alone is not a strong read. The gateway can route a rejected read to
another voter.

For **peer traffic**, the network trait implementation sends bounded messages.
The receiving adapter calls OpenRaft's `append_entries`, `vote`, or
`install_snapshot`. OpenRaft decides what those messages mean for consensus.

## Why there are adapters

OpenRaft deliberately leaves database persistence and network transport to the
application. Our adapters must preserve durable acknowledgements, bounded work,
full log identities, and shutdown ownership. Multi-Raft also needs one registry
of independent groups sharing the process's database and resource budgets.

The private wire DTOs bound collection decoding before allocation. They are a
transport boundary, not a second Raft model. `storage/mod.rs` exposes the storage
API, `storage/rocksdb/mod.rs` owns the group registry, and `storage/rocksdb/engine`
owns the native database and blocking I/O. The adapters describe what to persist;
OpenRaft decides which log entries are committed.

## Current verification boundary

The library runtime has passed a three-node test with four data groups: writes,
strong reads, leader shutdown, reads/deletes through the surviving quorum, and
disk reopen with replica catch-up. Metadata bootstrap and an identical repeated
bootstrap have also passed. See [runtime tests](tests/runtime.rs).

The public client has also passed PUT/GET/DELETE through a real gateway into
all four data groups. A second test starts three actual node binaries, obtains a
successful PUT, kills all three processes, reopens their existing databases,
and verifies the value through a new gateway/client. It then deletes the value
and verifies that every node exits successfully on SIGTERM. The gateway runs
inside that test process. See [process recovery tests](tests/processes.rs).

Snapshot and first replica-movement scenarios have dedicated suites alongside
their implementation. See the workspace's [capabilities and current limits](../../README.md)
for the distinction between verified first movement and unfinished repeated
movement coordination.
