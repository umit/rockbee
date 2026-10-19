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
| [cli.rs](src/cli.rs) | Parse process arguments | Command-line options |
| [config/mod.rs](src/config/mod.rs) | Validate one node's settings | Configuration object |
| [cluster/mod.rs](src/cluster/mod.rs) | Own the database, listeners and Raft groups | Lifecycle/composition root |
| [cluster/service.rs](src/cluster/service.rs) | Route KV operations through consensus/read authority | Application service |
| [cluster/bootstrap.rs](src/cluster/bootstrap.rs) | Publish the initial immutable directory | Startup orchestration |
| [raft/types.rs](src/raft/types.rs) | Define commands and OpenRaft associated types | Request types and interface type parameters |
| [storage/log.rs](src/storage/log.rs) | Persist votes and Raft log entries | Durable log repository |
| [storage/state.rs](src/storage/state.rs) | Apply committed commands to RocksDB | State-machine persistence |
| [rpc/network.rs](src/rpc/network.rs) | Implement OpenRaft's network traits | Outbound transport adapter |
| [rpc/server.rs](src/rpc/server.rs) | Deliver peer messages to OpenRaft | Inbound transport adapter |

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
transport boundary, not a second Raft model. `storage/mod.rs` contains shared
database ownership and write-batch bookkeeping; it is the next readability
candidate after the working path is stable.

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

Durable snapshot publication/installation and fourth-node movement
are unfinished. The initial directory remains immutable; changing membership
requires the later metadata transition protocol.
