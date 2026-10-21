# Local configuration

Node and gateway executables accept **`--config FILE`** for TOML startup settings.
The existing command-line settings remain supported. Run the commands below
from `crates/rockbee-raft-kv`.

```text
Client -> gateway :9100 -> node data :8101 / :8102 / :8103
                           OpenRaft peer :7101 / :7102 / :7103
                           separate local RocksDB directories
```

## Three nodes and one gateway

The ready-to-use files are [node-1.toml](node-1.toml),
[node-2.toml](node-2.toml), [node-3.toml](node-3.toml), and
[gateway.toml](gateway.toml). The node files share a cluster ID, four data
groups, hash seed, and three initial members. G0 is the additional metadata
group. Each group starts with all three nodes as voters; OpenRaft elects its
leader dynamically.

Build the executables:

```sh
cargo build --locked --offline -p rockbee-raft-node -p rockbee-raft-gateway
```

On macOS, use the SDK `CXXFLAGS` invocation in the
[workspace README](../README.md) if the native compiler needs standard-library
headers.

Start node 2 and node 3 first, then node 1, each in its own terminal:

```sh
target/debug/rockbee-raft-node --config configs/node-2.toml --create
```

```sh
target/debug/rockbee-raft-node --config configs/node-3.toml --create
```

```sh
target/debug/rockbee-raft-node --config configs/node-1.toml --create --initialize
```

After the initial directory is published, start the gateway:

```sh
target/debug/rockbee-raft-gateway --config configs/gateway.toml
```

`RockBeeRaftClient` connects to `127.0.0.1:9100`. Ctrl-C or SIGTERM closes
each process through its normal shutdown path.

## Reading the files

| Field | Meaning |
| --- | --- |
| `mode` | Node startup mode: `bootstrap` for an initial node, `join` for a new data node. |
| `cluster_id` | The same nonzero, 32-character hexadecimal identity throughout the cluster. |
| `node_id` | The initial node process's stable identity. |
| `data_dir` | Local RocksDB and snapshot directory; a relative path is resolved against the **TOML file's directory**. |
| `shards` | Immutable data-group count, default `4`; G0 is additional. |
| `hash_seed` | Immutable key-to-group hash seed, default `0`. |
| `[[nodes]]` | Initial members, each with `node_id`, a Raft `peer_address`, and a backend `data_address`. |
| `listen` | Gateway's public client address. |
| `[[seeds]]` | Discovery candidates, each with `node_id` and a backend `address`. |

Gateway seeds are starting points for a validated metadata read, not a fixed
leader list. Node peer ports carry private OpenRaft RPC; data ports carry
gateway-to-node requests.

Unknown fields, invalid identities, malformed addresses, and invalid group
settings are rejected before storage or listeners are opened. `--config`
cannot be combined with inline identity, address, or shard settings. This
avoids silently overriding the file. Files are read at startup.

## Creation and restart

Storage lifecycle remains explicit on the command line:

- `--create` creates new storage and refuses existing storage.
- `--open` requires existing, compatible durable state.
- `--initialize` starts initial group setup from the lowest configured node
  ID. It does not assign a permanent leader or reset existing Raft history.

For restart, use the same files with `--open` instead of `--create`:

```sh
target/debug/rockbee-raft-node --config configs/node-2.toml --open
target/debug/rockbee-raft-node --config configs/node-3.toml --open
target/debug/rockbee-raft-node --config configs/node-1.toml --open --initialize
```

These are again separate foreground processes. `create`, `open`, and
`initialize` are deliberately not TOML fields. Keep the initial bootstrap
settings on restart; current membership is recovered from durable state,
including a completed explicit replica move.

## Joining a fourth node

[node-4-join.toml](node-4-join.toml) describes a separate data node and its
metadata discovery seeds:

```sh
target/debug/rockbee-raft-node --config configs/node-4-join.toml --create
```

Join mode uses `[local_node]` instead of `node_id` and `[[nodes]]`. Its
`startup_timeout_ms` defaults to `10000` and must be in `1..=60000`.
It cannot use `--initialize`. Restart the same node with `--open`.

Starting D does not make it a voter: the explicit replica-movement flow adds
it as a learner, catches it up, and changes the selected group's membership.
The first `ABC -> ABD` move is implemented. Automatic placement, rebalancing,
and the end-to-end coordinator for repeated movement remain unfinished.

## Inline settings

Existing scripts can continue using `--node-id`, `--cluster-id`, `--node`,
`--data-dir`, and the other inline flags without `--config`. Run either
executable with `--help` for that syntax. Inline `--data-dir` retains its
existing working-directory-relative behavior.

For inline node startup, `--server-request-mib 512 --server-response-mib 512`
sets the aggregate gateway-to-node request and response admission budgets to
512 MiB each. Both bootstrap and join mode accept these options; omitted budgets
remain 64 MiB. These are bounded admission counters, not eagerly allocated
buffers or a limit on total process RSS. They do not change RocksDB caches or
the Raft peer transport. The node's native mutation allowance uses both budgets,
so raising only the response budget can leave request admission as the limit.
These inline flags cannot be combined with `--config`.

The manual process soak accepts `ROCKBEE_SOAK_NODE_BUDGET_MIB=512` to pass both
node flags, and `ROCKBEE_SOAK_GATEWAY_RESPONSE_MIB=512` for its gateway response
budget. The manifest records the chosen budgets. `ROCKBEE_SOAK_CLIENTS=100` and
`ROCKBEE_SOAK_CONCURRENCY=100` create 100 independent connections with one worker
each; the test's gateway connection limit expands to admit them. Any failed
operation stops the soak and remains a failure in its report.
# Optional Nginx / three-gateway local soak

The ignored `soak::nginx::three_gateway_soak` test uses real gateway executables
behind an Nginx TCP `stream` proxy, sharing the same three node executables:

```text
100 public client connections -> Nginx -> 3 gateway processes -> 3 Raft nodes
```

Build both applications first, then run from the Raft workspace:

```sh
cargo build --release --locked -p rockbee-raft-node -p rockbee-raft-gateway
ROCKBEE_SOAK_GATEWAY_BIN="${PWD}/target/release/rockbee-raft-gateway" \
ROCKBEE_SOAK_NGINX_BIN="$(command -v nginx)" \
ROCKBEE_SOAK_SECONDS=300 \
ROCKBEE_SOAK_CLIENTS=100 \
ROCKBEE_SOAK_CONCURRENCY=100 \
ROCKBEE_SOAK_GATEWAY_RESPONSE_MIB=512 \
ROCKBEE_SOAK_NODE_BUDGET_MIB=512 \
cargo test --release --locked -p rockbee-raft-node --test processes -- \
  soak::nginx::three_gateway_soak --ignored --exact --nocapture
```

Nginx must include `--with-stream` support (`nginx -V`). The test generates its
own loopback ports, config, PID file, and logs under a unique `reports/soak/`
directory. `topology.txt` records child PIDs and listening addresses;
`nginx-access.log` records upstream selection per TCP session. `least_conn`
balances connections; an established connection stays on one gateway. Proxy
upstream retries are disabled in this experiment.

The test owns one foreground Nginx process with `master_process off`, three
gateways, and three nodes. This simplifies exact process cleanup and is a local
test arrangement, not a production master/worker configuration. Workers drain
before clients close; Nginx and gateways then stop before the nodes. SIGTERM and
SIGINT follow the same cleanup path. The ordinary one-gateway soak is unchanged.

Gateway inline startup also accepts `--server-request-mib`,
`--server-response-mib`, and `--max-connections`. These configure aggregate
admission budgets and a connection limit, not total RSS. Defaults are unchanged;
inline overrides cannot be combined with `--config`.
