# Local configuration

The node and gateway currently use explicit command-line settings. Run commands
from `crates/rockbee-raft-kv`. All three nodes must use the same cluster ID, shard
count, hash seed and initial node list. Peer ports carry OpenRaft RPC; data ports
carry gateway-to-node requests. The gateway has its own public client port.

```text
Client -> gateway :9100 -> node data :8101 / :8102 / :8103
                           OpenRaft peer :7101 / :7102 / :7103
                           separate local RocksDB directories
```

Build both applications:

```sh
cargo build --locked --offline -p rockbee-raft-node -p rockbee-raft-gateway
```

On macOS, use the SDK `CXXFLAGS` invocation explained in the
[workspace README](../README.md) if the native compiler needs standard-library
headers. Build settings do not change the runtime's durability semantics.

## Three nodes, separate terminals

Start node 2 and node 3 first, then node 1. Node 1 is the explicit logical
initializer; it may route metadata publication to another node elected leader.
Each command below stays in the foreground. Ctrl-C or SIGTERM closes that node.

Node 2:

```sh
target/debug/rockbee-raft-node \
  --node-id 2 --cluster-id 01010101010101010101010101010101 \
  --data-dir data/local/node-2 --create --shards 4 --hash-seed 0 \
  --node 1@127.0.0.1:7101@127.0.0.1:8101 \
  --node 2@127.0.0.1:7102@127.0.0.1:8102 \
  --node 3@127.0.0.1:7103@127.0.0.1:8103
```

Node 3:

```sh
target/debug/rockbee-raft-node \
  --node-id 3 --cluster-id 01010101010101010101010101010101 \
  --data-dir data/local/node-3 --create --shards 4 --hash-seed 0 \
  --node 1@127.0.0.1:7101@127.0.0.1:8101 \
  --node 2@127.0.0.1:7102@127.0.0.1:8102 \
  --node 3@127.0.0.1:7103@127.0.0.1:8103
```

Node 1:

```sh
target/debug/rockbee-raft-node \
  --node-id 1 --cluster-id 01010101010101010101010101010101 \
  --data-dir data/local/node-1 --create --initialize --shards 4 --hash-seed 0 \
  --node 1@127.0.0.1:7101@127.0.0.1:8101 \
  --node 2@127.0.0.1:7102@127.0.0.1:8102 \
  --node 3@127.0.0.1:7103@127.0.0.1:8103
```

`--create` refuses existing storage. To restart, use the same identity, addresses,
directory and shard settings, replacing `--create` with `--open`. Missing or
incompatible durable state is an error. Only the lowest configured node ID can
use `--initialize`; existing Raft histories are not initialized again.

## Gateway

After the initial directory is published, start a gateway in another terminal:

```sh
target/debug/rockbee-raft-gateway \
  --listen 127.0.0.1:9100 --cluster-id 01010101010101010101010101010101 \
  --seed 1@127.0.0.1:8101 --seed 2@127.0.0.1:8102 --seed 3@127.0.0.1:8103 \
  --shards 4 --hash-seed 0
```

The public `RockBeeRaftClient` connects to `127.0.0.1:9100`. A gateway requires
a validated linearizable metadata read before serving. A listening node is not
proof that every hosted group currently has quorum authority.

For an automatically cleaned-up demonstration using temporary directories and
owned ephemeral ports, run the
[process recovery test](../apps/rockbee-raft-node/tests/processes.rs):

```sh
cargo test --locked --offline -p rockbee-raft-node --test processes
```

The initial directory is immutable in this stage. Snapshot installation and
changing replica placement are still under development; these examples do not
enable an automatic membership manager or autoscaler.
