# Task 5a peer RPC checkpoint

## Implemented boundary

`apps/rockbee-raft-node/src/rpc/` now contains the node-private peer boundary:

```text
OpenRaft group -> PeerNetwork -> TCP connection -> PeerServer -> hosted OpenRaft group
```

- `PeerNetworkFactory` implements OpenRaft's factory interface and creates lazy,
  one-request TCP clients for one target/group pair.
- `PeerNetwork` implements AppendEntries, Vote and InstallSnapshot. It uses the
  OpenRaft RPC hard deadline for queueing, connect, write and read.
- `PeerServer` owns the accept loop and child tasks. `close()` signals the loop
  and joins it without dropping the stored join handle if its own caller is
  cancelled.
- A 42-byte fixed prelude carries protocol version, cluster, source, target,
  group and kind. The receiver validates it and admits the operation lane before
  allocating the variable-size body.
- Inbound/outbound connection, control and snapshot permits are independent, so
  bidirectional replication cannot exhaust one shared permit pool.
- Snapshot and control work have independent pools. A saturated incoming listener
  drops an unadmitted socket instead of creating a queued child task.
- Private OpenRaft bodies are converted through `dto.rs`. Its bounded serde
  visitor checks sequence size before reserving a vector. Append entries,
  membership configs/nodes/endpoints, snapshot metadata/ID and snapshot chunks
  all have explicit limits before conversion into OpenRaft types.
- Every wire `LogId` preserves term, committed-leader node ID and index. The
  regression test covers previous/entry/commit append positions and both
  snapshot log positions with distinct nonzero leader node IDs.

## Evidence

On macOS with the active SDK C++ include path and `CARGO_BUILD_JOBS=2`:

```text
cargo test -p rockbee-raft-node --test peer --lib --locked --offline
16 passed, 0 failed

cargo clippy -p rockbee-raft-node --all-targets --locked --offline -- -D warnings
exit 0
```

The focused tests cover fixed-header round trip, wrong target/cluster and kind
mismatch rejection, invalid resource limits, the public resource API, and
accepted/rejected bounded sequence limits. They also show one maximum legal
1 MiB value fits in a peer frame. No multi-node replication, vote, or
snapshot success is claimed here; those belong to runtime integration.

## Open review items

The bounded DTO and leader-identity P0 are fixed. Header/body/admission/write
phases now have a bounded timeout and shutdown cancellation; the idle-socket
close regression exercises that path. More than 128 entries and a conservative
allocation-free byte upper bound return a positive append split hint before DTO
copies. Snapshot admission is lane-first in both directions: outbound bulk work
waits for a snapshot permit before taking a connection, while inbound excess
snapshot work is rejected immediately after its fixed prelude. A deterministic
test proves a saturated bulk lane leaves a control connection and control permit
available. The preflight is deliberately conservative instead of an exact
borrowed postcard measurement, so it can split a batch earlier than necessary.
Independent verifier review remains required before task acceptance.
