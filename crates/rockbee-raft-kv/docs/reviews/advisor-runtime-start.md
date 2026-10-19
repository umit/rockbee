# Runtime start: private peers and static metadata

Status: implementation advice, source-checked against cached OpenRaft 0.9.25; no build or runtime claim. Root accepted the simple connection-per-RPC direction. Verifier agrees, subject to real replicated group-0 metadata and accommodation of a maximum legal entry.

```text
Client -> gateway -> selected data-group leader -> private peer RPC -> replicas
             |
             +-> group-0 leader -> linearizable static directory
```

## Small private peer boundary

Keep these types inside the node crate. `PeerNetworkFactory` implements `RaftNetworkFactory<TypeConfig>` with `type Network = PeerNetwork`; `new_client(target: u64, node: &BasicNode)` constructs a client without connecting. `PeerNetwork` implements the existing `append_entries`, `vote`, and `install_snapshot` methods. Each opens one TCP connection, writes one frame, reads one response, and closes it. Use the existing frame codec with private DTOs; no new generic multiplexing framework is needed for this first path.

Use a single absolute deadline derived from `RPCOption::hard_ttl()` for admission, connect, write, and read. Cancellation closes this connection. Raft replication retries are normal internal behavior and do not authorize replaying an uncertain public mutation.

The bounded header identifies protocol version, cluster, sender, target, group, and RPC kind. Validate it against local identity and a hosted group before dispatch. A response must match the requested identity/group/kind; a one-request connection needs no pending-ID map. Identity fields protect against accidental misrouting, not malicious impersonation; they are not authentication. Peer and public/backend listeners remain separate.

Use process-shared admission objects, not fresh limits per `PeerNetwork` or per group. Bound incoming connection tasks before spawning them, frame bytes before allocation, and outgoing operations before encoding/connect. Give control traffic and snapshot chunks separate reserved slots/budgets so a snapshot cannot consume every vote/append slot. One connection per request prevents a snapshot frame from blocking a vote on the same stream. Accepted server work retains its permits until actual work completes even if the remote socket disappears; a disconnected caller must not create unaccounted background jobs.

## DTO mappings and allocation limits

Use small node-private bounded wire representations, then consume them into OpenRaft requests:

| Wire representation | OpenRaft representation |
| --- | --- |
| Vote request/response with numeric IDs and log IDs | `VoteRequest<u64>` / `VoteResponse<u64>` |
| Append: vote, previous log ID, bounded entries, leader commit | `AppendEntriesRequest<TypeConfig>` |
| Entry: log ID plus Blank / validated Command / bounded membership | `Entry<TypeConfig>` |
| Membership: at most two voter configurations, bounded voter IDs and node endpoints | `Membership<u64, BasicNode>` |
| Snapshot: vote, last log ID, bounded membership, bounded snapshot ID, offset, bounded bytes, done | `InstallSnapshotRequest<TypeConfig>` |
| Append success/partial success/conflict/higher vote; snapshot response vote | Corresponding native response variants, without inventing success |

Reasonable explicit prototype limits are 128 entries per append, 16 distinct nodes per membership, two joint configurations, 256 bytes per endpoint/snapshot ID, and 256 KiB per snapshot chunk. Validate public node IDs as nonzero at this boundary even though OpenRaft's internal type is `u64`. Preserve full log IDs and all membership information on conversion. A membership endpoint can use a bounded string converted to `BasicNode.addr`; parse/validate the intended address format before use. Restrict node count consistently in bootstrap/directory configuration so the peer representation cannot reject a legitimately created local membership.

Implement bounded sequence/map/byte decoding that rejects a size hint or next element beyond the cap *before* allocating it. A post-deserialization `Vec::len()` check does not bound deserialization. Avoid directly deriving deserialization for arbitrary upstream `Vec<Entry>` / `Membership` / recursive error payloads and calling the outer frame limit sufficient. Bound diagnostics and encode explicit peer failure codes; map unexpected remote/protocol failures to a transport error, never a Raft success. Fatal local storage errors still stop that local group. A small typed snapshot mismatch response may be added when needed; do not flatten it into success.

Budget simultaneous frame storage, decoded values, and conversion/encoding copies conservatively; consume owned buffers where possible. Frame size is not a hard process RSS cap. Limits must be aggregate over hosted groups. Exact byte budgets belong in node configuration, with one shared owner and documented worst-case retained buffers.

## A maximum value must replicate

The public 1 MiB value plus maximum key and bounded envelope must fit in one 2 MiB peer frame. Test the real encoded maximum entry. Set `Config::max_payload_entries = 128` to align the normal upstream batch size with the decoder limit, and preflight actual encoded append size before writing to the socket.

If a multi-entry append does not fit, return `RPCError::PayloadTooLarge(PayloadTooLarge::new_entries_hint(n))`, where `n > 0` is a fitting prefix count. Let OpenRaft retry the smaller batch. Do not silently trim entries and report whole-batch `Success`. A single legal entry that cannot fit is a configuration/representation defect; never call the hint constructor with zero or loop forever with an ineffective hint of one.

Set `Config::snapshot_max_chunk_size = 256 * 1024` explicitly. Its 0.9.25 default is 3 MiB, exceeding this codec. `PayloadTooLarge::new_bytes_hint` is crate-private, and the default chunked snapshot sender treats `PayloadTooLarge` as terminal; dynamic snapshot shrinking is not the append hint mechanism. Snapshot support must remain explicitly unavailable until the durable file-backed Task-4 store exists; a working peer method cannot make an unsupported store return fake success.

## First directory is replicated static placement

Group 0 is a real Raft group. Store the bounded, versioned `ClusterDirectory` at a reserved internal key using the existing `Command::Put`. Include cluster identity, hash/config version, immutable seed and shard count, node endpoints, per-group voters, and directory revision. Bootstrap has one designated initializer and produces a single immutable initial value; restart verifies it instead of overwriting it. Do not implement later updates as a read/check followed by an unconditional Put.

Expose an engine-free backend `ReadDirectory` operation that explicitly addresses group 0, performs the leader read barrier, then reads a coherent applied view. The reserved key never enters ordinary hash routing or public mutation access. Gateway startup validates cluster/config identity and the linearizable directory before accepting routed work. A cached directory supplies placement; it does not establish leadership or make GET linearizable. The destination data node validates cluster/group/key ownership and performs its own authority/read checks.

Directory revision is a configuration version, **not** a membership-controller fence. Static placement delivers the initial runtime; Task 7's stale-controller, overlapping-move, and durable fencing work remains unfinished. Do not claim those gates passed from this bootstrap design.

## Focused acceptance checks

- Maximum legal entry fits; two oversized-together entries cause a positive split hint and eventually replicate both, without false success.
- Hostile entry/membership lengths and snapshot IDs reject before disproportionate allocation; wrong cluster/target/group/kind never dispatches.
- Snapshot load consumes its bulk budget while a vote still obtains a reserved control slot; all concurrent groups share the same process cap.
- Dropping/timing out a client connection cannot release reservations while its owned server operation remains active.
- Gateway cannot start from an uncommitted/nonlinearizable directory; restart cannot replace an existing directory with stale bootstrap configuration.

Source anchors: OpenRaft `src/network/{factory,network,rpc_option}.rs`; `src/config/config.rs` (`max_payload_entries`, `snapshot_max_chunk_size`); `src/error.rs` (`PayloadTooLarge` constructors); `src/network/snapshot_transport.rs` (`PayloadTooLarge` terminal branch). No tests were run for this advisory note.
