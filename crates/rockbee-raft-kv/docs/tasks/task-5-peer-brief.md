# Task 5a: private bounded Raft peer RPC

Implement only node `src/rpc/**`, `tests/peer.rs`, and the `pub mod rpc` declaration
in node `lib.rs`. Root owns config/cluster/runtime and protocol/backend schemas;
storage worker owns `raft/types.rs`, storage and snapshots. Coordinate before any
manifest change or shared file edit. No commits/push, no `.herdr`, no legacy edits.

Read `docs/reviews/advisor-runtime-start.md`, agreed design, and the cached exact
OpenRaft0.9.25 API. Types are available at `crate::types::{TypeConfig,Command,Entry}`.
Use meaningful modules (`message`, `network`, `server`, `admission`), clear English
docs/comments understandable to a Java developer. Keep helpers concrete; explain
macros where they hide control flow. All behavior tests use owned resources.

Deliver a `PeerNetworkFactory`/`PeerNetwork` implementing real OpenRaft traits and
a peer TCP server. API for root integration:

```rust
pub struct PeerIdentity { pub cluster_id: ClusterId, pub node_id: NodeId }
pub struct PeerLimits { /* validated defaults; aggregate shared budgets */ }
pub struct PeerResources { /* shared incoming/outgoing/control/bulk admission */ }
impl PeerResources { pub fn new(limits: PeerLimits) -> Result<Self, PeerError>; }
impl PeerNetworkFactory {
    pub fn new(identity: PeerIdentity, group_id: GroupId,
               resources: Arc<PeerResources>) -> Self;
}
pub struct PeerServer { /* owns listener worker + all accepted child tasks */ }
impl PeerServer {
    pub fn start(listener: TcpListener, identity: PeerIdentity,
                 groups: BTreeMap<GroupId, openraft::Raft<TypeConfig>>,
                 resources: Arc<PeerResources>) -> Self;
    pub async fn close(&mut self); // joins owned work, cancellation-safe ownership
}
```

Adjust names if needed but notify root promptly. Group map is fixed for the first
runtime; all configured groups exist on each initial node. Node addition is a
later task. No fake snapshot success: forward actual install RPC into OpenRaft;
Task3 store currently returns Unsupported and Task4 will replace that behavior.

One TCP request/response per private Raft RPC is acceptable and intentional for
the first path. Use codec2MiB but private bounded DTOs, not arbitrary upstream
collection/error serde. Validate version/cluster/target/nonzero source/group/kind.
Identity checks prevent misrouting, not unauthenticated malicious impersonation.
Use deadline from RPCOption::hard_ttl spanning queue/connect/write/read. No task
or buffer allocation before aggregate admission. Keep votes/appends and snapshot
bulk independently admitted; identify lane before allocating the full frame (a
small fixed prefix is fine) and require frame kind matches that prefix. Server
tasks/bytes retained until actual accepted operation and output finish; dropping
socket must not detach unaccounted Raft work. Close must stop acceptance and join
owned connection tasks. Bound idle read/write waits.

Default prototype limits:128entries/append,16nodes/membership,2joint configurations,
256byte endpoint/snapshot-ID,256KiB snapshot chunk. Wire conversion preserves full
log IDs/votes/membership. Preflight encoded append size; overlarge multi-entry
batch returns positive fitting-prefix PayloadTooLarge hint, never silently trim.
Single maximum legal1MiB value plus64KiB key must fit. Root will set OpenRaft config
max_payload_entries=128 and snapshot_max_chunk_size=256KiB. All other defaults kept.

Tests first: bounded message/identity roundtrip, malformed collection counts,
maximum legal entry fit, oversized batch positive hint, group/target mismatch,
separate control/bulk admission and server-owned cleanup. Real multi-node voting/
replication tests are root's next integration task; peer test can use owned Raft
fixtures if economical, but do not fabricate evidence of full path.

Main native target is currently owned by storage worker. Do NOT start another
native target. Coordinate with storage/root for sequential Cargo after release.
Use CXXFLAGS='-isystem /Library/Developer/CommandLineTools/SDKs/MacOSX.sdk/usr/include/c++/v1'
and CARGO_BUILD_JOBS=2, --locked --offline. TCP tests need require_escalated due
documented sandbox bind EPERM. Report actual evidence in task-5-peer-report.md;
save a node rpc baseline/diff if useful, never mix storage changes into review.
