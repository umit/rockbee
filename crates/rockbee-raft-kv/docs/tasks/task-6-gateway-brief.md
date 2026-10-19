# Task 6a: bounded typed server and gateway

Implement the real gateway library/binary plus the reusable typed data server
needed by gateway AND node backend. Work only inside this independent workspace.
No commits/push, no `.herdr`, no legacy edits, no engine/runtime/TCP tuning.

Ownership:
- `protocol/src/transport/server/**` and minimal export in transport/mod.rs.
- Gateway `src/{server,routing,admission,metrics,config}/**`, lib.rs/main.rs/tests,
  gateway Cargo.toml (workspace Bytes/Tokio/serde/thiserror only if needed).
- Tests in protocol `tests/server.rs` and gateway tests.
- Root owns backend/directory protocol and node runtime; peer worker owns node RPC;
  storage worker owns storage/snapshots. Coordinate manifest/lock changes with root.

The engine-free contracts now compile and pass tests:
`backend::{BackendRequest,BackendOperation,BackendResponse,BackendReply}` and
`routing::{ClusterDirectory,NodeEndpoint,GroupPlacement}`. Inspect source.
`BackendOperation::{Directory,Leader{group_id},Kv{group_id,request}}` addresses
cluster+target. Directory is a real linearizable group0 read, not a static seed map.
Leader replies are hints only. Public KV wire is unchanged.

## Typed data server (two actual consumers)

Keep generic machinery small, with this integration API (notify root if renamed):

```rust
pub trait RequestHandler: Send + Sync + 'static {
    type Request: WireRequest<Self::Response> + DeserializeOwned;
    type Response: Serialize + Send + 'static;
    fn handle(&self, request: Self::Request, deadline: Instant)
        -> impl Future<Output = Self::Response> + Send;
}
pub struct ServerConfig { /* validated process-global limits; Default */ }
pub struct Server { /* owned accept/task lifecycle, not generic publicly */ }
impl Server {
    pub fn start<H: RequestHandler>(listener: TcpListener, config: ServerConfig,
                                   handler: Arc<H>) -> Result<Self, ServerError>;
    pub fn stop_accepting(&self);
    pub async fn close(&mut self); // retain handles across cancelled close
}
```

Use the existing generic envelopes/codec. Validate envelope VERSION/nonzeroID/
budget1..60000/body before calling handler. Preserve each requestID in response;
out-of-order responses are allowed, frame writes are serialized. Duplicate active
request IDs retire that connection, without executing the duplicate. Invalid
message must never dispatch. No automatic replay. Response uses same ID andVERSION.

Aggregate limits across all accepted connections: connections, outstanding work,
retained request bytes, retained response bytes. Admission before spawning tasks
and allocating full frames. Reserve maximum response capacity BEFORE handler so
a GET cannot materialize a large value before being charged. Keep both request
and response ownership charged until actual task/frame lifetimes end, including
slow output and disconnected clients. Include encoded/decoded overlap, generic
inline ownership and `WireRequest::RESPONSE_DECODE_HEADROOM` as applicable.
No RSS claim. Defaults explicit and conservative, e.g.64connections/128operations,
64MiB each request/response budget; impossible/overflowing values reject at start.
Request decoding is bounded by the concrete schemas, not arbitrary Serde magic.

Use bounded per-connection output and owned JoinSet/tasks. If caller disconnects,
accepted handler work MUST retain global permits until its actual future ends.
Do not cancel accepted Raft mutation work on socket loss/deadline and instantly
free capacity while it continues inside OpenRaft. Keep a global request permit
through handler completion. Node handler may await client_write beyond request
deadline; client Link already reports Unknown at its deadline. This preserves
bounded pending proposals under quorum loss. Gateway handler uses a bounded Link
and returns its typed result normally. No detached/untracked tasks.

Idle/partial-header/payload/output waits bounded. Derive server request deadline
from received budget and observed frame-receive start; do not charge preceding
idle connection time. Check expired work before calling handler. If request
cannot be decoded/admitted safely, closing stream is conservative (client may
report Unknown); never fabricate NotSent from server. Accepted handler lifetime
does not terminate just because connection write deadline expired.

close stops acceptance and idle network work, then joins accepted handlers.
For node shutdown root first calls stop_accepting, concurrently shuts down Raft
groups to resolve pending writes, then joins server. Document that server cannot
force an arbitrary handler to finish. Drop initiates stop; explicit close joins.

## Gateway runtime

`GatewayConfig` has clusterID, expectedShardConfig, seed `(NodeId,SocketAddr)` list,
public listen address, backend LinkConfig, ServerConfig, bounded startup timeout.
Expose `Gateway::connect(config, deadline)` as RequestHandler for KvRequest and
KvResponse, with `close` to retire/join shared backend links. Expose convenience
`GatewayRuntime::start(config)` + `start_with_listener(config, TcpListener)` and
`close(&mut self)` so tests/root can own already-bound ephemeral listeners.

Startup reads actual Directory via bounded Backend Link from seeds. If a seed
is not metadata leader, query Leader(group0), try known hint/other seeds inside
one absolute startup deadline. Validate returned cluster, expected shard mapping,
directory uniqueness/endpoints/voters and nonzero revision before serving data.
No accepting from unverified static seeds. Cache bounded directory/routing hints
only, no user key/value cache.

Each key hashes to its data group. Reuse at most a bounded connection per known
node (maximum16), safely serializing connection creation. Local backend admission
NotSent can become public Rejected because gateway proves no Raft submission.
Backend transport MayHaveBeenSent ALWAYS becomes Unknown for mutation. Any
unexpected/invalid response after handoff also Unknown. Never retry Unknown.
Only explicit matching pre-submission Rejected(NotLeader) may route again, using
validated hints/candidates within same deadline and bounded attempts. Reads can
retry failures within deadline; no automatic retry storm. A newly connected Link
is for safe/future work, never implicit replay. Preserve Committed only from an
actual node response. Node validates cluster/group/key and read authority again.

Use named helpers and simple types, Java-readable function flow. Public docs
explain Result/Option/trait boundaries, not syntax on every line. Metrics can be
simple bounded atomics/struct snapshot (accepted/rejected/active/backend failures),
no new exporter dependency. Keep main thin. CLI may be small explicit arg parser:
`--listen`, `--cluster-id`32hex, repeated`--seed id@host:port`, `--shards`default4,
`--hash-seed`default0. `--help` must be honest, errors exitnonzero. Owned SIGINT/
SIGTERM shutdown; no killing other processes. Root node config will match.

## Tests/evidence

Tests first, then implement. Focus server malformed/duplicate/correlation,
response reservation before handler, slow consumer saturation, disconnected
accepted work retains permits, cancelled-close/reclose, and handler cleanup.
Gateway controlled backend fixtures: real directory required, correct shard/
identity, NotLeader safe reroute, dropped reply Unknown with exactly one mutation
submission, eventual safe next reconnect, missing/empty values, startupfailclosed.
Do not claim fixture Committed proves Raft durability. Root will run actual
three-process/four-data-group+metadata integration.

Use separate protocol/gateway-only target `target/gateway-task-6`,2jobs,
--locked --offline. TCP tests require sandbox escalation for owned127.0.0.1:0.
Do NOT build node/native deps. Source changes must compile before shared checks;
coordinate minimal export patches. Report task-6-gateway-report.md with exact
evidence, gaps and publicAPI. Prioritize working path; no extra feature planning.
