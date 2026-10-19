# Rockbee Multi-Raft KV Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development.
> Tasks use checkboxes; each implementation has an independent review.
> User authorization overrides skill defaults: stay on main, do not commit or
> push, and keep all artifacts inside this module. Continue without approval gates.

**Goal:** A public byte-KV client and bounded gateway serving four OpenRaft data
groups across three RocksDB nodes, with durable mutations, linearizable reads,
recovery, failover, and safe replica movement to a fourth node.

**Architecture:** Four packages in an independent nested Cargo workspace.
The protocol owns engine-free messages and shared transport. The client exposes
eager mutation handles. The gateway routes and bounds work. Nodes host multiple
Raft groups and one shared RocksDB with group-isolated records; a metadata group
coordinates cluster configuration and placement.

**Tech Stack:** Rust 2024, Tokio, bytes/serde/postcard, OpenRaft =0.9.25
(`serde`, `storage-v2`), rocksdb 0.25.0. Dependencies stay module-local.

**Spec:** [design.md](design.md). **Evidence:** [progress.md](progress.md).
**Acceptance catalogue:** [failure-matrix.md](failure-matrix.md), with individual
case IDs, observable outcomes and pending/verified status.

## Global constraints

- Everything lives under repository-relative `crates/rockbee-raft-kv/`.
- Existing Rockbee and unrelated edits remain unchanged. `.herdr/` is forbidden.
- Work on main; no commits, pushes, merges, or publication are authorized.
- Client/gateway must not depend on node, OpenRaft, RocksDB, or parent crates.
- PUT/DELETE submit eagerly and return a completion handle. Bound operation count
  and bytes. Do not replay uncertain mutations; dropping a handle is not rollback.
- Success requires stable quorum log persistence and durable leader application.
  A transport failure after handoff is Unknown unless a matching response proves
  pre-submission rejection. Request IDs correlate; they do not deduplicate.
- GET uses the owning leader's linearizable barrier and applied-state wait.
- No user-value cache in gateway. No TTL, SQL, vectors, cross-shard transactions,
  follower reads, lease shortcuts, automatic balancing, or performance tuning.
- Snapshots stream through files; no full-state in-memory map or snapshot buffer.
- Tests own unique directories/processes. Never stop unrelated services, modify
  LocalStack, or reuse production storage. Fault-injection copies use own target dirs.
- A passed scaffold or unit test is not proof of the complete application path.

## Fixed initial contracts

The first topology has three nodes, four data groups, and one metadata group.
Group ID 0 is metadata; logical shard `s` owns data group `s + 1`. Node IDs and
request IDs are nonzero u64. Cluster identity is a nonzero 16-byte identifier.
Shard count and hashing configuration are immutable after bootstrap.

Wire v1 is a u32 big-endian length followed by postcard bytes. Decode must reject
trailing bytes, unknown version, invalid IDs, empty keys, zero time budget,
oversized keys/values/frames, and unsupported operations before executing work.
The generic codec's type parameter is trusted application code; supported wire
messages require adversarial length checks. It does not sandbox arbitrary custom
Deserialize implementations or unused collection types.
Limits: keys 64 KiB, values 1 MiB, ordinary frames 2 MiB. Empty values are valid.
Use `Bytes` for opaque payloads. Error messages are bounded, not arbitrary chains.
Stable key mapping is versioned FNV-1a 64 with explicit seed (offset basis XOR
seed), wrapping multiply, then modulo shard count. It is not a security hash.

`RequestEnvelope { version, request_id, budget_ms, request: KvRequest }` and
`ResponseEnvelope { version, request_id, response: KvResponse }` are public.
`KvRequest::{Put { key, value }, Get { key }, Delete { key }}`.
`KvResponse::{Value(Option<Bytes>), Mutation(MutationOutcome), ReadError(...)}`.
`MutationOutcome::{Committed, NotSent(...), Rejected(...), Unknown(...)}`.
`Rejected` is permitted only when the service proves no consensus submission.
Reads may retry within one deadline; mutation retry needs explicit safe rejection.

An absolute local `Instant` controls every local wait; each hop sends only the
remaining duration, clamped to the configured maximum. Connection, admission,
queueing, execution wait, and response all spend the same deadline budget.

Transport defaults are conservative, configurable limits rather than tuning:
128 pending operations and 16 MiB retained request bytes per connection; server
process admission is 1,024 operations and 64 MiB retained request bytes. Response
queues have separate bounded byte/count admission. Connections are limited too.
Validate limits against the maximum frame; do not reserve permits after allocation.
Peer replication and snapshot traffic have separate listeners and budgets.
After Raft submission, the node retains its work permits until the underlying
proposal resolves, even when the caller times out or disconnects. A fatal or
abandoned consensus result retires that group's admission until it is safely
restarted; timeout storms must not fill OpenRaft's internal unbounded channels.

## Review focus

1. Commit succeeds but response disappears: Unknown and exactly one submission
   in observed transport history (Tasks 2, 6, 8).
2. Old leader can still talk to a client but lacks quorum: no stale strong read
   and no successful mutation (Tasks 5, 8).
3. Crash between data and applied-index changes, or halfway through snapshot:
   coherent old/new state only, with membership recovered (Tasks 3, 4, 8).
4. Slow peer/client or hot unavailable group: bounded memory, prompt deadline,
   and healthy groups retain independent progress (Tasks 2, 5, 6, 8).
5. Controller loses membership-change response: reconcile actual committed
   membership before resuming; never create a second group (Tasks 7, 8).

## Package/file map

All paths below are relative to this workspace.

- `crates/rockbee-raft-protocol/src/{lib,ids,wire,codec,shard}.rs`: engine-free
  contracts; `transport/{mod,link,server,admission}.rs`: shared bounded TCP.
- `crates/rockbee-raft-client/src/{lib,client,mutation,error}.rs`: public API.
- `apps/rockbee-raft-node/src/{lib,main,config,types,group,node}.rs`: startup and
  OpenRaft types; `storage/{mod,keys,log,state,snapshot}.rs`: persistence;
  `peer/{mod,network,server}.rs`: private RPC; `control/{mod,metadata,placement}.rs`:
  metadata and resumable operator-directed moves.
- `apps/rockbee-raft-gateway/src/{lib,main,config,gateway,routing,metrics}.rs`:
  validated startup, bounded route/connection reuse, request path.
- Package `tests/` directories hold behavior tests. Gateway `tests/support/`
  owns process fixtures; `configs/` holds reproducible local examples.

## Task 1: Validated public protocol and stable sharding

**Files:** protocol ids/wire/codec/shard modules, `tests/protocol.rs`, workspace
and protocol manifests and lockfile. No engines in the public dependency graph.
**Interfaces:** ID constructors/accessors; `KvRequest::validate()`;
`encode_frame<T: Serialize>(&T) -> Result<Vec<u8>, ProtocolError>`;
`decode_frame<T: DeserializeOwned>(&[u8]) -> Result<T, ProtocolError>`;
`ShardConfig::new(count, seed)` and `group_for_key(&[u8]) -> GroupId`.

- [ ] Write tests for opaque zero/non-UTF8 bytes and empty values, fixed hash
  vectors, count-independent node changes, invalid IDs, max boundaries,
  truncated/oversized/trailing frames, malicious inner collection lengths,
  unsupported versions and response mismatch. Validate IDs during deserialization
  too; zero shard count and metadata group as data destination are rejected.
- [ ] Run focused tests and record the intended red result.
- [ ] Implement typed contracts and strict framing with bounded allocation.
- [ ] Pass protocol tests and strict Clippy. Independent verifier checks parser
  adversarial cases and public dependency boundary; advisor checks contracts.

## Task 2: Bounded multiplexed transport and eager public client

**Files:** protocol transport modules and tests; client modules and
`tests/client_contract.rs`. **Interfaces:** `Link::connect(address, config)`;
`Link::submit(request, deadline) -> Result<PendingResponse, AdmitError>`;
`RockBeeRaftClient::connect(address, config)`;
`put(key: Bytes, value: Bytes) -> Result<MutationHandle, AdmitError>`;
`delete(key: Bytes) -> Result<MutationHandle, AdmitError>`;
`async get(key: Bytes) -> Result<Option<Bytes>, ReadError>`.
`MutationHandle: Future<Output = MutationOutcome>`. Configuration supplies request
timeout and process/connection limits. Public client uses the same Link as gateway.

- [ ] Test eager submission without awaiting handle, multiplexed out-of-order
  replies, local saturation NotSent, dropped handles, EOF/partial write Unknown,
  deadline spanning queueing, unexpected/duplicate replies, bounded slow egress.
- [ ] Observe focused failures; implement one writer/reader pair, bounded pending
  map, byte/count permits, generation retirement, and cancellation-safe cleanup.
- [ ] Verify no pending-map lock crosses await, no auto mutation replay, no
  correlation ID reuse/wrap, and transport failure resolves every pending handle.
- [ ] Independent verifier exercises adversarial peer fixture and reviews certainty
  transitions. Document client examples with honest semantics.

## Task 3: Durable, group-isolated RocksDB storage adapter

**Files:** node types and storage modules, `tests/storage.rs`, node manifest.
**Interfaces:** OpenRaft `RaftLogReader`, `RaftLogStorage`, `RaftStateMachine`,
`RaftSnapshotBuilder`; `Storage::open(path, budgets)` and per-group handles.
Fixed CFs are `raft_log`, `raft_meta`, `state`, `snapshot_meta`. Prefix every key
with group ID; log keys append big-endian log index. State also carries generation.

- [ ] Fetch pinned engine sources and confirm actual 0.9.25 trait contracts.
  Test durable vote/log/commit/apply recovery, delete and membership recovery,
  isolation across five groups, range truncation and purge boundaries. Persist a
  granted vote, restart, and prove a second candidate cannot win the same term.
- [ ] Run the failing adapter tests; implement FIFO per-group log I/O ownership,
  sync WAL writes, append callback only after persistence, and atomic synchronous
  WriteBatch of effects + applied identity + membership. Keep blocking I/O off
  async executor workers. Share process cache/write-buffer resources.
- [ ] Run OpenRaft storage test suite (snapshot-dependent cases complete Task 4),
  restart tests, and Clippy. Verifier audits durable-boundary ordering directly.

## Task 4: File-backed snapshots with atomic generation installation

**Files:** storage snapshot module, tests/snapshots.rs.
**Interfaces:** `SnapshotData = tokio::fs::File`; stream one consistent RocksDB
group view including applied log and membership. Files have versioned header,
bounded records and checksum. Use unique partial files and inactive generations.

- [ ] Test round trip, deletes, multi-group isolation, empty snapshot, truncated
  or corrupted input, wrong cluster/group, duplicate/out-of-order keys, restart
  at publication boundaries, a stale builder racing a newer installed snapshot,
  and state bigger than the configured snapshot-memory budget.
- [ ] Implement bounded streaming, fsync + rename + directory sync for file
  publication; atomically switch generation/applied/membership only after all
  imported state is durable. Use a RocksDB read snapshot for generation+value
  coherence; reclaim old generations only under its MVCC safety guarantees.
- [ ] Pass complete OpenRaft storage suite and restart/partial-install tests.
  Independent verifier checks file publication and generation GC ordering.

## Task 5: Real Multi-Raft nodes, private peer RPC, and strong reads

**Files:** node/config/group/peer modules, node main, tests/node_cluster.rs.
**Interfaces:** node binds separate data/control and peer listeners; every peer
envelope binds cluster ID, target node ID, group ID, RPC kind and deadline. Peer
types stay private; implement OpenRaft `RaftNetworkFactory`/`RaftNetwork`.
`Node::start(config)` returns a shutdown/ready handle; graceful close joins tasks.

- [ ] Test three distinct node processes/dirs host 4 data groups + metadata;
  validate bootstrap identities, restart without reinitializing, wrong-cluster
  and wrong-group rejection, oversized peer request before allocation.
- [ ] Implement durable bootstrap marker/config check; only explicit bootstrap
  initializes a pristine cluster. Implement bounded replication and streamed
  snapshots, Raft writes, linearizable read barrier plus applied-state wait.
  OpenRaft 0.9.25 ensure_linearizable already waits for apply; read the active
  generation, applied identity and value through one RocksDB read snapshot and
  check it is at least as new as the barrier. Do not mix generation observations.
- [ ] Test failover retains successful PUT/DELETE, quorum loss cannot succeed,
  and a stopped/slow group does not globally serialize healthy group work.
  Stalled-quorum deadline/disconnect storms retain proposal permits until actual
  resolution and cannot grow hidden Raft queues without bound.
- [ ] Verifier reviews peer authority/identity and reads against actual OpenRaft
  source, then validates real network tests. Do not substitute in-memory clusters.

## Task 6: Metadata-aware gateway and complete public request path

**Files:** protocol engine-free routing metadata, node control/metadata,
gateway modules/main, gateway tests/end_to_end.rs and support fixtures.
**Interfaces:** validated `ShardMap` includes cluster, monotonic revision, hash
configuration and routes. Gateway connects to seeds and requires a linearizable
metadata read before ready; map refresh is single-flight and monotonic.

- [ ] Test public client -> gateway -> 3 real nodes for all operations and every
  data group; immediate GET after success sees that or a later write.
- [ ] Implement shared bounded backend links, safe routing-rejection handling,
  one end-to-end deadline, max connections and process-level work/egress budgets.
  Recheck group ownership and cluster identity at node before submission.
- [ ] Test stale routes, wrong cluster, metadata outage at startup vs after ready,
  response loss after commit without replay, slow client, and overload isolation.
- [ ] Expose bounded metrics (counts/bytes/queue wait/backend duration/Unknown),
  no key-labelled cardinality. Verifier checks complete path and package isolation.

## Task 7: Durable fourth-node replica movement

**Files:** control/placement.rs, engine-free admin messages, local CLI command,
gateway tests/placement.rs. **Interfaces:** operator requests `MoveReplica`
with job ID, group, old/new voters; persisted metadata job drives reconciliation.

- [ ] Test ABC -> ABD while traffic runs: D starts learner, catches up, joint
  consensus commits final voters, map publishes afterward. C removed only after
  durable proof; no new independent group with the same identity.
- [ ] Implement idempotent controller transitions by inspecting actual committed
  membership after every uncertain RPC; fence concurrent/conflicting jobs.
- [ ] Kill/restart controller before and after membership commit; lose responses;
  verify resumption, durable values/deletes, and unchanged unrelated groups.
- [ ] Independent verifier and advisor jointly audit transition evidence and
  retained-data safety. Explicit leadership transfer only if supported by 0.9.25.

## Task 8: Failure-oriented process suite and operational verification

**Files:** gateway tests/failures.rs and support owned process/proxy harness;
module docs/operations.md and reports/. **Interfaces:** fixtures allocate unique
paths and ports, own child handles, install cleanup before launch, and never use
broad process-kill commands. Proxy fault controls target owned connections only.

- [ ] Exercise leader kill, old-leader minority partition, process restart,
  commit-response loss, partial snapshot, interrupted move, saturated gateway,
  slow egress, SIGINT/SIGTERM cleanup. Assert values, durability and outcomes.
- [ ] Run formatting, all-target check, strict Clippy, all meaningful package and
  process tests sequentially. Resolve reproducible failures before changing code.
- [ ] Have independent verifier inspect evidence and add missing adversarial
  coverage. Advisor challenges robustness claims against actual results.

## Task 9: Storage faults and fail-closed recovery

**Files:** node tests/storage_faults.rs and private test-only I/O fault controls.
**Interfaces:** test hooks surround actual adapter persistence boundaries, never
user-configurable production shortcuts. Any copied fault workspace has its own
`CARGO_TARGET_DIR`. Model write failures at the real adapter result boundary.

- [ ] Inject log append, vote save, apply batch and snapshot publication failures;
  assert no false success, no early LogFlushed success callback, and group admission
  closes after fatal storage failure. Healthy groups have independent admission,
  but a shared-engine failure can legitimately require the whole node to stop.
- [ ] Exercise storage directory permission/lock errors and corrupt application
  metadata/snapshot bytes; startup must fail with context and retain evidence.
  Never silently initialize empty replacement data when recovery fails.
- [ ] Test interrupted cleanup of inactive state generations and snapshot files;
  recovery only trusts the published manifest. Test DELETE after snapshot+tail
  recovery to prevent resurrection. Document limits of synthetic versus real
  ENOSPC/power-loss tests; no filling the user's disk to force an error.
- [ ] Independent verifier traces each injected failure through the actual API
  response and restart. Tests cannot report success solely from the injected hook.

## Task 10: Recorded-history consistency checks

**Files:** gateway tests/history.rs and tests/support/history.rs.
**Interfaces:** record invocation/completion order, key, operation, observed value,
and explicit outcome. A bounded per-key linearizability checker is test-only.

- [ ] Validate the checker with known legal and illegal concurrent histories:
  stale successful GET, resurrected DELETE, overlapping writes, and lost write
  replies. Unknown/pending writes may have committed; include optional effects
  while respecting real-time order of completed operations. Rejected/NotSent
  mutations must have no effect.
  Unknown notification is not execution completion: that accepted operation may
  apply after the notification and after intervening reads. Treat it as incomplete
  until observed actual completion or final quiescence. Count observed submissions
  separately to prove no replay; identical PUT/DELETE can hide replay in KV state.
- [ ] Capture real public-client histories across failover, gateway response loss,
  snapshot catchup and ABC->ABD movement. Verify each key's linearizability;
  do not infer cross-key transactions from per-key checks.
- [ ] Keep runs bounded and deterministic enough to reproduce. Persist the seed,
  topology and smallest failing history. Verifier checks the checker against
  hand-proven counterexamples before accepting system results.

## Task 11: Repeatable disruption and movement soak

**Files:** gateway tests/soak.rs and support fault schedule/reporting.
**Interfaces:** seed determines owned-node restart, proxy partition and move
schedule. Hard runtime deadline and cleanup apply on success, failure and signals.

- [ ] Run reproducible short schedules with mixed PUT/GET/DELETE over every group,
  leader churn, retained minority, snapshot catchup and controller restart.
- [ ] Attempt duplicate/conflicting moves for one group, lose responses at learner
  and membership stages, reconnect removed C and verify it cannot serve a stale
  strong read or acknowledge fresh writes. Preserve other groups' memberships.
- [ ] Reopen the surviving cluster and compare successful effects/history after
  the schedule; assert no detached tasks or child processes remain.
- [ ] Keep longer runs explicit local commands, not default slow unit tests or CI
  benchmarks. Verifier independently selects at least one new reproducible seed.

## Task 12: Resource bounds and reclamation under sustained load

**Files:** gateway tests/resources.rs, node tests/reclamation.rs, local report
runner. **Interfaces:** observable admission/queue/inflight/generation counters
are bounded-cardinality diagnostics; RSS is a measurement, not a fixed guarantee.

- [ ] Saturate count/bytes with near-limit values, many connections, a slow reader,
  failed leader, and repeated deadline/drop storms. Assert hard internal count/byte
  bounds and eventual permit reclamation; measure peak/plateau RSS separately.
- [ ] Build/install multiple snapshots during traffic; verify obsolete generation
  and image cleanup is bounded and never invalidates a pinned GET or transfer.
  Retained logs obey recovery policy; ordinary disk growth is reported explicitly.
- [ ] Repeat with more groups under the same configured cache/write-buffer budget
  and confirm budget objects are shared. Do not claim identical RSS or disk latency.
- [ ] Verifier checks measured versus asserted limits and tests healthy-group
  progress when one group consumes its per-group allowance.

## Task 13: Operable startup, shutdown and diagnostics

**Files:** node/gateway config and diagnostic commands, configs/README.md,
docs/operations.md and focused command tests.
**Interfaces:** readiness means identity/storage/config checks and required
metadata availability completed. Liveness is distinct from group quorum health.

- [ ] Reject incompatible persisted identity/hash/shard/format configuration,
  duplicate IDs/addresses and accidental bootstrap on existing storage. Provide
  clear error context without exposing values or credential material.
- [ ] Show group role/leader hint, committed/applied progress, membership, snapshot
  progress, queue counts/bytes and move-job state. Do not equate a leader hint
  with current read/write authority or label Unknown as a failed write.
- [ ] Test ordered shutdown under load, pending-handle resolution, restart, and
  owned-process signal cleanup. Repeated SIGINT/SIGTERM must not stop other apps.
- [ ] Verifier runs documented local commands from a fresh unique directory and
  confirms diagnostics explain a lost-quorum and interrupted-move scenario.

## Task 14: Reproducible local report and final review

**Files:** client example/load runner, configs/ local topology, docs/operations.md,
README.md, docs/progress.md. No CI benchmark additions.

- [ ] Provide commands for bootstrap, client API, restart, move, shutdown and
  bounded load. Report end-to-end latency percentiles/throughput/errors/Unknown,
  process CPU/RSS and replication/queue observations under stated limits.
- [ ] Separate warm/cold, uniform/hot-key, mixed load and replica movement results.
  No premature tuning or claims of independent-machine or 500 GB capacity.
- [ ] Final independent code/spec review, whole affected-path verification,
  parent/nested dependency checks and clean scope inspection. Fix findings with
  regression tests; publish a concise status with exact remaining limitations.

The objective is complete only after the whole path and failure/movement tests
pass. Elapsed time or a passing build alone cannot complete it.
