# Runtime bootstrap decision and peer review

Status: advisor static review; discussed directly with verifier by message, independent response/tests pending. No code changes or builds. No disagreement has yet been raised, so no Judge is requested.

## Recommended bootstrap operation

Add an engine-free internal `BackendOperation::BootstrapDirectory` with **no directory payload**. Only the designated logical initializer invokes it. The destination verifies cluster/target, uses group zero, and chooses its locally validated canonical initial `ClusterDirectory` (sorted nodes/placements/voters, revision one). All nodes must have the same immutable initial configuration. The response is a distinct typed bootstrap result: success/already-present, safe pre-submission rejection with optional leader hint, or uncertain outcome. Existing expected-response matching must cover the new variant.

Receiver flow:

1. Reserve bounded operation/response resources and obtain a real linearizable group-zero read. A leader hint alone is insufficient.
2. Existing identical directory: report success without writing. Existing different/corrupt directory: return configuration failure, retain evidence, never overwrite. Absent directory: proceed only within the explicitly enabled static bootstrap mode.
3. Check authority before submission, then submit the reserved-key `Command::Put` using the configured canonical bytes. A role change may still race this check. Once `client_write` is invoked, timeout, transport loss, or `ForwardToLeader` is **Unknown**, never proof of rejection. Retain proposal reservations until actual backend completion.
4. Return successful publication only after the ordinary durable Raft apply response. On Unknown, the initializer discovers a current candidate and performs a new linearizable `Directory` read before deciding what to do next. Equal value completes bootstrap; a different value stops it; unavailable quorum waits/fails readiness under the bounded deadline.

**An absent read after Unknown does not prove an earlier proposal cannot apply later.** Repeating this internal operation is safe only through semantic idempotence: all outstanding bootstrap writes contain exactly the same immutable bytes, and directory updates are disabled for this phase. Duplicate internal log entries are possible. This is not a permission to replay uncertain public mutations, and it is not an exactly-once claim.

Before Task 7 introduces changing directory values, replace ordinary Put with an apply-time put-if-absent bootstrap command, or prove every old bootstrap executor/proposal durably retired and drained. A read/check followed by unconditional Put cannot protect a newer directory from a delayed old proposal. Revision one is not a fence.

The alternative without a new RPC is a leader watcher on every node that publishes the same configured value. It is semantically possible in the same immutable phase, but changes the single-initializer lifecycle and repeats orchestration in background workers. Prefer the explicit bounded internal operation. Do not expose group-zero reserved-key writes through the public KV API.

Tests required: initializer is not metadata leader; leader changes between read and submission; response lost after apply; Unknown followed by temporarily absent read then delayed original apply; equal bootstrap after restart; conflicting configured directory cannot overwrite existing value. Count duplicate internal proposals separately from public no-replay tests.

Exact upstream evidence: OpenRaft 0.9.25 `src/core/raft_core.rs`, `Command::DeleteConflictLog`, sends `ClientWriteError::ForwardToLeader` to existing log-indexed pending responders after truncation. This confirms post-invocation ForwardToLeader is not exclusively pre-append rejection. Earlier source analysis is recorded in `advisor-authority.md`.

## Peer review — changes required before approval

### P0: preserve the full committed leader identity

`apps/rockbee-raft-node/src/rpc/dto.rs` currently defines `WireLogId { term, index }` and reconstructs `CommittedLeaderId::new(term, 0)`. This drops the original leader node ID on every request LogId conversion: previous log, entries, leader commit, snapshot last log, and stored membership log.

Our workspace enables OpenRaft `serde` and `storage-v2`, not `single-term-leader`. Exact 0.9.25 `src/vote/leader_id/mod.rs` selects `leader_id_adv.rs`, where `CommittedLeaderId<NID>` aliases `LeaderId<NID> { term, node_id }`. Node ID is part of identity and ordering. Add it to the DTO and preserve it exactly. Test all request LogId positions with a nonzero leader ID, including equal-term/different-node identities. The existing maximum-entry encode-only test does not catch this corruption.

### P1: stalled peer sockets can prevent shutdown and starve control traffic

`rpc/server.rs::serve_connection` has no deadline or shutdown branch while reading the fixed header/body, waiting for a lane, or writing a response. The accept loop joins every child forever. One idle connection can therefore prevent `close()` from finishing; enough incomplete/snapshot connections can consume all incoming connection slots despite separate control/snapshot operation pools.

Add bounded receive/admission/response-write phases and shutdown cancellation before dispatch. Once an operation reaches OpenRaft, retain its reservations and drain its actual completion; do not simply wrap the whole handler in timeout and discard consensus work. Reject or boundedly wait on a saturated snapshot lane so waiting snapshot connections cannot monopolize all control connection capacity. Test idle prelude, partial body, saturated snapshot lane, and non-reading response peer with owned sockets and phase barriers.

### P1: frame rejection currently happens after large copies and serialization

`rpc/message.rs::encode_request` first clones every command into owned DTOs; `encode_wire` then calls unrestricted `postcard::to_allocvec`, and only afterward compares with the 2 MiB limit. A valid 128-entry batch with 1 MiB values can produce roughly 128 MiB of copied payload plus serialization before returning a split hint. Shared operation counts bound concurrency but do not make these buffers fit a 2 MiB work budget.

Preflight count and exact serialized size through borrowed wire views/a bounded sizing path before payload cloning or allocating an encoded frame, then issue a positive append split hint. Encode a fitting request into a bounded preallocated slice. Account for upstream request ownership separately; do not call its existing allocation a transport frame guarantee. Test a large multi-entry input is split before large transport allocation, while one maximum legal entry round-trips successfully. Keep node `Config::max_payload_entries` aligned to 128 or return a proper count split hint: current count rejection is a generic protocol error and default upstream 300 would not shrink from that error.

### Small boundary fixes

- `PeerLimits::validate` must reject semaphore counts above `Semaphore::MAX_PERMITS`; otherwise the fallible `new` API can panic on configuration. Reject snapshot chunk limits above the DTO's fixed 256 KiB cap, and account for framing overhead rather than merely allowing chunk bytes equal frame bytes.
- Bound/check header source against the actual request vote's candidate/leader identity before dispatch. Check hosted group before decoding a large body. Header identity fields are routing checks, not authentication.
- Reject duplicate voters/nodes and inconsistent/empty membership configurations instead of silently collapsing duplicates into sets/maps. Preserve legitimate empty default membership in an empty snapshot if OpenRaft requires it; distinguish that from a malformed membership log entry.

Positive findings: request bodies now use private bounded DTOs; no direct unbounded upstream membership deserialization occurs on the request socket path. Fixed prelude validation precedes body allocation. Success responses come only from actual OpenRaft method success; error mapping does not invent Raft success. Incoming/outgoing lane pools are separate and factory construction is lazy. These are useful foundations, but do not remove the above blockers.

Root and verifier were notified of the P0 immediately. Independent verifier owns empirical regression tests and final peer approval; advisory review alone does not approve the task.
