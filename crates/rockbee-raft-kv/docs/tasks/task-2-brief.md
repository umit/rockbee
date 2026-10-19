# Task 2: Bounded transport and eager client

Read this first; requirements for the independent module at
`/Users/umitunal/IdeaProjects/rockbee/crates/rockbee-raft-kv`.
Scope: protocol transport modules, compatible generic envelopes, client package,
their tests and only needed workspace dependencies. No engine/node/gateway runtime
edits, no commits/push, no subagents. `.herdr/` forbidden. Existing Rockbee unchanged.

Binding API/state details are in `docs/reviews/advisor-transport.md`; deterministic
tests and ownership detail are in `docs/reviews/verifier-transport.md`. Read both.
They are agreed requirements, including whole-connection retirement on post-handoff
expiry. Avoid copying speculative engine code into this task.

Deliver one generic TCP Link and a public RockBeeRaftClient. Envelopes become
`RequestEnvelope<R = KvRequest>` and `ResponseEnvelope<R = KvResponse>` preserving
Task1's wire/public defaults. Use `WireRequest<Resp>` with a Copy expected-response
token, body validation, conservative retained byte accounting and response matching.
Do not keep the request payload merely to validate its reply.

`Link::connect(address: SocketAddr, config: LinkConfig, connect_deadline: Instant)`
is async. `try_submit(request, deadline) -> Result<PendingResponse<Resp>, AdmitError>`
is synchronous/eager. PendingResponse resolves to `Result<Resp, TransportFailure>`;
failure contains `Delivery::{NotSent, MayHaveBeenSent}` and typed reason.
`Link::close()` joins owned workers. No hidden reconnect or replay inside Link.

Public client: `RockBeeRaftClient::connect(address, ClientConfig)` async;
`put(key: Bytes, value: Bytes)` and `delete(key: Bytes)` synchronously return
`Result<MutationHandle, AdmitError>`; `get(key: Bytes)` async returns
`Result<Option<Bytes>, ReadError>`. MutationHandle is a Future of MutationOutcome.
ClientConfig carries request timeout and LinkConfig. Explicit close supported.
For v1 a retired connection requires a newly connected client; do not silently
replay pending operations. Documentation must explain eager handles, drop behavior,
uncertainty and collateral Unknown after connection retirement.

Use Tokio1 (net,io-util,sync,time,rt-multi-thread,macros), only needed utilities;
no custom allocator/lock-free structures or runtime/TCP tuning. One reader, writer
and bounded deadline coordinator per link; no detached per-request tasks/tombstones.
An ordinary short-held mutex or actor is fine. Never await under map mutex.

Defaults:128 outstanding operations,16MiB request retained-byte budget,16MiB receive
budget. Configuration must admit at least one legal maximum frame and associated
decode headroom; validate zero/overflow/impossible limits. Count+byte permits bind
actual retained ownership, including queue items surviving pre-handoff expiry.
Reserve before request encoding and before incoming payload allocation. Exact
postcard size pass is available if it simplifies safe reservation; account for
request and frame overlap. Bytes backing allocations retained by slices are a
documented caller-input limitation, not a promised total RSS cap.

Hold receive/result reservations while a decoded response waits in its completion
channel; release when the caller consumes or drops it. Otherwise unpolled handles
can accumulate unaccounted values. Once a result is returned to the caller, its
future ownership is outside Link's budget. Do not allocate values beyond budget
just to block later on egress. Server side pre-materialization response reservation
belongs Task6, but Link RX must already reserve before decoding.

Queued expiry and writer handoff have one atomic winner. Before the first socket
write poll, writer marks Handed. Pre-handoff timeout => NotSent with zero bytes
ever written; post-handoff timeout => retire generation, all handed work uncertain,
queued proven-unsent work NotSent. Partial write never allows a following frame on
that stream. Stamp remaining budget after queueing and check absolute deadline
again before write and response delivery. Stale timers cannot retire healthy links.
Checked nonzero IDs never wrap/reuse inside a connection. Duplicate/unknown/wrong
kind/invalid response => retire. A completed response wins once; later events cannot
change it. Dropped handles do not remove accepted work or release its reservations
prematurely.

Tests first: use controlled peer fixtures and explicit synchronization, not flaky
wall-clock races. Cover eager unpolled submission, dropped handle, out-of-order
correlation, count/byte saturation, before/after handoff expiry, partial writes,
missing response/no replay, late and duplicate response, wrong kind/NotSent wire,
ID exhaustion, stale timer, slow receive/decoder cap, and complete shutdown cleanup.
Use test-only deterministic hooks or an internal generic AsyncRead/AsyncWrite
fixture if required for partial writes; no production fault switches. All owned
ports use 127.0.0.1:0 and unique temporary data if any. No unrelated processes.

Do not implement a generic server framework in this task. A tiny test peer can
frame messages directly. Task6 will introduce bounded server dispatch with response
reservations before materialization; private peer server is Task5.

Environment evidence: a minimal bind to 127.0.0.1:0 fails with EPERM under the
default sandbox. Run real TCP tests with sandbox escalation justified by this
authorized local test topology; do not misreport permission failure as product
failure. Capture actual outcomes and any auto-review rejection. Native dependency
build may still own main target; use `CARGO_TARGET_DIR=target/transport-task-2`
for client/protocol-only tests, with two build jobs if a fresh build is needed.

User readability requirement: short named orchestration steps, cohesive files,
clear struct/trait responsibilities. Do not hide public APIs in local macros.
Use the real WireRequest trait because transport operates on distinct message
families; do not introduce unrelated inheritance/factories or clones/locks for style.

Run focused red/green tests, cargo fmt --all -- --check, client+protocol all-target
Clippy with -D warnings. Root coordinates Cargo ownership/native build timing.
Write docs/tasks/task-2-report.md with status, files, actual red/green evidence,
ownership/state decisions and concerns. Return concise report; independent verifier
will review the diff and exercise missing adversarial cases before acceptance.
