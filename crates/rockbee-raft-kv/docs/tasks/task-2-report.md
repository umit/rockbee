# Task 2: Bounded transport and eager client

Implementation ready for independent verification, 2026-10-19. No commits or
pushes. Scope: independent Raft protocol/client packages, their manifests/lock
entries and this report. Existing Rockbee runtime and `.herdr/` were untouched.

```text
Existing: Client -> Gateway -> Writer / Readers -> SlateDB
New:      RockBeeRaftClient -> bounded typed TCP Link -> controlled test peer
Pending:  RockBeeRaftClient -> Gateway -> backend Link -> Node -> Raft/RocksDB
```

The new transport/client path works against real owned localhost peers and
controlled in-memory byte streams. Gateway/node runtime integration is unfinished
and belongs to later tasks; this report does not claim Raft durability or a
complete deployed KV system from transport fixtures.

## Files and package structure

Protocol now has public domain modules with stable crate-root reexports:

- `identity/mod.rs`: unchanged checked typed IDs.
- `kv/{request,response,wire_request}.rs`: byte operations, mutation certainty,
  response compatibility and the meaningful `WireRequest<Resp>` trait.
- `message/{envelope,failure}.rs`: defaulted generic envelopes and bounded errors.
- `codec/mod.rs`: existing bounded postcard framing, exported allocation-free
  encoded-size pass and an internal capacity-limited encoder for admission.
- `routing/mod.rs`: unchanged deterministic shard mapping.
- `transport/`: `Link` orchestration; admission reservations; limits/errors;
  completion ownership; pending state; reader/writer/deadline workers; tests.
- `error.rs`, `limits.rs`, `validation.rs`: private shared validation helpers.

Client has `api/{client,mutation}.rs`, `config/mod.rs`, `error.rs`, stable root
exports and TCP integration tests. Production dependencies added are the already
workspace-defined Tokio, Bytes and thiserror. Tokio `test-util` is enabled only
as a protocol dev dependency. No allocator, TCP/runtime tuning, reconnect pool,
server framework or per-request task was introduced.

Protocol has `#![warn(missing_docs)]`. Strict Clippy therefore checks every public
module/type/field/variant/method has rustdoc. The crate introduction explains
`Result`, `Option`, enums, modules and the message-family trait for a Java reader.
Existing executable wire example and all golden frames remain valid. Root owns
the additional Java-oriented protocol README.

## Public behavior

`Link<Req, Resp>::connect(address, config, absolute_deadline)` opens one TCP
connection. `try_submit(request, deadline)` synchronously validates and admits an
owned request, returning `PendingResponse<Resp>`. No polling starts admission.
`PendingResponse` yields `Result<Resp, TransportFailure>`; the failure carries a
typed reason and `Delivery::{NotSent, MayHaveBeenSent}`. Link never reconnects or
replays. `close().await` retires the generation and joins all three workers.

`RockBeeRaftClient` exposes synchronous `put(Bytes, Bytes)` and `delete(Bytes)`
returning `Result<MutationHandle, AdmitError>`, and async `get(Bytes)` returning
`Result<Option<Bytes>, ReadError>`. Handles await to `MutationOutcome`.
`ClientConfig` carries request timeout and `LinkConfig`; timeouts outside
1 millisecond through 60 seconds are rejected. Defaults are 128 operations,
16 MiB request ownership and 16 MiB receive ownership.

Transport handoff uncertainty maps to mutation `Unknown`; proven local non-handoff
maps to `NotSent`. A matching valid server outcome is preserved. Remote `NotSent`
is invalid; a server that declines received work must use `Rejected`. GET
preserves missing, empty, binary values and explicit read failures. A retired
client needs an explicit new `connect` call. Dropping an accepted handle discards
observation without cancelling, rolling back or prematurely releasing admission.

## State, deadlines and reservation ownership

A short-held ordinary mutex owns pending entries and one bounded ordered deadline
set. Entries retain only deadline, phase, small Copy expected-response token,
completion sender and shared count/metadata reservation. They never retain the
request payload for response matching. No map lock spans an await; concrete
response validation also runs outside the lock, followed by a state/deadline
recheck before delivery.

Queued expiry and writer handoff compete under that same lock. Handoff occurs
immediately before the first write poll; it conservatively means possibly sent
even if the fixture accepts zero bytes. Queued expiry removes the pending/timer
entry and reports `NotSent`, while the actual queue item retains its reservations
until the writer drains it. After any handed expiry, EOF, I/O failure or protocol
violation, the entire generation retires: handed work is uncertain and proven
queued work is unsent. No next frame can follow an interrupted partial write.
Concurrent handed mutations can therefore receive collateral `Unknown`.

The writer stamps remaining budget after queueing, performs bounded encoding,
and checks the absolute deadline again at handoff. Reader delivery checks that
same absolute deadline even when the timer actor is delayed. Completion removes
the exact deadline entry; stale timers cannot retire newer healthy work. IDs use
checked nonzero monotonic allocation and never wrap/reuse within a generation.
Unknown, duplicate, incompatible or invalid replies retire it. A completed result
wins once and survives later stream failure.

Count and metadata reservations are shared by queue, pending entry and completion
channel, ending at the last actual owner. Request-body and encoded-frame overlap
is reserved before encoding and retained until the writer drops both. The size
pass includes worst-case correlation ID/budget, and the writer enforces that
measured encoding capacity even for a changing serializer. Fixed ownership
headroom plus actual generic inline sizes cover queue/map/channel metadata.

RX reads only the four-byte header first, validates its bound, reserves encoded
plus decoded headroom, then allocates/reads/decodes payload bytes. The result's
receive reservation stays in the completion channel until consumed or dropped.
An unpolled handle therefore cannot accumulate unaccounted decoded values.
Returned values become caller ownership. Slow-result tests show decoding waits
at the receive budget before allocating the next payload. Impossible/overflowing
configuration limits are rejected before connection activation.

`close` is idempotent and serialized by a worker-join mutex separate from the
pending mutex. Join handles stay in its vector while awaited, so cancellation of
a close future does not detach them from a later close. Dropping the last Link
initiates retirement; explicit close is the API for awaiting worker completion.

## Actual red/green and verification evidence

Commands ran from `/Users/umitunal/IdeaProjects/rockbee`, using the separate
`CARGO_TARGET_DIR=target/transport-task-2` with two build jobs. Client/protocol
commands did not build node/RocksDB or interfere with root's native target.

Before implementation, the eager-client integration test failed to compile on
missing `ClientConfig`/`RockBeeRaftClient`. An initial dependency-incomplete run
also reported missing Bytes/Tokio; after adding test dependencies, the repeated
red run reported only the missing client API. This is API red evidence, not a
claim that every adversarial case first failed behaviorally. Initial compile
errors (missing trait associated type and a request borrow spanning await) were
resolved by an explicit associated type and a scoped encoding function, without
adding an unnecessary `Sync` bound.

A private fault-injection copy removed only the reader's final absolute-deadline
check. It used its own Cargo target, never the real workspace target:

```text
CARGO_TARGET_DIR=/private/tmp/rockbee-task2-late-response-red-9em1lrw7/target
cargo test --offline \
  --manifest-path /private/tmp/rockbee-task2-late-response-red-9em1lrw7/Cargo.toml \
  -p rockbee-raft-protocol \
  transport::tests::reader_rejects_late_success_even_when_deadline_actor_is_delayed \
  -- --exact

FAILED: called Result::unwrap_err() on an Ok value: Value(None)
0 passed; 1 failed; exit 101
```

The unmodified real workspace passes that case and the affected complete suites:

```text
CARGO_TARGET_DIR=target/transport-task-2 CARGO_BUILD_JOBS=2 \
  cargo test --offline --locked --manifest-path crates/rockbee-raft-kv/Cargo.toml \
  -p rockbee-raft-protocol -p rockbee-raft-client

client TCP tests:          4 passed, 0 failed
protocol transport tests: 19 passed, 0 failed
protocol contracts:       9 passed, 0 failed
independent adversarial:  2 passed, 0 failed
v1 golden frames:         4 passed, 0 failed
protocol rustdoc:         1 passed, 0 failed
```

Real TCP tests ran with sandbox escalation for owned `127.0.0.1:0` listeners,
as required by the documented environment EPERM baseline. They passed. No
approval rejection occurred. All listeners and streams are fixture-owned; no
existing service/container/data was touched. Most transport races use bounded
Tokio duplex streams, explicit observed bytes/write-poll notifications and paused
time instead of wall-clock races. The public timeout smoke test uses a real
100 ms deadline after the peer explicitly observes its mutation.

Strict Clippy first exposed missing rustdoc after the new lint was enabled
(65 public variant/field items). Added meaningful documentation, then reran it.
Final check results are recorded below after those documentation-only edits.

## Coverage map and limits

| Contract | Executable evidence |
| --- | --- |
| Eager unpolled submission, dropped observer | TCP client eager/drop test; transport drop/admission test |
| Out-of-order correlation, retained completed-result admission | Two unpolled requests with reversed replies and permit readback |
| Queued expiry and retained queue storage | Blocked writer, paused expiry, zero second-frame bytes, repeated saturation attempts |
| Zero-byte post-handoff expiry | Test-only AsyncWrite records first poll and accepts no bytes |
| Partial write timeout/error | One-byte duplex capacity, timeout/disconnect, no following frame |
| Missing response, collateral uncertainty, no replay | Two handed requests retire; separate replacement stream; TCP mutation smoke test |
| Reply beats timer / late reply / delayed timer | Completed stale deadline; late retired reply; independently delayed coordinator |
| Duplicate, unknown ID, wrong kind, remote NotSent | Active stream fails closed; completed result preserved |
| ID exhaustion | Final u64 ID emitted, next submission retires without wrap |
| Count/byte saturation and dropped work | Count/result ownership, maximum PUTs and close permit readback |
| Receive/result ownership cap | Three maximum replies; third remains undecoded until an observer drops |
| Hostile outer/inner lengths | Rejection with receive-permit cleanup; existing independent hostile tests |
| Independent message family | Ping/Pong trait implementation and generic envelope transport |
| Deadline budget after queueing | Paused queue delay reduces transmitted budget; expired admission writes nothing |
| Shutdown cleanup | Queue/writer/awaiting-reply retirement; empty owned worker vector and permit readback |
| Wire/API preservation | Original 15 protocol tests and four literal v1 golden frames |

Server pre-materialization GET response reservation and aggregate connection
admission are Task 6 responsibilities; this client-side Link does not implement
or claim them. Trusted concrete message decoders must obey the documented
allocation bounds; arbitrary serde collection visitors are not sandboxed.
Caller `Bytes` slices may retain larger backing allocations than logical length,
so these are logical ownership budgets rather than a total RSS guarantee.
No allocator/performance/load benchmark, real gateway integration, Raft commit,
reader-convergence or node-process recovery claim is made here. Root coordinates
the entire workspace/node suite separately while native storage work proceeds.

## Final checks after public-documentation edits

```text
CARGO_TARGET_DIR=target/transport-task-2 CARGO_BUILD_JOBS=2 \
  cargo clippy --offline --locked --manifest-path crates/rockbee-raft-kv/Cargo.toml \
  -p rockbee-raft-protocol -p rockbee-raft-client --all-targets -- -D warnings
  exit 0 (including protocol missing_docs enforcement)

cargo fmt --manifest-path crates/rockbee-raft-kv/Cargo.toml \
  -p rockbee-raft-protocol -p rockbee-raft-client -- --check
  exit 0

cargo fmt --manifest-path crates/rockbee-raft-kv/Cargo.toml --all -- --check
  exit 1: concurrent out-of-scope node tests/storage.rs and tests/support/mod.rs
  require formatting; root/storage owner informed; these files were not edited
  by the transport task

CARGO_TARGET_DIR=target/transport-task-2 CARGO_BUILD_JOBS=2 RUSTDOCFLAGS='-D warnings' \
  cargo doc --offline --locked --manifest-path crates/rockbee-raft-kv/Cargo.toml \
  -p rockbee-raft-protocol --no-deps
  exit 0
```

The final complete affected test run after enabling documentation lint again
passed 19 transport, 15 original protocol, 4 TCP client and 1 rustdoc tests.
Additional independent clone/lazy-GET/cancel-close checks are owned by the
verifier in a separate test target. Independent approval remains pending.
