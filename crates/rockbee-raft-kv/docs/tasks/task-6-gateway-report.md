# Task 6a: typed server and gateway

Implemented the engine-free receiving side used by both the public gateway and
the node data service. No commit or push was made. Existing Rockbee and
`.herdr/` were untouched.

```text
Public client -> GatewayRuntime -> typed Server -> Gateway handler
                                      |
                                      +-> bounded Link<BackendRequest, BackendResponse>
                                                   -> node data server (root-owned)
                                                   -> Raft / RocksDB (root-owned)
```

## Public API

`rockbee_raft_protocol::transport::server` exports:

- `RequestHandler`: a typed application capability with associated `Request`
  and `Response` types and `handle(request, deadline)`.
- `ServerConfig`: aggregate connection/work/request/response limits and
  bounded I/O timeout. `validate()` is public so node startup can reject bad
  configuration before it opens storage or starts Raft groups.
- `Server`: `start(listener, config, Arc<handler>)`, `stop_accepting()`, and
  cancellation-safe `close(&mut self)`.

The server validates envelope version/ID/budget/body before dispatch. It
reserves global request storage before allocating the payload and reserves the
maximum response (including frame/value overlap and concrete decoder headroom)
before calling a handler. Duplicate active IDs, invalid input, and saturation
retire the one client stream. Accepted handlers remain joined and charged after
socket loss; no accepted mutation is cancelled or replayed by the server.

`rockbee-raft-gateway` now exposes:

- `GatewayConfig` / `SeedNode`, with bounded, validated bootstrap seeds.
- `Gateway::connect(config, absolute_deadline)`, which reads and validates the
  durable directory before serving. It can follow a metadata-leader hint only
  to another configured seed and only before the same deadline.
- `GatewayRuntime::{start,start_with_listener,close}` for application and
  owned-listener test lifecycle.

Gateway hashes each public key using the persisted directory mapping; it keeps
only directory and bounded backend-link state, never user KV data. It retries a
mutation only when the node explicitly returns matching pre-submission
`Rejected(NotLeader)`. A backend `MayHaveBeenSent` transport failure becomes
`Unknown`; it is not replayed. Reads may move to another voter before their
deadline.

## Java-oriented documentation and tests

The protocol README now adds a typed-server section explaining the trait as a
small Java-like capability interface, associated request/response types,
`impl Future`, admission ownership, duplicate IDs, disconnect behavior, and
ordered shutdown. It also adds `tests/server.rs` to the test map.

The new focused protocol server tests cover:

- validated request ID/version preservation;
- duplicate active ID closes the stream without a second handler call;
- response budget rejection before handler execution;
- a disconnected accepted request still occupies its global operation slot
  until the handler finishes.

The gateway fixture uses a real owned localhost client, gateway listener, and
typed backend listener. It proves that startup requires a valid directory and
that a public PUT is routed through the gateway/backend message family to an
explicit `Committed` fixture response. It does **not** prove a durable Raft
commit; root-owned node integration provides that evidence.

## Verification

All commands ran in the nested workspace with the isolated gateway target:

```text
CARGO_TARGET_DIR=target/gateway-task-6 CARGO_BUILD_JOBS=2 \
  cargo test -p rockbee-raft-protocol -p rockbee-raft-gateway --locked --offline

gateway fixture:                 1 passed
protocol transport/unit:        20 passed
protocol contracts/backend:     13 passed
protocol server:                 4 passed
protocol adversarial/lifecycle:  3 passed
protocol v1 wire:                4 passed
protocol rustdoc:                1 passed

CARGO_TARGET_DIR=target/gateway-task-6 CARGO_BUILD_JOBS=2 \
  cargo clippy -p rockbee-raft-protocol -p rockbee-raft-gateway \
  --all-targets --locked --offline -- -D warnings
exit 0
```

The localhost cases used owned `127.0.0.1:0` listeners under the authorized
test escalation. No existing process, container, or storage path was touched.

## Remaining integration gate

This task deliberately does not claim that a fixture `Committed` is durable.
The node runtime must supply real `BackendRequest` handling, OpenRaft
submission/read authority, and its three-node/four-data-group process test.
That root-owned path is still the required proof for the end-to-end goal.

## Focused follow-up review fixes

Advisor and verifier identified a cancellation bug in the first server loop:
when a handler completed while the next TCP frame was only partly read,
`select!` could drop that receive future and treat the remaining bytes as a new
header. The server now pins one receive future for each frame and keeps polling
it while handler joins are drained. Only server shutdown/socket retirement can
cancel that partial receive. The deterministic held-A / partial-B / complete-A
/ remainder-B regression passes with B's original ID and bytes exactly once.

Gateway now retries an explicit `ReadError(NotLeader)` on another voter. A
three-endpoint fixture verifies an empty value survives this route change. Its
backend links use a bounded per-node generation slot: the directory map lock is
released before dialing, slot lock waits observe the operation deadline, and a
retired generation is replaced only for a new locally-unsent request. A late
failure from an older generation cannot evict a newer one. Existing handed
mutations remain `Unknown` and are never submitted again.

`ServerConfig` also rejects a response queue count above Tokio's supported
permit counter before spawning any task. The thin CLI validates hexadecimal
cluster IDs by bytes before indexing, installs shutdown signals before startup,
and closes the owned runtime even when waiting for a signal reports an error.
