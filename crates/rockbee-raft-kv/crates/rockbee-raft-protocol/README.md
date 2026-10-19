# Understanding the protocol

This crate defines how Rockbee Raft components exchange requests and results.
It contains byte-key/value messages, validation, framing, stable shard selection,
and a bounded asynchronous connection. It has no RocksDB or OpenRaft dependency.

The intended application flow is:

```text
Application -> public client -> gateway -> owning Raft group -> RocksDB
                 |                |               |
                 +------ protocol contracts ------+
```

The protocol and client have tests. The gateway, Raft runtime, and storage path
are being implemented separately. A protocol test cannot establish that a real
cluster durably committed a write.

## Where to start

A Cargo **crate** is a compilation and dependency unit, roughly comparable to a
Java library module. A Rust **module** groups names and controls visibility,
roughly comparable to a Java package. We keep related types together instead of
creating a file for every type.

| Read in this order | Responsibility | Main types/functions |
| --- | --- | --- |
| [`identity`](src/identity/mod.rs) | Distinguish IDs and enforce their invariants | `RequestId`, `NodeId`, `ClusterId`, `GroupId` |
| [`kv`](src/kv/mod.rs) | Describe operations and their possible results | `KvRequest`, `KvResponse`, `MutationOutcome` |
| [`message`](src/message/mod.rs) | Add correlation, version, budget, and failure information | `RequestEnvelope`, `ResponseEnvelope`, `Failure` |
| [`codec`](src/codec/mod.rs) | Turn messages into bounded frames and back | `encode_frame`, `decode_frame` |
| [`routing`](src/routing/mod.rs) | Map a key to a stable logical group | `ShardConfig` |
| [`transport`](src/transport/mod.rs) | Send concurrent requests over one connection | `Link`, `PendingResponse`, `LinkConfig` |
| [`transport::server`](src/transport/server/mod.rs) | Accept bounded typed requests for the gateway and nodes | `RequestHandler`, `Server`, `ServerConfig` |
| [`lib.rs`](src/lib.rs) | Expose the public API and a runnable example | Crate-root re-exports |

`pub use` in `lib.rs` is an export, not inheritance. Both of these paths name the
same type:

```rust
use rockbee_raft_protocol::KvRequest;
// The longer path is rockbee_raft_protocol::kv::KvRequest.
```

## Rust concepts through a Java lens

These comparisons help with reading; they are not exact language equivalences.

| Rust | Useful Java comparison | Important difference |
| --- | --- | --- |
| `struct RequestId(u64)` | A small final value class wrapping a `long` | The wrapped integer is unsigned; a private field protects construction |
| `impl RequestId` | Methods belonging to that class | Methods are written separately from the data declaration; there is no class inheritance |
| `trait WireRequest<Resp>` | A generic capability interface | `Link` uses a concrete implementation through generics; this is not a hierarchy of base classes |
| `enum KvRequest` | A sealed interface with PUT/GET/DELETE record variants | Each variant can carry different data; `match` must handle the possible variants |
| `Result<T, E>` | A success-or-error return value | Recoverable errors are explicit values; `?` returns the error to the caller |
| `Option<T>` | `Optional<T>` | `None` represents absence without a nullable pointer |
| `&T`, `&[u8]` | Reading a referenced object or byte sequence | The borrow checker prevents invalid lifetimes and conflicting mutation |
| `Bytes` | An immutable shared byte buffer | Cloning usually shares storage; a small slice can retain a larger allocation |
| `Arc<T>` | Shared ownership of an object | Reference counting keeps it alive; it does not itself make mutations safe |
| `async fn` / `.await` | An asynchronous method / awaiting a future | Calling an `async fn` produces a future; polling drives it. Some APIs here deliberately submit synchronously instead |

For example, `Result<Option<Bytes>, ReadError>` has three distinct meanings:

```text
Ok(Some(bytes)) -> key exists (bytes may be empty)
Ok(None)        -> key does not exist
Err(error)      -> read failed; absence was not established
```

The public client uses that return type for GET. The wire represents the same
distinction with `KvResponse::Value(Some(...))`, `Value(None)`, and `ReadError`.

### What are the macros doing?

Macros generate Rust code at compile time. The closest familiar comparison is
a Java annotation processor, although Rust macros can also generate expressions
and control flow. Not every `#[...]` attribute is itself a macro.

| Syntax in this crate | What it provides |
| --- | --- |
| `#[derive(Debug)]` | A debug representation, useful in test failures and logs |
| `#[derive(Clone, Copy)]` | Explicit cloning, and implicit copying for eligible small value types |
| `#[derive(Eq, PartialEq, Ord, PartialOrd, Hash)]` | Equality, ordering, and hashing implementations |
| `#[derive(Serialize, Deserialize)]` | Serde encoding/decoding implementations for the declared fields/variants |
| `#[serde(try_from = "u64", into = "u64")]` | Instructions to the Serde derive: decode through our validating conversion |
| `#[derive(Error)]` with `#[error(...)]` | `thiserror` generates standard error/display implementations |
| `#[tokio::test]` | A test entry point with a Tokio runtime for an async test body |
| `#[cfg(test)]` | A compiler attribute that includes the item only in test builds |
| `#[must_use]` | A compiler attribute warning when a significant return value is ignored |
| `tokio::select!` | Wait for multiple async branches and execute a ready branch |

Read a `select!` as "wait for a response **or** a timeout **or** shutdown".
The losing futures are dropped for that invocation; this does not automatically
undo external work they started. `biased;` means check ready branches in source
order, so an earlier shutdown branch can take priority. The surrounding code
must still preserve ownership and handle partially written frames safely.

These macros remove repetitive implementations. The request lifecycle,
validation rules, and error decisions remain in named functions you can follow.

## IDs are different types for a reason

- `RequestId` correlates a response with a request on a connection. Zero is invalid.
  It is **not** a persisted idempotency token and does not make retries safe.
- `NodeId` identifies a storage process in the public configuration. Zero is invalid.
- `ClusterId` identifies a cluster. The all-zero identifier is invalid.
- `GroupId` identifies a logical Raft group. Zero is reserved for metadata;
  data groups start at one. A numeric `GroupId` alone does not prove that the
  group exists or belongs to a particular node.

The private fields prevent accidentally passing a request ID where a node ID is
required. Constructors return `Result` for fallible validation. Serde uses the
same checks when decoding the validated ID types.

## Follow a request through the layers

Consider PUT with key `product/42` and an opaque value. JSON is allowed as bytes,
but this protocol does not parse it or infer a schema.

1. `KvRequest::Put { key, value }` describes the operation.
2. Local validation checks key/value bounds before admission.
3. `Link::try_submit` reserves count and byte capacity, assigns a request ID,
   and queues the request immediately. It returns a `PendingResponse`.
4. The writer computes the remaining time budget and encodes a `RequestEnvelope`.
5. A frame carries a four-byte big-endian payload length followed by a Postcard
   payload. A TCP read can return only part of either; transport assembles a frame.
6. The receiver must validate the decoded envelope before dispatching its body.
7. A response carries the same request ID. Transport validates its version,
   correlation, result kind, and field bounds before completing the waiting caller.

The higher-level client wraps this in `put`, `get`, and `delete`. See
[`RockBeeRaftClient`](../rockbee-raft-client/src/api/client.rs).

### Encoding is separate from validation

`encode_frame` and `decode_frame` are generic framing functions. A correctly
framed message can still contain a wrong version, expired/invalid budget, or an
inappropriate response kind. Decoding is not permission to execute it.

For public KV messages, use:

```rust
use bytes::Bytes;
use rockbee_raft_protocol::{
    KvRequest, RequestEnvelope, RequestId, decode_frame, encode_frame,
};

let outgoing = RequestEnvelope::new(
    RequestId::new(7)?,
    1_000,
    KvRequest::Get { key: Bytes::from_static(b"product/42") },
)?;
let frame = encode_frame(&outgoing)?;
let incoming: RequestEnvelope = decode_frame(&frame)?;
incoming.validate()?;
# Ok::<(), rockbee_raft_protocol::ProtocolError>(())
```

The equivalent example in `lib.rs` runs as a documentation test. Public envelope
fields allow direct construction, so receivers must still validate them.

### A generic envelope is not a universal message type

`RequestEnvelope<R = KvRequest>` means the body type is a generic parameter, with
`KvRequest` as the default. `RequestEnvelope` therefore means
`RequestEnvelope<KvRequest>` when no other body type is given.

`Link<Req, Resp>` shares connection machinery between concrete message families.
`WireRequest<Resp>` supplies validation, byte accounting, and response matching.
Its associated `Expected` type is a small compatibility token. For KV it says
"read" or "mutation", so transport need not retain the original key/value solely
to check a later response.

This trait is a contract for trusted implementations. Implementing Serde alone
does not prove that arbitrary nested collections decode within the transport's
memory accounting. A new message family needs its own bounds and hostile-input
tests. Raft engine messages belong to the node's private peer protocol.

## Mutation results: what is actually known?

| Outcome | Meaning | Caller implication |
| --- | --- | --- |
| `Committed` | The server reports durable consensus completion | The write succeeded under the server's durability contract |
| `NotSent(failure)` | Local transport proves it did not hand off the request | This attempt did not reach the server |
| `Rejected(failure)` | The server proves rejection before consensus submission | This attempt was not submitted to Raft |
| `Unknown(failure)` | The request may have committed | Do not automatically replay it as if it failed |

`NotSent` is local-only. A server response containing it is invalid. A server
that received a request and refused it before consensus submission uses
`Rejected`. An error after submission cannot automatically be reclassified as
`Rejected`, even if the reason mentions a leader change.

`FailureReason` tells you **why** something failed. `MutationOutcome` tells you
**what is known about its effect**. `Deadline` or `Disconnected` alone cannot
answer whether a write happened.

The protocol cannot independently prove a server's durability promise. That
requires storage and real-cluster tests in addition to these message tests.

## Eager submission, deadlines, and connection lifetime

`Link::try_submit` and client `put`/`delete` are ordinary synchronous methods:
admission happens before they return. Awaiting the returned handle observes the
result. Dropping it discards observation; it does not cancel accepted work.
The handle is similar to an already-started `CompletableFuture`, rather than a
lazy operation that starts only when someone waits.

There are two stages: `Result<MutationHandle, AdmitError>` reports immediate local
admission; awaiting an admitted handle yields `MutationOutcome`. Successful
admission alone says nothing about durable completion.

GET is an ordinary `async fn`: constructing its future does not submit a request.
Its admission and timeout start when the future is polled. This intentionally
differs from eager PUT/DELETE; do not assume every Rust future starts immediately.

```text
validate -> reserve -> queued -> handoff -> awaiting reply -> complete
                        |          |
                        |          +-> missing reply: may have been sent
                        +------------> expiry before handoff: not sent
```

Handoff is recorded immediately before the first socket write is polled. A write
can become uncertain even when transport cannot confirm any byte was written.
This conservative boundary prevents a false "not sent" claim.

A deadline covers queueing and network work. The local deadline uses Tokio's
monotonic `Instant`; the wire carries a remaining millisecond budget, not a wall
clock timestamp that assumes synchronized machines.

After a handed request times out, the entire connection generation retires.
Other handed requests can also become uncertain. Proven queued requests fail
as unsent. A partial frame is never followed by another request on that stream.
Duplicate, unknown-ID, or wrong-kind responses also retire the connection.

A `Link` never reconnects or replays requests automatically. Cloning it shares
the same connection and limits. Explicitly connecting again creates a new
generation. `close().await` retires the connection and joins its owned workers.
Calling `close` on any clone retires the shared connection for every clone.
Dropping one clone while another still owns the link does not close that link.

## The typed server: where a decoded request goes

The server is the receiving-side counterpart to `Link`. It has no OpenRaft,
RocksDB, or gateway dependency. A gateway and a node each provide a small
`RequestHandler` implementation with a concrete request and response type:

```rust,ignore
impl RequestHandler for Gateway {
    type Request = KvRequest;
    type Response = KvResponse;

    fn handle(&self, request: KvRequest, deadline: Instant)
        -> impl Future<Output = KvResponse> + Send
    {
        async move { self.route(request, deadline).await }
    }
}
```

For a Java reader, this is an interface with one asynchronous method and
associated request/response classes. Rust keeps the concrete types in the
implementation, so the compiler rejects a node handler attached to a public KV
listener by mistake. `impl Future` is the method's concrete asynchronous return
type; callers do not need to name it.

`Server::start(listener, config, Arc::new(handler))` creates an owned accept
tree. For every request it reads the frame header, reserves the aggregate byte
budget before allocating the payload, decodes and validates the envelope, then
reserves a maximum response before calling `handle`. A duplicate active request
ID, invalid frame/body, or saturated budget retires that one TCP stream without
executing uncertain work. Responses keep the same ID and may be written out of
order relative to handler completion, while a per-connection writer serializes
the actual bytes.

Accepted handler work remains owned after a client disconnects. This matters for
Raft mutations: disconnecting a socket must not cancel a proposal while it is
still pending inside consensus and falsely make capacity appear free. `close()`
stops acceptance and idle network I/O, then joins accepted handlers. It cannot
force arbitrary application code to finish; node shutdown first stops new
requests, lets Raft resolve its owned proposals, then joins the server.

### Resource limits follow ownership

The defaults are 128 outstanding operations, 16 MiB of request byte budget,
and 16 MiB of receive byte budget per link. They are bounded admission settings,
not a claim that the whole process uses that much RAM.

- Queued items retain reservations until physically removed, even after expiry.
- Dropping a handle does not free the capacity of work that is still in flight.
- Receive capacity is reserved before allocating/decoding a frame.
- Completed but unconsumed results retain count/receive reservations.
- After the caller consumes a result, its data belongs to the caller's budget.
- Shared `Bytes` slices may retain larger caller allocations than their logical
  length; request accounting is not a strict process RSS bound.

## Routing and wire compatibility

`ShardConfig` uses versioned seeded FNV-1a64 to map nonempty byte keys to data
groups `1..=count`, where count is in `1..=256`. Group zero is excluded.
Changing node membership does not change a key's group. Changing the hash version,
seed, or shard count can change placement and requires a migration design.
FNV is deterministic; it is not a security hash or a hot-key solution.

Current message limits are:

| Field | Limit |
| --- | --- |
| Key | 1 byte through 64 KiB |
| Value | 0 bytes through 1 MiB |
| Encoded frame payload | 1 byte through 2 MiB |
| Failure message | Up to 1,024 UTF-8 bytes |
| Wire request budget | 1 through 60,000 milliseconds |

The four-byte frame header is outside the payload limit. Postcard depends on
the serialized field and variant order. Moving Rust files should not change
the wire format; changing declarations can. Fixed byte fixtures in
[`tests/wire_v1.rs`](tests/wire_v1.rs) detect selected compatibility regressions.

## Tests and how to run them

Run these from the `crates/rockbee-raft-kv` workspace directory:

```sh
cargo test -p rockbee-raft-protocol --locked --offline
cargo clippy -p rockbee-raft-protocol --all-targets --locked --offline -- -D warnings
cargo doc -p rockbee-raft-protocol --no-deps --locked --offline
```

The protocol transport unit tests use in-memory duplex streams and controlled
time, so they do not need a listener port. The client integration tests use an
owned localhost listener:

```sh
cargo test -p rockbee-raft-client --locked --offline
```

`--offline` assumes dependencies have already been fetched. These commands do
not build the node's RocksDB/OpenRaft dependencies.

| Contract | Tests to read |
| --- | --- |
| ID and shard validity, exact bounds, missing versus empty values, response matching | [`tests/protocol.rs`](tests/protocol.rs) |
| Actual maximum-value encoding and hostile inner lengths/unknown variants | [`tests/verifier_adversarial.rs`](tests/verifier_adversarial.rs) |
| Selected v1 wire bytes stay unchanged | [`tests/wire_v1.rs`](tests/wire_v1.rs) |
| Eager submission, out-of-order replies, dropped handles, queued/handed timeouts, partial writes, no replay | [`src/transport/tests.rs`](src/transport/tests.rs) |
| Typed server validation, response reservation, duplicate IDs, retained accepted work after disconnect | [`tests/server.rs`](tests/server.rs) |
| Count/byte saturation, retained results, decoder admission, ID exhaustion, late replies | [`src/transport/tests.rs`](src/transport/tests.rs) |
| Public client behavior over real local TCP | [`client tests`](../rockbee-raft-client/tests/client.rs) |
| Executable public API example | [`lib.rs`](src/lib.rs) documentation test |

These tests check behavior at boundaries instead of duplicating every getter.
Passing them establishes the tested protocol/client contracts. Cluster elections,
durable storage, linearizable reads, snapshots, and replica movement require
their own integration and failure tests.
