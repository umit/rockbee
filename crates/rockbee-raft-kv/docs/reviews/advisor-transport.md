# Task 2 transport decision

Status: proposed API, reviewed directly with the independent verifier; no code
or builds performed. The following is a correctness-first first implementation.

```text
Existing: Client -> Gateway -> Writer/Readers -> SlateDB
Planned:  Client -- Link<KvRequest,KvResponse> --> Gateway
                 -- Link<BackendRequest,BackendResponse> --> Node -> Raft/RocksDB
Node <-------------- private peer RPC --------------------> Node
```

## Choose generic transport, concrete message families

Use `Link<Req, Resp>` over message bodies. Avoid one universal request enum:
adding metadata/bootstrap/movement messages should not make the public client
listener accept internal operations. The gateway/node backend uses a separate,
small engine-free enum when Task 6 introduces it. Private peer messages stay in
the node package and need not use the public data transport at all.

Make the current envelopes generic with default type parameters. Their public
field layout and wire encoding remain the same:

```rust
pub struct RequestEnvelope<R = KvRequest> {
    pub version: u16, // Keep the actual Task 1 version field type.
    pub request_id: RequestId,
    pub budget_ms: u64,
    pub request: R,
}

pub struct ResponseEnvelope<R = KvResponse> {
    pub version: u16,
    pub request_id: RequestId,
    pub response: R,
}

pub trait WireRequest<Resp>: serde::Serialize + Send + 'static {
    type Expected: Copy + Send + 'static;

    fn validate(&self) -> Result<(), ProtocolError>;
    fn retained_bytes(&self) -> Result<usize, ProtocolError>;
    fn expected_response(&self) -> Self::Expected;
    fn validate_response(
        expected: Self::Expected,
        response: &Resp,
    ) -> Result<(), ProtocolError>;
}

impl<Req, Resp> Link<Req, Resp>
where
    Req: WireRequest<Resp>,
    Resp: serde::de::DeserializeOwned + Send + 'static,
{
    pub async fn connect(
        address: SocketAddr,
        config: LinkConfig,
        connect_deadline: Instant,
    ) -> Result<Self, ConnectError>;

    pub fn try_submit(
        &self,
        request: Req,
        deadline: Instant,
    ) -> Result<PendingResponse<Resp>, AdmitError>;

    pub async fn close(&self);
}

// Future<Output = Result<Resp, TransportFailure>>
pub struct PendingResponse<Resp> { /* private */ }

pub enum Delivery {
    NotSent,
    MayHaveBeenSent,
}

pub struct TransportFailure {
    pub delivery: Delivery,
    pub reason: TransportFailureKind,
}
```

These are signatures, not a request for a generic pluggable transport framework.
Keep one TCP implementation. The expected-response token for KV can simply be
`Read` or `Mutation`; backend requests can use their own small enum. No request
payload is retained solely to check response type after it has been sent.

Link owns IDs and envelope deadlines, so callers cannot reuse wire IDs. The
gateway preserves the original request ID in its response and observability
context while each backend link allocates its own correlation ID. Server request
IDs are scoped to the accepted connection, not a process-global pending table.

The client maps MayHaveBeenSent to mutation Unknown and NotSent to mutation
NotSent. A matching response may instead carry Committed or explicit Rejected.
Transport does not infer consensus outcomes or retry any operation. In particular,
wire NotSent is invalid as a server mutation response; a received but unsubmitted
request produces Rejected. GET adapts transport failure to ReadError.

## Connection lifecycle and timeout rule

Each Link is one connection generation, with one writer, one reader, and one
bounded deadline actor. Its nonzero monotonic request counter uses checked
increment; exhaustion retires the link. Connection managers may create a fresh
Link for future attempts; a Link never silently migrates outstanding operations
to another socket. Numeric IDs may be reused only in a different connection
generation, whose reader and pending map are disjoint.

`try_submit` validates, immediately tries count/byte reservations, allocates an
ID, creates the pending entry, and enqueues the owned request synchronously.
Failure is locally NotSent. Polling PendingResponse is never the trigger for
submission. Dropping it only discards observation: the request and reservations
remain owned by connection workers until their actual lifecycle completes.

Use a tiny per-request state: Queued, Handed, Complete. Immediately before the
first socket write poll, the writer atomically wins Queued -> Handed against the
deadline actor. If expiry wins, no socket write for that request is permitted.
If handoff wins, lack of a final response is conservatively MayHaveBeenSent,
even if the kernel actually received zero bytes. Do not hold the pending-map
lock across encoding, socket operations, or any await.

The writer sets budget_ms from the remaining local deadline immediately before
encoding/handoff. If encoding consumes the remaining time, expire before the
write. Every local wait uses the original Instant. Each receiving hop derives a
local deadline from the supplied remaining duration; the initiating client's
deadline remains the final end-to-end timeout even though clocks are not shared.

**First implementation: any post-handoff deadline retires the whole connection.**
Complete the expired and other handed pending requests as MayHaveBeenSent;
complete proven-unhanded queued requests as NotSent. Close both socket halves,
stop the old reader/writer, drain owned queues, and release reservations only as
their retained buffers/tasks are destroyed. An interrupted partial frame always
requires retirement. Never send another frame after a timed-out partial write.

This deliberately permits collateral Unknown results on concurrent mutations.
It is a documented availability tradeoff, not a false outcome guarantee. The
verifier and advisor agree it is the smallest bounded first version. Later
optimization can retain expired correlation slots under an explicit bound; v1
needs no tombstone map at all. Late replies on a retired socket are discarded.
An unknown or duplicate reply on an active socket is a protocol violation that
retires it; response ID and expected kind must both match.

Exactly one response/expiry/retirement transition may complete a pending request.
The deadline actor rechecks the entry and generation before acting: a stale timer
for an already completed request cannot retire a healthy connection. Reader
dispatch checks the absolute deadline before delivering success; an expired
handed response follows retirement semantics instead. Every reader, writer, and
timer belongs to its immutable connection generation, never a replacement map.

Pre-handoff expiry may notify the caller immediately, but its queued item still
owns the byte/count permits until the writer skips and drops it. This prevents
an expired-request storm from leaving an unaccounted backlog. A bounded deadline
heap contains only currently retained entries, not every historic request.

## Memory and server ownership boundaries

- Validate limits before activation. Reserve count and conservative bytes
  before transport allocation. The simplest encoding reservation is retained
  request bytes plus maximum encoded-frame capacity; reduce reservations only
  after the corresponding object is destroyed. Temporary body/frame copies and
  decoder output must be included. Logical Bytes length alone cannot establish
  a hard process RSS bound when callers supply slices retaining larger backing
  allocations; document this or normalize those buffers within reserved space.
- Receive only the fixed frame-length header first, validate it, reserve receive
  bytes, then allocate the frame. Generic DeserializeOwned is not by itself an
  allocation-safety proof; each concrete decoder still needs hostile nested
  length tests and bounded construction.
- A server must reserve bounded response capacity **before** materializing a GET
  value, not just before putting its encoded response on a queue. Tiny requests
  can otherwise cause many full-size result allocations. Reserve a conservative
  maximum for the response kind initially. Request and response budgets are
  distinct, and release only when transport stops retaining their objects.
- Server connection workers decode/validate and dispatch under process and
  connection admission. A RequestContext carries request ID, connection
  identity, absolute deadline, and owned response reservation. Application
  handlers return only the body; shared server code writes the matching envelope.
  No handler writes directly to the socket or invents transport IDs.
- Connection loss must not abort an already-submitted Raft operation or recycle
  its node proposal permit. The node owns that worker independently of response
  delivery. Its request/byte reservation survives external timeout until actual
  Raft completion; Fatal stops group admission before reservations recycle.
  This node policy is separate from retiring client/gateway transport links.

## Focused tests before integration

1. Call PUT without polling its handle; server observes it. Drop handle; operation
   still executes and permits eventually return.
2. Force expiration on both sides of the atomic Queued/Handed transition. Only
   the winning pre-handoff case proves NotSent.
3. Two handed operations and one queued operation share a link. Expire one
   handed operation; assert both handed results uncertain, queued result NotSent,
   old socket retired, and no replay on a replacement link.
4. Interrupt a frame after a short write, then enqueue another request; assert
   the second frame is never appended to that stream.
5. Deliver a late response after retirement, a duplicate active response, a
   mismatched response kind, and counter exhaustion. Assert bounded state and
   no response delivered to another request/generation.
6. Stall writer, expire queued requests repeatedly, and check retained count,
   bytes, and timer entries remain bounded until workers drain them.
7. Flood tiny GETs returning maximum values to a non-reading client; assert
   response reservations bound allocation before data retrieval/encoding.

No pooling/reconnect, routing, or automatic retry policy belongs inside Link.
The client and gateway may own small connection managers without duplicating
framing, request state transitions, or certainty accounting.
