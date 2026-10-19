# Task 2 transport contract and adversarial checks

Status: proposed implementation contract, 2026-10-19. Advisor and verifier agree
on the choices below; root owns the final implementation ruling. No code,
tests, Cargo commands, or runtime validation were performed in this review.

```text
Planned: Client -> bounded Link -> Gateway -> bounded Link -> group leader
                      request/response admission               -> Raft
```

## Shared transport API boundary

Use a typed `Link<Req, Resp>` shared by the public client and gateway backend.
Public KV bodies and later engine-free routed backend bodies remain distinct;
do not grow a universal public/peer operation enum. Peer Raft messages remain
private to the node package.

A small request contract validates the body, gives a conservative retained-byte
bound, and extracts a tiny expected-response token. The response contract checks
request ID and compatible response kind against that token. Pending entries must
not retain a second copy of the full request merely to validate a response.
The transport reports unsent versus possibly sent failures without interpreting
Raft or mutation outcomes. The client maps those to NotSent versus Unknown.
An explicit, valid server Rejected outcome retains its pre-submission meaning.

`NotSent` is local to the originating Link. A server that received a request but
declines it before consensus submission returns Rejected. It must not emit
NotSent as though that described the client's transport. For GET, valid responses
are Value or ReadError; PUT/DELETE require Mutation. An unexpected wire outcome
or incompatible response kind is a protocol failure, not proof of rejection.

The eager public PUT/DELETE call synchronously attempts bounded admission and
returns a handle. Polling the handle never starts submission. Dropping it never
cancels accepted submission, rolls back work, or frees reservations prematurely.
Admission failure is represented by the documented local unsent error/outcome;
it must not leave an enqueued or spawned request behind.

## Handoff, deadline, and connection retirement

Recommended simple v1 choice: the first expired request that has crossed handoff
retires its entire connection generation. This avoids an unbounded set of late
reply tombstones. It can affect other multiplexed requests; document that cost.

| State/event | Required result |
| --- | --- |
| Rejected local admission | Unsent; no queue item, write attempt, or request task |
| Queued expiry wins atomically against handoff | NotSent; writer must never write its bytes |
| Writer wins handoff immediately before first socket-write poll | Possibly sent from then onward, even if actual byte count later proves zero |
| Deadline, EOF, write failure, cancellation, or malformed response after handoff | Mutation Unknown unless a matching valid explicit final outcome already won |
| Valid matching reply wins before deadline | Deliver that result exactly once and retire its pending entry |
| First post-handoff expiry | Retire generation; handed requests fail as possibly sent; queued requests proven never handed fail as unsent |

The queued-to-handed transition must be atomic with expiry's competing state
transition. A lock/actor/state machine is an implementation choice; there must
be exactly one winner. A partial header/body write makes the stream unusable for
subsequent frames. Close it rather than resuming a new request mid-frame.

Every local wait spends the original absolute deadline, including reconnect,
admission, writer queue, socket write, reply decode, and response delivery. Stamp
the transmitted remaining budget after queue wait. Check the local deadline
before delivering a decoded response; a late success must not bypass expiration
because its timer task was scheduled late. This does not promise scheduler-level
wall-clock precision.

A completed request's stale timer cannot retire a healthy connection. Reader,
writer, timeout, and retirement events carry generation identity; a retired
reader cannot match a new connection's pending ID. IDs never wrap or collide
among pending requests; exhaustion is an unsent failure/retirement decision.
Unknown/duplicate reply IDs retire the stream and never alter an already
completed outcome. Reconnection does not replay uncertain mutations. Reads and
explicit safe-rejection retries, where implemented, retain the same deadline.

Use a bounded number of owned actors per connection: reader, writer, and
deadline/coordination work. Avoid detached tasks or independently retained timer
entries per submit that can outlive the bounded pending table. Queued expired
items may retain their permits until physically drained; the retained memory
must stay bounded even after their caller receives NotSent.

## Request, decoder, and response memory ownership

1. Reserve request count and conservative retained bytes before transport-owned
   cloning, encoding, or enqueue. Include simultaneously retained logical Bytes,
   serialized frames, headers, and per-request metadata. Existing caller-owned
   buffers are outside transport ownership until admitted; do not claim a total
   process memory cap from this boundary.
2. Read only the fixed header before validating frame length. Reserve bounded
   receive bytes before allocating its payload. Decoder allocations/copies must
   have reserved headroom and inner hostile length prefixes must be rejected.
3. Bound server response count and bytes independently of request admission.
   Reserve response capacity before GET value materialization and encoding;
   1,024 tiny GETs must not allocate 1,024 maximum values while awaiting egress
   admission. A server execution context should own this reservation.
4. While an encoded frame and decoded response or logical response coexist,
   account for both. Release permits when ownership actually ends, not merely
   when a timeout notifies the caller. After a completed value is transferred to
   the caller, its continued retention is caller-owned and outside Link's bound.
5. All queues need byte and count caps; connection counts are bounded too.
   Validate configurations admit at least one legal maximum frame and cannot
   deadlock by requiring more permits than their capacity. Response writers
   must progress without acquiring request admission or a lock held by its
   blocked producer.
6. Node proposal reservations are separate: retirement of a network Link must
   not release the node's already-submitted Raft-work permit. An owned worker
   drains that work after its client/gateway waiter disappears.

## Deterministic adversarial acceptance matrix

Use explicit fixture barriers/controlled time rather than timing-only sleeps.
These are contracts for later tests, not tests already executed.

| Fixture schedule | Required evidence |
| --- | --- |
| Stop writer before handoff, expire queued request | NotSent and peer sees zero request bytes |
| Stop immediately after handoff but before write poll, expire | Unknown and retired connection even if peer sees zero bytes |
| Permit one header/body byte, then expire/fail writer | Unknown; no following frame on that stream; all permits eventually released |
| Peer observes full mutation, withholds reply past deadline | Exactly one observed submission; Unknown; reconnect sends no replay |
| Drop handle before writer runs and after handoff | Accepted request remains eager; no rollback or premature reservation release |
| Race matching reply against expiry in both orders | One terminal result; late reply cannot become success; stale timer cannot affect another request |
| Return out-of-order valid replies | Each matching handle receives the correct result |
| Return duplicate, unknown ID, wrong response kind, or invalid wire outcome | Connection fails closed; no success/rejection is invented for handed mutations |
| Delay old-generation reply until a new connection exists | Old reply never matches new pending work |
| Saturate count, then bytes, while handles are dropped | New admission fails unsent; retained count/bytes/tasks remain at documented caps |
| Many tiny GETs produce maximum values for a slow client | Response materialization, encoded buffers, and egress stay inside response reservations |
| Huge outer length or tiny frame with hostile inner length | Reject before unbounded allocation; release receive permits on decode failure |
| Expire queued items while writer is blocked | Terminal outcomes are prompt within scheduling limits; stale queue storage remains bounded and eventually drains |
| Shutdown/disconnect during queued, writing, and awaiting-reply states | Every pending handle resolves once; only owned tasks close; no later write after a NotSent result |
| Exhaust correlation IDs/configure impossible byte caps | No wrap, silent reuse, or permit wait that can never succeed |

**Review verdict:** this contract is implementable without protocol redesign.
Task 2 may proceed once root records the connection-retirement choice and
reservation ownership. Runtime boundedness and certainty claims require the
matrix evidence; compiling the transport is insufficient.
