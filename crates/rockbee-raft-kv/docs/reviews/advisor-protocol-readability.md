# Protocol/client readability review

Status: focused read-only review of current protocol/client source and test
assertions. No builds or source edits. Task 2 is still being finished; this is
not a claim that its tests were independently executed in this review.

```text
put/delete -> validate/reserve -> queue -> encode/handoff -> TCP
handle     <---------------- validated correlated response <--+

Gateway/Raft durability integration: unfinished in this reviewed code
```

## Immediate documentation priorities

1. **Distinguish eager mutations from lazy GET.** PUT/DELETE are synchronous
   methods returning an observer of already-admitted work. `get` is an ordinary
   Rust async function: creating its future does not submit a request, and its
   timeout starts when that function body is polled. Do not describe every
   returned Future as Java CompletableFuture-style already-running work.
2. **Explain both result stages.** `put(...) -> Result<MutationHandle, AdmitError>`
   answers whether local admission succeeded. `handle.await -> MutationOutcome`
   answers what is known about the mutation. Immediate Err proves no submission;
   a handle may later produce NotSent, Rejected, Committed, or Unknown. Unknown
   is an explicit result value, not permission to repeat the write. Dropping the
   handle abandons observation, not the mutation.
3. **Explain shared lifecycle.** Cloning the client shares the same connection,
   request IDs, and limits. Closing any clone retires that connection for every
   clone. Dropping one clone alone does not retire remaining owners. Explicit
   `close().await` also joins owned workers; Drop cannot asynchronously wait for
   their termination. A retired client needs explicit reconnect.
4. **Define handoff precisely.** Handed means the local writer crossed its
   certainty boundary before the first socket write poll. It does not mean the
   peer received bytes or Raft accepted/committed the mutation. Timeout after
   that boundary conservatively produces uncertainty even with zero actual
   bytes written; v1 retires the shared connection, affecting concurrent work.
5. **Separate decoding and semantic validation.** Codec checks lengths/framing;
   serde enforces private ID invariants; envelope/body validation checks version,
   budget, operation compatibility and field bounds. Public envelope fields allow
   direct construction, so using `new` once does not eliminate boundary checks.
   The existing crate-level example correctly shows decode followed by validate.

These belong in the root-owned protocol README and short public method rustdoc,
not in new abstractions or a large tutorial embedded in implementation methods.

## Accurate Java analogies

| Rust concept here | Useful Java comparison | Crucial difference |
|---|---|---|
| `Result<T, E>` | Explicit success/error result type | Failure is returned data; `?` propagates it without catch/throw control flow |
| `KvRequest` / `MutationOutcome` enums | Sealed interface with known permitted variants and pattern matching | Variants can carry typed payloads; exhaustive match makes missing cases visible |
| Private ID newtypes | Validated immutable value objects for different ID domains | NodeId and RequestId remain distinct compile-time types; serde invokes their checked conversion |
| `WireRequest<Resp>` | A capability interface for a request family | `Expected` is one associated token type selected by that implementation; generic Link uses it without inheritance or casting |
| MutationHandle implementing Future | Observer of already-submitted asynchronous work | Ordinary Rust futures are generally lazy; this handle is eager because submit already happened |
| `Bytes` | Shared immutable byte-sequence object | Cloning may share allocation; slices can retain larger backing memory, so logical-byte limits are not RSS limits |
| `Arc` and owned permits | Shared references plus resource objects used with try-with-resources | Last-owner Drop deterministically releases the permit; no reliance on garbage collection or finalizers |
| `close().await` | Shutdown followed by await-termination | Closing one shared client clone affects all clones; cancellation of waiting does not roll back work |

`WireRequest` is justified: public KV and future backend message families reuse
one bounded transport. Its methods have specific jobs: validate body, account
for retained bytes, remember a tiny expected-response category, and validate the
response. Do not add separate traits to every envelope/error/helper merely to
make the project resemble Java interfaces. `Serialize`/`DeserializeOwned` are
encoding capabilities, not proof an arbitrary third-party schema has bounded
allocation; the trusted-family contract already documents that limitation.

## Meaningful test coverage and focused gaps

Existing tests are behavior-oriented. Protocol tests cover malformed frames,
hostile nested lengths, fixed hash vectors, actual maximum wire payloads and
wire compatibility. Transport tests cover eager submission, out-of-order replies,
unpolled result reservations, dropped handles, both sides of timeout/handoff,
partial frames, generation retirement, stale deadlines, ID exhaustion, and
bounded receive ownership. Real-TCP client tests distinguish missing from empty
values, preserve rejection, and observe no mutation replay after lost replies.
These provide more value than constructor/getter assertion counts.

Three useful additions, without duplicating those existing tests:

- **Public clone lifecycle:** with a one-operation limit, submit through one
  clone and prove another shares admission; drop one clone and prove the other
  remains usable; close a clone and prove all remaining clones reject new work.
  Include justified completion of an already-handed mutation on closure.
- **Lazy GET contrast:** construct and hold a GET future without polling; a
  controlled peer sees no request. Poll it and then observe GET. Contrast with
  the existing unpolled eager-mutation test. This protects the precise API
  behavior users will learn from the docs, including when its deadline begins.
- **Cancelled close ownership:** hold a worker at a deterministic fixture point,
  poll then cancel close, release the worker and call close again. Confirm the
  second close joins every owned worker and reservations reclaim. The current
  implementation deliberately retains join handles across cancellation; test
  that lifecycle guarantee rather than relying on an explanatory comment.

Use controlled observations/barriers or paused time, not an arbitrary sleep as
proof nothing happened. These are focused gaps, not a demand for new architecture
or duplicating every transport test through the thin public wrapper.

## Safety assessment

No actionable API safety defect was identified in the inspected flow. Admission
errors are locally unsent; response kind/ID checks precede delivery; remote
NotSent is rejected; and decode/headroom contracts are explicit. Durable server
semantics are still a later integration obligation: fixture Committed responses
prove client mapping only, not real Raft durability. Future gateway/node handlers
must preserve that distinction and cannot blindly map post-submission
ForwardToLeader to Rejected.

Sources inspected: protocol `src/{lib,kv/wire_request,kv/response,message/envelope}.rs`,
`src/transport/{mod,config,state,pending,workers,tests}.rs`, protocol integration
tests, client `src/{lib,error,api/client,api/mutation,config/mod}.rs` and
`tests/client.rs`. Existing Rockbee was not read or changed for this review.
