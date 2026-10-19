# Client lifecycle statistics

Status: finite implementation proposal; bilateral review pending. No implementation or dependency change is claimed here.

```text
Client PUT / DELETE (eager), GET (first poll)
  -> bounded Link admission -> queued -> handed -> terminal decision
                                             -> stats -> application exporter
                                             -> completion handle
```

## Scope and public API

Keep the initial slice to fixed, clone-shared client statistics. Preserve the existing client constructor and mutation API. Add:

```rust
impl RockBeeRaftClient {
    pub fn stats(&self) -> ClientStatsSnapshot;
    pub fn stats_handle(&self) -> ClientStatsHandle;
}
impl ClientStatsHandle {
    pub fn snapshot(&self) -> ClientStatsSnapshot;
}
```

`ClientStatsHandle` is a cheap clone of bounded statistics storage; it MUST NOT retain `Link::Inner`, sockets, worker handles, or result payloads. An exporter can retain the handle after the last client drops and see final client-side retirement outcomes. Separately connected clients have separate statistics; client clones share one set. No reset method in this slice.

Use named `put`, `get`, and `delete` fields in the snapshot, backed by fixed storage, with no map keyed by arbitrary strings. There are no key/value, request-ID, address, tenant, or error-text labels. A snapshot is a set of concurrent observations, not a transactional view across counters: document that accounting identities are checked after quiescence, not during concurrent updates.

## Exact meanings

| Field | Meaning |
|---|---|
| `attempted` | One public PUT/DELETE call, or one first poll of GET that reaches submission |
| `admitted` | Request successfully enqueued and entered in the pending map |
| `admission_failed` | One failed submission, partitioned by the existing finite `AdmitError` variants |
| `pending` | Directly maintained number of admitted requests without a terminal transport decision |
| mutation outcomes | Exactly one of `committed`, `rejected`, `not_sent`, `unknown` for each terminal admitted PUT/DELETE |
| GET outcomes | Exactly one of `hit`, `miss`, `remote_error`, `transport_error` for each terminal admitted GET |
| terminal duration | Monotonic elapsed time from start of submission to its winning terminal decision; fixed count, sum, and maximum |

`Value(Some(Bytes::new()))` is a hit. Local admission failure is not remote `Rejected` and is not also terminal `NotSent`. An admitted request expiring before handoff is terminal `NotSent`; after possible handoff it is `Unknown`. A received `MutationOutcome` retains its exact classification. GET transport failures remain read failures, not mutation `Unknown` counts. Preserve a fixed per-outcome duration breakdown if needed for success versus failure latency; never merge admission failures into successful latency.

GET futures never polled produce no attempts. Dropping a submitted GET future or mutation handle does not cancel work or suppress terminal metrics. Counting is independent of whether oneshot delivery succeeds. `Unknown` means the client cannot determine the mutation result; it is not proof of server failure or non-commit. These metrics describe what the client transport received or decided, not what the application awaited and not the eventual consensus result.

`pending` is not retained admission count: completed but unpolled results can still hold count/RX permits, while expired queue items can retain request bytes until drained. Do not derive pending by subtracting separately sampled totals. Resource reservation gauges and wire-byte counters are outside this first slice; adding them later must name reservations separately from network traffic and use actual permit ownership.

## Small transport seam

Keep the existing uninstrumented generic `Link::try_submit`. Add an explicitly tracked submission path taking a concrete protocol-owned statistics token. Suggested internal shape:

```rust
pub struct SubmissionStats<Resp> { /* private fields */ }
// Only the KV specialization has a public construction path through the
// fixed ClientStatsHandle and a three-variant operation enum.
// There is no user-supplied closure, exporter, or extensible observer trait.

impl<Req, Resp> Link<Req, Resp> /* existing bounds */ {
    pub fn try_submit_with_stats(
        &self,
        request: Req,
        deadline: Instant,
        stats: SubmissionStats<Resp>,
    ) -> Result<PendingResponse<Resp>, AdmitError>;
}
```

The token contains the small operation tag, start time, and shared fixed counters. Its KV specialization uses only a built-in response classifier. A private function pointer is acceptable to connect the generic transport to this concrete classifier; it is not a public callback facility. No request cloning or payload retention for classification. `ExpectedKvResponse::Mutation` currently merges PUT/DELETE, so capture the operation before moving the request. Do not add metrics requirements to every `WireRequest` implementation.

Factor existing submission into one shared implementation, with optional tracking, so admission logic does not diverge. Count a failed admission once at the outer submission result, regardless of which internal reservation stage failed. Publish `admitted` and increment the direct pending gauge at insertion while the state lock prevents a worker winning completion first. `attempted` starts before validation/admission. Terminal accounting must not depend on the client wrapper returning from `try_submit`.

The winning removal remains under `Shared.state`: success, queued expiry, expiry during handoff, full generation retirement, close, drop, protocol failure and ID exhaustion all converge on one completion routine. Capture terminal time at the winner. Remove entries under the lock, then complete their statistics and oneshot delivery outside the lock, preserving ownership of `Admission` and receive permits. `retire_locked` can return the existing `mem::take` map for draining; do not allocate an additional unbounded completion vector. Every caller, including ID exhaustion, must drain it. Update the direct pending count exactly once on removal, and finalize statistics before oneshot send. No task per request, detached metric drain, arbitrary callback, exporter code, I/O or histogram work under the pending mutex.

Use fixed atomic counters with explicit overflow policy: saturate cumulative counts/duration sums and expose a fixed overflow indicator rather than wrap. Duration values can use integer microseconds, with a documented sub-microsecond rounding policy, converting to seconds only for export. `max` uses an atomic maximum. The existing `ownership_bytes` calculation includes `size_of<Entry>()`; verify the added token/time fields remain charged. No extra metrics allocation per submitted request beyond cloning an existing statistics `Arc`.

## Export integration

First implementation exposes snapshots without adding a dependency. This meets local `client.stats()` inspection and gives an application-owned exporter a stable input. The earlier scoped-exporter recommendation can register a `prometheus-client` collector over these handles. It must aggregate multiple clients explicitly; having each client set the same labelled gauge would silently overwrite other clients. Fixed operation/outcome labels are sufficient. Export durations in seconds; count/sum/max do not imply percentile estimates.

An optional future `metrics` facade adapter can register handles once against an explicitly supplied `&dyn metrics::Recorder`, then retain handles; `Recorder` supplies `register_counter`, `register_gauge`, and `register_histogram`. It need not install a global recorder or own HTTP. If histograms become requested, record actual terminal observations, not periodically sampled cumulative averages. Do not add this second integration to the initial slice merely because it is available. See the official [Recorder API](https://docs.rs/metrics/0.24.6/metrics/trait.Recorder.html) and scoped [prometheus-client Registry](https://docs.rs/prometheus-client/latest/prometheus_client/registry/struct.Registry.html).

Expected overhead is a start timestamp, a small token/Arc clone, and fixed counter updates per lifecycle transition. This is a code-level cost estimate, not a measured throughput guarantee. Preserve the current runtime/TCP settings. Benchmark only after correctness, using the existing stable soak comparison rather than a new performance framework.

## Focused acceptance tests

1. Clone sharing and isolation: two clones contribute to one snapshot; an independent connection does not. A retained stats handle does not keep the final client/connection alive.
2. Eager/lazy boundary: dropped PUT handle still records its received terminal outcome; an unpolled GET records nothing; a GET dropped after submission still records its terminal outcome.
3. Local rejection: invalid request/count saturation/closed connection each count one attempt and one admission failure, with no admitted or terminal outcome increment. Test multiple reservation stages cannot double-count.
4. Mutation certainty: server Committed/Rejected/NotSent/Unknown responses and pre-/post-handoff transport failures map to the exact fixed outcome; no retry is introduced.
5. GET values: missing, empty, nonempty, remote error and transport error have distinct correct counters.
6. Winning race: deterministic deadline-versus-response and close-versus-response schedules produce exactly one terminal count and pending zero, regardless of receiver drop. Exercise queued expiry and ID-exhaustion retirement as separate completion callsites.
7. Ownership distinction: hold a completed unpolled result; terminal count is already visible and logical pending is zero even though admission capacity remains retained. Consume/drop and verify capacity release without another terminal increment.
8. Time/accounting: controlled clock proves submission-to-terminal duration excludes later application polling delay; finite overflow policy is tested at the statistics accumulator boundary. After close/drain, `attempted = admitted + admission_failed` and `admitted = sum(terminal outcomes)` for each operation.
9. Existing complete client/server/gateway lifecycle tests remain green. A small public-path fixture checks statistics through the real client API; it must not claim that mock backend dispatch proves a durable Raft commit.

No tests of metrics-library internals, HTTP endpoint, labels framework, wire-byte accounting, observer abandonment counters, or server-side Raft metrics are required for this slice.
