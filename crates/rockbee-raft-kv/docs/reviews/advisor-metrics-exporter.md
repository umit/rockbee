# Minimal metrics exporter proposal

Status: source-grounded recommendation, not implemented or dependency-approved. Advisor sent the design to verifier; independent implementation review remains pending. Stable soak and client/server communication work retain priority. No Cargo or production changes here.

```text
Gateway / Node owned metrics -> scoped registry -> optional loopback /metrics
                                                    <- Prometheus -> Grafana
```

## Library and ownership decision

Recommend `prometheus-client` with an owned registry per runtime. Register typed counter/histogram handles once, retain their clones with the component, and expose an immutable registry to the HTTP service. Existing counters/permit state can use a collector instead of redundant mutable gauges. The library supplies registration, collectors and OpenMetrics encoding; no global recorder is necessary. Use seconds for duration units. See [Registry API](https://docs.rs/prometheus-client/latest/prometheus_client/registry/struct.Registry.html) and [crate encoding example](https://docs.rs/prometheus-client/latest/prometheus_client/).

Serve only `GET /metrics` through mature Hyper HTTP/1, using its `serve_connection` service API. Own a small bounded connection JoinSet; acquire admission before spawning/encoding, apply an overall scrape/connection deadline, and join or abort-and-join these read-only tasks on shutdown. Hyper supplies HTTP parsing, header limits, keep-alive control and header timeout support; the latter requires a timer. Use Tokio adapters and a full bounded response body, not handwritten HTTP parsing. An explicit loopback-only optional listen address is sufficient for the first milestone; disabled means no listener/task. Scrapes read observations only: no Raft read barrier, DB scan, compaction or data-plane permit acquisition. See [Hyper HTTP/1 builder](https://docs.rs/hyper/latest/hyper/server/conn/http1/struct.Builder.html).

Alternative: `metrics-exporter-prometheus` 0.18.3 can return an owned recorder/exporter using `build`; `install`/`install_recorder` install globally and are unsuitable for isolated multi-runtime tests. Its current `build` source additionally spawns an upkeep loop without returning that task handle. `build_recorder`, direct `Recorder::register_*` handles, a separately owned upkeep task and an owned HTTP service avoid both concerns, but add plumbing we do not currently need. Do not assume returning an exporter future means all maintenance tasks are owned. See [builder API](https://docs.rs/metrics-exporter-prometheus/0.18.3/metrics_exporter_prometheus/struct.PrometheusBuilder.html), [builder source](https://docs.rs/metrics-exporter-prometheus/0.18.3/src/metrics_exporter_prometheus/exporter/builder.rs.html), and [recorder API](https://docs.rs/metrics-exporter-prometheus/0.18.3/metrics_exporter_prometheus/struct.PrometheusRecorder.html). Pin and verify selected versions during implementation; add no general telemetry framework or new workspace package now.

## First useful measurements

- Request outcomes and duration by fixed operation and observation point. A node's durable commit, gateway's generated reply and client's received outcome are different events. The server cannot know whether a reply reached the client; do not label its commit count as client success or turn reply loss into a second failed mutation. Client Unknown belongs to client observations; gateway backend uncertainty may be a distinct counter.
- In-use operation/connection/byte reservations from configured capacity minus existing available permits. Name these reserved resources accurately: a completed unconsumed result can retain a reservation. They are not necessarily active network requests. Queue occupancy requires a real queue observation; do not infer it from all reserved permits. Individual gauges are operational observations, not one globally atomic snapshot.
- Transport retirements and admission failures using finite reason enums. Fixed latency buckets in seconds, with an overflow bucket; HDR remains the soak tool, not a reason to implement another production histogram.
- Per configured Raft group: role, term, last applied/log/snapshot/purged positions and bounded peer replication observations from OpenRaft's existing metrics receiver. The upstream 0.9.25 fields are in `src/metrics/raft_metrics.rs`; effective membership/leader metrics are diagnostics, not proof of committed authority.
- Storage native lane usage, synchronous append/apply/snapshot duration and failure counts at existing completion boundaries. Metrics never alter durable acknowledgement or cancellation ownership.

Use only finite operation/reason/lane/role labels and configured group/node/peer IDs, bounded by existing topology. Never label keys, values, request IDs, job IDs, arbitrary error strings or connection generations. Pre-register finite combinations where practical; bound response size from the metric schema. Avoid group × peer × operation × reason cross-products unless an actual question needs them.

Current gateway code has three atomics, not an exporter. In particular, `Gateway::handle` increments `accepted` before `route`, while its snapshot field documentation says backend handoff. Export that existing observation as handler invocations or correct its counting point deliberately; it is not a committed-mutation counter. OpenRaft already provides internal observations. Neither source currently establishes an HTTP metrics endpoint.

## Focused acceptance gates

Two runtime registries/listeners in one process must remain isolated. Verify declared metric names/units/finite labels, one known outcome trace, reservation gauges while a disconnected accepted handler is held and after release, and no fabricated client-success count on reply loss. Scrape disabled/bind-failure/start/close/repeated-close paths must be explicit. Hold a slow scrape during shutdown and prove listener/connection tasks terminate and the port can be rebound. Confirm scraping remains available without quorum and does not invoke consensus/storage work. No benchmarks or claims of negligible overhead until measured.

Remaining work: verifier challenge, user/root selection of the implementation milestone and dependency versions, then implementation and focused tests. Grafana deployment/dashboard provisioning and public/non-loopback authenticated exposure are outside this first proposal.
