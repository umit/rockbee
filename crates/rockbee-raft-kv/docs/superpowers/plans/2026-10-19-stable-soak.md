# Stable three-node soak implementation plan

> Execute inline with the approved advisor/verifier predesign; no additional agents, commits or production tuning.

**Goal:** An ignored Rust soak test that validates repeated public-client operations against three real nodes and reports honest bounded HDR measurements.

**Architecture:** `tests/processes.rs` keeps ownership of exact CLI children and temporary databases. Its private `tests/support/soak.rs` owns bounded configuration, worker cycles, HDR metrics, structured stop/drain/join and persistent report artifacts. One shared client connection and one gateway serve four data groups.

**Tech Stack:** Existing Tokio/client/gateway/process fixture; user-requested `hdrhistogram =7.6.0` test dependency with default features disabled.

**Spec:** `docs/reviews/advisor-soak-predesign.md`, `verifier-soak-predesign.md`, and root's explicit HDR correction supersede the earlier suggested bespoke histogram.

- [x] Focused red tests: invalid duration/concurrency; successful vs partial-cycle/error accounting; bounded HDR overflow is explicit; worker drain/abort ownership before cleanup.
- [x] Implement six-step worker-exclusive PUT/GET/update/GET/DELETE/absence cycles. Values encode seed/worker/cycle/version. No uncertain mutation replay or mismatch polling.
- [x] Add four-group key selection and excluded readiness/warmup, then a common measurement release. Stop new cycles at expiry/signal/failure; bound draining and join/abort+join all workers.
- [x] Persist unique ignored reports: per-operation summary CSV, per-second validated throughput, manifest and three owned node logs. Include actual elapsed through final drain, errors/partial work, profile, topology, payload, seed and shutdown result.
- [x] Compile, focused tests, two-second smoke; review the source freeze before any long run. Run a 60-second concurrency-one debug baseline with progress updates. No release rebuild or runtime tuning.

Limits: warm closed-loop observed throughput is not maximum capacity. Parent SIGKILL cannot execute cleanup. Censored/aborted work and histogram/time-range failures remain explicit. No unbounded latency vectors or sample queues.

Checkpoint completed: six focused helpers and existing process recovery pass; two-second smoke and 60-second c1 debug baseline passed. Actual parent SIGTERM check joins workers, closes all resources and retains a FAIL report/logs with zero surviving owned children. Advisor/verifier final source review remains pending before any ten-minute run.
