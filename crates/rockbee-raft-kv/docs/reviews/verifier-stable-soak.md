# Stable three-node soak verification

Spec verdict: APPROVED for the stable warm closed-loop checkpoint. Quality verdict: APPROVED for this scope. Reviewed the frozen `tests/support/soak.rs`, shared process helpers and recorded artifacts; verifier ran no Cargo or soak process.

```text
shared Client clones -> in-process Gateway -> 3 owned node binaries
per worker: PUT -> exact GET -> update -> exact GET -> DELETE -> exact absence
```

The source uses disjoint worker keys whose bounded search covers all four groups, with 1 KiB values containing worker/cycle/version identity. Startup and four-group warmup precede the common monotonic start barrier. Each operation is timed before eager mutation admission; failed steps end their dependent cycle. Unknown is classified and never replayed, and a wrong read is not polled into success. Read/admission/transport/rejection/Unknown/value mismatch counts stay separate.

Duration and concurrency have finite limits. HDR 7.6.0 has fixed 1-microsecond-resolution/3-significant-figure storage with explicit 60,000,000-microsecond input cap and no auto-resize. Completed attempts, HDR samples, out-of-range latency and censored work are separately reported. Partial cycles preserve their successful prefix without inventing completion. Actual elapsed includes bounded drain through worker completion; both aggregate and per-second successful-operation rates use that interval rather than only configured seconds. Latency population is all completed attempts, including failures, rather than successful calls alone.

Handled signals request stop inside the resource owner. Workers stop starting new cycles, finish their current bounded cycle, then a JoinSet drain aborts and joins any overrun before client/gateway/child cleanup. In-flight work becomes censored rather than receiving a fabricated latency. Exact child kill/wait and node-before-TempDir ownership remain intact; logs are copied to a unique retained report directory. Parent SIGKILL/abort remains outside destructor cleanup.

## Evidence inspected

- `soak-unit-green.log` records the initial five helper tests. `soak-report-red.log` reproduces root's missing first_failure report guard. Final `soak-processes-green.log` records six helper tests plus process recovery passing; manual soak remains intentionally ignored by default. `soak-clippy.log` finishes successfully.
- `soak-smoke.log` and `soak/stable-WfSmsZ/manifest.txt`: 2-second configured smoke passes, 404 cycles/2,424 operations, all four groups, zero errors/censoring and clean shutdown.
- `soak-60s-c1.log` and `soak/stable-mBkDks/manifest.txt`: 60-second configured concurrency-one baseline passes; actual 60.005166583 seconds, 12,159 cycles/72,954 successful operations, 1,215.795 operations/second, group cycles `[3040,3040,3040,3039]`, zero errors and clean client/gateway/all-node shutdown. These are worker-run debug-build warm results, not verifier-run performance measurements.
- `soak-signal-check.log` and the retained `stable-Ng55IZ` manifest: direct parent SIGTERM produces intentional validation failure/exit 101, joined workers, clean client/gateway/node shutdown and zero surviving owned children. Cancellation is not misreported as successful soak completion.

Advisor and verifier directly discussed the implementation boundaries after the predesign. Advisor reported no new counterexample from those boundaries but did not independently reread this final harness; the source-review approval here is the verifier's own. No Judge disagreement.

Remaining limits: one gateway, shared public connection, debug build, 1 KiB values and a small reused warm key set. This is not maximum capacity, open-loop latency, cold storage behavior, process RSS/CPU qualification, a multi-client/multi-gateway claim, a ten-minute run, injected-fault correctness, movement or TTL acceptance. A signal arriving during startup follows bounded setup/cleanup rather than an empirically enumerated cancellation point; the documented direct-signal run covers the measurement owner.
