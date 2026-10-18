# Local Criterion benchmarks

The `warm_get` benchmark starts and owns one local writer, reader, and gateway
topology. It prepares distinct exact-byte fixtures, waits for reader convergence,
then measures one public `RockBeeClient` with up to 128 in-flight GETs.

Run it locally:

```sh
cargo build --release --locked -p rockbee-writer -p rockbee-reader -p rockbee-gateway --bins
cargo bench --locked -p rockbee-bench --features benchmarks --bench warm_get
```

It is feature-gated and is not executed by the normal CI test workflow.
Its dependencies are optional, so normal workspace builds do not compile Criterion.

This is a Rust executable: it starts the services on dynamic localhost ports,
uses a unique prefix in a fresh local filesystem object store, then shuts down
only its own children. It preserves the existing Tokio, TCP, and SlateDB settings.
No LocalStack container or existing database is used.

On macOS and Linux, SIGTERM and Ctrl-C (SIGINT) also shut down the owned services
and write `cleanup.json`, including if interrupted during service startup.
An interrupted run exits with status 143 (SIGTERM) or 130 (SIGINT).
Services run in separate Unix process groups so the benchmark controls their
shutdown when the terminal sends Ctrl-C.
Connection errors caused by this shutdown are treated as cancellation. Without
a shutdown signal, request failures still fail the benchmark visibly.
An interrupted run is not a valid performance result.
Signal handling runs on a separate waiting thread; the measured GET pipeline
does not poll a cancellation flag or acquire a process-management lock.

The workload is 1,024 distinct 1 KiB values, one client TCP connection and a
continuous bounded pipeline of 128 GETs. Setup waits for every durable PUT and
for eventual reader convergence. Criterion uses 10 seconds of warmup, a target
60-second measurement and 50 flat samples. Every measured response is checked
against its expected bytes; any missing value, mismatch or request error fails
the measurement. Setup and storage initialization are excluded from timing.

Criterion reports throughput in elements/second, where each element is a GET.
One Criterion iteration accounts for 128 GETs. Its time estimate is amortized
pipeline time per 128 operations, **not individual request latency or p99**.
This warm, closed-loop, 1 MiB fixture measures the whole local client/gateway/
reader path. It does not establish cold S3 performance or a capacity ceiling.
The client and all services share the host CPU; other applications and power
conditions can affect the result.

Reports, configuration, readiness logs, and cleanup results are stored in a new
ignored `reports/criterion-<uuid>/` directory. Open `criterion/report/index.html`.
The report includes native Criterion plots and raw JSON sample data.

`ROCKBEE_BENCH_BIN_DIR` selects a directory containing the three service binaries
(by default the release directory beside the benchmark). Rebuild them before
each comparison. `ROCKBEE_BENCH_REPORT_DIR` optionally chooses a new report
directory; existing directories are rejected to preserve earlier measurements.
