# Stable three-node soak checkpoint

Implemented an ignored manual Rust test in `apps/rockbee-raft-node/tests/support/soak.rs`, reusing exact CLI-child ownership in `tests/processes.rs`. No production defaults or behavior changed. The user-requested `hdrhistogram =7.6.0` dependency is development-only.

```text
one shared public Client (clones) -> one gateway -> three CLI node processes
                                    four data Raft groups -> local RocksDB
```

Each worker owns four deterministic keys, one per data group. A cycle performs PUT(v1), exact GET(v1), update(v2), exact GET(v2), DELETE, exact GET(None). The 1,024-byte values encode seed, worker, cycle and version. Every failed step stops its dependent cycle; uncertain mutations, admission failures, remote/transport read failures and wrong values are separate outcomes. There are no measurement retries. All four groups complete excluded warmup before the common measurement release.

Six fixed HDR histograms per worker record completed attempts in microseconds with three significant figures, explicit 60,000,000-us limit and auto-resize disabled. Zero-microsecond observations are recorded as zero at one-microsecond resolution. Out-of-range samples fail validation instead of clamping. Fixed per-second counters include successful prefixes of partial cycles; completed-cycle counts remain distinct. Actual measured elapsed includes final worker drain and join. Censored operations are counted without fabricated latency samples.

SIGINT/SIGTERM handlers are installed before children spawn. Signals stop new cycles; current work drains for at most 35 seconds, followed by abort-and-join if required. All workers are joined before client, gateway and exact owned children close. Unique ignored `reports/soak/stable-*` directories preserve summary CSV, per-second goodput CSV, manifest and all three node logs before temporary databases are removed.

## Actual evidence

| Check | Result | Evidence |
|---|---|---|
| Initial settings/accounting/drain regressions | 3 expected failures | `reports/soak-unit-red.log` |
| Independent accounting-error report regression | expected failure, then fixed | `reports/soak-report-red.log` |
| Final affected process suite | 7 passed; manual soak ignored | `reports/soak-processes-green.log` |
| Strict scoped Clippy | passed | `reports/soak-clippy.log` |
| Two-second c1 smoke | passed; 404 cycles, 2,424 operations | `reports/soak-smoke.log`, `reports/soak/stable-WfSmsZ/` |
| 60-second c1 baseline | passed; 12,159 cycles, 72,954 operations | `reports/soak-60s-c1.log`, `reports/soak/stable-mBkDks/` |
| Direct parent SIGTERM | cleanup passed; canceled validation correctly failed | `reports/soak-signal-check.log`, `reports/soak/stable-Ng55IZ/` |

The c1 baseline used **debug / aarch64 / macOS**, one sequential-await worker, one shared client connection, four groups and seed 20261019. Actual elapsed was **60.005166583 seconds**, including drain: **1,215.795 successful operations/s**, **202.633 complete cycles/s**. Group cycle counts were `[3040, 3040, 3040, 3039]`. Errors, partial cycles and censored operations were all zero; client, gateway and three node shutdowns all succeeded.

| Operation | p50 us | p95 us | p99 us |
|---|---:|---:|---:|
| PUT | 1146 | 1446 | 1974 |
| GET after PUT | 418 | 602 | 796 |
| update | 1134 | 1417 | 1940 |
| GET after update | 417 | 598 | 769 |
| DELETE | 1054 | 1333 | 1896 |
| GET absence | 411 | 591 | 802 |

The SIGTERM check used `ROCKBEE_SOAK_READY_FILE` to obtain the actual parent-owned child PIDs after workers started, signaled only that parent, and verified zero surviving owned children, clean worker joins/resource closure and retained node logs. The parent returned exit 101 because a canceled validation must not be reported as a successful soak.

## Manual invocation

From `crates/rockbee-raft-kv`:

```sh
ROCKBEE_SOAK_SECONDS=60 ROCKBEE_SOAK_CONCURRENCY=1 \
CXXFLAGS='-isystem /Library/Developer/CommandLineTools/SDKs/MacOSX.sdk/usr/include/c++/v1' \
CARGO_BUILD_JOBS=2 cargo test -p rockbee-raft-node --test processes \
soak::stable_three_node_soak --locked --offline -- --ignored --exact --nocapture
```

Duration is bounded to 1–3600 seconds (default 60); concurrency to 1–64 (default 1). `ROCKBEE_SOAK_SEED` selects a u64 seed. Higher concurrency can encounter real admission limits; the harness reports these and does not tune or retry them away.

This is **warm closed-loop operating-correctness evidence**, not maximum throughput, cold-cache latency or an open-loop capacity test. No release rebuild, CPU/RSS sampling, injected node faults, membership movement or TTL changes are included. Parent SIGKILL cannot run cleanup. The 60-second baseline preceded only the report-failure guard and two test-style fixes; final tests, cancellation proof and strict Clippy cover that final harness. Final advisor/verifier source review remains pending before any ten-minute run. Native/source ownership was returned to root for the separate client-server correctness fix.

## Multiple public client connections (prepared; matrix not yet run)

The harness now accepts `ROCKBEE_SOAK_CLIENTS` (default 1, range 1–64, no greater than `ROCKBEE_SOAK_CONCURRENCY`). Each configured client is created by an independent `RockBeeRaftClient::connect` call to the same owned gateway address. Worker `id` uses connection `id % client_connections`; cloning that client within workers preserves multiplexing on that connection. Worker concurrency and TCP client count are separate settings.

```text
N independent public client connections -> one gateway -> three real node processes
workers assigned by worker_id % N                          four data groups
```

Each connection completes excluded warmup against all four data groups. A failed connect, warmup, or handled signal reaches the common cleanup path, which closes every connection already created before closing the gateway and owned nodes. Warmup authority timeout is returned as a setup failure so partial startup does not bypass explicit client cleanup. Workers still stop on the first failed dependent step and never replay Unknown mutations.

The manifest records `client_connections`, `total_workers`, `gateways=1`, `node_processes=3` and the worker-to-connection mapping, alongside the existing concurrency field and per-client shutdown results. `client_connections` is the requested configuration; a setup failure can stop before all requested connections are created, and its error plus per-client shutdown entries identify that partial startup.

After the authorized local commit, the planned 60-second matrix is:

| Public client connections | Total workers | Gateway | Real nodes |
|---:|---:|---:|---:|
| 1 | 1 | 1 | 3 |
| 2 | 2 | 1 | 3 |
| 3 | 3 | 1 | 3 |
| 1 | 8 | 1 | 3 |

Set both variables explicitly, for example `ROCKBEE_SOAK_CLIENTS=2 ROCKBEE_SOAK_CONCURRENCY=2`. The one-client/eight-worker run remains a separate multiplexing regression. This source checkpoint has been formatted but not compiled or run: native Cargo ownership belongs to the peer-fix worker, and the user requested commit before the matrix.
