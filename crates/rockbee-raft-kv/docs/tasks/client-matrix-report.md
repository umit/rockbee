# Separate client connection matrix

Verified after the local code checkpoint `7d2ff9f`, with no product-code changes
between cases. These are warm closed-loop correctness runs on macOS aarch64 in
the debug profile, with 1 KiB values, four data groups and seed 20261019.
They are not maximum-capacity or long-duration measurements.

```text
N independent TCP clients -> one in-process gateway -> three node processes
                                                        -> OpenRaft -> RocksDB
```

Each case ran sequentially for 60 seconds. Every client independently connected
to the same gateway address and warmed all four groups. Client connection log
counts match N; each worker selected one of these distinct connections. The
gateway runs in the test process; the three nodes are separate CLI processes.

| Client connections | Workers | Measured seconds | Successful operations | Errors | Artifact |
| ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 1 | 60.004296542 | 73,344 | 0 | `stable-Mxil47` |
| 2 | 2 | 60.004809625 | 118,320 | 0 | `stable-1OH76d` |
| 3 | 3 | 60.006072459 | 129,186 | 0 | `stable-Q1eLrK` |

All cases have zero Unknown mutations, remote/transport read failures, wrong
values, censored attempts and partial cycles. Every worker joined before
resource close; every client, gateway and owned node reported successful
shutdown. Each completed cycle contains PUT, GET, update, GET, DELETE and
absence GET. Run logs are `reports/matrix-clients-{1,2,3}.log`; artifact manifests,
summary CSVs and node logs are under `reports/soak/` (local, Git-ignored).

The separate one-client/eight-worker regression also passed at `stable-YFt5NP`:
60.008801750 seconds, 170,238 successful operations and zero errors. That run
exercised the fresh read-barrier retry within the original deadline. Its native
storage pauses and peer deadlines remain visible and are not claimed resolved.

Other checkpoint checks: 149 workspace tests passed, with the manual soak
ignored in the default test list and run explicitly above; strict Clippy,
rustfmt and the diagnostics-disabled build passed. Broader failover/fault soak,
learner snapshot catch-up and fourth-node membership movement remain separate
work; this report does not claim the broader goal complete.
