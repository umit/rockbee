# Coordinator audit: logging and c8 timeout classification

## Verdict

**PASS for the fixed c8 soak and the requested logging scope.** The final
same-scenario run is `stable-YFt5NP`, not the earlier failed `stable-Sc1F8R`.
Its manifest records `status=PASS`, 60.008801750 measured seconds, 170,238
successful operations, zero errors, zero `Unknown` mutation outcomes, zero
remote-read errors, and orderly client, gateway, and three-node shutdown.
The command log reports the sole soak test passed in 61.84 seconds and ends
with Cargo's successful test result; the manifest is the source of the
`status=PASS` marker.

This is a warm closed-loop debug-profile result, so it is evidence for this
fixed c8 workload only. It is not a maximum-capacity measurement or a general
claim that native storage stalls are solved.

```text
Client -- PUT / GET / DELETE --> Gateway --> Node (group leader)
                                              |
                                              +--> OpenRaft replicas --> RocksDB
```

## c8 evidence

The final run exercised the exact availability repair. At 20:01:08 on node 1,
group 2 logged a typed `quorum_not_enough` first barrier after 51,691 us, with
4,946,205 us remaining from the original request budget. After the bounded
heartbeat-paced delay, its fresh second barrier recovered in 474 us, retaining
4,894,333 us. The workload completed with no `read_remote`, `wrong_value`, or
mutation `unknown` outcomes.

The final node logs still contain 50--77 ms peer RPC deadlines and synchronous
native-write observations, including 186,951 us. The retry therefore remains
an availability repair that preserves the incident signal; it is not a causal
diagnosis or a remedy for those pauses.

The adapter change is scoped correctly: `PeerError::Deadline` becomes
OpenRaft `RPCError::Timeout` with the original action, local ID, target, and
TTL. Other peer errors retain `Unreachable`. The focused regression has
recorded red and green evidence (`reports/peer-timeout-red.log` and
`reports/peer-timeout-green.log`); the green log reports one passing test.
No timer, mutation retry/replay, durability acknowledgement, or public
`MutationHandle` ownership behavior changed in the inspected code.

## Logging scope

The node and gateway binaries initialize `env_logger` once. The client and
protocol are library crates and use the `log` facade, leaving logger ownership
to their embedding application; the root README gives the required client
initialization example. Logs cover client connection/admission/terminal
outcomes, gateway routing/failures, transport lifecycle and framing failures,
Raft RPC/fresh quorum-barrier retry outcomes, and storage slow writes. They avoid key,
value, and snapshot contents.

`GatewayMetrics` remains a small in-process atomic diagnostic snapshot. It is
neither a metrics exporter nor a tracing/OpenTelemetry integration. Static
inspection found no application tracing subscriber, OpenTelemetry setup,
Prometheus exporter, or new metrics export endpoint. This matches the stated
`log` plus `env_logger` scope.

## Evidence reviewed

- `reports/logging-check.log`: compilation/check evidence for the logging
  change.
- `reports/peer-timeout-red.log` and `reports/peer-timeout-green.log`: focused
  regression before and after the Deadline-to-Timeout adapter correction.
- `reports/soak-60s-c8-timeout-classification.log`: final process exit and
  Cargo test success; the corresponding manifest supplies `status=PASS`.
- `reports/soak/stable-YFt5NP/manifest.txt`, `summary.csv`, and `node-*.log`:
  workload result, exercised typed retry, preserved timeout/native-write
  evidence, and clean shutdown.

## Post-change validation

The complete workspace test command recorded in
`reports/workspace-after-logging.log` exited 0: **149 passed, 0 failed, and
1 ignored**. The ignored test is the manual soak; the separately recorded c8
soak is `stable-YFt5NP` above. The strict Clippy run with diagnostics enabled
(`reports/clippy-after-logging.log`) exited 0, and the diagnostics-disabled
build check (`reports/default-build-after-logging.log`) exited 0. Formatting
also passed.

These are validation results for the logging and narrow timeout-classification
change. They do not by themselves prove the fourth-node, membership, TTL, or
native-pause work complete.

## Independent client matrix

**PASS for the three fixed client-count cases.** I independently inspected
the matrix command logs, manifests, and summary CSVs. They ran sequentially
against one in-process gateway and three owned node CLI processes:

| Clients/workers | Artifact | Measured seconds | Successful operations | Errors |
| ---: | --- | ---: | ---: | ---: |
| 1 | `stable-Mxil47` | 60.004296542 | 73,344 | 0 |
| 2 | `stable-1OH76d` | 60.004809625 | 118,320 | 0 |
| 3 | `stable-Q1eLrK` | 60.006072459 | 129,186 | 0 |

Each manifest has the matching `client_connections` and `total_workers` value,
one gateway, and three node processes; each records clean worker/client/
gateway/node shutdown. The matrix logs show respectively one, two, and three
public client connection and closure records. Each summary has zero Unknown
mutations, transport or remote-read errors, wrong values, and censored
attempts. This is warm closed-loop correctness evidence on the fixed
one-to-three-client range, not a scaling, capacity, failover, or long-duration
claim.

## Remaining work

The fourth-node, membership, and TTL plans are outside this audit and remain
unfinished. The one-to-three-client matrix above does not replace broader
failover/fault or membership movement coverage. The surviving peer deadlines
and native-write pauses need a separate evidence-led investigation; this
report does not authorize further behavior changes.

## Correction

The first draft incorrectly showed the legacy Rockbee SlateDB writer/reader
flow and described the command log as ending in `status=PASS`. This independent
Raft workspace uses `Client -> Gateway -> shard leader -> OpenRaft replicas ->
RocksDB`; its command log ends in Cargo's passing test result, while
`stable-YFt5NP/manifest.txt` contains the PASS status. The first draft also
called the c8 operation a stale-read retry. It is a fresh, sequential
linearizable quorum-barrier retry before the coherent local view, as verified
in `cluster/service.rs`.
