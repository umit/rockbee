# Judge decision: c8 strong-read budget

**Decision: conditionally accept the narrow proposal.** It is a correct
availability repair for the observed transient quorum-probe failure, provided
the implementation and regression meet the conditions below. It does not
identify or cure the native/RocksDB pause which produced that failure, so the
new logs must preserve the evidence rather than making c8 look healthy by
silently hiding attempts.

```text
GET(deadline) -> strong_read -> fresh linearizable barrier -> coherent local view
                         | QuorumNotEnough only, before deadline
                         v
                wait <= heartbeat and remaining budget -> fresh barrier
```

## Evidence and scope

The current service calls `ensure_linearizable()` once and maps every
non-forwarding error to `Unavailable`; its `strong_read` has no deadline
argument. `stable-b6lFue` stopped at 38.876896 s with four remote-read errors
and 100160 successful operations. The four affected reads began at about
38.3159 s and each failed roughly 52 ms later with `not enough for a quorum`.
At the same wall-clock point, node 1 reported 51--52 ms peer `read_header`
deadline errors and nodes 1--3 each recorded synchronous RocksDB work of about
97 ms. PUT and DELETE completions followed those read failures.

Thus connection reuse did not remove the immediate source of the failed
barrier. A later barrier may legitimately succeed once peer progress resumes;
returning that later result remains linearizable because every returned value
is still read only after its own successful barrier and coherent applied-state
check. This is a bounded retry of an authority check, not a stale read or a
replay of a mutation.

## Required implementation limits

1. Thread the *same absolute* `Instant` into every `strong_read` caller,
   including directory and bootstrap-directory reads. Do not mint a new budget
   per attempt.
2. Retry only the exact OpenRaft shape
   `RaftError::APIError(CheckIsLeaderError::QuorumNotEnough(_))`. A
   `ForwardToLeader`/NotLeader result and fatal, storage, state-view, decode,
   or other Raft errors remain terminal. Never retry PUT, DELETE, or any
   conditional mutation.
3. Keep attempts sequential. After a qualifying failure, sleep no longer than
   `min(heartbeat_interval, deadline - now)`, then create a *fresh* barrier.
   No parallel probes, zero-delay spin, Raft timer/configuration change, or
   gateway cache is acceptable.
4. Check the deadline before each barrier, after its completion, and after the
   coherent local read. Do not return a view once the original budget expired.
   The submitted `ensure_linearizable` future must stay owned and be awaited;
   do not use a timeout/select that drops it while releasing accepted-handler
   accounting. Consequently an individual barrier can finish after the
   observed deadline, after which the service returns `Deadline` rather than a
   value. This is an existing cancellation/accounting limitation, not a reason
   to detach the future.
5. Emit only the requested log/env_logger evidence: attempt number, group,
   typed outcome, elapsed time, and remaining budget. Log each qualifying
   failure and final result so success after a retry does not erase the
   RocksDB/peer-delay signal. Do not add metrics, protocol fields, timers, or
   another test scenario in this change.

`CheckIsLeaderError` in the pinned OpenRaft 0.9.25 exposes precisely the
`ForwardToLeader` and `QuorumNotEnough` variants, and
`ensure_linearizable()` waits for applied progress with no supplied timeout.
That supports the explicit match and ownership rule above.

## Required focused regression

Use the existing c8 three-node stable-soak path and its fixed workload/seed;
do not create a new scenario. The regression must reproduce a qualifying
first-barrier `QuorumNotEnough` during the already observed pause, then show:

- one or more sequential fresh barrier attempts within the original request
  deadline;
- no GET `ReadRemote`/`Unavailable` from that transient window and no wrong
  value;
- no mutation retries or `Unknown` outcomes introduced by this policy; and
- diagnostics/logs retaining the peer timeout and native synchronous-write
  evidence if it recurs.

A clean run without a logged qualifying first failure does not exercise this
repair. If the original deadline expires, terminal `Deadline` is correct and
must not be relabelled as success or hidden by the separate known
Deadline-to-Unreachable 500 ms backoff issue. That classification fix is
explicitly out of scope for c8.

## Safety and resource judgment

The proposal does not mask an infrastructure issue if the attempt logs remain
visible and c8 is described as an availability workaround for transient peer
progress delay. It would mask it if retry counters/errors are discarded or if
the test treats a successful retry as proof that the 97 ms native pause is
solved. The real pause remains unfinished work after c8; it needs a separate,
evidence-led investigation once this same failing regression is green.

## Update: c8 timeout classification, `stable-Sc1F8R`

**Decision: accept the narrow semantic correction, with a focused adapter
regression.** `PeerNetwork::call()` creates `PeerError::Deadline` only when
its own `timeout(option.hard_ttl(), ...)` expires, but `rpc_error()` currently
maps it with every other `PeerError` to `RPCError::Unreachable`. That loses a
meaning OpenRaft exposes explicitly: `Unreachable` asks replication to start
its backoff, while `RPCError::Timeout` does not take that 500 ms unreachable
backoff path. The pinned OpenRaft replication code branches exactly this way.

The implementation must construct `RPCError::Timeout(Timeout { ... })` only
for `PeerError::Deadline`, carrying the request's `RPCTypes`, local node ID,
target node ID, and `option.hard_ttl()`. It must do so separately for append,
vote, and install-snapshot calls, whose action differs. All I/O, connect,
EOF/reset, admission, frame, decode, identity, and protocol failures stay on
their existing error paths; in particular, transport I/O remains
`Unreachable`. Do not change a timer, replay a mutation, add a retry loop, or
try to collapse the outer OpenRaft timeout race into this change.

```text
adapter timeout(PeerError::Deadline) -> RPCError::Timeout -> no Unreachable backoff
actual I/O / peer failure              -> RPCError::Unreachable -> existing backoff
```

`stable-Sc1F8R` is evidence that the read fix worked in its intended scope:
group 4 logged a first `QuorumNotEnough` after 247.324 ms, then a second,
fresh barrier succeeded in 144.035 ms with about 4.56 s of the original budget
remaining; the report has zero read errors. It is **not** evidence that the
cluster incident is resolved. The same run failed at 57.019 s with six
mutation `Unknown` outcomes. In the final window, all nodes show 171--181 ms
synchronous native writes beginning near 19:57:51.705, followed by 50--52 ms
outgoing deadlines and leader changes around 19:57:52.282 and 19:57:52.418.
That sequence supports a plausible contribution from wrong timeout
classification, but does not establish it as the cause of the disk pauses or
of the election/unknown-mutation incident.

Required regression: a focused adapter test must exercise `PeerError::Deadline`
for each relevant call shape and assert `RPCError::Timeout` contains the
correct action, local ID, target, and TTL; an ordinary I/O failure must still
assert `RPCError::Unreachable`. Re-run the same c8 stable-soak afterward and
keep its logs. Do not move to a third fix or infer an architectural diagnosis
until the timeout result and retained disk/leader evidence are reviewed.

## Final scoped c8 checkpoint verdict: `stable-YFt5NP`

**PASS for the authorized c8 checkpoint.** This verdict covers only the
fresh-barrier read-budget repair, the Deadline-to-Timeout adapter correction,
and their requested logging/verification scope. It does not close the broader
failure matrix, membership work, TTL work, or the investigation into native
storage pauses.

```text
Client -> Gateway -> Raft leader -> peer RPC / RocksDB replicas
                         |              |
                         |              +-- deadline is Timeout; I/O stays Unreachable
                         +-- fresh QuorumNotEnough barrier retry, then coherent view
```

Independent evidence supports the checkpoint:

- `stable-YFt5NP/manifest.txt` reports PASS after 60.008801750 s, 170238
  successful operations, zero errors, no `Unknown`, no remote-read error, no
  wrong value, and clean client/gateway/node shutdown.
- Node 1 exercised the policy, rather than merely passing without it: group 2
  logged a typed first quorum failure after 51691 us with 4946205 us remaining,
  then a fresh second barrier completed in 474 us with 4894333 us remaining.
- The same logs retain the incident signal: 50--77 ms peer deadlines and
  native synchronous writes as high as 186951 us. This pass therefore does
  not establish causality or claim the pauses were fixed.
- The source preserves the limits judged above: same absolute deadline through
  all `strong_read` callers; only typed `QuorumNotEnough` retries; sequential
  heartbeat-paced fresh barriers; no mutation replay; deadline checks before,
  after barrier, and after the coherent local read.
- The adapter now maps only `PeerError::Deadline` to a fully populated
  OpenRaft `Timeout` and retains `Unreachable` for other peer errors. The
  focused red/green evidence records the pre-change assertion failure and the
  post-change passing test for all three RPC action types plus a non-deadline
  `Unreachable` assertion.
- Supplied terminal evidence is internally consistent: workspace tests report
  149 passed, 0 failed, 1 manual soak ignored; strict clippy, formatting, and
  diagnostics-disabled build checks pass. The manual soak's separate process
  exit is zero and its manifest is the authoritative PASS record.

No further behavior change is authorized by this verdict. Treat surviving
native-write/peer-deadline evidence as an open incident signal and review a
separate architecture/evidence plan before any third fix.

## Final scoped client-connection matrix verdict

**PASS for the reported 1/2/3 independent-client matrix on checkpoint
`7d2ff9f`.** `client-matrix-report.md` matches the three manifests and summary
CSVs: 1/1 completed 73344 operations in 60.004296542 s, 2/2 completed 118320
in 60.004809625 s, and 3/3 completed 129186 in 60.006072459 s. Each records
PASS, zero errors, no partial cycles, clean client/gateway/owned-node shutdown,
one in-process gateway, three node processes, and the declared number of
client connections and workers. The corresponding run logs contain 1, 2, and
3 client-connected records, respectively; the tested code checkpoint was
`7d2ff9f`, before the final documentation-only amendment.

```text
1 / 2 / 3 independent TCP clients -> one gateway -> three Raft nodes
```

This is correctly framed as sequential warm closed-loop correctness evidence
for the stated debug workload, not a maximum-capacity, long-duration, failover,
or broader-matrix claim. No discrepancy was found. No source, Cargo, or test
action was performed for this review.
