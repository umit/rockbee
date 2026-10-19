# Authority findings for Tasks 5–7

Status: read-only findings from cached OpenRaft 0.9.25; no implementation/build.
Prioritize the working KV path. Placement fencing below is a later gate, not a
claim that a movement protocol is implemented or fully validated.

```text
Planned: Client -> Gateway -> data-group leader -> durable Raft/apply result
Metadata controller -> fenced placement control -> membership API
                                             (unfinished, Task 7)
```

## Confirmed write-error certainty

`Raft::client_write` calls `client_write_ff`, which submits a ClientWriteRequest,
then waits for the responder. It does not expose a public accepted/not-accepted
flag alongside an error. In `core/raft_core.rs`, `write_entry` first asks the
engine for leader authority, then appends and registers a responder indexed by
the assigned log index.

There is also a **post-append** ForwardToLeader path: the `DeleteConflictLog`
command truncates the log, removes indexed pending responders from
`client_resp_channels`, and sends each ClientWriteError::ForwardToLeader. Thus
ForwardToLeader does not exclusively identify the initial pre-append rejection.
The verifier independently confirmed this branch.

For this project's contract, node-generated explicit Rejected is allowed only
when its own handler proves it has not submitted to consensus: validation,
admission, or an explicit routing refusal before invoking client_write. Once
client_write is invoked, map ForwardToLeader, fatal/closed response channel, and
deadline/disconnection without durable success to Unknown. Leader hints can
refresh routing for future attempts, but do not authorize replay of this attempt.
Although a particular truncated conflict entry may be known uncommitted inside
the engine, the generic public error does not carry that provenance and the
project's Rejected contract is specifically pre-submission.

Focused regression: hold an appended proposal without quorum, install a
conflicting winning-leader suffix, and observe the pending responder's
ForwardToLeader. Assert node returns Unknown, gateway does not replay, and
instrument actual proposal count separately from transport attempts. Contrast
with a handler routing refusal that never invokes client_write.

Source locations: cached `openraft-0.9.25/src/raft/mod.rs:651`,
`src/core/raft_core.rs:485`, and `src/core/raft_core.rs:1663`;
[versioned public API](https://raw.githubusercontent.com/databendlabs/openraft/v0.9.25/openraft/src/raft/mod.rs),
[versioned core](https://raw.githubusercontent.com/databendlabs/openraft/v0.9.25/openraft/src/core/raft_core.rs).

## Confirmed membership integration limits

`change_membership(members, retain)` sends a joint-membership change, waits,
and may then send the final uniform change. Leadership loss can leave the group
joint. Neither change_membership nor add_learner accepts an expected application
epoch/job ID. `with_raft_state` exposes an immutable state callback, not an atomic
application-fence-plus-membership operation.

The blocking add_learner implementation also logs the result of its catchup
metrics wait and then returns the earlier membership response. Do not treat its
Ok alone as durable proof that the learner is still caught up/current leader
authority holds; inspect actual membership/progress before promotion.
[Versioned membership API implementation](https://raw.githubusercontent.com/databendlabs/openraft/v0.9.25/openraft/src/raft/impl_raft_blocking_write.rs),
[read-only core callback type](https://raw.githubusercontent.com/databendlabs/openraft/v0.9.25/openraft/src/raft/external_request.rs).

Persisting `(group, epoch, job_id, old_voters, new_voters, phase)` and validating
it before calling a membership API is necessary but **not sufficient fencing**.
A delayed operation can pass validation, pause, then execute after a newer job.
A process-local mutex alone does not serialize effects across successive leaders.
The two internal stages of change_membership make this especially important.

## Safe scope decision and conservative fallback

Do not enable arbitrary concurrent/superseding movement RPCs with check-then-call
validation. Finish the ordinary KV/replication path first; Task 7 remains gated
until actual side-effect ordering is proved by the stale-controller schedule.

A conservative implementation option, if needed before a stronger integration,
is explicit **control-lane quiescence** between epochs:

External controllers may only request/reconcile the persisted job with its
immutable target, never invoke raw arbitrary voter-set changes. Failover resumes
the same job. The verifier agrees this conservative restriction/drain protocol
is safe in principle and that broader placement availability remains unproved.

1. Metadata consensus allocates the next per-group epoch and one immutable job;
   competing jobs are refused while that transition is active.
2. Before activation, every node eligible to execute old membership work closes
   its old-epoch control lane, drains the actual outstanding membership futures,
   durably records the rejected-through epoch, then acknowledges. Queued/delayed
   old-epoch requests are rejected on arrival before any membership invocation.
3. Only after all required acknowledgements does metadata consensus activate the
   next epoch. Eligible nodes execute one serialized job/stage stream. New or
   rejoining executors install the durable fence before control readiness.
4. A controller restart reconciles the persisted phase/acknowledgements and actual
   committed membership. It never declares drain based on a deadline or lost RPC.

This is a proposed safety-first fallback, not the final Task 7 design: all
possible old executors must be accounted for, including removed/rejoining nodes.
An unavailable executor or unresolved membership future blocks epoch advancement;
ordinary KV availability is separate. It avoids claiming that an application
epoch check is atomic with OpenRaft's membership side effect, at the cost of
control-plane availability. A less restrictive design needs a separately proved
conditional membership/leadership fence; it cannot be inferred from current APIs.

Required later test: pause an old operation **after validation and before API
submission**, attempt advancement, prove it cannot be declared activated while
old execution remains possible, then release/drain and verify the next epoch.
Also pause between joint and uniform stages and repeat after controller restart.
Do not count rejection of a stale request before validation as proving these races.
