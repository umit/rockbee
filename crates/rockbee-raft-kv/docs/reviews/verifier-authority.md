# Mutation certainty and placement authority

Source review, 2026-10-19. No production edits, builds, faults, or runtime proof.
Placement recommendations are a future Task 7 gate; they do not expand today's
core client/gateway/three-node milestone or the failure matrix.

```text
Mutation: Gateway -> data-group authority -> client_write -> durable result
Control:  metadata job -> fenced group executor -> membership consensus
```

## ForwardToLeader does not prove pre-submission rejection

In cached OpenRaft 0.9.25:

- `src/raft/mod.rs:651`: `client_write` sends work through `client_write_ff`,
  then awaits its responder.
- `src/engine/engine_impl.rs:261`: `get_leader_handler_or_reject` can emit
  ForwardToLeader before accepting a leader operation.
- `src/core/raft_core.rs:1658`–`:1684`: `Command::DeleteConflictLog` truncates
  an existing log suffix, removes log-indexed client responders, and sends those
  responders the same `ClientWriteError::ForwardToLeader` variant.

Therefore the returned variant does not identify only the pre-append branch.
After calling `client_write`, classify missing/failing mutation results as
Unknown under this project's contract unless an independently established
pre-consensus rejection applies. A leader hint may update routing for later
requests; it must not cause automatic replay of this attempt.

**Counterexample schedule:** A accepts mutation M into its local log and keeps
the responder. A loses leadership; another leader replaces the conflicting
uncommitted suffix. A's DeleteConflictLog branch sends ForwardToLeader for M.
A naive adapter labels it Rejected and the gateway retries, despite the public
contract reserving Rejected for proven pre-consensus submission. The branch
proves that this attempt entered consensus; its error variant alone supplies no
origin-stage proof to the public transport.

**Necessary test:** force an accepted log-indexed mutation to lose its suffix,
observe the actual ForwardToLeader response from that branch, and require the
public outcome Unknown with no second proposal. Contrast with an explicit node
handler rejection before calling `client_write`, where Rejected is justified.
This supplements, and does not replace, commit-response-loss testing.

## Delayed controller work can reverse a newer move

**Counterexample schedule:** metadata controller C1 reads job J1 and target ABD,
then pauses before its data-group membership RPC. C2 replaces it, completes J1,
and a newer job J2 changes that group to ACD. C1's old RPC reaches the current
data leader afterward. If the leader merely checks that the caller once held
metadata leadership, or trusts the supplied voter set, it can revert voters to
ABD. A metadata-only mutex/revision does not fence the data-group effect.

Checking a persisted data-group epoch and subsequently calling
`change_membership` also has a gap: another leadership generation can advance
the epoch between those steps. OpenRaft 0.9.25's public membership method accepts
changes and retain policy, not an expected application epoch/vote. Its wrapper
also performs joint and final uniform changes. An old async wrapper may still
have work remaining after its caller times out. A local mutex alone is not a
cross-leader fencing proof.

## Smallest safe bounded prototype contract

1. Persist one active job per data group with monotonically increasing epoch,
   job ID, immutable old/new voters and progress. Same ID with different intent
   is rejected; conflict never silently replaces the active job.
2. Controllers submit/reconcile this named job. They never directly expose an
   arbitrary voter-set RPC. A per-group executor owns and serializes all actual
   learner/membership futures, retaining ownership through external timeout.
3. Resume the same immutable job by inspecting actual committed membership.
   Before accepting a **new epoch or target**, durably retire the preceding
   executor epoch on every node that could still execute that job, then drain
   all actual membership futures/core handoffs. A fatal/abandoned result is not
   a drain acknowledgment; reconciliation or owned core shutdown is required.
4. Persist retirement so restart and late RPCs cannot resurrect old work. A
   node that cannot prove old work drained cannot acknowledge retirement. Until
   all required retirements are known, defer the next job. Offline executors can
   therefore block a subsequent move: document this availability tradeoff.

This conservative contract avoids pretending a check-then-call wrapper provides
an atomic fence. A more available conditional-membership integration requires
its own source-backed design. Neither approach requires cross-shard transactions.

**Necessary test:** pause C1 before its membership side effect, replace it,
complete/reconcile J1, retire/drain its executors, then complete J2. Release all
delayed J1 RPCs and restart an old executor. Committed membership, active job,
and routing remain at J2; no cleanup removes a J2 replica. Separately withhold
one retirement acknowledgment and prove J2 cannot activate rather than claiming
a safe fence. Record membership LogIds and proposal counts, not just job labels.

**Verdict:** mutation classification has a concrete source-backed rule now.
Placement fencing remains an explicit Task 7 integration obligation. Advisor and
verifier agree that a metadata epoch check or a leader-local mutex by itself is
insufficient; no implementation has been claimed safe by this note.
