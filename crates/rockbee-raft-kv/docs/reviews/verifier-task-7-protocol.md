# Task 7a control DTO review

Final spec verdict: APPROVED for Task 7a DTOs. Final quality verdict: APPROVED for that scope. The findings below are corrected as recorded in final acceptance. No metadata persistence or movement integration is implemented by this change. No Cargo or production edits by verifier.

Reviewed `control/{movement,metadata}.rs`, its public export, `tests/control.rs`, the protocol report and current Task 7 brief. Fixed-size IDs/three-voter arrays, canonical exact replacement, group-zero/greater-than-256 rejection, endpoint shape/conflict validation, bounded diagnostics and the full `(term,leader_node_id,index)` representation are appropriate. `derive_intent` checks actual configured group/revision; decoded type validation does not pretend to know the durable directory. Prospective-catalog union, permanent job conflict and authority remain metadata-apply/runtime duties, correctly stated in the documentation.

P2, `metadata.rs::AdminResult::validate`: it checks only diagnostic length. `AdminResult::Completed { record: MoveRecord { progress: Pending, ... } }` therefore decodes successfully despite contradicting its completion discriminant. Require Completed progress for that variant. Also traverse `MoveIntent::validate` for embedded intents so validation of directly constructed Accepted/AlreadyAccepted/Completed responses cannot bless a malformed canonical target. Private wire validation already rejects a malformed decoded intent, but public construction can bypass it. Advisor independently agrees the contradictory completion is a correctness gate; worker notified for focused red/green tests.

Additional intrinsic input check sent to worker: reject `MoveReplica.remove_node == add_node.node_id` during wire conversion; no directory can make it a replacement. Directory-dependent source-voter membership/count and stored-job conflicts still belong in derivation/apply, not a new protocol framework.

The report records six focused tests and isolated strict Clippy passing before these findings. Those cover important initial cases but do not exercise contradictory completion progress. Once corrected, inspect the focused result and preserve original wire variants/field order; no need to broaden this into an arbitrary collection framework or run native node builds.

This approval gate is independent of Task 7 movement success. DTOs cannot prove a supplied log position committed, persist a permanent one-job restriction, wait for D, publish placement, or fence a later target. The explicitly restricted ABC -> ABD implementation and all associated runtime gates remain pending.

```text
operator DTO -> validated fixed intent -> future metadata apply/reconciler
              DTO scope approved; live movement unfinished
```

## Final acceptance

The frozen source now rejects Completed envelopes containing Pending records; `AdminResult::validate` checks every embedded intent, including directly constructed values; and decoded MoveReplica rejects identical removed/replacement IDs. Two focused tests exercise those regressions. The worker reports both failures reproduced before the fix, eight control tests passing afterward and strict protocol all-target Clippy passing. Verifier read the changed source/tests/report and did not rerun the identical suite.

Direct advisor/verifier exchange agreed on the semantic blocker and its resolution, with no Judge disagreement. Wire variants/field order and full committed-log identity remain intact. The worker synced the explanatory metadata wording to the now-selected standalone metadata chunks; that documentation correction does not change this DTO behavior or confer metadata-apply safety.
