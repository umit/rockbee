# Logging and strong-read budget checkpoint

Read-only source review; no builds, source edits, tests or new scenarios by verifier.

```text
Client -> persistent Link -> Gateway -> Node -> fresh read barrier -> coherent view
                   application-owned log/env_logger evidence
```

Logging source verdict: no concrete payload leak, pending-mutex logger callback, ACK/replay change or request wire change found. Reviewed client API, transport admission/state/workers/server, gateway, node entrypoint/cluster/RPC/storage call sites. `EncodedResponse.request_id` is local queue metadata; the existing ResponseEnvelope remains the wire object. Pending retirement/expiry/admission logging occurs after unlocking. Library code emits log events; app/harness entrypoints own env_logger initialization, with info defaults and per-operation trace events. Native gate/lane ownership is retained.

Evidence limits sent directly to root: client mutation completion messages run when the handle is polled, so they do not account for dropped/unpolled outcomes. This is logging, not complete terminal metrics. Normal RPC failure logs currently have operation/error/elapsed but not last network phase; last-phase evidence still lives in the older opt-in diagnostic Span. If that path is removed under the user's no-spans instruction, retain its causal phase/cancellation distinction in ordinary logs. Synchronous stderr logging can perturb timing when verbose levels are enabled; no performance claim follows from enabling it.

Predesign verdict: APPROVE the exact policy in `judge-read-budget.md`, also directly discussed with advisor. Thread the original absolute deadline through data, directory and bootstrap reads. Retry only typed `RaftError::APIError(CheckIsLeaderError::QuorumNotEnough(_))`, sequentially, with a fresh barrier and pacing capped by remaining budget/existing heartbeat. Keep forwarding, fatal, storage and coherence errors terminal. Await each actual submitted barrier without timeout-drop/parallel spawning; check deadline before/after barrier and after coherent view. Never return a value after a failed barrier or expired budget, and never retry a mutation under this rule.

Implementation approval is separate: current reviewed `strong_read` is still the earlier one-attempt function. The sole existing c8 regression must exercise an actual qualifying failed barrier and successful later barrier within the same budget; a run without that event does not validate the new path. Keep failed-attempt and slow-native logs. Successful read recovery would demonstrate the availability policy, not removal of the measured native pause or resolution of the independent Deadline-to-Unreachable policy issue. No new scenario was requested.

## Published implementation follow-up

Root subsequently published the retry implementation. Independently inspected `strong_read`, all three data/directory/bootstrap call sites and guarded slow-persist logging: source approval, no safety/resource blocker found. It matches the exact typed sequential retry and awaited ownership above, checks the budget around successful barriers/views, and keeps fatal/forwarding/storage/coherence errors terminal. This is source approval for the existing c8 check, not a passing-run claim. One evidence detail was sent to root: per-attempt elapsed time requested by Judge is missing from the retry messages (remaining time is present; the start message is trace-level). The existing feature/env diagnostic recorder is explicitly retained for phase evidence in this run, addressing the ordinary-log last-phase limitation noted above.
