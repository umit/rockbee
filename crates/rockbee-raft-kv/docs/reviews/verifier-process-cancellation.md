# Process-fixture cancellation review

Spec verdict: APPROVED for handled parent SIGTERM/SIGINT cleanup. Quality verdict: APPROVED for this narrow test-only change. No builds or source edits by verifier.

Reviewed `apps/rockbee-raft-node/tests/processes.rs`, `reports/process-parent-cancellation.md`, and the current `reports/three-process-recovery.log`.

Both Unix signal listeners are installed before `recovery_scenario` can spawn a child. The scenario is polled directly by `select!`, not detached into another task. When a signal wins, dropping that future drops `ProcessCluster`; its node vector is declared before the temporary directory, so each exact owned `Child` is killed if needed and waited before disk cleanup. Cleanup does not find processes by name, port or repository path. Cancellation deliberately fails the test after cleanup rather than reporting a successful recovery scenario.

The test-only ready-file barrier records the three actually spawned child PIDs and holds the scenario alive. Root's controlled direct-parent SIGTERM report records the reproduced failure (parent signal exit -15, three surviving owned children) and fixed result (parent exit 101, zero surviving children). Verifier read this evidence but did not rerun the external harness. The ordinary three-process recovery log separately shows one passing run in 0.95 seconds; that validates the normal fixture and node SIGTERM path, not every parent-cancellation phase.

Limits remain explicit: parent SIGKILL, abort, or other termination that bypasses destructors cannot run this cleanup. An external owner/harness must retain fallback cleanup for those cases. SIGINT shares the inspected handler path but has not separately received the documented injected signal. The controlled ready barrier does not empirically enumerate cancellation during every later recovery phase; ownership remains in the same scenario future throughout.

Advisor subsequently independently read the source and red/green evidence and concurred with spec and quality approval under these exact boundaries. No Judge disagreement. No production Task 4 snapshot code was reviewed here.

```text
test parent --handled signal--> drop scenario
                                 -> kill/wait 3 owned children -> remove temp data
parent SIGKILL cleanup: outside this guarantee
```
