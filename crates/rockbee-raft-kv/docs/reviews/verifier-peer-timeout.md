# Peer deadline classification review

Spec and quality verdict: APPROVE the narrow source change. Independently inspected frozen `rpc/network.rs` and `peer-timeout-{red,green}.log`; verifier ran no Cargo, test or soak.

```text
local peer Deadline -> OpenRaft Timeout(action, source, target, original hard TTL)
other errors -> existing policy
```

All three RPC implementations capture `RPCOption::hard_ttl()` before moving the option into the call and supply the correct AppendEntries/Vote/InstallSnapshot action. The shared mapper changes only Deadline to Timeout; non-Deadline errors remain Unreachable. Existing remote Failure-to-Network and append PayloadTooLarge behavior remain intact. No Raft timer, request timeout, public mutation certainty, replay, gate/lane ownership, logging payload or locking change was found.

The focused helper regression failed before correction and passes afterward. It checks all three action values, local/remote IDs, exact duration and a non-Deadline protocol error retaining Unreachable. These logs prove the helper regression, not a controlled real-socket timeout for all three methods; wrapper integration was inspected statically. Additional scenarios are deferred under the user's current instruction.

Advisor directly concurred. This removes an independently established unjustified Unreachable/backoff classification. It does not establish that all observed stalls took that branch or that the underlying native pause is fixed. Existing c8 `soak-60s-c8-timeout-classification.log` is the separate runtime gate; its outcome is pending at this source-review checkpoint.

## Existing c8 completion inspected

Scoped runtime verdict: APPROVE this regression checkpoint. `soak-60s-c8-timeout-classification.log` finishes with one passing test; `soak/stable-YFt5NP/manifest.txt` reports PASS after 60.008801750 measured seconds, 170,238 successful operations, 28,373 complete cycles across all four groups, zero errors/partial cycles, clean worker join and client/gateway/all-node shutdown. Each of the six operation rows has 28,373 successes and zero Unknown/read errors/wrong values/censoring.

The retry path was exercised, not merely present: `node-1.log:68–69` records group 2's first barrier QuorumNotEnough after 51,691 microseconds, then a successful fresh second barrier in 474 microseconds with about 4.89 seconds of original budget left. Per-attempt elapsed logging requested during review is now present. Peer deadline events and synchronous writes around 187–190 ms remain in the same log; this success does not prove native stalls removed. It is one warm debug c8 regression on one public connection, not the client/concurrency matrix, repeated-run qualification, maximum capacity, or physical power-loss evidence. Verifier inspected worker/root-run artifacts rather than running this experiment.
