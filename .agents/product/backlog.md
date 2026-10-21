# Rockbee product backlog

Initial triage: 2026-10-21. Sources inspected at branch
`feat/raft-kv-multiraft`, HEAD `01dc69d`; these entries are documentation-backed
proposals, not fresh runtime verification or permission to implement them.

This is the single outcome-level product index. Reuse linked engineering plans
for implementation tasks and failure scenarios. A proposed item may be outdated;
its first step is to establish current scope and evidence.

States: `proposed`, `accepted`, `in progress`, `blocked`, `done`, `deferred`,
`superseded`. All initial items below are **proposed**, without an implementation
owner. Order expresses a triage recommendation, not an accepted delivery schedule.
Record owner/dependencies on assignment and evidence when closing an item.

| ID | Outcome / track | Status and priority rationale | Evidence and confidence | Next decision or validation |
|---|---|---|---|---|
| RB-P01 | Establish a current capability/evidence checkpoint. Both tracks, Raft first. | Proposed; prerequisite for trustworthy planning. | README and planning records contain dated checkpoints. Historical documentation, not a reported failing build; current completion requires relevant evidence. | Reconcile the relevant current source, command/configuration and existing artifacts. Identify stale claims without rerunning expensive suites merely to change a count. |
| RB-P02 | Demonstrate the required durability and consistency under failure. Raft. | Proposed; correctness evidence should precede new reliability promises. | [Raft limitations](../../crates/rockbee-raft-kv/README.md#current-limitations-and-open-work) describe remaining failure validation. Consult detailed local planning notes when available; exact outstanding cases need revalidation. | Identify which cases remain after later commits; choose the smallest consequential uncovered public-path failure with explicit acceptance evidence. |
| RB-P03 | Define and validate customer-visible overload and unavailable-group behavior. Raft. | Proposed; affects predictability under partial failure. | [Raft limitations](../../crates/rockbee-raft-kv/README.md#current-limitations-and-open-work) describe shared admission and absent group fairness. Documentation evidence only. | Inspect current admission paths and define the desired outcome under lost quorum alongside healthy traffic; measure before selecting a control scheme. |
| RB-P04 | Make startup, diagnosis, recovery, and deployment requirements understandable. Raft. | Proposed; operational usefulness depends on the intended environment. | [Raft limitations](../../crates/rockbee-raft-kv/README.md#current-limitations-and-open-work) identify incomplete operational tooling and security. Documentation evidence; current exact coverage unverified here. | Inventory present diagnostics and walkthroughs; define deployment/threat model and prioritize a bounded operator journey. Assess backup/restore requirements separately from replication. |
| RB-P05 | Complete safe repeated group movement if it is required for the next milestone. Raft. | Proposed; dependent on choosing a cluster-management outcome. | [Raft limitations](../../crates/rockbee-raft-kv/README.md#current-limitations-and-open-work) describe a verified first move and unfinished subsequent coordinator flow. Documentation evidence only. | Revalidate successor implementation and choose acceptance for repeated moves. Keep automatic placement/splitting/rebalancing as separate decisions. |
| RB-P06 | Establish representative latency, capacity, and operating cost evidence. Raft initially. | Proposed; needed before production sizing or scale promises. | [Raft README](../../crates/rockbee-raft-kv/README.md#recorded-verification-checkpoint) bounds historical local warm-load evidence and lists unverified 500 GB/maximum capacity. | Agree workload, data/cache state, hardware/network, latency/error objectives, and failure headroom. Then authorize and schedule comparable measurements. |

## Customer and product discovery

The [brief's hypotheses](brief.md#customer-and-use-case-hypotheses) are open
questions, not accepted features. No external interviews, validated customer
demand, pricing study, or production adoption evidence were inspected for this
initial ledger. Update that status only with attributable evidence.

## Stewardship

- Search this index and the relevant technical plans before adding items.
  Preserve IDs and link duplicates/superseding work rather than silently deleting.
- Capture owner-requested ideas and material sourced gaps within the existing
  product scope as proposed. Do not turn every hypothetical discussion into work.
- Preserve accepted priorities. State proposed priority changes and their reasons;
  ask only when a material product tradeoff needs the owner's decision.
- Keep actual assignments, blockers, and completion evidence current when they
  change. Backlog maintenance does not start implementation or background workloads.
