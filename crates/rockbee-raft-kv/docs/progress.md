# SDD ledger — plan: docs/implementation-plan.md

## Current state

- Current investigation: the c8 soak also failed after peer connection reuse
  (`stable-b6lFue`, 38.876896 seconds, four quorum-read errors, zero Unknowns).
  Reuse alone did not fix the incident. The latest source adds `log` events and
  application-owned `env_logger`, as requested; no tracing/OTel integration.
  Advisor, verifier and Judge approved a narrow read-availability change:
  retry only a typed `QuorumNotEnough` with a fresh barrier and the original
  request deadline. Disk pauses and Deadline-to-Unreachable classification are
  separate issues. The logged c8 regression (`stable-Sc1F8R`) recovered a
  qualifying read on its second fresh barrier, but still failed at 57.019092
  seconds with six Unknown mutations after leader changes. No wrong values or
  read errors were observed. The focused adapter regression now proves the
  incorrect Deadline-to-Unreachable mapping. After its narrow correction,
  `stable-YFt5NP` passed 60.008802 seconds with 170,238 successful operations,
  zero errors/Unknowns/wrong values and clean shutdown. It exercised a failed
  quorum probe followed by a successful fresh barrier within the original
  budget. Native write pauses remain visible. Full workspace validation passed
  with diagnostics enabled: 149 tests, zero failures, one manual soak ignored
  (that soak passed separately as above). Strict Clippy and format checks also
  passed. After local code checkpoint `7d2ff9f`, the 1/2/3-client matrix also
  passed sequential 60-second runs with zero errors and clean shutdown:
  73,344 / 118,320 / 129,186 successful operations. See
  `docs/tasks/client-matrix-report.md` and
  `docs/reviews/judge-read-budget.md`.

- Location: `crates/rockbee-raft-kv/`; independent workspace on
  `feat/raft-kv-multiraft` (new local branch requested by the user).
- Existing application unchanged. The user authorized a local checkpoint commit
  followed by sequential 1/2/3-client tests with one gateway and three nodes.
  No remote push was requested.
- The first public path is implemented and verified locally: public client to
  gateway to three real nodes, across metadata group 0 and four data groups,
  backed by RocksDB/OpenRaft. The node binary has a real CLI and `--help` path.
- The verified first path covers PUT/GET/DELETE, leader stop with quorum reads,
  reopen/catch-up, public-client routing, and a three-independent-OS-process
  SIGKILL/reopen preservation run. Durable streamed snapshots are now verified
  in storage scope. It does not yet prove physical power-loss behavior, replica
  movement, or production capacity.
- User requested continuous work with an advisor and independent verifier.
- Advisor: `/root/raft_advisor`; verifier: `/root/raft_verifier`.
  They exchange findings directly; root resolves decisions and tracks evidence.
- Judge: `/root/raft_judge`. At the user's request, material problems and
  design questions receive an independent Judge review even when the advisor,
  tester and verifier agree. Source/test evidence takes precedence over voting.

```text
Existing: Client -> Gateway -> Writer/Readers -> SlateDB -> Object storage
New:      RockBeeRaftClient -> Gateway -> 3 nodes / Raft groups 0..4 -> RocksDB
```

## Execution rulings

- Ruling: keep all plans, reports and review artifacts inside this module.
  The earlier main/no-commit instruction was superseded by the user's explicit
  request for a new branch and a local commit before the next test matrix.
- Ruling: initial data group IDs are 1..=4 and metadata is 0 — separate identities
  prevent data/control collision — changing this after bootstrap requires migration.
- Ruling: group-isolated state shares one RocksDB per process — shared resource
  budgets avoid per-shard cache multiplication — storage layout must prove isolation.
- Ruling: empty keys are rejected while empty values remain valid — explicit
  first-version API restriction simplifies invalid-request handling — applications
  needing an empty key must encode one or request a compatible later extension.
- Ruling: NotSent is a locally synthesized outcome and invalid on server wire —
  a received request can only be explicitly Rejected before Raft submission —
  clients must conservatively classify invalid remote outcomes as Unknown.
- Ruling: v1 retires the entire connection on a post-handoff timeout — bounded
  late-reply handling without tombstones — other handed requests may also become
  Unknown; future connections never replay them. Advisor and verifier agreed.
- Ruling: transport is a generic Link over concrete message families — public KV
  and internal metadata/routing remain distinct — a small trait is required but
  no engine types enter the public protocol and connection logic is shared.
- Ruling: Unknown notification does not end the underlying write in history
  checks — accepted writes can apply later — tests must retain incomplete writes
  until quiescence and count submissions separately from final values.
- Ruling: protocol codec supports trusted concrete message types, not arbitrary
  unbounded user Deserialize implementations — avoid a 200-line forwarding layer
  for unused collections — future collection-bearing wire types need their own
  adversarial limits. Tiny frames use bounded exact-size allocation.

## Preflight interface scan

| Tasks | Producer -> consumer | Check/result |
| --- | --- | --- |
| 1 -> 2, 5, 6 | IDs, framing, sharding, outcomes -> transports/services | Engine-free; metadata/peer envelopes added only by owning tasks |
| 2 -> 6 | Link and client -> gateway public/backend connections | One deadline and explicit handoff boundary; no mutation replay |
| 3 -> 4, 5 | group storage + Raft types -> snapshots/node | Storage suite spans 3/4; Task 3 cannot claim snapshot completeness |
| 4 -> 5, 7 | durable snapshot transfer/install -> catchup/moves | No full-state Vec; generation switch atomic |
| 5 -> 6, 7 | node RPC/authority -> gateway/controller | Destination checks identity/ownership; hints confer no read authority |
| 6 -> 7 | metadata map/jobs -> placement publication | Revision monotonic; actual committed membership authoritative |
| 2, 5, 6, 7 -> 8 | network/runtime/placement -> failure harness | Real path plus owned faults; no fixture-only success claim |
| 3, 4, 5 -> 9 | actual persistence boundaries -> failure injection | Fail closed; no fake success or destructive disk fill |
| 6, 8 -> 10 | public invocation/outcome traces -> history checker | Unknown writes optional; Rejected/NotSent no effect |
| 7, 8, 10 -> 11 | move/fault harness + checker -> replayable soak | Seed and cleanup are required; removed C checked |
| 2, 4, 6, 8 -> 12 | counters/snapshot lifecycle -> resource tests | Hard internal bounds distinct from measured RSS |
| 5, 6, 7 -> 13 | startup/control state -> diagnostics | Readiness != liveness != quorum authority |
| 8..13 -> 14 | verified semantics -> load/report | Identical durability/consistency; warm vs cold explicit |
| 1 | parser tests vs messages/limits | Boundary and malformed-input tests specified |
| 2 | eager/bounds tests vs API | Handle drop != cancellation; pending cleanup included |
| 3 | persistence tests vs sync atomic writes | Process restart != power-loss validation |
| 4 | snapshot tests vs file/generation publish | Partial install must preserve old state |
| 5 | cluster tests vs bootstrap/peer/node | Multiple independent processes required |
| 6 | E2E/admission tests vs gateway | Includes read/write/delete across every group |
| 7 | interruption tests vs durable controller | Lost responses reconciled from committed membership |
| 8 | failure tests vs ownership | Cleanup signals installed before launch |
| 9 | fault tests vs persistence | No silent empty-store replacement |
| 10 | checker vs concurrent histories | Checker validated with known counterexamples |
| 11 | soak vs reproducibility | Bounded deadline, seed, owned cleanup |
| 12 | resource checks vs measurements | No RSS cap inferred from cache budget |
| 13 | command tests vs operator promises | Fresh directory examples and failure diagnostics |
| 14 | reports vs claims | No production-capacity extrapolation |

## Verification evidence

The following first-path evidence is current. Earlier scaffold/dependency
milestones are retained below as history and are not a statement of current
runtime capability.

- `reports/public-path-and-cli.log`: five passed checks, including three real
  runtime cases with public-client routing across four data groups and two CLI
  argument/help cases. `target/debug/rockbee-raft-node --help` exits zero.
- `reports/runtime-peer-final.log`: 21 passed checks (14 node-library, two CLI,
  two peer and three real runtime cases). `reports/runtime-workspace-clippy.log`
  records strict workspace all-target Clippy success.
- `reports/runtime-first.log`: first owned runtime check for PUT/GET, leader
  stop, quorum reads/DELETE, reopen, and catch-up.
- `reports/three-process-recovery.log`: public PUT, SIGKILL of all three owned
  node processes, reopen of the same disks, preserved GET, DELETE/None, then
  clean SIGTERM exit. This is separate-process crash/reopen evidence, not a
  physical power-loss guarantee.
- `reports/process-parent-cancellation.md` and its bilateral review reproduce
  parent SIGTERM leaving three fixture children, then verify the fix: handled
  parent signal exits intentionally with 101 after owned-child cleanup, leaving
  zero children. Parent SIGKILL remains outside destructor-based cleanup.
- Task 4 final evidence: 27 node-library plus 34 storage integration tests,
  followed by eight CLI/peer/runtime/process tests, for **69 passing tests**;
  the full unmodified upstream storage suite, strict Clippy and rustfmt passed.
  See `docs/tasks/task-4-report.md` and `docs/reviews/verifier-task-4.md`.

Historical bootstrap evidence:

- Scaffold checks before relocation: fmt, check, strict Clippy and build passed;
  both placeholder applications intentionally exited 1 at that earlier stage.
- 2026-10-19 verifier rechecked metadata after relocation: parent 12 packages,
  nested 4 packages with independent target directory. No engines added yet.
- Implementation plan drafted; advisor and verifier approved Task 1 contracts.
  Reports: `docs/reviews/{advisor,verifier}-initial.md`. Their requested serde,
  inner-length, vote-restart, retained-proposal, snapshot-publication and coherent
  GET checks were incorporated into the plan/Task 1 brief.

## Task status

Task 1: complete (local uncommitted; spec and quality approved by verifier).
Evidence: 9 implementer tests, workspace tests, fmt and strict protocol Clippy;
2 additional verifier adversarial tests passed in a separate target directory.
Reports: `docs/tasks/task-1-report.md`, `docs/reviews/verifier-task-1.md`.
Task 1b: complete (local uncommitted; spec and quality approved by verifier).
Explicit IDs replace the macro;
message responsibilities split and validation reads as named steps. 15 tests and
1 rustdoc pass, with four wire fixtures passing both before and after refactor;
strict Clippy and format checks passed. Report: `docs/tasks/task-1b-report.md`.
Review: `docs/reviews/verifier-task-1b.md`; source comparison preserves wire
declarations, serde, validation precedence, borrowing and codec allocation work.
Task 2: complete; independent verifier approved the bounded transport and eager
client contract. Task 3: complete for basic durable storage; 34 focused storage
tests passed and verifier approved its bounded, synchronous persistence scope.

Task 5 peer RPC: complete for its bounded private-RPC scope. Advisor/verifier
final approval follows leader-ID preservation, phase deadlines, admission
isolation, and preflight split tests. Task 6a typed server/gateway: complete for
its scope. Advisor/verifier final approval covers partial frame preservation,
response-write error handling, read retry, `Unknown`/no replay and later fresh
reconnect, and proven-`NotSent` failover. These approvals do not include
snapshots, membership movement, physical power loss, or resource soak.

Root owns the verified first public three-node/four-data-group runtime path and
the separate-process crash/reopen fixture. Task 4: complete for durable streamed
snapshot storage; bilateral final approval follows the membership-coherence
correction and 69 passing tests. It remains storage scope only: physical power
loss and learner snapshot catch-up are not established. Task 7a: complete for
its engine-free restricted control DTO boundary; eight tests and strict Clippy
pass, and its final reviewer approval covers `CompletedPending` and same-node
regressions. The metadata implementation and real new-node movement are deferred
by current priority, not canceled. The approved scope remains one immutable
ABC→ABD target per group, not general repeated epochs.

The active first priority is a stable normal three-node soak: one client, one
in-process gateway, three CLI node processes, and 1 KiB values, followed by the
same topology with two and three independent public client TCP connections. The
separate one-client/eight-worker run remains the multiplexing regression. The
test-only matrix support is ready and the 1/2/3-client cases passed; exact local
artifacts and limitations are in `docs/tasks/client-matrix-report.md`.

`hdrhistogram = 7.6.0` with default features disabled is a node test-only
dependency and replaces the bespoke latency histogram. The earlier c1 run is a
warm closed-loop debug baseline of 1,215.795 operations/second with one worker;
it is neither maximum capacity nor evidence for current peer-control edits.
Known costs requiring measurement include private peer connection lifecycle,
synchronous durable stages, and quorum-authorized GETs. Gateway currently has
three in-process counters only; an HTTP exporter is proposed but not implemented.

Bounded reusable private control TCP connections are implemented after c8
diagnostics recorded fresh-connect deadline pressure. They alone did not fix c8;
the latest passing run also includes the read-budget and Timeout-classification
corrections described above. The
previous server writer-exit fix and 143-test workspace result are baseline
evidence for their old source revision; they do not verify the active peer
changes. A focused red/green peer checkpoint, an updated full workspace result,
and all matrix artifacts require the coordinator/advisor/verifier/Judge packet
defined in `docs/reviews/coordinator-test-audit.md`.

Remaining priorities are uncertain mutation outcomes, resource ownership/leaks,
slow-client blocking, restart durability, and measured performance. Tasks 8–14,
general movement, fault history, resource/soak work, and final report remain
pending. The 60-case matrix is an acceptance backlog, not passing proof.

The user's additional background TTL plan is complete and bilaterally approved
in `docs/tasks/ttl-plan.md` and the advisor/verifier TTL review documents. It
specifies committed conditional expiration, explicit clock assumptions,
canonical non-expiring metadata, format migration, bounded cleanup and test
gates. TTL implementation remains future work; this is a plan approval.

User-requested FNV library migration is complete: `fnv = 1.0.7` replaces the
handwritten loop, with unchanged seed initialization, raw-byte hashing and
HASH_VERSION 1 mapping. Independent fixed vectors pass before and after;
`reports/workspace-after-fnv-library.log` records the full workspace check.

The requested multiplexed eight-worker run failed on five earlier revisions. The first
failed at 50.993 seconds with three Unknown mutations; the first bounded
diagnostic failed at 28.002 seconds with quorum-read errors and Unknowns; the
second diagnostic failed at 44.761 seconds with a quorum-read failure. See the
first three c8 artifacts and exact limits in `docs/reviews/coordinator-test-audit.md`.
No timing, durability, certainty, or replay default changed to conceal those
failures. The additional failures and first passing c8 run are recorded above
and in `docs/reviews/coordinator-logging-c8.md`; one pass is not long-soak or
maximum-capacity evidence.

Client stats/metrics are now explicitly requested. Advisor/verifier are
designing terminal accounting independent of handle polling, bounded operation
labels, separate admission/NotSent/Rejected/Unknown outcomes and an
application-owned metrics exporter. No client metrics implementation or
exporter is claimed yet.

## Latest scope steering

User briefly requested readability everywhere, then explicitly narrowed it to
the Raft project first. Current scope is ONLY `crates/rockbee-raft-kv`; existing
Rockbee remains unchanged. The legacy audit was stopped before any edits.
No global search/replace or blanket formatting churn. Named functions, cohesive
files and meaningful traits are preferred; avoid hidden macro-generated public
APIs and needless abstraction. Local commit is now authorized; no push. Raft
objective remains active.

The first end-to-end vertical slice is now complete and has evidence listed in
the current verification section. The delivery priority is its next correctness
gates: snapshot transfer/recovery, separate-process crash/reopen, and the
remaining failure and movement cases. The 60-case matrix remains the full
acceptance backlog; passing a vertical-slice test does not complete it.

User additionally requires Java-oriented protocol explanations, meaningful unit
tests, and active advisor/verifier participation. Root added the protocol README
with a module map, Rust/Java comparisons, macro glossary, request lifecycle and
test map. Task 2 owner documents the public source; advisor completed a focused
readability/coverage review and verifier is independently reviewing Task 2.
Macros remain where useful, with control-flow/cancellation decisions explained.

Task 2 complete: independent verifier spec/quality APPROVED. Implementer passes
19 transport +15 protocol contract/adversarial/golden +4 TCP client +1 rustdoc;
verifier adds three passing lifecycle cases (clone/close, lazy GET, cancelled
close/reclose). Strict scoped Clippy/fmt and strict rustdoc pass. Reports are
`docs/tasks/task-2-report.md` and `docs/reviews/verifier-task-2.md`.
Historical early red evidence is retained in `reports/backend-red.log`.
Directory/bootstrap, backend routing, and the root runtime are now implemented
for the first public path. Static bootstrap configuration is group-zero state;
it is not Task 7 membership fencing.

## Current coordinated handoff

```text
RockBeeRaftClient
        |
        v
Gateway public server  -- bounded Backend Link --> Node backend handler
                                                    |
                                                    v
                         peer RPC <--> OpenRaft group 0 + groups 1..4
                                                    |
                                                    v
                                                RocksDB
```

The diagram is the verified first working vertical slice. It has public-client,
gateway, three-real-node/four-data-group runtime evidence and a separate-process
crash/reopen recovery run. Durable file snapshots are storage-verified. Learner
snapshot catch-up, fourth-node movement, physical power-loss behavior, and the
broader failure matrix remain separate work.

### Ownership and Cargo coordination — 2026-10-19

| Owner | Active boundary | Required handoff before integration |
| --- | --- | --- |
| Root | backend/directory protocol; node config, cluster and runtime | Owns the verified real three-node path and separate-process crash/reopen tests. |
| Peer worker | `apps/rockbee-raft-node/src/rpc/**`, peer tests | Task 5 approved; no active peer implementation work. |
| Gateway worker | protocol `transport/server/**`; gateway package/tests | Task 6a approved; only regression maintenance if new integration evidence requires it. |
| Storage worker | Storage diagnostics and stable normal soak fixture | Source checkpoint returned to root; no concurrent native Cargo. |
| Advisor | Architecture and failure-mode review | Challenge contracts and handoff assumptions; advice alone is not correctness evidence. |
| Verifier | Independent targeted review | Inspect final diffs and run adversarial/behavior tests; approval is evidence only for the reviewed scope. |
| Judge | Independent decision review | Examine material problems/questions even when other reviewers agree; require discriminating evidence for open hypotheses. |

Advisor consultation is mandatory before implementation of every decision that
changes storage/durability, consistency, snapshots, Raft replication,
membership, routing, or bootstrap authority. The advisor is the designated
distributed database and Raft reviewer for this project; reviews must distinguish
OpenRaft source facts, publicly published ZippyDB/Meta material, and engineering
inference. The independent verifier remains a separate gate. Neither role is a
substitute for executable evidence from the affected path.

For every task, the advisor and verifier discuss both the planned
contract/acceptance tests before code and the implementation/evidence after
code. Their discussion artifact records agreement, open questions, and the
sources and tests behind each conclusion. Agreement is not proof of behavior.
Root also sends material problems and design questions to an independent Judge,
even when the reviewers agree, as explicitly requested by the user. The packet
contains proposals, objections, sources, tests and the unresolved questions.
The Judge may require a focused test. An empirical counterexample cannot be
overruled by a vote; any point without evidence remains open. Review slots are
released after finite reports so implementation and testing can continue.

### Bootstrap decision

The first initializer may not be the eventual group-0 leader. Root's proposed
internal operation is `BootstrapDirectory`: it contains no caller-supplied
directory payload; a receiving node uses only its already validated configured
initial directory. It is allowed only before Task 7 begins directory updates.
An uncertain response must be reconciled by a real linearizable directory read,
never by blindly replaying the mutation. Advisor/verifier reviewed the immutable
bootstrap phase; the root implementation has two focused passing bootstrap
checks. It remains valid only before mutable Task 7 directory updates, which
need an apply-time fence or a proven executor-retirement protocol.

### Latest implementation checkpoints — 2026-10-19

- Root's first owned runtime milestone passed and is recorded in
  `reports/runtime-first.log`: PUT/GET, leader stop, quorum reads and DELETE,
  then reopen/catch-up. It is a first three-node/four-data-group runtime check,
  later extended by `reports/public-path-and-cli.log` to the public
  Client-to-Gateway path.
- Gateway Task 6a and Peer Task 5 are final-approved in their narrow scopes by
  direct advisor/verifier discussion. Their final reviews remain explicit about
  excluded snapshot, movement, power-loss and soak claims.
- Task 4 durable snapshots are final-approved for the storage scope after the
  membership-coherence correction. Its final 69-test evidence includes the full
  upstream suite, strict Clippy and rustfmt; learner catch-up is still separate.
- Task 7a restricted control DTOs are final-approved with eight tests and strict
  Clippy. Metadata application and live ABC→ABD movement are deliberately
  deferred, so this is not movement success or general epoch support.
- The normal c1 soak passed on an earlier source revision. Five c8 attempts
  failed before the read-budget and timeout-classification corrections; the
  latest c8 run passed. The 1/2/3-client matrix also passed after the local code
  checkpoint, with the same gateway/node topology.
  The coordinator audit labels older green logs as baselines.
- The parent-cancellation fixture is bilateral-approved for handled SIGTERM/
  SIGINT cleanup. It intentionally reports failure after cleanup; parent SIGKILL
  and external cleanup remain outside its guarantee. No Judge disagreement is
  active at this checkpoint.

Native node Cargo is sequential while shared sources move. The owner of the
current slot announces a stable source window before another native check. Every
node build uses the active macOS SDK libc++ include path, two Cargo jobs,
`--locked`, and `--offline`:

```sh
CXXFLAGS='-isystem /Library/Developer/CommandLineTools/SDKs/MacOSX.sdk/usr/include/c++/v1' \
CARGO_BUILD_JOBS=2 cargo <command> -p rockbee-raft-node --locked --offline
```

Gateway/protocol-only checks use their own target directory and do not build the
node. A gateway protocol export can nevertheless break node compilation, so its
worker must announce a source-stable checkpoint before root starts the next node
build. Root owns the authorized local checkpoint commit; no push is requested.
