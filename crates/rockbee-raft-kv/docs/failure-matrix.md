# Failure and acceptance matrix

Status: **all 60 cases below are Pending**. Test names and fixtures are proposed
acceptance contracts, not evidence that tests exist or pass. This matrix maps to
[implementation-plan.md](implementation-plan.md), Tasks 1–14. A case becomes
verified only with an exact command, code revision, assertions, and result linked
from [progress.md](progress.md). An injected failure is evidence only when the
actual public/storage outcome and recovered state are checked.

```text
Existing: Client -> Gateway -> Writer/Readers -> SlateDB -> object storage
Planned:  Client -> Gateway -> owning data-group leader <-> voting replicas
                                   |                         |
                                RocksDB                   RocksDB
                     metadata group -> routing and move jobs
```

## Gates and evidence classes

- **P0:** correctness gate; the affected path cannot be described as working
  before this passes. Process cases become required when that path exists.
- **P1:** operational/resource gate; required before completion of the prototype.
- **P2:** measured-report gate; required to support the stated report, without a
  throughput target or optimization requirement.
- **X:** separate real power-loss qualification; remains unvalidated in this
  local prototype and blocks hardware power-loss claims, not prototype completion.
- **D:** deterministic unit/adapter/controlled-scheduler test. Persistence cases
  reopen a unique real RocksDB directory; synthetic faults remain labeled.
- **P:** owned process test through actual TCP and executable boundaries, with
  unique directories and targeted proxy faults. No unrelated process/container
  is stopped, and no existing storage is modified.
- **S:** measured bounded soak, with a fixed seed, stated runtime/workload,
  counters and sampled RSS; not an assertion of a hard RSS ceiling.
- **H:** physical host/storage power-loss experiment, not executed here.

Unless a case states otherwise, process topology is one real gateway and three
node processes hosting four data groups plus metadata. Movement adds one owned
fourth node. Each scenario has a hard deadline and cleanup guard. A failed wait,
lost message, or timeout cannot count as an expected rejection without examining
the operation's outcome and any durable effects. No case requires new product
features, authentication, automatic balancing, or performance tuning.

## Protocol and admission boundaries

| ID | Tasks | Gate / class | Fault or input | Required outcome | Proving test |
|---|---|---|---|---|---|
| F01 | 1 | P0 / D | Opaque keys containing zero/non-UTF8 bytes; empty values; empty key; exact maximum and one-over key/value sizes | Valid bytes round-trip unchanged; Some(empty) differs from missing; empty key and oversize payload rejected before dispatch; limits use encoded/byte sizes correctly | `protocol::opaque_and_size_boundaries`, table of PUT/GET/DELETE boundaries |
| F02 | 1, 2 | P0 / D | Truncated header/body, length above 2 MiB, trailing encoded bytes, concatenated frames | Reject malformed frame without dispatch; stream codec consumes exactly one legal frame at a time | `protocol::strict_frame_boundaries` plus fragmented input fixture |
| F03 | 1, 2 | P0 / D | Small postcard body advertises enormous inner byte/string/collection length | Decoder errors without allocation proportional to hostile declared size; receive reservation precedes frame allocation | `protocol::hostile_inner_lengths` with allocation/reservation instrumentation |
| F04 | 1 | P0 / D | Zero node/request/cluster IDs, unsupported wire version, zero budget, invalid operation tag encoded directly | Deserialization/validation rejects bypasses of public constructors; no handler execution | `protocol::invalid_wire_identity_and_version` |
| F05 | 1, 6 | P0 / D | Fixed keys/hash seeds; zero or overflowing shard count; change node placement without changing shard config | Hash vectors stable; metadata group 0 never a data destination; invalid counts rejected; node-count change does not remap keys | `shard::fixed_vectors_and_invalid_counts` |
| F06 | 2, 12 | P0 / D | Count, byte, connection, and maximum-frame reservations saturated independently | Eager submission fails immediately as NotSent when local admission fails; no allocation or pending entry escapes the applicable reservation | `transport::each_admission_limit` with retained-buffer counters |
| F07 | 2, 6, 12 | P1 / D | Tiny GET requests each produce maximum-size values while output socket never drains | Response reservation precedes database value materialization and encoding; request plus encoded copies and egress remain within their own budgets | `server::reserve_response_before_get` using controlled materialization and stalled writer |
| F08 | 1, 5, 14 | P0 / D | Client/gateway dependency inspection; peer/admin request presented to public listener | Public packages do not compile/link engines; public service cannot dispatch internal message families | `protocol::public_message_family` and recorded Cargo metadata/tree assertion |

## Multiplexing, certainty, and deadlines

| ID | Tasks | Gate / class | Fault or input | Required outcome | Proving test |
|---|---|---|---|---|---|
| F09 | 2 | P0 / D | Submit PUT/DELETE but never poll handle; separately drop handle immediately | Writer receives eager submission; dropping observation does not cancel mutation; retained permits release only when owned work ends | `client_contract::eager_and_dropped_handle` |
| F10 | 2 | P0 / D | Out-of-order responses, wrong response kind, wrong ID, duplicate completed reply | Valid replies reach their request; invalid reply retires active generation and never completes another operation | `transport::correlation_and_kind` |
| F11 | 2 | P0 / D | Scheduler pauses at Queued -> Handed while deadline fires on each side | Expiry winner prevents any socket write and yields NotSent; handoff winner yields uncertainty if no final response; one terminal completion | `transport::handoff_expiry_race` with deterministic barriers |
| F12 | 2 | P0 / D | Socket accepts a prefix of a frame, then errors or exceeds deadline | Handed request uncertain; stream retired; no following frame appended to partial frame; remaining queued requests NotSent | `transport::partial_write_retires_generation` |
| F13 | 2, 6 | P0 / D | One handed request expires with another handed and one queued on same connection | Under v1 retirement policy, handed outcomes uncertain and queued outcome NotSent; no replay; late response cannot reach replacement generation | `transport::deadline_retirement_and_late_reply` |
| F14 | 2 | P0 / D | Response completion races stale timer; forced request counter exhaustion | Completed request's stale timer cannot kill healthy connection; exhausted counter never wraps/reuses ID in that generation | `transport::terminal_timer_and_id_exhaustion` |
| F15 | 2, 6 | P0 / D | Admission, connection, queueing, and encoding each consume parts of one deadline | Remaining budget shrinks at each hop; no new full timeout after retry/handoff; expired response is not delivered as timely success | `transport::one_end_to_end_budget` using controlled clock and hop observations |
| F16 | 2, 5, 12 | P1 / P | No quorum; repeated deadline/disconnect/handle-drop storms until limits fill | Submitted node proposals retain reservations after callers disappear; rejection remains bounded; Fatal closes group admission before permit recycling | `failures::orphaned_proposals_keep_admission` with internal proposal counts and public outcomes |

## Durable storage and fail-closed recovery

| ID | Tasks | Gate / class | Fault or input | Required outcome | Proving test |
|---|---|---|---|---|---|
| F17 | 3, 9 | P0 / D | Pause/fail vote and log persistence immediately before/after actual write completion | Vote returns only after persistence; LogFlushed success only after durable append; write ordering is FIFO within group; error never emits success callback | `storage::durable_vote_log_callbacks` with adapter-boundary trace |
| F18 | 3, 5, 9 | P0 / P | Persist grant to candidate A; crash/reopen; candidate B asks in same term | Same-term conflicting vote cannot be granted after restart; persisted vote and node identity survive | `node_cluster::no_double_vote_after_restart` |
| F19 | 3, 9 | P0 / D | Fail/cut bounded apply chunks containing PUT/DELETE, blank entry, and membership updates | Reopen yields a coherent committed prefix of complete chunks; each chunk's ordered effects/final applied identity/resulting membership publish atomically; no partial chunk is visible; whole iterable need not be all-or-none | `storage::atomic_apply_recovery` against real bounded RocksDB WriteBatches |
| F57 | 3, 5, 6, 8 | P0 / P | Hold durable-log callbacks below quorum, then separately release durable quorum while holding leader durable-apply completion on public client->gateway->nodes path | No public success before durable quorum AND durable leader application; releasing both permits eventual success and correct GET. Verify absence through controlled phase/completion events, not elapsed sleeps | `failures::public_ack_waits_for_both_durable_barriers` with independently controlled log/apply boundaries |
| F20 | 3, 4, 9 | P0 / D | Truncate conflicting suffix and purge durable prefix at index boundaries, including maximum group prefix | Persisted purge marker agrees with surviving contiguous logs; neighboring group's keys/logs untouched; recovery has required snapshot/state | `storage::truncate_purge_group_bounds` |
| F21 | 9, 13 | P0 / D | Inject append/vote/apply/snapshot publication write error, including simulated ENOSPC | Actual API reports failure/uncertainty appropriately; fatal storage failure stops admission; reopen never silently resets state | `storage_faults::write_failures_fail_closed`; synthetic ENOSPC is labeled and never fills user disk |
| F22 | 9, 13 | P0 / P | Storage directory locked by owned node; permission denied; corrupt application metadata or snapshot descriptor | Startup returns bounded contextual failure; evidence retained; no replacement empty bootstrap or user-data deletion | `storage_faults::startup_errors_preserve_evidence` with a fixture that confirms the intended OS error occurred |
| F23 | 3, 9 | P0 / D | Corrupt one group's serialized metadata, then exercise diagnostic/recovery/cleanup paths | Corruption is detected, not treated as missing data; no sibling prefix erased. Group-local recovery may isolate it; shared-engine failure may stop whole node | `storage_faults::corruption_does_not_cross_group_cleanup` with byte-for-byte sibling evidence |

## Snapshots, deletion, and ordering

| ID | Tasks | Gate / class | Fault or input | Required outcome | Proving test |
|---|---|---|---|---|---|
| F24 | 4 | P0 / D | Concurrent apply while snapshot builder captures group data and membership | Image represents one coherent RocksDB view and its matching applied/membership identity, including empty state; no cross-group records | `snapshots::consistent_view_under_apply` |
| F25 | 4, 9 | P0 / D | Truncated record, oversized field, checksum mismatch, duplicate/out-of-order keys, wrong cluster/group/format | Installation rejected before publication; active generation/data/applied/membership unchanged; input memory bounded | `snapshots::reject_invalid_images` table of distinct corruption classes |
| F26 | 4, 8, 9 | P0 / P | Owned process stops at image fsync, rename/directory sync, staged import, or metadata publication boundary | Reopen chooses complete old or new published state only; inactive files/generations never become active by filename inference | `snapshots::publication_crash_cuts` with observed phase markers; process-crash evidence only |
| F27 | 4, 5 | P0 / D | GET barrier completes while installation changes active generation | Generation/applied/value come from one read view covering barrier; no mixed generations or view below barrier. A pinned old view remains valid if it covers the barrier and is linearizable across overlapping install | `snapshots::install_racing_strong_get` with controlled barriers |
| F28 | 4, 12 | P0 / D | Old builder finishes after a newer snapshot install; old GET/transfer remains pinned during GC | Stale builder cannot regress published snapshot; active pointer remains newer; pinned reader/transfer stays valid; reclaim occurs after safety conditions | `reclamation::stale_builder_and_pinned_gc` |
| F29 | 4, 8, 9 | P0 / P | PUT, snapshot, DELETE in log tail, crash and catchup; also delete before snapshot and later recreate | Deleted value never resurrects; recreated value follows log order; recovered membership/applied state matches final history | `failures::delete_snapshot_tail_order` |
| F30 | 4, 12 | P1 / S | Snapshot state exceeds configured snapshot-memory budget; throttled transfer/import | Instrumented buffers/queues stay within streaming budget; no full-state buffer; checksum and final data verified; peak RSS reported separately | `resources::snapshot_larger_than_memory_budget` with explicit fixture size and chunk maxima |

## Bootstrap, authority, and gateway routing

| ID | Tasks | Gate / class | Fault or input | Required outcome | Proving test |
|---|---|---|---|---|---|
| F31 | 5, 13 | P0 / P | Pristine explicit bootstrap; repeated bootstrap; ordinary restart with existing logs | Only permitted initial bootstrap creates memberships; repeat cannot create second group; restart recovers existing votes/logs/config | `node_cluster::explicit_bootstrap_and_restart` |
| F32 | 5, 13 | P0 / P | Reopen with mismatched cluster/node/hash/seed/shard-count/format version; duplicate node identity/address in config | Refuse incompatible startup before serving; persist existing data unchanged; clear error identifies mismatched field without values | `config::persisted_identity_and_version_mismatch` parameterized owned startup fixture |
| F33 | 5 | P0 / P | Peer RPC has wrong cluster/group/target/sender identity or unsupported RPC version; oversized private frame | No dispatch to unintended group; bounded decode rejection; legitimate group remains correct; checks are not claimed as authentication | `node_cluster::peer_identity_and_frame_rejection` |
| F34 | 5, 8, 10 | P0 / P | Confirm current leader isolated from voter quorum, drain/account for earlier work, then invoke fresh requests over surviving client connection | Old leader cannot successfully acknowledge fresh mutation or return unauthorized strong GET; majority elects and serves; pre-cut completed barriers are not misclassified | `failures::reachable_minority_leader` |
| F35 | 5, 8, 10 | P0 / P | Kill leader immediately after observed PUT/DELETE success; query new leader through gateway | Successful mutation preserved or superseded by later legal mutation; success never relies on lost replica alone | `failures::success_survives_leader_loss` |
| F36 | 5, 8, 10 | P0 / P | Quorum loss followed by rejoin with divergent uncommitted tail and snapshot-lagging replica | No success without quorum; catchup restores coherent log/state; strong operations resume; retained Unknown operations handled by history checker | `failures::quorum_rejoin_and_tail_reconcile` |
| F37 | 6 | P0 / P | Gateway starts without metadata quorum; separately loses metadata after valid startup; stale map/hint supplied | Startup fails closed; known groups may continue under their own authority; no unverified new routes; refresh never regresses map revision | `end_to_end::metadata_startup_outage_and_stale_routes` |
| F38 | 6, 8, 10 | P0 / P | Drop committed mutation reply at node->gateway and gateway->client separately; contrast explicit pre-submission routing rejection | Response-loss case yields Unknown with exactly one Raft proposal and no automatic replay after uncertain attempt. Proven rejection may cause multiple backend transmissions but no rejected proposal effect | `failures::response_loss_and_safe_rejection` with separate proxy-transmission and node-proposal counts |

## Placement and controller reconciliation

| ID | Tasks | Gate / class | Fault or input | Required outcome | Proving test |
|---|---|---|---|---|---|
| F39 | 7, 8, 10 | P0 / P | Move ABC -> ABD during mixed traffic across all groups | D joins learner and catches up; supported membership transition commits final voters; route publication follows committed membership; unrelated groups unchanged | `placement::move_with_live_traffic` with membership/route timeline |
| F40 | 7, 8, 11 | P0 / P | Controller loses learner-add or final membership-change response, then restarts | Reconcile actual committed membership and persisted job; no duplicate independent group or blindly repeated conflicting transition | `placement::lost_response_reconcile` with phase-specific proxy cuts |
| F41 | 7, 8, 11 | P0 / P | Controller/leader changes while membership is joint, before final uniform membership | Resume from observed joint state through valid consensus; do not treat joint configuration as finished placement or discard required replicas | `placement::restart_in_joint_membership` |
| F42 | 7, 11 | P0 / P | Duplicate same job; different concurrent move for same group; old controller resumes after replacement | Same job reconciles idempotently; conflicting transitions fenced/rejected; stale controller cannot regress committed placement/job progress | `placement::duplicate_conflicting_and_stale_controller` |
| F43 | 7, 9, 11 | P0 / P | Reconnect removed C before/after local cleanup; interrupt cleanup midway | C cannot acknowledge fresh mutations or unauthorized strong reads; deletion requires durable final-removal proof; no other group data removed | `placement::removed_replica_and_cleanup_restart` |
| F59 | 7, 8 | P0 / P | D has caught up as learner but is not promoted; partition A+D from original voters B+C, then invoke fresh mutation on A | Learner receipt cannot substitute for original voter quorum; A+D cannot acknowledge mutation; legitimate B+C majority may proceed | `placement::learner_does_not_count_as_voter` |
| F60 | 7, 8 | P0 / P | Pause transition in committed joint ABC/ABD configuration; partition B+C from A+D, then invoke fresh mutations on both sides | Neither old-only nor new-only majority has both required majorities; neither side can acknowledge until valid quorum restored | `placement::joint_requires_both_majorities` |

## History checking and bounded disruption

| ID | Tasks | Gate / class | Fault or input | Required outcome | Proving test |
|---|---|---|---|---|---|
| F44 | 10 | P0 / D | Hand-proven legal overlaps and illegal stale GET, lost acknowledged write, resurrected DELETE histories | Checker accepts legal histories and rejects each illegal counterexample; observed completion order constrains real-time order | `history::checker_known_counterexamples` |
| F45 | 10 | P0 / D | Unknown followed by missing GET then later observed value; Rejected/NotSent operations; duplicate user attempts; search-budget exhaustion | Unknown notification is not execution completion: proposal may apply later; proven rejection has no effect; attempts remain distinct; exhausted search reports inconclusive, never pass | `history::certainty_and_inconclusive_results` |
| F46 | 8, 10 | P0 / P | Record per-key public histories during failover, response loss, snapshot catchup and movement | Each bounded history has a legal per-key linearization or fails with reproducible evidence; no cross-key transaction claim | `history::public_failure_histories` with operation invocation/completion IDs |
| F47 | 11 | P1 / S | Fixed-seed short mixed-operation schedule with targeted restart, partition, rejoin and move | All scheduled faults recorded; bounded history/state checks pass after reopen; seed reproduces fault schedule; independent verifier supplies one new seed | `soak::seeded_disruption` with seed, actual event times, topology and artifacts |

## Resource isolation, reclamation, and shutdown

| ID | Tasks | Gate / class | Fault or input | Required outcome | Proving test |
|---|---|---|---|---|---|
| F48 | 5, 6, 12 | P1 / P | One group reaches proposal allowance or stalls; other groups retain quorum and receive traffic | Healthy groups make bounded-time progress; one hot/unavailable group cannot consume all group admissions; no application-wide data-write lock | `resources::unavailable_group_isolation` with per-group completions and queues |
| F49 | 5, 12 | P1 / S | Slow snapshot receiver plus slow follower under client saturation | Peer/control capacity remains available; encoded replication/chunk inflight counts bounded; reported lag recovers when fault removed | `resources::bulk_transfer_control_headroom` with bounded proxy queues |
| F50 | 12 | P1 / S | Repeated near-limit payload, connection/deadline/drop storms across fixed-duration phases, then idle recovery | Hard counters never exceed configured limits; permits/timers/pending maps reclaim after work resolves; RSS peak/plateau measured without declaring hard cap | `resources::saturation_then_reclamation` with phase-by-phase counters |
| F51 | 12 | P1 / S | Repeated snapshot/install/GC cycles; rerun with more groups under same cache/write-buffer configuration | Obsolete generation/image retention follows pin/recovery policy; shared budget objects are actually shared; retained logs/disk growth measured explicitly | `resources::generation_cycles_and_shared_budgets` |
| F52 | 8, 13 | P1 / P | SIGINT/SIGTERM repeated while queued, handed and committed-response-pending operations exist | Pending observers resolve with justified certainty; shutdown joins owned workers or reports bounded failure; restart preserves successes; unrelated processes remain alive | `operations::shutdown_storm_under_load` with unrelated sentinel process |
| F53 | 8, 11, 13 | P1 / P | Fixture startup fails halfway; assertion/panic/deadline occurs during active proxy/child work | Cleanup handles were installed before launch; no child/proxy/task orphan; cleanup touches only fixture-owned paths/processes | `support::cleanup_on_partial_start_and_failure` |
| F54 | 13 | P1 / P | Start with bad config/storage, then healthy process lacking quorum; inspect readiness/liveness/diagnostics | Readiness waits for required checks; liveness does not imply quorum authority; diagnostic role/hints/queues/membership/job state explain condition without exposing values | `operations::readiness_and_failure_diagnostics` |
| F55 | 13, 14 | P1 / P | Run documented bootstrap, client CRUD, restart, move, inspect and shutdown from fresh unique directories | Commands execute real full path; configs/addresses/IDs match; result matches documentation and all owned processes exit | `operations::documented_fresh_start_walkthrough` independently run by verifier |

## Reporting and explicit qualification limits

| ID | Tasks | Gate / class | Fault or input | Required outcome | Proving test |
|---|---|---|---|---|---|
| F56 | 14 | P2 / S | Bounded warm/cold, uniform/hot-key, mixed operation and movement workloads under recorded payloads/concurrency | Report end-to-end latency percentiles, successful throughput, errors/Unknown, CPU/RSS, lag/queue observations; define cold state; no comparison beyond measured conditions | `report::local_baseline_workloads` with commands and raw artifacts |
| F58 | 3, 4, 9, 14 | X / H | Actual independent-host power interruption at acknowledged mutation, file publication, and recovery boundaries | Durable successes and coherent recovery survive device-specific persistence behavior; results require real hardware/filesystem evidence | **Not run / unvalidated.** Dedicated future power-cut qualification; SIGKILL, mocked fsync, and local three-process tests do not discharge this case |

## Completion and evidence rules

Every row starts **Pending**, including deterministic cases similar to any
earlier tests. Link the actual proving test only after its assertions are
reviewed. A row may parameterize tightly related boundary inputs, but distinct
durability phases must each be exercised and reported; one successful phase
cannot cover the whole row.

History fixtures use distinct write values and invocation/definitive-completion
partial order, never final-state expectations based solely on response order of
overlapping operations. An Unknown notification does not close the possible
execution interval: `PUT(v) -> Unknown -> GET(None) -> GET(v)` can be legal when
the submitted proposal applies between the reads. Treat that proposal as
incomplete/optional through the observation horizon unless actual server
completion evidence narrows it. Repeated identical PUT/DELETE operations hide
replay in final state; F38 requires submission/proposal instrumentation as well
as public history. Learner/minority/joint-quorum tests confirm partition and
membership phase before fresh invocations; earlier in-flight work is accounted
for separately.

Default suites use short bounded D/P cases. Longer S runs are explicit commands,
not background work or CI benchmarks. Record load configuration and test-host
limits; use internal count/byte assertions for hard bounds and measurements for
RSS/CPU/disk. A reproducible soak seed fixes the fault schedule, not the entire
operating system's thread interleaving; capture actual event histories too.

P0 and P1 failures block completion of their affected milestones. P2 gates
specific report claims. F58 remains explicitly unvalidated unless separately
authorized and physically exercised. Process-kill evidence and synthetic I/O
errors must never be relabeled as real power-loss or real ENOSPC validation.

Task 14 also requires final dependency/scope/evidence review and complete affected
path verification after final fixes: parent workspace unchanged, private fault
copies with independent target directories, recorded fmt/check/Clippy/unit/process
commands, and no Pending case represented as passed. This administrative gate is
separate from the 60 behavioral/qualification cases rather than duplicating them.
