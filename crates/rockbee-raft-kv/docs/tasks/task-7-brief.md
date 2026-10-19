# Task 7 first milestone — one durable ABC → ABD move per group

Status: pre-implementation brief. Root explicitly selected the restricted first milestone below. Advisor and verifier discussed the exact OpenRaft 0.9.25 behavior directly and both approved the specification and predesign quality after clause review. No unresolved disagreement requires a Judge. No production changes, builds, or movement-test success are claimed.

```text
operator -> group-0 conditional move record
                  -> existing data leader -> add D learner -> prove catch-up
                  -> OpenRaft joint ABC/ABD -> committed/applied ABD
                  -> group-0 conditional directory publication -> gateway refresh
```

## Scope decision, and what remains unfinished

For each data group, accept **one immutable job and target for its lifetime**. ABC → ABD is the first case. The same job ID with identical fields resumes; a different payload, different target, or even a new job ID with the same target is rejected permanently, including after completion/restart. Retain the job in snapshots and metadata forever for this milestone. Do not permit deletion/replacement of the job, rollback to ABC, endpoint reassignment, arbitrary raw membership calls, or a second move epoch.

This restriction makes every delayed authorized side effect point toward the same result. It avoids claiming an application epoch check atomically fences OpenRaft's membership API. It is **not** the general repeated-movement manager: later different targets, superseding jobs, and Task-11 repeated-move schedules remain pending. Preserve those original gates in reporting; rejection tests and repeated same-job reconciliation do not satisfy them.

The stronger alternative is the previously documented all-executor quiescence protocol: before activating a new target epoch, durably fence every possible old executor, drain actual old membership futures/core handoffs, then commit activation. Offline executors block advancement. A less restrictive alternative needs a separately reviewed conditional membership integration with OpenRaft; its current API has no expected application epoch or expected vote. Neither local mutex nor read/check/call is a substitute.

## Keep the implementation small

Add concrete `control/{metadata,movement}.rs` node modules and bounded engine-free admin DTOs. Keep OpenRaft as the consensus implementation. Use named functions such as `accept_move`, `read_move`, `reconcile_move`, `ensure_learner`, `wait_for_catchup`, `observe_membership`, and `publish_placement`; no generic workflow engine, new trait hierarchy, or custom election/quorum logic.

Root owns runtime/CLI and gateway changes; storage changes require the Task-4 worker's source window to finish first. Do not edit snapshot files concurrently. Extend the existing state apply path only through agreed small hooks.

## Engine-free admin boundary

Use a fixed-size nonzero job ID (for example 16 bytes), validated GroupId/NodeId, existing bounded NodeEndpoint, and explicit bounded result enums:

```text
MoveReplica { job_id, data_group, remove_node, add_node: NodeEndpoint,
              expected_directory_revision }
GetMove { data_group }
ReconcileMove { data_group, job_id }
ObserveMembership { data_group }
```

`MoveReplica` derives old/target voters from the validated current placement; it does not accept a raw voter set. Only replace one of three voters with a distinct node, producing three target voters. Reject group zero for this milestone. `ReconcileMove` obtains the committed job itself through a linearizable metadata read; the caller cannot supply phases, addresses, epochs, or membership arguments. `ObserveMembership` is an internal read using actual group authority, not a local leader hint.

Return a small typed acceptance/conflict/status or Unknown, with bounded diagnostics and optional routing hints. After a metadata proposal/API invocation loses its response, read durable status and reconcile; do not infer absence of effects from a timeout. These internal idempotent operations do not change public uncertain-mutation no-replay semantics.

Bound jobs by configured data groups, node identities by the existing 16-node limit, and all decoded nested collections before allocation. Count the **union of published and prospective job nodes** toward that limit. Check ID and peer/data endpoint uniqueness across both the directory and every persisted job. Same D ID with the same endpoints can serve several groups; conflicting endpoints for that ID cannot be accepted by racing jobs. Fixed per-group runner ownership plus a small process control semaphore bounds active work; duplicate requests return/reconcile the existing runner rather than spawn another task. No control task holds data-request admission while waiting indefinitely for unrelated groups.

## Conditional metadata application, before mutable directory publication

Extend durable Command by appending variants, preserving existing Put/Delete serialization tags and valid historical log replay. Replace **new** bootstrap unconditional-Put emission with apply-time conditional initialization before allowing revision-two placement. Do not suddenly reject historical bootstrap Put entries when D replays old logs or snapshots.

Require a quiescent all-node binary transition before enabling this milestone: stop/drain every old bootstrap executor, upgrade all participating nodes, then enable conditional metadata and moves. Mixed old/new control binaries are unsupported. Adding the new command does not fence an already running old handler that can still submit unconditional Put. A future alternative is a durable metadata-format flag whose apply semantics reject later ordinary reserved-key Puts while accepting historical earlier log entries; do not improvise that migration or reinterpret old enum tags in this milestone.

Use these concrete deterministic state-machine operations, executed only in group zero:

- `InitializeDirectoryIfAbsent { initial_directory }`: absent state creates directory and an immutable bootstrap-origin record atomically. Existing origin must match; current directory may legitimately have a newer revision. A compatible existing revision-one directory from the old implementation can acquire its origin marker through this conditional command. Never overwrite a newer directory on bootstrap retry.
- `BeginMove { expected_revision, immutable_job }`: atomically checks current directory/source voters, bounded prospective-node registry, and absent per-group job. Identical existing job returns AlreadyAccepted even after progress; any conflicting/new job returns Conflict. Store immutable old/target voter sets, D endpoint, job ID, and initial progress. No placement changes yet.
- `CompleteMove { expected_revision, group, job_id, observed_membership }`: requires the matching immutable job and a uniform target-voter observation. Atomically publishes D's endpoint if needed, target placement, incremented directory revision, and Completed job plus full membership LogId evidence. Identical completed job returns AlreadyCompleted; stale revision/conflicting job cannot overwrite current metadata.

`observed_membership` is trusted internal evidence collected by the authorized reconciliation path, not a caller-supplied cryptographic certificate. The metadata state machine cannot independently inspect another Raft group. Its safety depends on the stable-target property and the real committed/applied observation below. All external admin entry points must route through that observation.

Small apply results suffice: Applied/AlreadyPresent/AlreadyAccepted/AlreadyCompleted/Conflict. Do not clone the entire directory into every apply response; callers re-read it. A conditional conflict is a normal deterministic result: it still advances applied LogId in the synchronous batch and does not fail the Raft group.

Store directory, origin marker, and per-group jobs as reserved logical keys in group zero's generation-prefixed state, so the existing file-backed snapshot stream includes them. Do not hide job state only in a process cache or a column-family record omitted from snapshots.

**Chosen implementation: each metadata command is a standalone apply chunk.** Flush the preceding ordinary chunk before processing it; retain the existing group write session across the apply iterable. Inside the admitted native closure, load one bounded typed directory/origin/jobs view from the current coherent RocksDB snapshot, run the pure conditional transition, and write its effects plus applied identity/membership in one synchronous batch. The next metadata entry sees the preceding durable result, including a historical bootstrap Put from the same apply iterable. No same-batch overlay or duplicated deep working view is needed because two conditional entries never share a native batch. Ordinary data-entry grouping is unchanged.

Append `Command::Metadata(MetadataCommand)` and the metadata apply-result variant without changing existing Put/Delete/Applied serialization tags. Keep the pure transition in the concrete node control module; reject metadata commands outside group zero. A conflict emits a small deterministic status and advances applied progress without changing placement/job state. Invalid persisted metadata fails closed.

### Metadata work and admission bounds

Use explicit encoded caps of 64 KiB for each directory/origin value and 4 KiB for one job. Reserve 192 KiB for the complete set of at most three emitted metadata effects, including keys/native-record overhead; the `StateRecord` and batch header are additional. Prove these caps with maximum-shape serialization tests, including 257 placements with 16 voters and 16 IPv6 endpoints. Count serialized output before allocating effect buffers/native batches, and assert the resulting native batch stays within its configured budget before persistence. A tiny CompleteMove command does not justify charging only its input length.

For a runtime that admits control commands, validate a checked minimum covering the larger of the existing normal-entry charge and the 192 KiB metadata-effect reservation, plus the worst permitted state record and existing batch overhead. The default 4 MiB chunk remains sufficient. Also validate that the accepted command itself fits `max_entry_bytes` before proposal. Preserve generic storage-only small-budget tests: their raw-entry capability is distinct from production control admission. Prove the production minimum/admission boundary with tests; never accept a legal control proposal and discover an insufficient output budget only after commitment.

The typed view has a separate bound: at most one job per configured data group (256 maximum), directory and immutable origin, and at most 16 distinct prospective/published node identities across all jobs. Validate endpoint identity conflicts across that entire union, including pending jobs. Its decoded collection overhead is additional to the 192 KiB effect budget; neither is an RSS claim. Load this view only for metadata entries, without adding work to the ordinary data-KV path. Test two competing expected-revision-one commands in one apply iterable: the second sees the first durable result and conflicts.

Update directory validation now: preserve cluster identity, immutable hash/version/count, endpoints, and monotonic revisions; do not require complete equality with the original initial placement. Keep bootstrap-origin equality separate from mutable placement equality.

## Explicit D startup and rejoin

Add an explicit learner/join startup mode. D's local advertised endpoint must be separate from the initial ABC directory: current `NodeConfig::endpoint` membership lookup and `ClusterDirectory::initial` exact-three constructor cannot represent this join unaided.

Join configuration supplies D's ID/local peer+data addresses, existing metadata seed addresses, expected cluster/hash configuration, and explicit create/open storage intent. Fetch and validate an actual linearizable directory from an existing seed leader before join readiness; a newer compatible revision is valid. Validate D's proposed endpoint against published/persisted job metadata when accepting its move, not by silently rewriting initial ABC placement.

D starts its local data-group Raft instances **uninitialized**, with the existing internal `u64` ID and private network/storage adapters. Never call `initialize` on D; membership and state arrive from existing groups via add-learner replication and snapshots. Metadata group zero remains ABC and may be unhosted on D. If the minimal implementation starts an empty local group-zero handle, it must never initialize or serve it as authoritative metadata. Reopen validates persisted cluster/node/hash identity and preserves received histories. Join mode must reject `--initialize` and accidental fresh-cluster bootstrap.

Existing ABC nodes need no reconstructed independent group and no restart to add D. D's endpoint is available in the immutable job for learner replication and controller routing before it becomes part of the published placement. Published routing changes only after membership completion.

## Reconciliation against OpenRaft 0.9.25

Every execution begins with the durable immutable job and a fresh authoritative group observation. A local per-group controller mutex limits duplicate local work; **it is not a cross-leader fence**. Lost leadership/errors/Unknown resume by reading state, never by restoring a remembered old phase.

Branch on that observation before running steps: **uniform ABD** skips learner addition/catch-up/change and goes directly to final proof/publication; **joint ABC/ABD** resumes finalization toward the same ABD target and then publication; **uniform ABC** runs learner/catch-up before first promotion. Any other voter topology is an explicit conflict requiring investigation, not an invitation to reset membership. Catch-up is a prerequisite for first promotion, not a requirement that D remain a learner forever. If D is already the actual leader after promotion, use its authoritative applied membership for final proof; do not wait for `replication[D]`, since a leader's replication map normally describes its peers rather than itself.

1. Read actual committed/applied membership. ABC without D: call `raft.add_learner(d_id, BasicNode::new(fixed_peer_address), false)`. Already present D: verify its endpoint matches the immutable job and do not replace it. Never call SetNodes/RemoveNodes/AddVoters with arbitrary input. An in-progress membership error means observe/retry within a bounded control lifecycle.
2. Prove catch-up explicitly. Capture a leader authority/read barrier and vote, then wait for that leader's replication match for D to cover the captured full log identity (equal index requires matching full LogId; a later index covers it on the same valid history). Require the same current leader/vote and D still present; loss of leadership causes reconciliation on the new leader. A continuously changing tail need not stop to define this captured barrier. No promotion is based solely on an old persisted CaughtUp phase or an `add_learner(..., true)` return.
3. Call `raft.change_membership(target_voter_set, false)` using exactly ABD every time. OpenRaft supplies joint consensus and final uniform membership. Do not implement a separate joint quorum algorithm. Hold operation ownership/permits until the real future resolves, even if the admin caller disconnects. After any uncertain result, re-observe; do not mark completion from a leader hint or an uncommitted metric.
4. After a linearizable authority barrier, read the coherent applied state membership, or obtain the committed membership through `with_raft_state` and verify it has been applied. Require one voter configuration exactly ABD, D endpoint matching, C absent as a voter, and full committed membership LogId. `retain=false` removes C from final replication membership; preserve C's disk data rather than deleting it.
5. Submit `CompleteMove` to group zero with the current directory revision. A crash between data membership completion and metadata publication is resumed by observing uniform ABD and publishing; it must not run a rollback. A revision conflict from an unrelated group's completed move re-reads current directory and retries the same immutable job without clobbering other placements.

Exact source evidence:

- `src/raft/impl_raft_blocking_write.rs:49`: `change_membership(members, retain)` sends the same `ChangeMembers` twice, stopping if already uniform; leadership loss can leave joint state.
- `src/membership/membership.rs:278,328`: `next_coherent` computes from the current configuration; ReplaceAllVoters(ABD) moves ABC → joint → ABD, and ABD → ABD. It never captures and later replays a raw old ABC configuration for this API.
- `src/membership/membership.rs:329`: AddNodes(D) preserves voters and does not overwrite an existing D endpoint. A delayed same-job add-D after final ABD cannot demote D or restore C.
- `src/raft_state/membership_state/change_handler.rs:38`: changes require effective membership already committed, otherwise InProgress; this is OpenRaft's own membership sequencing.
- `src/raft/impl_raft_blocking_write.rs:125` and `src/raft/mod.rs:735`: blocking add-learner's catch-up helper can finish on leader loss/removal, uses a lag threshold, and its wrapper returns the earlier response. It is not our required catch-up proof.
- `src/core/raft_core.rs:562`: `metrics.membership_config` is **effective**, potentially uncommitted. `with_raft_state` is read-only; `membership_state.committed()` exposes the committed membership but cannot atomically fence a later API call.

The immutable-target argument covers both a delayed whole request and the delayed second native membership step: every allowed step preserves or advances the same target. It ceases to apply the instant a second target or endpoint replacement is permitted. A completed job may suppress new execution immediately; a previously validated executor released afterward is still harmless under the restricted operations.

Removal is scoped to the moved data group. Retain C's catalog endpoint and process: group zero and sibling data groups may still use ABC. Do not delete C's shared database, remove it globally, or make its healthy groups unavailable as a shortcut.

## Gateway refresh and authority

The existing gateway stores one directory after startup; that must change. Add bounded single-flight refresh with the same end-to-end deadline on stale routing/NotLeader/Unavailable paths and accept only compatible **higher** revisions (equal revision with different bytes is an error). Publish a newly validated directory atomically to readers; no KV cache. Slot creation remains bounded by the current directory/node limit.

The membership-before-map gap may temporarily make requests unavailable if D leads while the cached directory still lists ABC. Do not fabricate success or serve stale data to hide it. Once group zero publishes, the existing gateway must discover and route to D without restart. Leader hints remain routing suggestions; destination read/write authority checks remain unchanged. Do not replay a public mutation after any uncertain attempt while refreshing metadata.

## Required gates, with real failure schedules

1. Conditional metadata: absent/same/different bootstrap, bootstrap after revision two, same-job retry, different job/target, conflicting jobs in the **same apply iterable across standalone metadata chunks**, concurrent D endpoint conflict, normal conflict result with advanced applied LogId, restart/snapshot preservation. Historical Put/Delete/bootstrap logs still replay on D. Verify maximum encoded output and pre-proposal admission at configured budget boundaries.
2. Real ABC → ABD with the public client/gateway under traffic, D launched through join mode, streamed snapshot catch-up after retained logs are purged, successful PUT/DELETE preserved. The learner's receipt cannot count as a voter quorum before promotion.
3. Hold at learner addition, during catch-up, between joint and uniform steps, after uniform commit before map publication, and after publication before reply. Kill/restart the controller and lose replies; resume only the same job and derive each next step from actual membership. Other groups continue.
4. Pause a controller **after job validation but before native membership API submission**; replacement finishes ABD and publication; release old controller. It may add D or request ABD again but cannot restore C/change endpoints/regress metadata. Repeat with a native second step delayed and with restart. Permanently reject a conflicting later job; this is the selected restriction, not a test of general next-epoch fencing.
5. During joint ABC/ABD, isolate BC from AD so neither side has both required majorities; no newly invoked mutation succeeds. Confirm partitions before fresh operations; earlier in-flight valid quorum work is accounted separately.
6. After completed membership and confirmed isolation, issue fresh PUT and strong GET directly to removed C: neither may succeed through stale authority. A read whose legitimate barrier preceded the move may overlap it; do not reject that legal history. C's retained disk data is not evidence of authority.
7. Make D the observed actual leader through bounded test-controlled elections/partitions, then use the **already running gateway** for GET/PUT/DELETE. Waiting for observed leadership is required; a trigger/hint alone does not establish it. Test stale directory refresh, monotonic revision, metadata unavailability, and no uncertain mutation replay.
8. Operator status reports job, immutable target, observed committed membership, directory revision, and pending phase without claiming hints/effective membership are completion. Test processes own their data/PIDs and clean up on failure. Source review, affected tests and strict Clippy run in the coordinator's build window.

Completion report must say “one lifetime target per group, explicit join, durable restartable ABC→ABD” if these gates pass. It must continue to list general multiple epochs/targets, arbitrary node registration/removal, repeated-move soak and broader failure/resource gates as unfinished. OpenRaft's success on one movement does not validate that broader protocol.
