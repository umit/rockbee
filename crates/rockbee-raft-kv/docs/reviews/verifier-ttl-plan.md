# TTL plan adversarial review

Final predesign spec verdict: APPROVED for the revised TTL plan. Final predesign quality verdict: APPROVED for the same scope. The initial findings below are resolved by the final clauses recorded at the end. This reviews `docs/tasks/ttl-plan.md` and the actual pinned client, command, storage and snapshot APIs; no TTL code or dependency was added. Advisor and verifier directly agree; no Judge disagreement.

```text
relative TTL -> leader timestamp -> committed value/version
due hint -> conditional committed deletion -> fresh strong read
TTL remains design-only
```

## 1. Define the clock contract and exact inequality

Instantaneous pairwise skew alone does not imply an at-least-TTL real-time guarantee. All nodes can jump forward an hour together while pairwise skew stays zero; the current plan's real-time minimum would fail. If retaining that conditional guarantee, use the explicitly stated operational assumption `|C_i(t) - real_time(t)| <= B` at both issuance and expiry observation. Then the safe condition is:

```text
issued = writer wall clock when constructing this one command
expires = checked(issued + ttl_ms)
due_threshold = checked(expires + 2 * B)
due iff observing_wall_clock >= due_threshold
```

The worst writer offset is -B and observer offset is +B, hence 2B. Root/advisor selected a proposed immutable absolute-error allowance of 1,000 ms, valid 1..=60,000 ms. This is an operator assumption, not something `SystemTime` proves. Outside the bound an early expiry is possible; Raft's no-resurrection property still holds. Replace the contradictory phrase “observed clock plus allowance” everywhere: adding allowance to now has the opposite effect. Check every addition before proposal. Reject or ceil fractional-millisecond Duration; truncation can shorten the requested TTL.

Remove the durable time floor/atomic logical clock from the first design unless it adds a separately specified guarantee. A committed conditional delete already prevents resurrection; a backward clock can delay future expiry under the stated contract. Per-operation `last + 1` ticks would be especially wrong because request volume could advance expiry time faster than real time. Removing the floor also avoids an unnecessary StateRecord migration.

## 2. Preserve mutation and strong-read certainty

The eager method must use the existing client admission path; relative duration validation is local, absolute timestamp construction is once immediately before the serving leader calls client_write. TTL begins there, not at ACK. A timestamp overflow can be explicitly rejected before consensus. Public response loss stays Unknown with no replay.

A due GET is an operation with an internal conditional write, not a local filter. Its failed/uncertain expiration must yield a read error or a bounded fresh reconciliation under the original deadline; it must not fabricate None. An owned proposal retains its admission after the caller expires, until the actual native Raft result ends. ExpirationSkipped requires a fresh authority barrier/coherent record read, because a concurrent renewal may now be live. No-quorum expiry must not return local-clock absence.

Clarify “no resurrection” as the expired **version** cannot reappear without a later write. A later legal PUT may intentionally store identical opaque bytes. The old timer compares the entire LogId plus expiry and cannot delete a newer write.

## 3. Make compatibility concrete against current code

- Current `Command` is Put/Delete; preserve those tags and append after every variant actually present when implemented, including any intervening metadata work. Current `ApplyResult::Applied` must not silently become Written at ordinal zero; retain it and append new results, or explicitly justify/version that change. `StoredRecord.version` must be optional for legacy non-expiring records, as the plan itself permits them.
- Current state keys are `group_be | tag | generation | ...`; snapshot descriptors are in the separate snapshot_meta CF. `snapshot.rs::validate_reservations` and `collect_obsolete` scan only tag 1 today. Tags 2/3 require generation validation and bounded GC too, not only a new read helper. All three families must activate through the same state-generation pointer and preserve snapshot/MVCC semantics.
- Current StateRecord and StorageIdentity use postcard storage format 1; merely appending fields is not a migration decoder. Keep StateRecord unchanged if the floor is removed. Persist the new immutable clock policy through an explicit versioned configuration migration rather than assuming the old identity can deserialize a new field.
- Current snapshot image is RBRSNAP1 and descriptor is RBRDESC1, with a separately encoded header. Specify strict image **and descriptor** version dispatch, old published-image recovery, v1 legacy import, v2 record metadata/index rebuild and recovery checks. The real files are `storage/snapshot.rs` and `storage/snapshot/format.rs`; proposed new files should be labelled new.
- Permanently treating missing record metadata as legacy non-expiring conflicts with the promise to fail closed on missing/corrupt TTL metadata: a lost TTL metadata row becomes indistinguishable from a legitimate old key and can become immortal. Choose an explicit legacy marker/format and bounded quiescent migration, or another concrete distinction. V2 images must preserve that distinction; index rebuild is deterministic, not evidence of a missing source record being legitimate.
- Quiescent all-node upgrade is required. New conditional code cannot fence a still-running old writer/expiry-unaware builder. Preserve valid historical Put/Delete/bootstrap replay. A public protocol version bump changes envelope compatibility even when old operation tags remain unchanged; distinguish those claims in tests/docs.

## 4. Bound actual work, not merely command bytes

Use a shared process proposal cap plus per-group count/byte caps, retained through actual completion. “16 per group” can become thousands across the configured group count. Candidate charges include decoded version/expiry, key ownership, vector capacity and in-flight tasks, not just raw key bytes.

TTL renewal writes data, record metadata, deletes an old expiry key and inserts a new one. Those several maximum-length keys can expand beyond today's encoded-command work estimate. Update apply chunk output accounting and accepted production minima before allowing proposal; do not discover after commit that a valid entry cannot fit. Same-key transitions in an apply iterable must observe earlier effects. Small outcome statuses suffice; avoid returning whole records from every apply result.

Keep normal RocksDB compaction for physical reclamation. A new manual-compaction coordinator is an unmeasured runtime addition and should remain outside the first TTL implementation. Logical expiry correctness cannot depend on compaction timing.

## Focused acceptance additions

1. Two clocks at -B/+B demonstrate the 2B boundary; a common forward jump demonstrates why pairwise-only safety is not claimed. Exercise checked issued/TTL/grace overflow and fractional milliseconds.
2. Due GET with withheld quorum/expired caller never returns false None and retains its real proposal permit. A concurrent renewal plus stale expiry returns the renewed version after a fresh barrier.
3. Old non-expiring keys, explicit legacy markers, TTL keys, deletes and renewals survive format migration, v1/v2 descriptor/image reopen and snapshot plus replayed tail. Missing required metadata fails closed rather than becoming legacy.
4. Orphan tag-2/tag-3 generations are detected/reclaimed without touching siblings; replacement/delete/expire in one apply sequence maintains exact index correspondence. Maximum key/value/metadata effects fit each accepted production budget.
5. Many groups with stalled expiry proposals plateau at the process count/byte cap; caller timeout and leader change do not create detached replacement proposals. Injected clocks and observed Raft history drive the model, including optional incomplete Unknown writes.

The planner is idle pending root-directed correction. This report does not approve a real-time promise based on pairwise skew, a new compaction subsystem, or an unversioned compatibility change.

## Revised-plan clause review

The planner's subsequent revision correctly adopts absolute UTC error and the exact 2-epsilon condition, rejects fractional milliseconds, removes the floor and manual-compaction subsystem, retains existing apply-result tags, introduces explicit Legacy metadata and a per-generation V2 marker, and adds decoded-candidate/process proposal bounds. Those original findings are resolved at the plan level.

Three exact compatibility clauses remain requested before approval: migration/export policy must exclude TTL markers and auxiliaries from metadata group zero; strict descriptor version recovery must cover the actual RBRDESC1 codec as well as image RBRSNAP1; and immutable epsilon needs an explicit compatible persisted-config transition rather than extending format-one StorageIdentity implicitly.

Also require this fail-closed migration schedule: create V2 TTL data, lose its format marker, then reopen. Startup must not classify it as legacy and overwrite Some(version)/expiry with Legacy(None), making it immortal. Resuming an unmarked interrupted legacy migration is safe only if auxiliary rows prove they are partial Legacy records with no TTL index/new-version rows. The planner received that concrete distinction and test. Expired-version nonresurrection must allow a later legitimate write of identical raw bytes.

Final compatibility follow-up: those clauses are now explicitly present. Group zero retains RBRSNAP1/RBRDESC1 with no TTL auxiliary tags; data descriptors dispatch strictly between RBRDESC1/RBRDESC2; a replicated initialize-if-absent RBRTTLC1 config leaves StorageIdentity unchanged; and a durable migration journal plus auxiliary-shape checks distinguish interrupted legacy migration from missing-marker V2 corruption. The same-bytes later-write qualification is also recorded.

Startup ordering must be explicit: metadata Raft starts first to commit/read the immutable TTL policy, then data generations migrate offline, then data Raft/public TTL admission starts. The later blanket statement “network/Raft startup is not allowed” must be scoped to data Raft/public admission, otherwise the plan contains a startup dependency cycle.

The final bilateral scan also found a canonical replay defect: caught-up replica A materializes an existing historical Put as version None, while lagging replica B imports an older v1 snapshot and replays the same Put as version Some(LogId). Their identical applied LogId would then carry different metadata, causing Task 4's correct equal-progress snapshot check to reject a legitimate transfer. The simplest agreed correction is **all non-expiring rows, old or newly written, use explicit NonExpiring(None,None); only TTL writes have Some(version) and Some(expiry)**. Normal Put clears the old TTL/index, so an old ExpireIfMatch(expected Some version) cannot match the non-expiring row. No new ordinary-Put command or generic version framework is needed. Require version presence iff expiry presence, while record metadata itself is always required in V2.

Publish the V2 marker and delete the migration journal in **one** synchronous batch. Publishing the marker first and later deleting the journal would create a legitimate crash state that the current fail-closed marker/journal classifier rejects. Add both canonical replay and marker/journal crash-cut tests. Root and planner received these finite corrections. Predesign approval remains withheld until they are recorded; no disagreement/Judge escalation exists, and this review does not claim TTL implementation acceptance.

## Final approval

Verifier reread the final clauses: all historical and new ordinary Put operations materialize NonExpiring; only TTL writes materialize Expiring with applied full LogId and expiry. The A-migration/B-snapshot-plus-tail canonical replay test is explicit. Marker publication and journal deletion are one synchronous batch. Metadata group zero starts first for immutable policy adoption/read, while only data-group Raft/public admission waits for the offline migration; the previous global startup contradiction is removed.

These resolve the final counterexamples without a new ordinary-Put command, logical clock floor or manual-compaction coordinator. Advisor independently approved the final design and the planner confirmed the remaining editorial startup scope correction. Approval is to implement the staged plan and required tests, not a claim that TTL exists or that the clock-error assumption is verified by software. The conditional real-time minimum is valid only under the stated absolute UTC error bound; consensus-based nonresurrection is the separate unconditional logical guarantee.
