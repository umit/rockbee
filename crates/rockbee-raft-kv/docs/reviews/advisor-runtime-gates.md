# Peer, gateway, and snapshot handoff review

Review mode: read-only source and existing reports; no builds, production edits, or new empirical claims. Root reports the real node runtime test passed in `reports/runtime-first.log`; that evidence does not independently test public gateway routing or the receiving server's partial-frame interleaving.

## Task 5a peer: corrections verified, remaining gates

The LogId correction now preserves term, leader node ID, and index. Its reusable conversion covers append previous/entry/commit, vote last-log, and snapshot/membership log positions. The committed-leader identity regression exists. The request source is checked against its Raft vote. Semaphore maximums and the fixed 256 KiB snapshot DTO limit are validated. Pre-dispatch phases now have timeout/shutdown handling, and submitted OpenRaft calls retain their owned task until completion. Normal append batches are conservatively sized before DTO copying; more than 128 entries receives a split hint. These address the earlier identity and large-normal-append allocation findings.

Remaining concrete issues:

- Snapshot lane acquisition still waits **after** acquiring shared connection capacity, inbound and outbound. Enough snapshots can occupy every connection slot while waiting for eight snapshot permits, preventing votes/appends from reaching their separate operation pools. Outbound can acquire the operation lane before connection capacity; inbound should reject a saturated classified snapshot immediately or preserve explicit control headroom. Test control progress during saturated snapshot admission.
- `fitting_append_prefix == 0` currently becomes `new_entries_hint(1)`, even for a one-entry request. Smaller accepted frame configurations or an oversized metadata entry can therefore receive an ineffective hint forever. Reject impossible configuration/entry or return a non-shrinking terminal transport error; a split hint must actually reduce the request. Test the one-entry-too-large branch separately from the valid maximum-value case.

No fabricated Raft success was found. These remaining issues concern bounded progress/configuration, not permission to weaken mutation certainty.

## Task 6a gateway/server: approval withheld pending fixes

**Server frame cancellation:** `run_connection` constructs `receive_request` inside a `select!` racing handler completion. Its `read_exact` operations consume stream bytes into local buffers. If a handler completes during a partial next frame, selecting `handlers.join_next` drops the receive future and its buffers; the next iteration treats the remaining payload as a new frame header. Keep one pinned receive future across handler-completion turns, or use a dedicated owned reader. Dropping it is safe only when retiring the entire socket. The implementer received this exact interleaving and test proposal directly: hold A; partially receive B; finish A; send the rest of B; B must dispatch once with intact ID and bytes.

**Follower GET routing:** `Gateway::route` retries mutation `Rejected(NotLeader)` but immediately returns `ReadError(NotLeader)`. With sorted voters, a GET to the first follower cannot reach a later leader. Retry the explicitly safe read failure within the same deadline and test follower-first ordering with real backend replies.

**Retired links:** `backend_link` always returns the cached connection generation. After retirement, future operations continue submitting to that dead generation. Evict/replace it for future calls without replaying the uncertain operation that caused retirement. The implementation needs a race-safe replacement path with one bounded connection owner per target; no unbounded reconnect tasks.

**Cross-group blocking:** the global link-map mutex spans `BackendLink::connect`. A stopped endpoint can therefore hold unrelated group requests until its connect deadline. Limit the map lock to map inspection/publication; serialize connection establishment per target using bounded state. This is a required isolation repair, not a new connection-pool feature.

**Writer errors:** `write_responses` checks timeout's outer error but not `Ok(Err(io_error))`. Treat either as stream retirement before writing another frame; preserve accepted handler draining and reservations. `flush` should share a bounded output phase if it can wait.

Positive findings: response capacity is reserved before calling handlers; disconnected handlers remain in the owned JoinSet with request/work permits; `MayHaveBeenSent` maps to Unknown; the gateway stores placement and links, not KV values; close retains join ownership. These properties must survive the repairs.

## Task 4 snapshot pre-task approval with lock-order clarification

The brief's file-backed streaming, actual passed-File handling, durable image publication before atomic state+descriptor activation, stale-builder protection, and coherent RocksDB snapshots remain the intended design. Add this explicit ordering rule before coding:

> A builder finishes its scan and releases the snapshot-lane permit before acquiring the publication WriteSession. An installer may hold its group write gate while acquiring the snapshot lane. No job may hold that lane while waiting for a gate held by a lane waiter.

Without this rule, a builder holding the lane can wait for the installer-held gate while the installer waits for the builder-held lane. Publication uses the already completed immutable file, so releasing the scan lane does not weaken durability. Keep the image-open/publication mutex short and inside blocking callbacks; never hold it across await.

Bootstrap bilateral agreement: no-payload static initialization is valid only when all potential executors use identical canonical initial bytes and directory updates are disabled. A read-before-Put cannot protect simultaneous incompatible configurations that both read absent. Such configuration is an excluded violated startup precondition, not a race the current ordinary Put implementation claims to solve. No disagreement/Judge is presently needed.

All findings were sent directly to verifier and root; the server interleaving was also sent to its implementation owner before changes/tests. Independent verifier results and post-fix discussion determine acceptance. No passing node test overrides a counterexample on a different path.
