# Task 3: durable basic storage

Status: basic durable storage implemented and verified. Snapshots, the complete
upstream suite, and the running multi-node application remain unfinished work.
The independent verifier approved the basic storage scope after the budget
boundary fix; see [verifier review](../reviews/verifier-task-3.md).

```text
Existing: Client -> Gateway -> Writer/Readers -> SlateDB (unchanged)
New app:  Client -> Gateway -> group Raft -> LogStore / StateMachine -> RocksDB
                              unfinished   implemented and verified
Snapshots / full Raft startup with purged history: unfinished, Task 4
```

## Changes

- `apps/rockbee-raft-node/src/raft/types.rs` defines the node-owned PUT/DELETE
  command, apply response, and actual OpenRaft 0.9.25 configuration. The macro
  comment explains its type wiring; no application behavior is hidden in it.
  `crate::types` remains a reexport of the domain module.
- `src/storage/{mod,keys,log,state,snapshot}.rs` implements storage-v2 directly:
  `RaftLogStorage`, separate `RaftLogReader`, `RaftStateMachine`, and the concrete
  unsupported snapshot builder. Log/state writers are non-Clone. Readers clone
  the shared group owner.
- One process RocksDB has fixed CFs, a versioned synchronous identity record,
  one shared 64 MiB block cache, and one shared 32 MiB WriteBufferManager with
  stalls enabled. Existing per-CF RocksDB, Tokio and TCP defaults are preserved.
  Create/reopen validates identity, format, exact CF layout, empty default CF,
  options and the native database lock. Failed creation evidence is retained.
- Votes, committed IDs, log chunks, truncate and purge use WAL-enabled sync
  writes. Purge atomically updates its marker with the deletion range.
  Maximum-index boundaries do not wrap; log IDs are stored in full. Log state
  reads the purge marker and tail from one RocksDB snapshot.
- State apply commits one synchronous WriteBatch per bounded consecutive input
  chunk, combining data effects, applied LogId and membership. Blank and
  membership entries return responses too. A failed later chunk leaves only a
  coherent durable prefix. Empty values remain distinct from absent keys.
- All blocking operations own their group and lane permits. Mutations also own
  the per-group gate. Canceling an async waiter retires write admission while
  the actual blocking operation retains ownership; group reacquisition waits
  for all handles and jobs to finish. Read jobs use their own lane without the
  mutation gate. Different groups keep separate logical gates.
- Append's genuine `LogFlushed` callback transfers to the final blocking write
  and only reports success after all chunks are durable. Abandoned/error paths
  report failure. Tests obtain callbacks through OpenRaft's supported
  `RaftLogStorageExt::blocking_append`; no fabricated callback is used.
- `StateReader::read_local` returns generation/applied/value from one snapshot.
  It is a local read; Task 5 must supply the Raft authority/applied barrier.

## Budget boundary fix and memory limits

The independent review found two related, reproduced failures: an equal
entry/chunk byte budget was accepted despite lacking metadata space, and two
successive near-2 MiB memberships under the default 4 MiB chunk budget charged
the replaced old record as an additional write.

Options now validate a checked conservative minimum before native allocation:

```text
max_entry_bytes + 256
  + min(max_entry_bytes, MAX_KEY_BYTES + MAX_VALUE_BYTES + 128)
  + 64 + 128
```

Defaults are unchanged. State chunk admission removes the old record's write
reservation when membership replaces it. Both regressions failed before the
fix and passed afterward. Their complete failure output remains in
`reports/task-3-budget-red.log` and
`reports/task-3-membership-budget-red.log`.

Chunk preparation does not collect the whole iterator. The live work includes
bounded owned entries, their encoded buffers, the native WriteBatch, the prior
state/membership record, and at most one lookahead entry. Replacing a metadata
write reservation does not claim its owned memory is already freed. The API's
full-range log result and response vectors still scale with the caller's range
or entry count. Cache/WBM budgets and bounded work do not establish a hard RSS
limit or independent disk latency between groups.

## Verification evidence

All Cargo commands used:

```sh
CXXFLAGS='-isystem /Library/Developer/CommandLineTools/SDKs/MacOSX.sdk/usr/include/c++/v1' CARGO_BUILD_JOBS=2 cargo test -p rockbee-raft-node --locked --offline -- --test-threads=2
CXXFLAGS='-isystem /Library/Developer/CommandLineTools/SDKs/MacOSX.sdk/usr/include/c++/v1' CARGO_BUILD_JOBS=2 cargo check -p rockbee-raft-node --all-targets --locked --offline
CXXFLAGS='-isystem /Library/Developer/CommandLineTools/SDKs/MacOSX.sdk/usr/include/c++/v1' CARGO_BUILD_JOBS=2 cargo clippy -p rockbee-raft-node --all-targets --locked --offline -- -D warnings
```

- **34 storage tests passed:** five private native-failure/cancellation/coherence
  tests and 29 integration tests, including 14 named upstream cases. The whole
  node test checkpoint passed 38 tests including four then-current peer tests.
  Full output: `reports/task-3-tests.log`.
- Selected upstream cases: save_vote, initial_logs, get_log_entries,
  limited_get_log_entries, get_log_state, last_applied_state, purge_logs_upto_0,
  purge_logs_upto_5, purge_logs_upto_20, delete_logs_since_0,
  delete_logs_since_11, append_to_log, apply_single, apply_multiple. Every case
  builds a fresh database and returns the temporary-directory guard first.
- Focused coverage includes sync apply/identity/vote/committed restart; empty
  value versus DELETE; membership identity versus blank applied progress;
  append/truncate/purge restart; maximum index/group isolation; long iterator
  chunking; corrupt log/state/identity and unknown snapshot descriptors; option
  and lock refusal; actual native read-only write errors; failed second chunk
  atomicity including membership recovery; cancellation while native work is
  held; and concurrent purge/coherent log-state reads.
- Actual OpenRaft `StorageHelper::get_initial_state` successfully replays
  committed but unapplied unpurged logs after the database is reopened.
- Initial API absence was recorded as a compile-red failure in
  `reports/task-3-red.log`, not claimed as a runtime behavior regression. The
  first adapter test run is preserved in
  `reports/task-3-first-green-attempt.log` (see snapshot dependency below).
- Final package check and strict Clippy passed after the peer worker published
  a stable source checkpoint: `reports/task-3-check.log` and
  `reports/task-3-clippy.log`. The storage module narrowly allows
  `clippy::result_large_err` because the upstream trait fixes StorageError by
  value. Earlier checks against an in-progress peer rewrite failed and are
  preserved in `reports/task-3-{check,clippy}-rpc-in-progress.log`; these were
  reconciled by the peer owner, not hidden or counted as success.
- `rustfmt --check` passed for all owned production/test files. No commits,
  pushes, engine dependency changes, native environment mutations, or edits to
  existing Rockbee were made by this task.

## Explicit remaining work

`build_snapshot`, `begin_receiving_snapshot`, and `install_snapshot` return
Unsupported storage errors and do not alter active state. Current snapshot is
None only when the group has no descriptor; an unknown descriptor is an error.

The named upstream `get_initial_state_re_apply_committed` was run and failed
honestly: its fixture purges history, so upstream `StorageHelper` calls
`build_snapshot`. That case is explicitly ignored with its Task 4 reason and
is **not** counted among passing tests. Task 4 must implement real snapshots,
remove the ignore, run this case and the full upstream suite, and test network
snapshot transfer. Task 3 does not claim functional Raft recovery after purging
or a complete Raft service.

Task 5 still owes public Client/Gateway/three-node/four-group integration,
strong-read authority handling and the F57 quorum-plus-local-apply durability
barrier. Task 3 storage passes do not substitute for those live-path tests.
