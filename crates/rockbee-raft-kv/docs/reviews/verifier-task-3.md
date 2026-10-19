# Task 3 independent storage review

Reviewed 2026-10-19 against the Task 3 brief, current node storage/Raft types,
storage fixtures, and recorded node test/check/Clippy logs. This review is
limited to the new module and Task 3 storage behavior; concurrent peer/backend
work is outside its scope. No production edits or duplicate native builds.

```text
Reviewed: group log/vote/apply -> synchronous RocksDB WriteBatch -> completion
Pending:  real Raft service + Gateway + file snapshots + complete storage suite
```

**Spec verdict: APPROVED for basic Task 3 storage.**

**Quality verdict: APPROVED.**

No durable-ACK, atomic-prefix, cancellation, identity, or group-isolation blocker
was found in the reviewed basic adapter. Defaults admit the current public KV
limits. The actionable boundary finding below was reproduced and corrected by
the storage implementer, then independently re-reviewed. No remaining storage
finding blocks integration. This approval does not mark snapshot/full-suite or
concurrent peer validation complete.

## Resolved P2: accepted chunk configuration can reject its legal entry

Initial location: `src/storage/mod.rs:104`, StorageOptions validation, and
`:551`, EntryChunks admission; related state metadata admission is in
`src/storage/state.rs`.

Validation initially accepts `io_chunk_bytes == max_entry_bytes`, while append
charges 12 bytes plus 64 bytes per entry and apply additionally charges the
current state record plus 128 bytes. An entry within max_entry_bytes can then
fail the first empty chunk and retire group writes under a configuration that
passed validation. Large replacement memberships also risk counting both the
obsolete and replacement record as required native-batch contents.

Required correction: validate additive minimum capacity before database open;
ensure one legal entry plus its required native metadata fits, and estimate a
replacement membership using the record actually persisted. Keep ownership
accounting honest while old state and prepared inputs coexist. Do not silently
grow chunks, tune defaults, or discard valid entries. Add a focused accepted/
rejected configuration boundary test and a replacement-membership case.

Correction reviewed: validation now uses checked capacity for the maximum state
record plus one maximum legal normal command and batch overhead. Public KV bounds
give the normal-command bound; default 4 MiB chunks remain valid. ChunkMetadata
removes the obsolete persisted-record estimate when a membership replaces it,
while its comment preserves prior-state/input/encoded ownership accounting.

Both focused regressions failed before this fix, with exit 101 recorded in
`reports/task-3-budget-red.log` and `task-3-membership-budget-red.log`. They pass
in the latest full node test log: impossible capacity is rejected before creating
the DB, and two near-2-MiB membership entries replace each other under defaults.

## Source checks completed

- Shared cache and soft write-buffer manager are constructed once for all fixed
  CFs. WAL remains enabled; synchronous write options are used for identity,
  vote/log/committed updates, state batches and purge markers.
- Append invokes its genuine LogFlushed callback only after every chunk's sync
  result; the callback owner reports error on failure/abandonment. Final callback
  ownership is moved into actual native work. No successful persistence is
  synthesized by a timer or cancelled async waiter.
- Apply processes bounded consecutive chunks. Effects, final full applied LogId,
  and resulting membership are in one synchronous batch per chunk. Each entry
  gets an ordered result after durability. A later failure leaves a coherent
  completed-chunk prefix, not a purported whole-iterator transaction.
- A WriteSession holds the group gate; running blocking closures retain shared
  gate ownership, lane permits and group lifetime. Session abandonment retires
  writes. Duplicate writer acquisition is prevented while old handles or actual
  jobs remain. Read jobs use their bounded lane without acquiring the write gate.
- Startup validates options before native allocation; create/open modes prevent
  silent replacement, validate fixed CF layout and stored identity/format, and
  retain failed-open evidence. Missing state plus generation data is corruption.
- Keys separate metadata/data and group/generation prefixes. Inclusive MAX range
  endpoints do not wrap. Purge marker and deletion are atomic; truncate does not
  advance purge. Log-state marker/tail and local generation/applied/value use one
  RocksDB read snapshot. Full range reads remain complete; replication reads
  return bounded nonempty prefixes where available.
- Snapshot methods explicitly return Unsupported; current snapshot is None only
  when no descriptor exists. They do not fabricate recovery success. Internal
  Raft node zero remains representable without weakening public validated IDs.

## Evidence inspected

Latest `reports/task-3-tests.log`: five storage unit tests and 29 storage
integration tests pass; one upstream recovery case is explicitly ignored because
purged-log recovery builds a real snapshot. The run also happened to include
four peer tests; peer correctness is outside this storage verdict. The latest
run includes both budget regressions, concurrent purge/log-state snapshot
coherence, failed membership/data/applied chunk recovery, and log corruption.

After the budget fix, the full test command compiled successfully and passed,
and storage file formatting passed. An intermediate package check/Clippy was
blocked by a concurrent peer source rewrite. At the subsequent stable peer
checkpoint, both `reports/task-3-check.log` and `task-3-clippy.log` were rerun and
now show successful completion with exit zero, including strict node all-target
Clippy. The reviewer read those final logs; storage source was unchanged after
its successful affected test run. No fresh Clippy success was inferred from test
totals. The reviewer did not start another native build or duplicate the suite.

Inspected tests exercise real read-only-engine failure, held native I/O plus
cancelled waiter, failed later apply chunk, real reopen, membership/blank progress,
max-index/group isolation, committed-unapplied replay through StorageHelper,
identity/CF errors, orphan state, and unknown snapshot descriptor handling.
The full OpenRaft suite remains Task 4 work. Process crashes and synchronous
adapter results do not qualify actual device power-loss persistence.
