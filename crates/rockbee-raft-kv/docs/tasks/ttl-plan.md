# Raft KV TTL implementation plan

> **Status:** design and acceptance plan only. It deliberately changes no
> production code, wire format, dependency, Cargo manifest, or test. The three
> node soak and client/server correctness work remain higher priority.

**Goal:** add an optional TTL to a value without allowing local clocks,
> compaction, a restart, a leader change, or an old expiry job to resurrect or
> delete the wrong value.

**Scope:** public `put_with_ttl`, correct strong GET semantics, durable Raft
> expiry transitions, restart/snapshot recovery, bounded leader cleanup, and
> observability. `put`, `get`, and `delete` retain their current byte-KV API and
> behavior. TTL is per key in its existing data group; there are no transactions,
> range TTLs, follower reads, or arbitrary client supplied absolute timestamps.

## Why this is not a RocksDB compaction filter

Meta's 2021 ZippyDB article says its client may supply an expiry time, reads
> filter dead values, and periodic RocksDB compaction physically cleans them.
> That is a useful storage-engine technique, but it is not by itself a safe
> replicated-state-machine rule: replicas can observe different wall clocks and
> an old leader can be behind a new leader. Rockbee's OpenRaft state machine must
> therefore make the **logical deletion** a committed conditional command.
> RocksDB compaction is only a later, local physical-space concern.
>
> Sources: [Meta: ZippyDB](https://engineering.fb.com/2021/08/06/core-infra/zippydb/),
> current pinned `openraft = 0.9.25` source at
> `$CARGO_HOME/registry/src/.../openraft-0.9.25/src/raft/mod.rs:572-580`
> (`ensure_linearizable` waits for the applied index), and the existing
> [`state.rs`](../../apps/rockbee-raft-node/src/storage/state.rs) atomic
> `WriteBatch` apply path.

```text
client PutWithTtl(duration)
        |
        v
leader: relative duration -> one absolute expiry timestamp
        |                         (only before client_write)
        v
OpenRaft log: PutWithExpiry(key, value, version, expires_at)
        |
        v
every replica applies the same bytes -> data + record metadata + expiry index
        |
        +-- leader sweeper / strong GET observes due candidate
                |
                v
          ExpireIfMatch(key, expected_version, expected_expiry)
                |
                v
          committed conditional deletion -> strong GET may return None
```

The line above is intentionally two Raft commands. Time decides *whether the
leader proposes* the second command; replicas never independently decide that
time has passed while applying it.

## User-visible contract

### Public client API

Keep the eager mutation handle API. Add one method next to `put` in
`crates/rockbee-raft-client/src/api/client.rs`:

```rust
pub fn put_with_ttl(
    &self,
    key: Bytes,
    value: Bytes,
    ttl: Duration,
) -> Result<MutationHandle, AdmitError>;
```

`put()` means **no expiration** and clears a prior TTL atomically. A later
`put_with_ttl()` replaces both the value and TTL atomically. `delete()` removes
the value and any TTL metadata atomically. A zero TTL, a duration that cannot
be represented in milliseconds, a duration over `MAX_TTL_MILLIS`, or an expiry
timestamp addition overflow is a proven pre-submission rejection.

Initial fixed bounds:

```rust
pub const MIN_TTL_MILLIS: u64 = 1;
pub const MAX_TTL_MILLIS: u64 = 365 * 24 * 60 * 60 * 1_000;
```

`Duration` is accepted only when it is an exact positive number of milliseconds
(`subsec_nanos() % 1_000_000 == 0`); fractional milliseconds are rejected
rather than silently floored. Conversion to `u64` milliseconds and all deadline
additions are checked.

The client sends a relative `ttl_millis: u64`, never a client wall-clock
deadline. The serving data-group leader converts it once to a private absolute
`ExpiryAtMillis`. This avoids treating an arbitrary client clock as cluster
time. Do not add `put_until()` in the first version; it needs a separately
specified clock/authentication contract.

The regular mutation outcome contract does not change:

* validation/admission failure before handoff is `NotSent` or `Rejected`;
* a lost response after gateway/node/Raft handoff is `Unknown` and is never
  automatically replayed;
* `Committed` means the TTL-bearing write was committed and durably applied,
  not that it will necessarily remain readable for the entire duration if the
  command itself waited longer than that duration to commit.

TTL starts when the elected leader constructs the durable command immediately
before `Raft::client_write`, rather than at client-side method invocation or
commit acknowledgement. This must be stated in the public Rustdoc. A queued
write can legitimately commit after its short TTL has already elapsed; a
subsequent strong GET will cause its conditional expiration.

### Time and strong-read contract

There is no claim of a universal perfect clock. Bootstrap stores immutable
`max_absolute_clock_error_millis`, default **1,000 ms**, valid **1..=60,000
ms**. It is an operator assertion, not something Rockbee can prove at runtime:
at a TTL command's issuance and a later expiry observation,
`abs(node_utc_clock - real_utc) <= epsilon` must hold. Nodes with a broken NTP
clock violate the contract and can cause an early irreversible expiry.

`StorageIdentity` must remain its existing postcard v1 shape; do not append
epsilon to it. Add a separately framed, versioned `RBRTTLC1` cluster TTL config
record in the **metadata Raft group** containing only epsilon and its format
version. A new apply-time `InitializeTtlConfigIfAbsent` metadata command writes
it once. During a quiescent all-node TTL adoption, the metadata leader commits
that record from the explicit startup value; every node strongly reads it before
opening TTL data groups and refuses a missing/different local value. Existing
clusters without this record start in TTL-disabled mode unless this explicit
adoption operation is used. This is a replicated immutable configuration record,
not an uncoordinated local `StorageIdentity` change.

Startup is deliberately staged to avoid a dependency loop: first start only
metadata group 0 and its private peer path, initialize/read `RBRTTLC1` through
that Raft group, then run the local bounded migration for each data group while
public TTL admission and all data Raft groups remain stopped. Only after every
data group passes migration/validation may the node start data Rafts and accept
public TTL traffic. Group 0 is never migrated.

Each node uses `SystemTime` only to *propose* expiry work. The leader calculates
the exact checked timestamp `expires_at = issued_at + ttl_millis`, then treats
the value as due only when the checked predicate
`now >= issued_at + ttl_millis + 2 * epsilon` holds. The two error margins cover
a slow issuing clock and a fast observing leader. Under the stated absolute-UTC
assumption, this gives conditional at-least-TTL behavior; it does not prove the
assumption. Without it, TTL is merely a time-domain best effort. It can delete
later because of the margin, leader unavailability, Raft quorum loss, request
load, scheduler cadence, backward clock movement, or ordinary Raft commit time.

`GET` remains a linearizable leader read:

1. call `ensure_linearizable()` and read the coherent local state view;
2. if the record is live, return its raw value;
3. if its expiry satisfies the checked UTC predicate above, submit
   `ExpireIfMatch` and wait for its Raft outcome;
4. after an `Expired` or a conditional no-op, re-run the same strong read under
   the original request deadline. Return `None` only after the replicated state
   says it is absent.

If the expiry proposal loses leadership, times out, or loses its response after
submission, its Raft future and permit stay owned until completion. The GET
returns `ReadFailure` unless a fresh authoritative reconciliation finishes under
the original deadline; it never fabricates `None` from an uncertain outcome.

Consequences that must be documented: a value may be returned during the small
interval before a leader observes it due; once a committed expiration is seen,
no later strong GET can return the old value. A read never hides a value based
only on a replica's wall clock. Group 0's directory key has no TTL path.

## Safety model and counterexamples

### Invariants

1. Every logical value transition is one committed command and one synchronous
   RocksDB `WriteBatch` containing data, metadata/index changes, and the new
   `StateRecord` applied position.
2. `ExpireIfMatch` deletes only when both the current version and `expires_at`
   equal the command's expected values. A stale index item is harmless.
3. Once an expiry command applies, its raw data and metadata are deleted
   durably. A backward clock can delay a future proposal, but cannot resurrect
   an already committed deletion.
4. Snapshot build/install/recovery preserve data, record metadata, expiry index
   semantics, applied log identity, membership, and format marker as one state
   machine generation.
5. No replica's compaction/filter/sweeper directly removes a logical record.
   Only an applied Raft command can do that.

### Concrete failures this excludes

* **Per-replica read filter:** A has a fast clock and returns absent, then B
  becomes leader with a slow clock and returns the old bytes. Prohibited because
  absence follows a committed `ExpireIfMatch`, not local filtering.
* **Old timer after rewrite:** a timer made for version 17 fires after a normal
  `put()` version 18. Its expected version differs, so apply returns no-op and
  preserves version 18.
* **Same bytes, new value:** after version 17 expires, a later `put()` may write
  identical opaque bytes as version 18 and GET may rightly return them. The
  no-resurrection claim applies to the expired version, not byte equality.
* **Restart/backward clock:** a smaller fresh wall clock can postpone a new
  expiry proposal, but it cannot turn an already committed deletion back into
  a raw record.
* **Compaction race:** RocksDB may drop obsolete SST records only after a Raft
  apply deleted them. It never converts a still-live key into a missing key.

## Wire, log, and on-disk representation

### Public protocol, backward shape

In `crates/rockbee-raft-protocol/src/kv/request.rs`, append, never insert,
this variant after existing v1 tags:

```rust
PutWithTtl { key: Bytes, value: Bytes, ttl_millis: u64 }
```

Update `KvRequest::{validate, retained_bytes, expected_response}` in
`wire_request.rs`, routing key extraction, response validation, gateway routing,
and all exhaustive matches. Existing `Put`, `Get`, and `Delete` tags and raw
bytes remain unchanged. Public protocol version must be bumped deliberately or
feature-negotiated before mixed versions are supported; version one nodes may
decode the new enum as invalid, so rolling old/new node operation is explicitly
unsupported. Require a quiescent, all-node binary upgrade for the TTL release.

### Raft command

Append these variants after existing `Put` and `Delete` in
`apps/rockbee-raft-node/src/raft/types.rs` and `rpc/dto.rs`'s private
`WireCommand` (again preserving old discriminants):

```rust
PutWithExpiry {
    key: Bytes,
    value: Bytes,
    expires_at_millis: u64,
    issued_at_millis: u64,
},
ExpireIfMatch {
    key: Bytes,
    expected_version: LogId,
    expected_expires_at_millis: u64,
    observed_at_millis: u64,
},
```

Every non-TTL write—both the historical `Command::Put` tag and a new ordinary
public `KvRequest::Put`—writes `RecordMeta::NonExpiring` and removes a prior
expiry index atomically. Only `PutWithExpiry` writes
`RecordMeta::Expiring { version: entry.log_id, expires_at_millis }`.
Group 0 continues using its original raw Put path. This canonical rule is
independent of local migration timing: a caught-up node that migrates and a
lagging node that imports an old v1 snapshot then replays the same historical
log reach identical logical metadata at the same applied log ID. No extra
`PutRecord`/`PutV2` Raft command is needed.

The expiry timestamp is calculated only by `NodeService` before the single
`client_write`; no follower recalculates it. Command validation checks key/value
limits, a strictly positive `expires_at_millis - issued_at_millis` in
`MIN_TTL_MILLIS..=MAX_TTL_MILLIS`, and checked `2 * epsilon`/due-threshold
arithmetic before Raft submission. `ExpireIfMatch.observed_at_millis` must also
be at or after that same computed due threshold; therefore apply can reject an
early command without calling a clock. The peer DTO's preflight size estimator
must account for both new variants before allocating or cloning an append frame.

Keep every existing `ApplyResult` tag, including `Applied` at tag zero and any
metadata variants introduced before TTL. Append explicit expiry results:

```rust
pub enum ApplyResult {
    Applied,
    Expired,
    ExpirationSkipped,
}
```

Existing PUT/DELETE results remain `Applied` and map to `Committed`; the
internal GET and sweeper use `Expired`/`ExpirationSkipped` only to decide
whether to reread. Do not replace the enum or renumber old variants.
No user-visible response needs to expose Raft `LogId` yet.

### State keys, raw-value compatibility, and versions

Do **not** wrap an old user value in a new value envelope. Existing
`keys::data(group, generation, key)` continues storing exactly opaque user
bytes. Add two private prefixes in `storage/keys.rs`:

```text
data       = tag 1 | generation | raw key      -> raw user Bytes
record     = tag 2 | generation | raw key      -> RecordMeta::{NonExpiring, Expiring}
expiry     = tag 3 | generation | expiry_be | raw key -> expected version
format     = tag 4 | generation              -> StateFormat::TtlV2
journal    = tag 5 | generation              -> MigrationJournal::V1ToV2
```

`RecordMeta` has exactly two valid shapes: `NonExpiring { version: None,
expires_at: None }` for every legacy or ordinary PUT, and `Expiring {
version: Some(LogId), expires_at: Some(u64) }` for a TTL PUT. Only the latter
has an expiry index. This makes the state canonical across log replay and local
migration while still fencing stale expiry commands.
`expiry_be` gives ordered due-time scans. A normal legacy value is therefore
non-expiring but distinguishable from corruption. New PUT/DELETE/TTL commands
create/delete the two auxiliary records in the same batch as the raw value.

Do not add a field to `StateRecord`: postcard does not promise an older tuple
will decode merely because a new field has `serde(default)`. Preserve its
current encoding. Instead use the explicit per-generation `StateFormat::TtlV2`
marker. During the quiescent all-node TTL upgrade, a resumable bounded startup
migration scans every **data-group** v1 raw data row and writes
`RecordMeta::NonExpiring`; it publishes the V2 marker
only after every row is materialized. A crash before publication resumes the
same idempotent migration; **data-group** Raft startup and public admission are
not allowed until it completes. Metadata group 0 starts first as described
above. The journal is written before the first batch. The final migration
batch writes the V2 marker **and deletes the journal in one synchronous
RocksDB `WriteBatch`**; a crash cannot expose a marker-with-journal gap. An
explicit adoption may start an unmarked migration only
when **all** TTL auxiliary prefixes are absent. It may resume only when this
journal exists, no expiry-index rows exist, and every already materialized
record is `NonExpiring`; an unmarked store with a
`Some(version)`, any expiry index, or a stale V2 marker/journal combination
fails closed. Thus deleting a V2 marker can never reinterpret real TTL metadata
as legacy non-expiring data. In a V2 generation,
every raw row must have valid metadata, non-expiring rows must have no expiry
index, and expiring rows must have exactly one matching index. Missing metadata
in V2 is corruption and fails closed; it is never silently treated as immortal.

A new state reader returns a private
`StoredRecord { value, version: Option<LogId>, expires_at_millis }` rather than
an independently interpreted timestamp. Directory reads continue using a small
raw-value helper. Group 0 never runs v1->v2 migration and must reject every TTL
record/index/format/journal key.

### Snapshot and recovery migration

The current `RBRSNAP1` stream records only raw data keys, so it cannot carry a
TTL record/index safely. Add a version-two snapshot magic/header and keep a
strict v1 decoder:

* **group 0:** continues using its current raw `RBRSNAP1` image/`RBRDESC1`
  descriptor path and rejects TTL auxiliary state. It is metadata, never a TTL
  data generation.
* **v1 import:** import each old data row as a non-expiring raw value with an
  explicit `RecordMeta::NonExpiring`, then publish V2;
  this applies only to data groups.
* **v2 export:** stream each logical record as key, raw value, `RecordMeta`,
  with an explicit non-expiring marker. Rebuild the ordered expiry index while
  importing each bounded batch; do not serialize a duplicate index as source of
  truth.
* install writes the v2 generation plus rebuilt index, descriptor, state record,
  applied/membership and format marker in the same existing publication
  discipline.

The current snapshot descriptor is independently encoded as `RBRDESC1`; image
and descriptor version cannot be inferred from each other. Add `RBRDESC2` for
TTL data images and make `read_descriptor()` dispatch strictly on either magic:
`RBRDESC1` may reference only `RBRSNAP1`; `RBRDESC2` may reference only the
new TTL image/header. Recovery rejects an unknown magic, a mixed descriptor and
image, or a v2 descriptor without the data-generation format marker. The v1
descriptor remains readable for an old data image and is converted only by the
bounded migration or v1 snapshot import; never rewrite it optimistically during
open. Equal-progress validation uses the selected descriptor/image format's
full logical rows and metadata.

Snapshot build reads raw value and `RecordMeta` from one RocksDB MVCC view. Its
same-progress/equal-log identity checks compare both logical value bytes and
metadata, not just raw rows. Startup validation, generation allocation and
`collect_obsolete()` must cover tags 1, 2, 3, 4 and 5 together: a retired
generation cannot retain a live expiry index, metadata record, format marker or
migration journal.
V2 install imports to the inactive generation, validates every correspondence,
then atomically activates it. Held MVCC snapshot tests must prove old
generation keys of every tag survive until their safe reclamation point.

Existing pre-TTL RocksDB state is upgraded through the explicit bounded v1->V2
migration above; old log entries decode through preserved enum tags. A new TTL
log cannot be decoded by an old node, hence the all-node upgrade rule.

## Small, Java-readable module boundaries

No framework or inheritance hierarchy is needed. Keep functions short and name
the ownership boundary.

```text
protocol/kv/request.rs       public request validation
client/api/client.rs         eager public put_with_ttl admission
node/ttl/time.rs             **new:** UTC clock boundary and deterministic test clock
node/ttl/sweeper.rs          **new:** leader-owned bounded due-index scan/proposal
node/cluster/service.rs      **existing:** leader timestamp construction/strong GET loop
node/raft/types.rs           **existing:** replicated command/result types
node/storage/record.rs       **new:** RecordMeta encoding and transition helpers
node/storage/state.rs        **existing:** apply commands/coherent read views
node/storage/keys.rs         **existing:** stable private prefixes
node/storage/snapshot/*      **existing:** v1 import and v2 logical-record stream
node/metrics/ttl.rs          **new, only with exporter:** fixed-name metric inputs
```

Proposed interfaces:

```rust
trait TimeSource: Send + Sync {
    fn unix_millis(&self) -> Result<u64, TimeError>;
}

struct StoredRecord {
    value: Bytes,
    version: Option<LogId>,
    expires_at_millis: Option<u64>,
}

impl StateReader {
    async fn read_record(&self, key: Bytes) -> Result<StateRead, StoreError>;
    async fn due_expirations(
        &self,
        due_through_millis: u64,
        limits: SweepLimits,
    ) -> Result<Vec<ExpiryCandidate>, StoreError>;
}

async fn expire_due_record(
    service: &NodeService,
    group: GroupId,
    candidate: ExpiryCandidate,
    deadline: Instant,
) -> ExpireAttempt;
```

`TimeSource` is injected only at node construction in tests. Production uses a
tiny `SystemClock`; no test-only environment variable changes behavior. The
state apply function accepts no clock: all time is already in the command bytes.

## Bounded expiry scheduler and physical cleanup

Every hosted **data** group owns one cancellable scheduler task. It starts only
after the node runtime is ready, watches OpenRaft leadership, and stops/joined
with `NodeRuntime::close`. It does not run for group 0.

On leadership it:

1. obtains a linearizable barrier before scanning its local expiry index;
2. scans only a configured `SweepLimits { max_entries, max_key_bytes,
   max_in_flight }`, ordered by due timestamp;
3. for each candidate, issues at most one bounded `ExpireIfMatch` proposal;
4. treats lost leadership/timeouts as no result and retries only by a fresh scan
   after backoff, never by replaying the prior proposal;
5. yields between batches, has a minimum wake interval, and uses the same
   process/group admission accounting as other internal work.

Initial limits are fixed and validated: **256 index entries**, **1 MiB decoded
candidate bytes**, **16 in-flight expiry proposals per group**, **64 concurrent
expiry proposals process-wide**, and one scan per **100 ms** at most. The shared
process semaphore is acquired before retaining a candidate/proposal and is kept
until the underlying Raft proposal completes, including when its observer times
out. The apply-chunk byte accounting must charge metadata encoding, removal of
the prior expiry index, insertion of the new expiry index and the rewritten
`StateRecord`, not only the inbound Raft entry bytes. Same-key commands within
one apply chunk consult an overlay so later commands see earlier batch effects.

The scheduler's candidates are only hints. `ExpireIfMatch` checks the current
replicated `RecordMeta`, which protects renewal, delete, snapshot catch-up, and
an old scan. A caller GET uses the same helper and original deadline; it must
not wait behind an unbounded cleanup backlog.

Logical expiration deletes raw data, record metadata and expiry index during
state-machine apply. RocksDB's normal compaction subsequently reclaims obsolete
SST bytes. Do not use a TTL compaction filter or add manual compaction to the
first TTL milestone. A manual-compaction coordinator is deferred until disk and
write-latency measurements demonstrate a need; it must never be introduced as
a semantics fix.

## Metrics

The metrics exporter work is separate but TTL supplies fixed-cardinality inputs:

```text
rockbee_ttl_due_candidates_total
rockbee_ttl_expire_committed_total
rockbee_ttl_expire_skipped_total
rockbee_ttl_sweep_errors_total
rockbee_ttl_sweep_backlog_entries
rockbee_ttl_sweep_duration_seconds
```

Labels may be bounded `group_id` and outcome; never key, value, deadline, or
request ID. Client HDR latency remains a benchmark/reporting tool, not a server
metric and not a substitute for these counters.

## Staged implementation and gates

### Task 1 — protocol and eager client contract

**Files:** `protocol/src/kv/{request,wire_request}.rs`, protocol tests;
`client/src/api/client.rs`, client contract tests and Rustdoc.

- [ ] Add failing tests for zero, max, over-max, fractional-millisecond and
  `Duration` conversion overflow; existing PUT tags/frames must deserialize
  byte-for-byte unchanged.
- [ ] Add `PutWithTtl` at the end of the enum and `RockBeeRaftClient::put_with_ttl`.
- [ ] Prove it is eager, shares bounded link admission, dropped handle does not
  cancel it, and a post-handoff failure remains `Unknown`.
- [ ] Run focused protocol/client tests, formatting, strict Clippy. Advisor
  challenges the public semantics; verifier attacks decoder and certainty cases.

### Task 2 — deterministic time and replicated command shape

**Files:** `node/src/ttl/time.rs`, `raft/types.rs`, `rpc/dto.rs`, node unit tests.

- [ ] First write failing controlled-UTC tests for the 1 ms/365-day endpoints,
  fractional-millisecond rejection, checked expiry and `2 * epsilon` overflow,
  and a shared forward clock jump outside the operator assumption.
- [ ] Append `PutWithExpiry`/`ExpireIfMatch` after every existing command/result
  tag, preserve legacy serde tags, and add DTO decode and exact size-bound tests.
- [ ] Test every replica receives identical expiry bytes from one leader; no
  apply code calls `SystemTime`; an internal uncertain expiry read returns a
  read failure instead of `None`.
- [ ] Test `RBRTTLC1` init-if-absent: equal adopted epsilon is accepted;
  a different/missing local value refuses data-group TTL startup; existing
  `StorageIdentity` bytes remain unchanged.
- [ ] Run node unit tests and peer protocol adversarial tests. Review current
  pinned OpenRaft source before relying on a response type.

### Task 3 — atomic metadata/index state machine

**Files:** `storage/{keys,record,state}.rs`, `storage/tests.rs`.

- [ ] Start with failing atomic-restart tests for TTL write, ordinary overwrite
  clearing expiry, TTL renewal, delete, stale `ExpireIfMatch`, and two changes
  to the same key in a single apply chunk.
- [ ] Add the canonical replay regression: node A materializes an old
  `Command::Put` during migration; node B imports an older v1 snapshot then
  replays that same old Put tail. At equal applied `LogId`, raw bytes and full
  `RecordMeta` must be identical (`NonExpiring`), so equal-progress
  snapshot validation accepts both states.
- [ ] Implement one `apply_command` helper that reads current metadata through
  an apply-local view/overlay and writes raw bytes, metadata/index and
  `StateRecord` together. It returns explicit `ApplyResult`.
- [ ] Prove a fault at every RocksDB persistence boundary leaves either the old
  complete state or the new complete state, never orphaned raw data/index.
  Charge metadata/index output and old-index deletion in the batch budget.
- [ ] Run storage suite, OpenRaft storage suite, strict Clippy; advisor reviews
  stale-index and log-version invariants, verifier checks chunk overlay.

### Task 4 — strong GET and leader scheduler

**Files:** `cluster/service.rs`, `ttl/sweeper.rs`, runtime lifecycle tests.

- [ ] First fail tests showing a due record is not returned without a committed
  expiry and a non-due record is returned unchanged.
- [ ] Implement timestamp issue immediately before `client_write`, the bounded
  GET expiry/re-read loop, leader-only sweeper, and shutdown joining. Preserve
  proposal ownership after a GET observer deadline/disconnect.
- [ ] Add controlled-clock tests for the absolute-error endpoints, leader
  switch, backward clock, timer firing after rewrite/delete, and a shared clock
  jump outside the operator assumption. The test must assert Raft command
  history, not just an eventual `None`.
- [ ] Verify `ensure_linearizable` plus applied-state check is retained before
  every user-visible TTL decision. Run real three-node network tests.

### Task 5 — snapshots, upgrade, and crash recovery

**Files:** existing `storage/snapshot.rs` and
`storage/snapshot/{format.rs,tests.rs}`, storage recovery tests, operations
documentation.

- [ ] Write failing v1 import, v2 round-trip, partial v2 install, corrupted
  TTL metadata/index, descriptor/image format mismatch, group-0 TTL auxiliary
  key rejection, and snapshot-build-during-renewal tests.
- [ ] Implement versioned decoder/encoder, bounded v1 local-state migration,
  v2 index rebuild, generation format markers, and published-generation
  validation.
- [ ] Prove process SIGKILL/reopen and snapshot install cannot resurrect an
  already committed expiration or lose a later normal PUT.
- [ ] Prove a deleted V2 marker cannot be mistaken for v1: startup fails when
  `Some(version)`/expiry state exists without the marker, while a genuinely
  journaled partial legacy migration resumes boundedly.
- [ ] Inject a crash between the last legacy batch and publication. Reopen must
  see either the journal without marker and resume, or marker without journal;
  it must never observe both after the final synchronous batch.
- [ ] Document all-node upgrade, unsupported mixed binary versions, time-sync
  prerequisite, bounded-delay semantics, and recovery/operator actions.

### Task 6 — bounded scheduler, metrics, and soak/failure matrix

**Files:** `ttl/sweeper.rs`, metrics exporter inputs,
`tests/support/soak.rs`, process/failure tests and `failure-matrix.md`.

- [ ] Build a deterministic three-node soak: concurrent short/long/no-TTL
  keys, renewals/deletes, leader kills/restarts, client disconnects and slow
  readers. Track an in-memory reference model driven by the same deterministic
  clock and only compare after each completed strong operation.
- [ ] Assert fixed admission/queue bounds, no unowned child process, no
  automatic replay of Unknown writes, and monotonic logical absence after an
  expiry commits.
- [ ] Add a short-TTL burst; prove scheduler backlog drains in bounded batches,
  normal RocksDB compaction does not alter logical semantics, and reads/writes
  retain their semantics under cleanup pressure.
- [ ] Export bounded metrics, run workspace tests/format/strict Clippy, then
  advisor and verifier jointly review source plus the real three-node evidence.

## Acceptance catalogue

| ID | Scenario | Required observable result |
|---|---|---|
| TTL-01 | normal `put()` after TTL value | later GET returns new bytes; old timer cannot remove it |
| TTL-02 | TTL renewal before first expiry | only renewed deadline/version can expire the value |
| TTL-03 | delete then old expiry command | DELETE remains absent; old command is safe no-op |
| TTL-04 | lost mutation response after submit | client sees Unknown; no automatic resubmit; later strong GET observes one valid state |
| TTL-05 | leader changes while due candidate waits | new leader scans/reconciles; old leader cannot create resurrection |
| TTL-06 | node clock moves backward/restarts | expiry can be delayed; a committed expiry stays absent |
| TTL-07 | group snapshot build/install/reopen | v1 legacy no-TTL data remains readable; v2 TTL metadata and deletion semantics survive |
| TTL-08 | partial/corrupt snapshot or RocksDB fault | fail closed or recover old/new whole generation; never fabricate expiry/value |
| TTL-08a | missing V2 marker after TTL state exists | startup fails; it must not materialize a `Some(version)` record as legacy |
| TTL-09 | millions of due keys / slow client | fixed scan bytes/count plus group and process-wide proposal bounds; healthy groups keep progress |
| TTL-10 | no quorum at deadline | no false absent value from local time; later quorum causes committed expiry |
| TTL-11 | three owned node processes, repeated kill/reopen | values/deletes/expirations converge to reference model with no leaked processes |
| TTL-12 | short-TTL burst then normal compaction pressure | logical state is unchanged by physical compaction; scheduler stays bounded |

## Explicit non-goals

This plan does not promise a precise wall-clock deletion time, cross-key atomic
expiry, follower eventual reads, TTL update without a value write, old/new
binary rolling operation, manual compaction control, or an exact disk-byte
reclamation deadline. Fixed first-release values are `epsilon=1,000 ms`,
`epsilon` valid range `1..=60,000 ms`, scan interval `100 ms`, 256 entries,
1 MiB decoded candidates, 16 in-flight expiry proposals per group and 64 per
process. Any proposal to make replicas locally filter/delete expired keys must
be rejected unless it first proves no leader change or clock-skew counterexample
exists.

## Self-review

All requested areas are assigned: eager public API (Task 1), timestamp and wire
consistency (Task 2), stale conditional/deletion correctness (Task 3), strong
read and bounded leader work (Task 4), snapshots/restarts/migration (Task 5),
and physical cleanup/metrics/soak (Task 6). The highest-risk inputs—lost
acknowledgement, leader change, backward clock, stale expiry, partial snapshot,
and expiry burst—each have an acceptance row. No semantic correctness relies on
RocksDB compaction or an unreplicated wall-clock decision.
