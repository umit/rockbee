# Task 2 independent spec and quality review

Reviewed 2026-10-19 against the Task 2 brief, advisor/verifier transport contracts,
advisor readability review, implementer report, captured Task 2 diff and current
protocol/client source. Existing Rockbee was not inspected or changed for this
review. Native node/storage work remained outside the review/build target.

```text
Verified here: public client -> bounded multiplexed Link -> owned test peer
Pending:       public client -> Gateway -> Raft groups -> durable RocksDB
```

**Spec verdict: APPROVED.**

**Quality verdict: APPROVED.**

No actionable severity/file/line findings remain. Approval covers the tested
protocol/client boundary, not durable consensus or server response admission.

## Source assessment

- Generic defaulted envelopes preserve Task 1's public KV shape and field order.
  WireRequest supplies a Copy expected-response token and bounded-schema
  contract; pending entries do not keep the original body for reply validation.
- Submission is synchronous and bounded. Request count/metadata and body/frame
  reservations precede transport-owned allocation/enqueue. Sizing uses maximum
  ID/budget; the actual encoder cannot exceed the reserved payload capacity.
- Queued expiry and handoff use one state mutex and cannot both win. Handoff
  precedes the first write poll; the absolute deadline is checked after encoding
  and again before response delivery. A handed timeout retires the generation;
  partial frames never continue with another request. There is no replay or
  reconnect path in Link.
- Deadline entries are removed with completions; retirement clears pending
  state. Dropped observers and expired queue items retain their actual ownership
  reservations until those owners disappear. No per-request detached task or
  historical tombstone collection is introduced.
- RX validates the fixed header, acquires receive capacity, then allocates and
  decodes. Encoded/decoded overlap is covered; an unconsumed completion retains
  its count/receive permits. Returning a value transfers its ownership to the
  caller. Generic decoder and Bytes backing-allocation limits are explicit.
- Response ID, version, handed phase, expected kind, field bounds and deadline
  precede delivery. Remote NotSent is invalid and cannot create a false local
  non-delivery claim. Concrete response validation executes outside the pending
  map lock and state is checked again afterward.
- Clones share the same Inner, connection, IDs and budgets. Inner drop initiates
  retirement only when its final owner ends. Explicit close retires first, then
  joins owned workers; it retains each join handle while awaiting it, so a
  cancelled close does not lose ownership for a later close.

## Independent lifecycle coverage added

Coordinated with the transport implementer before editing. Added separate
`tests/verifier_lifecycle.rs` targets in the protocol and client packages:

1. **Client clone lifecycle:** one-operation admission is shared across clones;
   dropping the original leaves a clone usable; closing another clone retires
   all owners and resolves an already-handed mutation as Unknown without replay.
2. **Lazy GET contrast:** construct an unpolled GET, then submit an eager PUT.
   The peer must observe PUT first; only polling GET inserts it into the FIFO
   writer path. This uses operation ordering rather than a sleep as evidence.
3. **Cancel and reclose:** a controlled generic response validator holds the
   reader outside the pending lock. Poll close to Pending, cancel it, then poll
   another close. The second cannot finish until the reader is released, proving
   join ownership was retained. A release-on-drop guard also frees the fixture
   on failure. The handed observer receives justified transport uncertainty.

Ran only these new targets with owned localhost sockets and a separate target:

```text
CARGO_TARGET_DIR=/Users/umitunal/IdeaProjects/rockbee/crates/rockbee-raft-kv/target/verifier-task-2 \
CARGO_BUILD_JOBS=2 cargo test --offline --locked \
  -p rockbee-raft-protocol -p rockbee-raft-client --test verifier_lifecycle

client verifier lifecycle:   2 passed, 0 failed
protocol verifier lifecycle: 1 passed, 0 failed
```

Socket tests used the authorized sandbox escalation because the environment
blocks owned localhost listeners in the default sandbox. No approval rejection
occurred; no existing listener/process/container/storage was touched.

Targeted Clippy for both new test targets with `-D warnings` exits zero.
`rustfmt --edition 2024 --check` for both new files exits zero. The implementer's
19 transport tests, original 15 contract/golden/adversarial tests, four TCP client
tests and one rustdoc are documented in its report and were inspected rather
than redundantly rerun. Its package-specific formatting/Clippy checks pass;
concurrent node formatting is outside this task's affected source.

## Java-oriented documentation assessment

The protocol README accurately explains crate/module exports, struct/impl,
capability traits, enums, Result/Option, borrows, Bytes and Arc without claiming
language equivalence. It distinguishes eager mutation handles from lazy GET,
immediate admission from awaited outcome, clone drop from shared close, and
handoff from delivery/consensus. The macro section explains derive/Serde,
thiserror, Tokio test/select, compiler attributes, losing-future drop, and biased
selection. Keeping these macros while naming lifecycle functions is appropriate.

No new trait is needed merely for documentation. WireRequest has an actual
generic consumer and a second message-family test. Server pre-materialization
GET reservations, aggregate server limits, node proposal lifetime, quorum ACKs
and strong reads remain owning later-task integration gates. A fixture's
Committed reply establishes client mapping, not storage durability.
