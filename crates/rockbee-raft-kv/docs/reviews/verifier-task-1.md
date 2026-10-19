# Task 1 independent spec and quality review

Reviewed 2026-10-19 against `docs/tasks/task-1-brief.md`, the completed
implementer report, and `reports/task-1-review.diff`. Subsequent Task 3 engine
dependency declarations are outside this review; they were not mistaken for
Task 1 changes. No production edits, commits, or pushes were made.

```text
Complete: typed byte-KV messages -> validation -> bounded frame codec
Pending:  Client -> Gateway -> multiple Raft groups -> RocksDB
```

**Spec verdict: APPROVED.**

**Quality verdict: APPROVED.**

No actionable severity/file/line findings remain. Approval covers this protocol
layer, not transport certainty, Raft, storage durability, or a working database.

## Review evidence

- Private checked serde conversions preserve nonzero request/node/cluster IDs
  through decoding. Group zero is representable for metadata but routing uses
  a validated count `1..=256` and adds one after modulo, excluding metadata.
- Seeded FNV-1a64 uses the required offset basis, prime and wrapping multiply.
  Routing contains no node-membership input; independent fixed vectors are in
  the implementer tests. Hash version/count are validated during serde.
- Request and response validation enforce the brief's semantic limits and
  operation/ID/version correlation. Received NotSent is rejected; explicit
  Rejected and Unknown retain their separate meanings. Missing and empty stored
  values remain distinct.
- `codec.rs:14` performs a bounded counting pass before exact-sized allocation.
  The counting invariant never exceeds 2 MiB; the second pass writes through a
  fixed slice, so serializer size changes cannot grow the output buffer.
  `codec.rs:44` rejects outer-length mismatch and leftover postcard bytes.
- Inspected the actual cached postcard 1.1.3 `deserialize_byte_buf` and
  `deserialize_string` implementations: they take the declared slice only after
  comparing it to remaining input, before calling the allocating visitor.
  The cached bytes 1.12.1 visitor copies that validated slice. Supported envelope
  fields therefore cannot allocate proportional to a huge length in tiny input.
- The root's trusted-generic Deserialize ruling is documented on the helper.
  No unsupported promise of sandboxing custom visitors, arbitrary collection
  counts, or recursive schemas is introduced. Semantic validation remains an
  explicit separate trust-boundary call, as required by the brief.
- Captured Task 1 manifests/lockfile introduce only bytes, serde, postcard and
  thiserror with their transitive dependencies; no engine or existing Rockbee
  production dependency is introduced by that diff.

## Independent adversarial checks

Added `crates/rockbee-raft-protocol/tests/verifier_adversarial.rs` to close two
specific gaps in the existing coverage:

1. Encode/decode/validate an actual PUT at maximum key and value sizes, maximum
   request ID, and maximum budget. Truncate its value while keeping a legal outer
   frame length and require inner-field decoding to fail.
2. Attack actual PUT value, GET response value, and mutation diagnostic length
   fields with a tiny frame declaring `u64::MAX` bytes. Reject unknown operation,
   response, and failure-reason tags. Existing tests already attack GET key and
   ReadError diagnostic lengths.

Root authorized an isolated protocol-only Cargo run while native engine work
used the main target directory:

```text
CARGO_TARGET_DIR=/Users/umitunal/IdeaProjects/rockbee/crates/rockbee-raft-kv/target/verifier-task-1 \
  cargo test --offline --locked -p rockbee-raft-protocol --test verifier_adversarial

running 2 tests
test hostile_lengths_and_unknown_variants_in_supported_envelopes_are_rejected ... ok
test maximum_put_survives_actual_wire_encoding_and_validation ... ok
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

`rustfmt --edition 2024 --check` for the added test file exits zero. The existing
nine-test suite, workspace tests, formatting, and protocol Clippy are recorded
in the implementer's report and were inspected rather than redundantly rerun.
The independent tests prove rejection/roundtrip behavior; allocation ordering
was assessed from the concrete dependency implementation, not claimed from an
allocation profiler.

Future transport/server code must call semantic validation before dispatch and
reserve its receive/response memory before allocation. Those are owning Task 2
and later integration obligations, not missing Task 1 behavior.
