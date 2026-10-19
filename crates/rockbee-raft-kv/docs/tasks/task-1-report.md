# Task 1 report: validated protocol and stable sharding

Status: **DONE**, subject to independent review. No commits or pushes.

## Implemented scope

```text
Client [scaffold]
  -> typed request -> bounded frame codec + validation [Task 1 complete]
  -> gateway [unfinished] -> node / Multi-Raft / storage [unfinished]
```

This task implements the engine-free protocol package in the independent
`rockbee-raft-kv` workspace. It does not implement network handoff, dropped/deadline
mutation behavior, a Raft engine, storage, or gateway/node behavior. Existing
Rockbee production packages were not edited. No native dependency was built.

Files, relative to `crates/rockbee-raft-kv`:

- `Cargo.toml`: workspace declarations for bytes/serde/postcard/thiserror.
- `Cargo.lock`: generated dependency resolution; postcard is pinned to 1.1.3.
- `crates/rockbee-raft-protocol/Cargo.toml`: only the required protocol dependencies.
- `crates/rockbee-raft-protocol/src/lib.rs`: documented public exports.
- `crates/rockbee-raft-protocol/src/ids.rs`: validated typed identifiers.
- `crates/rockbee-raft-protocol/src/wire.rs`: messages, failure semantics, validation.
- `crates/rockbee-raft-protocol/src/shard.rs`: versioned seeded FNV-1a64 routing.
- `crates/rockbee-raft-protocol/src/codec.rs`: strict framing and bounded encoding.
- `crates/rockbee-raft-protocol/tests/protocol.rs`: nine external behavior tests.
- `docs/tasks/task-1-report.md`: this report.

## Test-first evidence

All Cargo commands below ran from `crates/rockbee-raft-kv`, not the outer Rockbee
workspace. The tests were written before protocol implementation. The first
attempt could not reach compilation because the offline cache lacked a package:

```text
$ cargo test -p rockbee-raft-protocol --offline
error: failed to download `cobs v0.3.0`
Caused by:
  attempting to make an HTTP request, but --offline was specified
```

The normal sandbox network attempt also failed before tests:

```text
$ cargo test -p rockbee-raft-protocol
error: failed to download from `https://static.crates.io/crates/embedded-io/0.4.0/download`
Caused by:
  [6] Couldn't resolve host name (Could not resolve host: static.crates.io)
```

Sandbox escalation succeeded and downloaded `embedded-io` 0.4.0 and 0.6.1,
`cobs` 0.3.0, and `postcard` 1.1.3. The intended first red was then observed:
the real integration tests could not import the missing public API from the
initial scaffold. This was a compilation red, not a runtime assertion red.

```text
$ cargo test -p rockbee-raft-protocol
error[E0432]: unresolved imports `rockbee_raft_protocol::ClusterId`,
`rockbee_raft_protocol::Failure`, ... `rockbee_raft_protocol::encode_frame`
error: could not compile `rockbee-raft-protocol` (test "protocol") due to 1 previous error
```

After the first implementation, a focused allocation regression was added
before changing the encoder from a maximum-size buffer to exact-size allocation.
The new assertion failed against the old implementation:

```text
$ cargo test -p rockbee-raft-protocol --offline frame_serialization_has_a_hard_payload_bound
running 1 test
test frame_serialization_has_a_hard_payload_bound ... FAILED
assertion failed: encode_frame(&42_u8).unwrap().capacity() <= 64
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 8 filtered out
```

The smallest fix was a bounded counting pass followed by serialization into an
exact-size fixed slice. All nine tests then passed. An intermediate Clippy run
reported four `unwrap_used` errors in test fixture helpers; these were replaced
with explanatory `expect` calls. Production code uses fallible propagation.

## Final verification and actual results

```text
$ cargo test -p rockbee-raft-protocol
Finished `test` profile [unoptimized + debuginfo] target(s) in 0.09s
running 9 tests
test frame_codec_rejects_bad_lengths_truncation_and_trailing_data ... ok
test failure_messages_are_bounded_by_utf8_bytes_even_after_decode ... ok
test opaque_bytes_and_empty_value_round_trip ... ok
test shard_mapping_matches_independent_fixed_fnv_vectors ... ok
test invalid_identifiers_cannot_be_constructed_or_deserialized ... ok
test tiny_frames_cannot_request_huge_collection_allocations ... ok
test requests_enforce_exact_field_and_budget_boundaries ... ok
test responses_correlate_ids_versions_and_operation_kinds ... ok
test frame_serialization_has_a_hard_payload_bound ... ok
test result: ok. 9 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

Protocol library unit tests and doctests each contained zero tests and passed.

```text
$ cargo test --workspace --offline
test result: ok. 9 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

The workspace command also compiled client, gateway, and node scaffolds. Their
unit/doctest targets each contained zero tests and passed; they do not demonstrate
working client/network/Raft/storage behavior. There were no failing tests.

```text
$ cargo fmt --all -- --check
# no output; exit 0

$ cargo clippy -p rockbee-raft-protocol --all-targets -- -D warnings
Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.06s
# exit 0
```

## Decisions and review concerns

- `RequestId`, `NodeId`, and `ClusterId` use private fields and checked serde
  conversions. Zero IDs cannot enter through constructors or deserialization.
  `GroupId(0)` remains representable as the reserved metadata group.
- Requests expose fields and have a validating constructor plus explicit
  `validate()`; decoding is deliberately separate from semantic validation.
  Responses validate their version, original request, correlation ID, operation
  kind, value length, and diagnostic length in `validate_for()`.
- `NotSent` remains serializable for local outcome handling but is always
  invalid in a received mutation response. A remote service proving rejection
  before consensus submission must use `Rejected`; `Unknown` preserves the
  possibility of commitment. Request IDs provide no deduplication or replay.
- Sharding uses fixed published FNV-1a64 vectors plus independently computed
  seeded/binary fixtures; expected results are literal constants, not derived
  from production code. Hash version and shard count are checked during serde.
  Node membership is absent from the routing inputs. FNV is explicitly
  documented as a non-security hash.
- Encoding rejects payload overflow while counting, before allocating output,
  and writes the second pass into a fixed exact-size slice. Postcard's exported
  `experimental::serialized_size` was inspected but not used: its `Size` flavor
  adds unchecked lengths and cannot stop an oversized stream. The small local
  `BoundedSize` flavor stops at the 2 MiB limit and also handles a serializer
  that continuously emits bytes without materializing an unbounded buffer.
- **Root-approved scope ruling:** untrusted wire decoding is supported for this
  crate's concrete protocol types, whose variable fields are `Bytes`/`String`.
  Postcard checks their declared inner lengths against actual input before
  allocation. Tests attack actual request key lengths and response diagnostic
  lengths as well as raw byte/string fields. The generic helper requires trusted
  `T`/serde implementations and does not claim to sandbox arbitrary maps,
  zero-sized sequences, or custom visitors. Future collection-bearing metadata
  schemas need explicit count/depth limits. A broader generic deserializer was
  removed following the root's simplicity ruling; this limitation is documented
  on `decode_frame` rather than hidden behind its generic signature.
- Exact limits and one-beyond values are tested for keys, values, budgets,
  diagnostics, and payload sizes. Missing values differ from empty values;
  opaque non-UTF-8 and zero bytes round-trip. Invalid frame headers, declared
  lengths, truncation, trailing frame/payload bytes, IDs, versions, and response
  kinds are rejected. Runtime mutation handoff/deadline semantics remain Task 2.

Independent review is still required. No storage process, LocalStack container,
existing data, staged IDE file, or `.herdr/` content was intentionally accessed
or modified by this task.
