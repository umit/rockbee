# Task 1b: Protocol readability report

Implemented 2026-10-19; independent review remains pending. Scope is the
`rockbee-raft-protocol` crate and this report. No runtime or performance claims.

```text
Complete: typed KV -> envelope -> bounded encode/decode -> semantic validation
Pending:  Client -> Gateway -> multiple Raft groups -> RocksDB
```

## Readable source flow

- `ids.rs`: explicit `RequestId` and `NodeId` definitions, constructors, checked
  `TryFrom<u64>` and infallible `From<Id>` implementations replace `nonzero_id!`.
  Private fields and checked serde conversions remain intact.
- `request.rs`: PUT/GET/DELETE payloads and key/value validation.
- `response.rs`: read results, mutation certainty, operation matching and payload
  validation. `MutationOutcome::validate_wire` owns the received-outcome rules,
  including rejection of client-local `NotSent`.
- `envelope.rs`: public envelope constructors and validation entry points.
  Requests check version, budget, then payload. Responses validate the original
  request, version, request ID, then operation compatibility and payload.
- `failure.rs`, `error.rs`, `limits.rs`, `validation.rs`: bounded remote failures,
  local errors, version/field limits, and shared field checks formerly mixed into
  `wire.rs`. Removed the now-redundant private `wire` module.
- `shard.rs`: named FNV offset basis/prime and maximum shard count;
  `group_for_key` shows key validation, seeded hash calculation, then mapping.
- `codec.rs`: named four-byte header size. Bounded counting followed by fixed-slice
  serialization and supported-type decoder constraints remain unchanged.
- `lib.rs`: convenient crate-root reexports and one executable rustdoc example
  demonstrating construction, encoding, decoding and explicit validation.

All existing public names, constructors, methods and field/variant order remain
available at the crate root. New helper methods/constants are private or
crate-private; the source-file movements do not require caller import changes.

## Trait choice and behavior contract

Kept concrete inherent validation methods. A new `Validate` trait would duplicate
their signatures and wrappers without an existing generic consumer. Standard
`TryFrom`/`From` and serde traits already express useful shared behavior for IDs.
Task 2's agreed `WireRequest<Resp>` trait is the meaningful upcoming abstraction:
it will unify generic transport admission, retained-byte accounting, expected
response tokens and response matching. Its binding specification remains in
`docs/tasks/task-2-brief.md`; it is outside this refactor.

No dependencies, clones, allocations, locks, boxing, dynamic dispatch, tuning,
wire schema changes or validation work were added to production paths. Existing
error precedence is preserved, including invalid original requests before response
checks, operation mismatch before unrelated payload checks, and remote `NotSent`
before diagnostic validation. This assessment is based on source comparison and
contract tests, not allocation profiling or runtime benchmarking.

## Verification evidence

Before changing production source, added `tests/wire_v1.rs` with four fixed
literal frame fixtures, independently derived from v1 field order, enum tags,
unsigned LEB128 integers and byte/string length prefixes. The fixtures cover
binary PUT, Committed, binary Value(Some), and Unknown(Unavailable) responses.
They verify both exact encoding and decoding plus semantic validation.

All commands ran from `crates/rockbee-raft-kv`:

```text
CARGO_TARGET_DIR=target/protocol-readability cargo test --offline --locked -p rockbee-raft-protocol --test wire_v1
  BEFORE refactor: 4 passed, 0 failed

CARGO_TARGET_DIR=target/protocol-readability cargo test --offline --locked -p rockbee-raft-protocol
  AFTER refactor: existing 9 + independent 2 + golden 4 passed, 0 failed
  rustdoc: 1 passed, 0 failed

CARGO_TARGET_DIR=target/protocol-readability cargo test --offline --locked -p rockbee-raft-protocol --doc
  1 passed, 0 failed

cargo fmt --all -- --check
  exit 0

CARGO_TARGET_DIR=target/protocol-readability cargo clippy --offline --locked -p rockbee-raft-protocol --all-targets -- -D warnings
  exit 0
```

The first strict Clippy run identified `unwrap()` calls in new golden-fixture
helpers. Replaced them with descriptive `expect()` messages and reran strict
Clippy successfully, followed by the complete protocol suite and format check.
Existing protocol tests and the protocol manifest were preserved unchanged.
The isolated Cargo target avoided the root's native dependency build. No node
runtime, transport, storage integration, commit or push was performed here.
