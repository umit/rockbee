# Task 1: Validated protocol and stable sharding

Read this first; this is your requirements. Module root is
`/Users/umitunal/IdeaProjects/rockbee/crates/rockbee-raft-kv`.
Scope: protocol package, workspace dependency declarations and generated lockfile.
No other production package edits, no commits/push, no subagents. `.herdr/` is
forbidden. Existing Rockbee unchanged. Idiomatic readable Rust, no optimization.

Implement the first independently testable layer for a real Multi-Raft KV system.
It must stay engine-free. Use bytes (serde), serde (derive), postcard 1.1.3
(use-std), thiserror 2; introduce only dependencies needed by this task.

Files: protocol `src/{lib,ids,wire,codec,shard}.rs`, `tests/protocol.rs`.
Use focused modules; docs explain public API and fallibility to a Java-background
reader without Java-like inheritance abstractions.

Contracts:
- Typed RequestId and NodeId are nonzero u64; ClusterId is nonzero `[u8;16]`;
  GroupId is u64 with 0 reserved metadata. Constructors/accessors and serde must
  preserve validity. ShardConfig count 1..=256 and explicit u64 seed; metadata
  excluded from key routing. Node membership never changes key mapping.
- Hash FNV-1a64: initial 0xcbf29ce484222325 XOR seed, for each byte XOR then wrapping
  multiply 0x100000001b3. Group = hash modulo count + 1. Version explicit and
  validated; no Rust DefaultHasher. Document non-security purpose.
- Wire VERSION=1; maximum key64KiB, value1MiB, ordinary frame payload2MiB.
  Empty key invalid, empty value valid. Opaque Bytes preserve non-UTF8 and zeros.
- `KvRequest::{Put { key, value }, Get { key }, Delete { key }}`.
  `RequestEnvelope { version, request_id, budget_ms, request: KvRequest }`.
  Budget u64 in milliseconds, 1..=60_000 inclusive initially. Constructor validates;
  public `validate()` protects direct struct construction and decoded data.
- `ResponseEnvelope { version, request_id, response: KvResponse }`.
  `KvResponse::{Value(Option<Bytes>), Mutation(MutationOutcome), ReadError(ReadFailure)}`.
  `MutationOutcome::{Committed, NotSent(Failure), Rejected(Failure), Unknown(Failure)}`.
  Failure has typed reason + bounded message (1024 UTF8 bytes max). Reasons only
  relevant core transport/service cases: InvalidRequest, Overloaded, Deadline,
  Disconnected, Unavailable, NotLeader, WrongCluster, WrongGroup, Protocol, Internal.
  Use readable names; no route hint integration until owning Task6.
  Rejected means proven before consensus submission. NotSent means before local
  transport handoff. Unknown means may have committed. IDs are not dedupe.
- `ResponseEnvelope::validate_for(&RequestEnvelope)` validates version, ID,
  outcome size and response kind (GET needs Value/ReadError; mutations Mutation).
- `encode_frame<T: Serialize>(&T) -> Result<Vec<u8>, ProtocolError>` produces
  u32 BE length then postcard bytes; reject payload overflow before unbounded
  allocation. Implement bounded serialization using postcard fixed slice or
  bounded writer as appropriate; do not serialize unbounded then size-check.
- `decode_frame<T: DeserializeOwned>(&[u8]) -> Result<T, ProtocolError>` strictly
  requires declared full length, nonzero length <=2MiB and no trailing postcard
  bytes. Supported envelope Bytes/String fields must reject attacker-declared
  huge lengths from tiny input. Generic T is trusted application code; no promise
  covers arbitrary custom Deserialize or collections absent from these envelopes.
  Envelope semantic validation is separate explicit API and used in tests.

Test-first cycle: write focused failing tests and record the intended failure,
then implementation. Tests assert external behavior, not mirror algorithms.
Fixed independent hash vectors required. Test exactly-max fields and one beyond,
malformed huge inner length, truncated/oversize/trailing frame, invalid IDs through
deserialize, zero shard count/unsupported hash version, missing values and all
mutation outcomes. Mutation dropped/deadline behavior belongs Task2, no stubs here.

Ruling during implementation: use postcard's supported concrete envelope decoder
with strict remainder checking; avoid a generic deserializer forwarding framework
for unused Map/Vec<()> types. Document trusted generic T boundary. Compute bounded
serialized size then encode into exact-sized fixed slice, rather than allocate the
maximum frame for every request. Future collection-bearing messages must separately
validate their construction/decoding limits.

Verification: cargo test -p rockbee-raft-protocol; cargo fmt --all -- --check;
cargo clippy -p rockbee-raft-protocol --all-targets -- -D warnings. Only you run
Cargo during this task; notify root before any native dependency build (not needed
here). Network/package cache access may need sandbox escalation; report exact error.

Report to docs/tasks/task-1-report.md: status DONE/DONE_WITH_CONCERNS/BLOCKED,
files, red/green commands and actual outputs, design choices, review concerns.
Return concise status/tests only; no commits. Root will supply independent review.
