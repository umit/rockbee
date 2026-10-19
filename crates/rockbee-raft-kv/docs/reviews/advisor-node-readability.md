# Node complexity and OpenRaft boundary

Advisor and verifier independently inspected the current node source and agree: **the node uses OpenRaft; it does not implement a second Raft consensus algorithm.** This is a read-only review, with no builds or production changes. Bootstrap's new code has static design approval under the agreed immutable common-configuration precondition; its behavioral tests remain required.

```text
NodeRuntime -> one OpenRaft instance per group
                  |-> RaftLogStorage / RaftStateMachine -> shared RocksDB
                  |-> RaftNetwork -> bounded private TCP -> remote OpenRaft
NodeService ------+-> client_write / ensure_linearizable
Bootstrap --------+-> initialize, then group-0 directory publication
```

## Who owns what

| Responsibility | Owner and concrete entry point |
| --- | --- |
| Elections, terms/votes, quorum decisions, log consistency and replication progress | OpenRaft, constructed in `cluster/mod.rs::start_group` through `Raft::new` |
| Commit/apply orchestration and membership consensus | OpenRaft; application supplies durable adapter methods |
| Strong-read authority barrier | OpenRaft `ensure_linearizable`; `strong_read` then reads a coherent applied RocksDB view |
| Mutation consensus | OpenRaft `client_write`; service translates its outcome conservatively |
| Peer consensus processing | OpenRaft `append_entries`, `vote`, `install_snapshot`; our peer dispatcher delegates to them |
| Bootstrap existing/pristine group checks | OpenRaft `is_initialized` / `initialize`, invoked by one logical initializer |
| Durable bytes, atomic apply, durable callback timing, shared DB/group boundaries | Our `storage` adapters; a consensus library cannot choose or correctly implement our storage layout for us |
| Socket framing, identity, bounded allocations/admission, transport cancellation | Our `rpc` adapter; OpenRaft's network trait deliberately leaves transport implementation to the application |
| Several groups in one process, metadata placement, API certainty, orderly startup/shutdown | Our `cluster`, `config`, and backend service |

The three peer methods and OpenRaft storage trait implementations are meaningful capability interfaces, comparable to Java interfaces injected into a library. `TypeConfig` chooses associated command/result/node/file/runtime types; it is not an application framework or another consensus engine. `Arc` and owned permits express shared lifetime and deterministic release, which Java garbage collection alone does not express.

## Concrete reduction/readability candidates

1. **Make the actual entry point visible.** `main.rs` still prints “workspace scaffold only; Multi-Raft runtime is not implemented” and fails. The tested implementation is the `NodeRuntime` library API. Keep the executable explicitly unfinished until a thin CLI calls validate/configure, start, optional initialize/bootstrap, await shutdown, close. Do not claim the binary works because library integration passes. Add a node README reading order: `config` → `cluster::NodeRuntime` → `NodeService` → `raft/types` → adapter of interest.
2. **Leave one private request wire path.** `rpc/message.rs::PeerRequestBody` still derives raw `Serialize`/`Deserialize`, although actual socket decoding deliberately goes through bounded wire DTOs. After confirming no call site needs those derives, remove them. This removes an accidental unsafe alternative and clarifies that DTO conversion is the required boundary. Do not replace bounded DTOs with direct upstream deserialization to reduce line count.
3. **Separate storage construction from batch bookkeeping when the milestone is stable.** `storage/mod.rs` combines DB options/opening, group ownership, blocking lanes, and entry chunk accounting. A private `batch.rs` for `EntryChunks`/`ChunkMetadata`/prepared-entry sizing is a useful extraction. Keep the gate/session lifetime contract together; avoid scattering a single durability operation across many tiny modules. This is a readability candidate, not permission for broad churn during active correctness fixes.
4. **Prefer concrete orchestration over a generic RPC framework.** The three small native RPC adapters and explicit service matches are readable. Their repetition reflects different upstream result types and certainty boundaries. No new trait-per-struct, inheritance hierarchy, dynamic plugin layer, or cross-protocol generic handler is needed.
5. **Explain the two intentional representations.** Validated public IDs and bounded wire DTOs protect external boundaries; OpenRaft's internal `u64` and types satisfy its API; storage records preserve durable identity. These are conversions with distinct contracts, not duplicate business objects to collapse indiscriminately.

The size is roughly 4.2k source lines at inspection, including storage and inline peer tests. The `raft/types.rs` configuration itself is about 53 lines. Line count alone would overstate the amount of custom consensus work.

## Bootstrap source check

The newly published `cluster/bootstrap.rs` sends no arbitrary directory payload, follows the locally observed group-zero leader only as a candidate, reconciles via a real `Directory` operation, and bounds attempts by one overall deadline with per-attempt budgets. The receiver repeats its own strong read: identical value succeeds, corrupt/different value rejects, absence permits the configured immutable Put. `mutate` rechecks deadline/observed role before submission and maps every post-`client_write` error to Unknown. Closing the caller link does not cancel the server-owned proposal.

This matches the agreed static design. Its prerequisite remains **identical canonical configuration on every potential executor**, with changing directory values disabled. Two incompatible configurations both reading absence are not protected by read-then-Put. Before directory updates exist, use apply-time put-if-absent or proven bootstrap retirement/draining. No claim of Task-7 fencing follows from this code.

One small consistency check should accompany snapshot integration: `strong_read` currently compares applied and barrier indexes only. For equal indexes, require the full LogId to match and fail closed on conflicting identity; do not let index equality hide a faulty generation install. Higher applied indexes still need to come from the coherent valid state-machine history. This is a defensive invariant, not a new consensus algorithm.

Bilateral outcome: no architecture disagreement and no Judge needed. Keep required adapters, improve the entry-point story, remove the unused raw wire alternative, and postpone mechanical storage extraction until correctness work is stable.
