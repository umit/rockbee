# Node complexity and bootstrap review

The node uses OpenRaft for consensus. No custom election, quorum, log-matching or joint-consensus algorithm was found in the reviewed application code.

| Responsibility | Current owner and source |
| --- | --- |
| Election, replication, commit and membership protocol | `openraft::Raft<TypeConfig>` created in `cluster/mod.rs::start_group` |
| Consensus submission and read authority | `cluster/service.rs::mutate` calls `client_write`; `strong_read` calls `ensure_linearizable` |
| Incoming native Raft operations | `rpc/server.rs::dispatch` forwards to `append_entries`, `vote`, `install_snapshot` |
| Durable log, state machine and snapshot backing | Local implementations of OpenRaft storage traits over RocksDB; required adapters, not a second consensus engine |
| TCP, bounded decoding and traffic admission | `rpc/`; OpenRaft does not supply this application's transport/resource policy |
| Five-group process lifecycle and immutable directory | `cluster/`; application orchestration around five upstream Raft instances |

The current `src/` count is 4,241 lines including 311 lines in `storage/tests.rs` and additional inline unit tests. This is not a production-only size measurement. The actual `raft/types.rs` configuration is 53 lines. Its macro declares associated types; it does not hide consensus logic.

Safest simplifications after the active correctness fixes:

1. Give readers a short entrypoint map: `cluster/mod.rs` starts/stops groups; `cluster/service.rs` serves commands; `storage/log.rs` persists consensus history; `storage/state.rs` applies commands; `rpc/network.rs` sends native RPCs. Keep named small functions rather than a new interface hierarchy.
2. Extract private batch sizing (`PreparedEntry`, `ChunkMetadata`, `EntryChunks`) from the 649-line `storage/mod.rs` to a private `batch.rs`, preserving the exact accounting and cancellation behavior. Keep the group gate/native-I/O ownership code visible together. Validate with existing storage boundary and cancellation regressions; no new abstraction or dependency is needed.
3. Remove unused raw serde implementations from the internal `PeerRequestBody` only after confirming callsites. Socket decoding already uses bounded wire DTOs. Having one request wire path makes the trust boundary clearer; do not remove bounded DTOs to reduce line count. Existing actual encoded round-trip and hostile-count tests validate this cleanup.
4. `main.rs` still prints scaffold-only and exits failure. The library runtime has integration evidence; the executable remains unfinished. A thin executable should call `NodeRuntime` rather than duplicate its lifecycle. Until implemented, documentation must distinguish the library milestone from a runnable node CLI.

Final entrypoint follow-up: root has now replaced the scaffold with a thin `main.rs` and `cli.rs`. Inspection confirms configuration validation and explicit create/open intent, byte-safe hexadecimal parsing, signal registration before runtime startup, initialization through existing `NodeRuntime`, and the same owned close path after bootstrap or signal-wait errors. The new node README explains actual OpenRaft ownership and a Java-familiar reading order accurately. No second lifecycle or consensus framework was introduced.

New bootstrap source statically matches the agreed restricted design: only the designated logical initializer orchestrates; each attempt first performs a real group-zero strong directory read; the receiver validates identical canonical immutable configuration; pre-submission rejection stays Rejected; all `client_write` errors become Unknown. A later absent read does not cancel an older proposal. Identical internal repetition is safe only while every receiver has the same immutable directory and updates remain disabled. This is static review, not new execution evidence.

Root subsequently supplied `reports/bootstrap-runtime-green.log`; verifier read it: the two runtime tests pass, covering four-group leader stop/restart and durable immutable-directory bootstrap with equal retry. The inspected test does not force the initializer to differ from the metadata leader, so that specific routing case remains an explicit acceptance gap. These are root-run integration results, not an independent verifier execution or a snapshot/membership acceptance claim.

Verifier also read `reports/public-path-and-cli.log`: two CLI unit tests and three runtime tests pass, including public Client -> Gateway -> three-node Raft PUT/GET/DELETE. This is the concrete public-path milestone; no process-signal shutdown test, snapshot transfer or fourth-node move is implied by those five checks.

Before snapshot acceptance, add the cheap defensive read check that equal applied/barrier indexes must also have the same full LogId. Higher applied indexes remain valid descendants and need not have the barrier's leader identity. The current index-only check has no demonstrated valid-history failure; this is a corruption/snapshot guard, not evidence that OpenRaft's barrier is broken.

Advisor and verifier independently agree on these boundaries and simplifications. No Judge disagreement; no source changes or Cargo commands in this review.

```text
Client -> Gateway -> NodeService -> OpenRaft (consensus)
                                  |-> RocksDB storage adapter
                                  |-> bounded peer TCP adapter -> OpenRaft
Thin executable added; durable snapshots and fourth-node movement unfinished
```
