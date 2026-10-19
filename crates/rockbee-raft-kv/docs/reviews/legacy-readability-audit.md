# Existing Rockbee readability audit

**Inactive:** the user subsequently limited readability work to
`crates/rockbee-raft-kv`. Do not execute these legacy recommendations under the
current task. Existing Rockbee source remains unchanged.

Read-only targeted audit, 2026-10-19. Started with `rg --files` over `apps/`,
`crates/`, and `tools/`; explicitly excluded `.herdr/`, the nested Raft module,
and target directories. Inspected the listed functions and their existing
behavior tests. No source edits, builds, tests, processes, or storage mutations.

```text
Client -> Gateway -- PUT/DELETE -> Writer  -> SlateDB -> Object storage
                  `-- GET ------> Readers -> SlateDB cache/view
```

The five candidates below are readability improvements, not identified behavior
bugs. Line references describe the source at review time. Prefer one focused
change and its existing checks at a time; preserve unrelated edits.

## Shared readability standard

Use named private functions for domain steps and keep the calling function a
readable outline of those steps. Explain a trait as a capability contract and an
`impl` method as behavior on its concrete type when that explanation helps the
Java-familiar reader. Explain ownership, Drop, and task cancellation where they
determine behavior. Keep existing types and explicit resource ownership; no new
dependency, class hierarchy, generic framework, clone, lock, task, or queue is
needed for these recommendations.

Existing `crates/rockbee-runtime/src/storage.rs` already separates and explains
ReadStorage, WriteStorage, QueryStorage, and lifecycle well. It is a useful style
reference and does not need a mechanical rewrite.

## 1. Gateway request routing

**Location:** `apps/rockbee-gateway/src/router.rs:54`, `Router::route`.

The function mixes operation validation, mutation certainty conversion, writer
dispatch, round-robin selection, per-reader attempt limits, and final error
selection. A reader has to hold all phases in mind to see that mutations execute
once and a missing value ends read routing successfully.

**Safest first change:** extract a synchronous `validate_route_request` helper
and a `route_read` method. The visible route outline becomes validate, mutation
to writer once, otherwise route through readers. A later `read_from_reader`
helper can name the attempt branch if its owned task/drop behavior stays clear.
Keep the same single deadline, round-robin increment, final-reader full remaining
budget, first application-error selection, and successful missing-value return.
Do not add caching or use another reader merely because a value is missing.

**Validation:** existing gateway tests for invalid inputs, writer-only mutation,
read failover, missing values, and `upstream_timeouts`; especially
`received_but_unforwarded_gateway_mutation_is_rejected_not_client_not_sent` and
`timed_out_toml_configured_mutation_is_unknown_and_is_not_replayed`.

## 2. RPC response preparation and driver responsibility

**Location:** `crates/rockbee-rpc/src/connection.rs:303`, `process_request`;
follow-up at `:188`, `internal_connection`.

`process_request` interleaves local status handling, handler invocation, typed
mutation-error downcasting, message extraction, response-size fallback, and
stats. The internal connection function embeds its request-reader actor and
three-way handler/driver/shutdown coordination in one body.

**Safest first change:** extract pure `response_from_handler_error` and
`validate_response_size` helpers from response preparation. Keep generic errors
Unknown and typed explicit rejection intact. Keep stats at the same point.
Then name the existing reader actor `read_internal_requests`, moving its body
unchanged and retaining the same JoinSet owner. Preserve biased select order,
high-water IDs, one staged frame, and the precise point permits reach the writer.
Do not move existing permit drops across a network await.

**Validation:** existing RPC multiplex, global-limit, response-limit, shutdown,
and connection handoff tests. Important schedules include
`writing_response_leaves_admission_for_the_next_request`,
`completed_gets_can_replenish_the_full_default_window`,
`eof_at_admission_limit_cancels_handlers`, and
`shutdown_drains_handler_destructor`.

## 3. Upstream Link connection actors

**Location:** `crates/rockbee-protocol/src/transport/link/mod.rs:243`,
`Connection::with_address`.

The constructor contains connection-state initialization and two long nested
async actors. In the reader actor, response decoding/correlation and state
mutation are embedded inside the I/O/cancellation loop. Their task and mutex
ownership are important but visually buried.

**Safest first change:** name the existing actor bodies `run_writer` and
`run_reader`, retaining the current spawn boundaries and owned arguments.
Optionally extract a synchronous `Shared::complete_response` for validation,
pending removal, and oneshot completion under the existing one lock scope.
Keep the same retirement reasons, biased cancellation, flush calls, generation
guards, and permit release behavior. A named helper must not lengthen the lock
scope or add an await inside it.

**Validation:** existing Link tests for partial-write cancellation, duplicate or
wrong-kind response, old generation guards, ID exhaustion, synchronous admission,
and timeout without replay. Also retain public client eager/drop/shutdown tests
because actor extraction must not make PUT submission lazy.

## 4. Local CLI startup outline

**Location:** `tools/rockbee-cli/src/local/run.rs:16`, `run`.

Three nearly repeated child argument lists and readiness waits obscure the
ordered lifecycle: resolve, spawn writer, record ownership, await ready, then
reader, then gateway; failure cleanup, ready reporting, and stop reporting follow.

**Safest first change:** extract `start_writer`, `start_reader`, and
`start_gateway` functions using the existing Paths/RunConfig/children/signals.
Keep every child inserted into the owned vector immediately after spawn and
before any readiness await. Alternatively start with pure argument-building
helpers for the writer/reader lists, leaving all ownership in `run`.
No new launcher trait or generalized process supervisor is needed. Preserve
unique prefixes/log paths, machine-readable output, signal registration order,
and cleanup of only owned child handles.

**Validation:** existing `rockbee-cli` `local_cli` integration tests for invalid
startup config, `startup_failure_cleans_only_the_writer_child_already_owned_by_run`,
`sigint_during_reader_startup_cleans_the_owned_writer_and_fails`, and nonzero
shutdown. These fixtures exercise owned child processes; no LocalStack change is
needed for this readability patch.

## 5. WAL export row conversion

**Location:** `crates/rockbee-query/src/exporter.rs:364`, `Worker::append_wal`;
related conversion at `:319`, `snapshot_checkpoint`.

The WAL loop combines stream setup, per-file ordinal tracking, table filtering,
value/tombstone decoding, ExportRow construction, batching, and cursor movement.
The correctness boundary is that a cursor advances only after complete file EOF
and publication remains a later CAS operation.

**Safest first change:** extract a private pure row-conversion function for the
value/tombstone match and ExportRow construction, consuming the already-owned
row and borrowing the prefix. Keep ordinal increment before table filtering,
merge-row rejection, existing byte conversions, batch flush placement, EOF
cursor advancement, and publication unchanged. Name the checkpoint-row converter
separately if useful; avoid forcing distinct source row types into a new trait.

**Validation:** existing exporter tests
`exports_real_parquet_and_replays_updates_and_tombstones`,
`snapshot_compaction_removes_superseded_versions`,
`stale_publication_preserves_cursor_and_uploaded_segments`, and
`retained_wal_gap_rebuilds_and_restart_keeps_manifest`. Reviewed fixtures use
their own in-memory object stores; no production object-store data is required.

## Validation and scope guidance

Run formatting and the affected package's existing tests after each chosen
extraction, then a package Clippy check when code is stable. Coordinate Cargo
with other implementers before running checks. Pure readability changes need
no tests that merely restate the new helper decomposition. If a reproducible
behavior failure appears, investigate it separately before changing semantics.

Start with gateway route validation and RPC response preparation: they offer
clear function names with small diffs and good existing behavior coverage.
Transport actor and process-lifecycle extractions warrant closer ownership/
cancellation review. All changes must retain eager PUT completion handles,
bounded multiplexing, durable mutation ACKs, no uncertain replay, and eventual
reader visibility; this audit makes no immediate read-after-write claim.
