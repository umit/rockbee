# Task 1b: Protocol readability for a Java-background Rust developer

Module: `/Users/umitunal/IdeaProjects/rockbee/crates/rockbee-raft-kv`.
User explicitly requests readable function-by-function flow and meaningful traits,
across all Rockbee. This task owns only protocol readability; later audits cover
other packages. No commit/push or subagents. `.herdr/` is forbidden. Root is
compiling native node deps; use `CARGO_TARGET_DIR=target/protocol-readability`
for focused verification, never compile node. No dependency changes.

Task1 behavior is independently approved. Read its implementation and
`docs/reviews/verifier-task-1.md`; preserve wire bytes, enum variant/field order,
limits, invalid-input behavior and all public constructors/validation methods.
Existing tests are the baseline; no implementation-mirroring tests for refactor.

Make source easy to follow:
- Replace the `nonzero_id!` macro generating two public types with explicit
  RequestId and NodeId structs/impls. Small explicit repetition is preferable
  here to hiding their API behind a macro. Keep private fields/serde validation.
- Split mixed wire.rs responsibilities into clear files such as request.rs,
  response.rs, envelope.rs, failure.rs, error.rs, limits.rs, validation.rs as
  appropriate. Avoid a file for every trivial function. lib.rs reexports remain
  convenient and retain existing public names.
- Let each public entry function read as a short meaningful sequence. In
  particular envelope validation should show version, budget/correlation and
  payload checks; extract nested mutation-result handling into named behavior
  on the relevant type. Keep straightforward match expressions where clear.
- Use meaningful names for FNV constants, max budget/shard count and framing
  header length. Extract hash calculation from group routing with a clear name.
- Add concise module/type docs explaining role and call flow. Prefer one compact
  working rustdoc example for encode -> decode -> validate over prose repetition.
- Traits express real shared behavior, not Java-style class hierarchies or
  factories. A small Validate trait may be introduced if it genuinely unifies
  current request/envelope/failure validation and upcoming transport; preserve
  convenient inherent methods so callers need not learn trait import rules just
  to validate. If this would create redundant wrappers, instead leave the concrete
  API and document the planned meaningful WireRequest trait in Task2. Choose the
  simpler readable result and explain it in the report.
- Do not add allocations/clones/locks/dynamic dispatch/boxing or change runtime
  settings. Named helper calls should preserve the existing ownership and work.
  Keep the codec's bounded two-pass serializer and supported-type decoder scope.

First add small fixed wire v1 golden fixtures for representative PUT request and
Committed/Value/Unknown responses (expected byte literals independently derived),
run them on current implementation to preserve wire contract. These are contract
checks, not tests of private helper arrangement. Then refactor.

Run all protocol tests (existing9 + independent2 + fixtures), rustfmt check,
strict protocol all-target Clippy, rustdoc tests. Report actual commands/results
and any public API movement in docs/tasks/task-1b-report.md. Return concise status;
root supplies independent review. Do not claim runtime/performance validation.
