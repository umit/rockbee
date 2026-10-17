# Contributing to Rockbee

Rockbee is an S3-native, opaque-byte KV pilot. Changes must preserve its public
contract: durable writer ACKs, no automatic replay of uncertain mutations, and
eventual reader GETs.

## Design and code

Rockbee follows the simple, incremental approach described in SlateDB's
[Clean Slate](https://github.com/slatedb/slatedb/blob/main/CLEAN_SLATE.md).
Prefer readable code and a small, complete change over an abstraction for a
possible future use case.

- Preserve the public contract first: keys and values are opaque bytes, accepted
  mutations are durably acknowledged, uncertain mutations are never replayed
  automatically, and reader visibility is eventual.
- Keep changes narrow and document a new behavior or configuration contract
  before adding it. Avoid speculative dependencies, layers, and tuning knobs.
- Add performance complexity only when a representative measurement identifies
  a bottleneck and shows the benefit. Record the workload and result with the
  change when it affects a material path.
- Return expected errors to the caller. Use `expect` only for a documented,
  local invariant; do not hide recoverable failures with `unwrap` or `expect`.
- Bound concurrency and queues at service boundaries. Do not block Tokio worker
  threads, and keep lock-held work small. Release synchronous locks before an
  `.await`; use an async lock only when the operation needs it.

On macOS, install Apple's Command Line Tools. If bundled DuckDB cannot find C++
headers, set `CXXFLAGS="-isystem $(xcrun --sdk macosx --show-sdk-path)/usr/include/c++/v1"`
for Cargo. In RustRover, add `CXXFLAGS` under **Settings > Rust > Environment
variables**, using the resolved SDK path rather than shell substitution.

Read the root README, the relevant package README, and existing tests before a
change. Local notes in sibling `../rockbee-docs/` are optional context, not a
build or contribution prerequisite.

## Tests

Add a focused, meaningful regression test with the package that owns the
behavior. Unit tests stay with code; reusable crate integration tests stay under
that crate. The opt-in LocalStack integration test belongs to
`crates/rockbee-test-support/tests/`.

## Verify

Rockbee pins Rust 1.98.1 and the `rustfmt` and `clippy` components in
`rust-toolchain.toml`. `rustfmt.toml` uses the Rust 2024 edition with the
standard formatter defaults.

Like SlateDB, the workspace warns about `unreachable_pub` and
`unused_qualifications`; every package inherits these lints, and CI treats
warnings as errors. Keep internal items private or scoped to their module or
crate, and avoid redundant paths for names already in scope.
Clippy also checks `unwrap_used` and `panic` outside tests, following SlateDB's
policy of explicit invariant messages. Tests may use `unwrap` and `panic` for
assertions, as configured in `clippy.toml`.
Test-only helpers outside test functions may need an explicit
`#[cfg_attr(test, allow(clippy::unwrap_used))]` attribute.

The formatter follows SlateDB's minimal configuration (`reorder_imports = true`)
with Rockbee's existing Rust 2024 edition. SlateDB's custom clock, random-number,
and task restrictions support its deterministic simulation framework; Rockbee
does not use that framework, so those project-specific restrictions do not apply.

- `make fmt` formats the workspace. Run it before committing a Rust change.
- `make fmt-check` verifies that formatting is already correct; it does not
  modify files.
- `make test` runs workspace targets and doctests without Docker.
- `make verify` runs `fmt-check`, strict Clippy, and `make test`.
- `make test-local` is the opt-in LocalStack integration test. It builds release
  role binaries, requires an existing configured `ENDPOINT` and `BUCKET`, and
  uses its own storage prefix and child processes. It must not disturb an
  existing LocalStack instance.

The normal measurement commands are `make benchmark`, `make benchmark-read`,
`make soak`, and `make visibility`; they expect the role services to be running.
GitHub Actions runs `make verify`. Rockbee is licensed under
[Apache-2.0](LICENSE); workspace packages inherit the license and repository
metadata from the root manifest. A security-reporting policy remains a separate
decision.
