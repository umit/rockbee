# Rockbee Raft KV Workspace Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to
> implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Create the requested independent module inside the existing repository,
with four compilable package boundaries and all new work inside `crates/rockbee-raft-kv/`.

**Architecture:** One nested Cargo workspace owns client/protocol library packages
and gateway/node application packages. Only the node will depend on OpenRaft and
RocksDB when engine implementation begins. This plan creates the package skeleton;
the database behavior described in the design is a separate implementation stage.

**Tech Stack:** Rust 2024, repository Rust toolchain, Cargo workspaces. No external
runtime or engine dependencies are needed for empty package boundaries.

**Spec:** [design.md](design.md)

## Global Constraints

- All new files belong under repository-relative `crates/rockbee-raft-kv/`.
- Keep a separate workspace and lockfile; do not change parent workspace members.
- Preserve existing user changes and do not inspect `.herdr/`.
- No commit, push, background services, or storage-data mutation in this stage.
- Do not expose stub success for PUT/GET/DELETE or imply a Raft service is running.
- Multi-Raft and gateway remain required in the first database implementation.

## Review Focus

- Parent workspace commands must not pull in experimental packages.
- Client/gateway dependency graphs must not pull in storage-engine dependencies.
- Application placeholders must fail explicitly instead of pretending to serve.
- Cargo.lock must be trackable, and nested build/data output must be ignored.
- Documentation must distinguish package compilation from database verification.

## Task 1: Create and verify the workspace boundaries

**Files:** `Cargo.toml`, `.gitignore`, `README.md`, four package manifests,
four `src/lib.rs` files, and two application `src/main.rs` files under this module.
Cargo generates this module's `Cargo.lock`.

**Interfaces:** No public KV behavior yet. Package names are
`rockbee-raft-client`, `rockbee-raft-protocol`, `rockbee-raft-gateway`, and
`rockbee-raft-node`. Gateway/node entry points return failure with an explicit
not-implemented message. Client, gateway, and node depend only on the shared
protocol package at this stage.

- [x] Create package manifests and documentation-only libraries. Add explicit
  application placeholders and ignore only generated module-local output.
- [x] Verify nested and parent `cargo metadata --offline --no-deps` show separate
  workspace roots and the expected package sets. Inspect the nested dependency
  graph for imports of parent Rockbee packages or engine dependencies.
- [x] Run `cargo fmt --all -- --check`, `cargo check --offline --workspace
  --all-targets`, and strict Clippy from the nested workspace.
- [x] Build nested applications offline and run each placeholder. Expect exit 1
  with the explicit not-implemented message and no listening process.
- [x] Verify ignored generated paths, trackable lockfile, and the final Git diff.
  Obtain a focused independent review of package boundaries and status claims.

No unit tests are added for empty manifests or documentation-only libraries.
These checks establish scaffolding only; they do not validate Raft, RocksDB,
networking, durability, or performance. Those require the subsequent database
implementation plan and its failure-oriented tests.

## Verification record

On 2026-10-19, metadata confirmed 12 parent packages and 4 nested packages with
separate workspace roots and target directories. Formatting, all-target checking,
strict Clippy, and the workspace build completed successfully offline. Both
application placeholders exited with code 1 and their not-implemented messages.
Git ignored module-local target/data/reports output and left Cargo.lock and docs
trackable. An independent review found no remaining scaffold-boundary issue;
its completed-checklist status finding was corrected here.
