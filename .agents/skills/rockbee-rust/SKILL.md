---
name: rockbee-rust
description: >-
  Apply Rockbee's Rust coding guidance when writing, reviewing, or refactoring
  Rust code. Covers ownership, errors, async/concurrency, public APIs,
  readability, and evidence-based performance decisions.
---

# Rockbee Rust guidance

Follow root `AGENTS.md` and the user's task scope before generic recommendations.
Use idiomatic Rust that a Java developer can follow: explicit responsibilities,
readable modules/functions, and useful public API and behavior documentation.
Avoid unnecessary abstractions, dependencies, and speculative tuning.

Read relevant categories in the preserved [upstream Rust guide](references/upstream-guide.md).
Use its category table and rule prefixes to select the material needed for the
change; do not load every rule for a small edit. For example, async changes use
`async-`, `own-`, and relevant `err-` rules; API work uses `api-`, `type-`, and `doc-`.

The reference is the existing project copy of leonardomso/rust-skills v1.5.1,
downloaded on 2026-10-07 from the [upstream SKILL.md](https://raw.githubusercontent.com/leonardomso/rust-skills/master/SKILL.md),
preserved with its metadata and MIT attribution.
Its Rust-version statements describe that guide, not the current toolchain.
Detailed `rules/` references belong to the upstream repository and are not
local files. Verify version-sensitive advice against the actual workspace.

Performance/compiler/runtime settings in the guide are optional recommendations,
not instructions to change project defaults. Retain existing Tokio, TCP, and
storage behavior unless evidence and the authorized task justify a change.
Preserve API contracts, mutation ownership, bounded multiplexing, and durability.
Reproduce failures, make focused fixes, and run checks appropriate to the change.
