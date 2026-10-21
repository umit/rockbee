# Rockbee product brief

Context snapshot: 2026-10-21, branch `feat/raft-kv-multiraft`, HEAD `01dc69d`.
This is an index of direction and inspected documentation, not a new test run
or proof of production readiness. Refresh the relevant evidence for each task.

## User-confirmed direction

- Build a robust database with a simple client API and understandable,
  idiomatic Rust that a developer with a Java background can follow.
- Preserve correctness and durability while improving measured performance;
  avoid speculative tuning and unsupported capacity claims.
- Develop the independent Raft workspace with OpenRaft/RocksDB and a gateway
  while retaining the original object-storage product as a separate track.
- Use a project-aware team with independent engineering review and a Product
  Manager that maintains gaps/TODOs and evaluates customer value.

Source: the project owner's instructions in this working session. These are
owner goals; they are not independently validated customer demand.

## Proposed vision and mission

**Vision — proposal:** Make reliable application data serving understandable,
operable, and affordable without hiding its consistency or durability tradeoffs.

**Mission — proposal:** Develop simple Rust data APIs, explicit guarantees,
repeatable verification, and measurable operating costs. Evaluate object-storage
serving and replicated local-disk KV under their own workload requirements.

These statements are proposals for product discussion, not adopted public copy.

## Product map

| Track | Documented capability and boundary | Starting sources |
|---|---|---|
| Rockbee core | Byte KV, flexible JSON tables, DuckDB SQL over a background Parquet projection, shared object storage, and independently provisioned readers. Reader GET and SQL visibility are eventual. | [Root README](../../README.md), [client guide](../../crates/rockbee-client/README.md), [query guide](../../crates/rockbee-query/README.md) |
| Rockbee Raft KV | Independent nested Rust workspace with a dedicated gateway, OpenRaft groups, RocksDB on local disk, byte PUT/GET/DELETE, durable consensus acknowledgements, and read barriers as described by its client contract. | [Raft README](../../crates/rockbee-raft-kv/README.md), [Raft client guide](../../crates/rockbee-raft-kv/crates/rockbee-raft-client/README.md) |

These descriptions come from the linked current documentation. Check relevant
code/tests before making stronger claims. Features, guarantees, and benchmark
results from one track must not be attributed to the other.

The Raft README lists unfinished repeated movement, automatic cluster management,
TTL/SQL/JSON/multi-key transactions, production security/operations, and capacity
validation. It requires local disk. Treat these as documented limitations to
revalidate when assigning work, not automatically approved roadmap features.

## Customer and use-case hypotheses

| Hypothesis | Current support | Useful validation |
|---|---|---|
| Core may fit catalogs, prepared application data, and AI outputs when asynchronous visibility is acceptable. | Use cases in the root README; no customer adoption evidence inspected. | Confirm freshness requirements, lookup/query patterns, data volume, and operating costs with an actual intended user. |
| Raft KV may fit applications needing durable replicated writes and strong per-key reads through a small API. | Documented design/contract; no production customer validation inspected. | Establish required failure tolerance, latency/error objectives, deployment constraints, and alternatives. |
| Simple operation and lower infrastructure cost may influence adoption. | Owner priorities and product positioning; no comparative cost or willingness-to-pay study inspected. | Compare an agreed representative workload and operating effort against the intended alternative. |

Do not describe these as what all customers want or reject. Customer segments,
commercial packaging, pricing, and release commitments remain open questions.

## Current evidence and planning pointers

- The [Raft README](../../crates/rockbee-raft-kv/README.md) records documented
  capabilities, limitations, and a dated verification checkpoint. Revalidate
  current completion against the relevant implementation and test evidence.
- Local plans and detailed review notes may exist under
  `crates/rockbee-raft-kv/docs/`. This directory stays outside version control;
  consult it when available, without making it required repository content
  or treating its historical records as current verification.
- Different dated test totals belong to their own revisions and configurations.
  Do not treat their difference alone as a regression or merge them into a new run.
- Old plans may contain superseded workflow instructions. Current user direction
  and root `AGENTS.md` govern work; Superpowers is disabled.
- [Product backlog](backlog.md) is the outcome-level index. Engineering details
  stay in their existing plans; this brief does not copy the full failure matrix.

## Open product decisions

- Which track and customer problem should drive the next delivery milestone?
- What workload, latency/error target, and recovery requirements define success?
- Which operational capabilities are required for the intended deployment?
- Which evidence would justify adding features rather than strengthening the
  current KV, failure handling, and operational experience?

The Product Manager should recommend answers using evidence and alternatives;
raise only decisions that materially affect the requested work.
