# JSON table export and SQL

This Rust crate owns the asynchronous DuckDB/Parquet projection. Writer and
reader applications own their SlateDB engines; this crate receives their object
store and storage prefix. The gateway routes requests without caching values.

```text
RockBeeClient -> Gateway -> Writer -> SlateDB WAL -> durable ACK
                 -> Reader -> SlateDB (table/raw GET)
Writer background -> DuckDB -> immutable Parquet + manifest -> Reader/DuckDB SQL
```

`duckdb = "=1.10506.1"` is the Rust binding, with bundled JSON and Parquet.
DuckDB's engine is C++; building this dependency requires a working C++ toolchain.
Rockbee's integration code is Rust. On a Mac where Clang cannot locate SDK libc++
headers, set `CXXFLAGS="-isystem $(xcrun --sdk macosx --show-sdk-path)/usr/include/c++/v1"`
when running Cargo. In RustRover, set the same `CXXFLAGS` under **Settings > Rust >
Environment variables**, using the actual SDK path: IDE environment fields do not
execute shell substitutions. No compiler wrapper or global compiler setting is
needed.

## Export lifecycle

Table PUT atomically writes the document and a persistent table catalogue entry.
Its acknowledgement waits for SlateDB durability, independently of export. A
single writer-owned worker checks registered tables every 500 ms. Restarting a
writer resumes export even without a new mutation.

Bootstrap and WAL-gap recovery flush the memtable, create a detached checkpoint
with a finite 24-hour lease, stream that pinned view into bounded batches and
then consume durable WAL after the checkpoint boundary. Checkpoint cleanup runs
on ordinary completion, failure and task cancellation. Gaps rebuild a generation;
they never silently advance the cursor past missing history.

Parquet rows carry `id`, JSON `value`, sequence, absolute expiry, tombstone,
WAL file ID and row ordinal. Upload immutable segments before publishing their
manifest. Immutable consecutive manifest revisions use create-only writes so the
publication rule also works with local filesystem object stores. Writer epoch
checks and conditional revision publication prevent an older generation from
replacing a newer published one. Table names are hex encoded in object paths to
preserve case-sensitive namespaces on local filesystems.

Compaction rebuilds the latest state after at least 128 delta segments and 60
seconds. It atomically replaces the referenced generation. Revision history and
unreferenced Parquet objects currently remain in object storage; automatic
garbage collection is future work. Account for this retention when running long
workloads. A failed upload/publication leaves the previous generation readable.

## Query behavior and bounds

Readers reload the manifest for every query and cache only immutable files in an
owned temporary directory. Each query registers one selected table as
`(id VARCHAR, value JSON)`. It chooses the newest version per ID before checking
tombstones, expiry or the user's predicates, preventing old values from
reappearing after updates. Queries never rescan SlateDB document data.

User SQL is bound to DuckDB's `query(?)` table function, which accepts a single
SELECT, including CTEs. It is never passed directly to the Rust binding's
multi-statement preparation path. External access and extension autoload are
disabled; only selected cached files and a private spill directory are allowed.
DuckDB also parses the original SQL into its native JSON syntax tree before
execution. Only the selected unqualified table and lexical CTEs are permitted.
Table functions are limited to `range`, `generate_series`, `unnest`, `json_each`
and `json_tree`; file/catalog access and nested SQL execution functions are
rejected, even for a known cached Parquet path. Expressions and aggregates still
run in DuckDB. This is not a tenant authentication service.

| Resource | Bound |
| --- | --- |
| SQL text | 64 KiB |
| Returned rows / JSON bytes | 1,000 / 512 KiB, explicit error on overflow |
| Concurrent SQL workers per reader | 2, immediate busy error on excess |
| Entire query deadline | 4 seconds, including downloads |
| DuckDB memory / spill per query | 256 MB / 1 GB |
| Local Parquet cache | 8 GiB and 16,384 files |
| One Parquet object | 32 MiB |
| Export batch | 1,000 rows or approximately 8 MiB before encoding |

Cancellation interrupts DuckDB and keeps admission occupied until the worker
actually exits. Running queries pin their cached files against eviction. Reader `[query]` settings in `configs/reader.toml` configure `timeout_ms`,
`memory_limit_mb` and `max_concurrent`; `--check-config` prints resolved values.
Defaults stay unchanged. Memory accepts 1..=16384 MB and concurrency 1..=128;
deadlines must be positive and representable. Increase client and gateway
deadlines too when increasing the reader deadline. These
are bounded defaults; a dataset exceeding the cache working set or deadline
returns an error. There is no claim that arbitrary million-row queries always
fit those limits.

SQL is eventual and can lag point GET. `exported_wal_file_id` is a diagnostic
watermark, not a client mutation receipt or a cross-reader read barrier. A known
table awaiting its first export reports a warming error; a table not yet known
to a lagging reader may still appear empty. Poll read queries when eventual
convergence is acceptable; do not automatically replay uncertain writes.

## Verification

`cargo test -p rockbee-query --lib` covers publication, real Parquet, fencing,
WAL gaps, compaction, checkpoint cleanup, SQL isolation and bounds.

`cargo test -p rockbee-query --test scale -- --ignored --nocapture` builds one
million durable documents in an owned temporary local store, bootstraps real
Parquet and checks cold/warm SQL aggregates. Its debug/local-file timings do not
measure production S3 performance.
