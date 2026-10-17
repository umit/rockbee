# Rockbee reader

`rockbee-reader` owns the read-only SlateDB engine in [its library](src/lib.rs).
The executable fixes its role to reader; conflicting CLI or TOML roles fail before
binding or opening storage. Shared configuration and RPC lifecycle helpers live
in `rockbee-runtime`.

```text
RockBeeClient -> Gateway -> Reader -> SlateDB -> object store (raw/table GET)
                           +-> DuckDB -> published Parquet (table SQL)
```

Readers reject all mutations and may lag the writer. Raw and table GETs use
SlateDB's own caches; SQL uses the independently published Parquet projection.
Reader freshness and SQL projection freshness are separate eventual processes.

Reader SQL limits are configured at startup in the top-level TOML `[query]`
section, separately from SlateDB's `[reader]` and `[memory]` options:

```toml
[query]
timeout_ms = 4000
memory_limit_mb = 256
max_concurrent = 2
```

`timeout_ms` must be positive and produce a representable deadline.
`memory_limit_mb` accepts 1–16384 MB per DuckDB connection; `max_concurrent`
accepts 1–128 workers per reader. These limits are not a process RSS ceiling.
`rockbee-reader --config configs/reader.toml --check-config` prints resolved
values without opening storage. Writers reject a `[query]` section.
