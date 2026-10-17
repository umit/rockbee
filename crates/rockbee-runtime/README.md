# Rockbee runtime

Shared configuration, object-store setup, memory/TTL/key helpers, generic TCP
serving, and process lifecycle. `Storage` is implemented by the writer and reader
application libraries. `Server<S>` binds its listener before opening `S`, so an
occupied address cannot acquire a new writer epoch.

```text
fixed writer/reader binary -> runtime Server<Writer/Reader> -> role storage
```

There is no shared production role-dispatch storage engine. The runtime has no
Parquet or DuckDB dependency. Each role owns its actual SlateDB implementation.
