# Rockbee writer

`rockbee-writer` owns the writable SlateDB engine in [its library](src/lib.rs).
The executable fixes its role to writer; conflicting CLI or TOML roles fail before
binding or opening storage. Configuration, RPC serving, and process lifecycle
helpers live in `rockbee-runtime`.

```text
RockBeeClient -> Gateway -> Writer -> SlateDB WAL -> object store
                              |
                              +-> table exporter -> Parquet + manifest
```

Opaque bytes use physical key prefix zero. JSON table documents use a disjoint
prefix and publish a persistent table catalogue marker in the same durable batch.
PUT and DELETE acknowledge SlateDB durability. The background Parquet exporter is
independent of mutation acknowledgements. Shutdown stops export before closing
SlateDB and still attempts database close if exporter shutdown fails.
