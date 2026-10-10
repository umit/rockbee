# Node library source guide

`rockbee-node` is the shared Rust library for the writer and reader applications.
Its name stays because both roles are storage nodes. It is one package in the
root [Cargo workspace](../../Cargo.toml), alongside the packages in `apps/`.
A package is defined by a `Cargo.toml`; `apps/` and `crates/` simply organize
source code. A library is linked into an executable, not started as a separate
service.

The normal topology has three separately running binaries: gateway, writer,
and reader. Each has its own RAM and TCP port (defaults: `7100`, `7101`, `7102`).
Writer and reader use the same object-store prefix, not shared process memory.
Gateway routing lives in [apps/rockbee-gateway](../../apps/rockbee-gateway/README.md).

```text
apps/rockbee-writer/src/main.rs --> rockbee_node::writer::run
apps/rockbee-reader/src/main.rs --> rockbee_node::reader::run
crates/rockbee-node/src/
  lib.rs          library modules and public exports
  writer/mod.rs   SlateDB Db: open, PUT, DELETE, GET, close
  reader/mod.rs   SlateDB DbReader: open, GET, close
  runtime.rs      common configuration, startup and ready output
  backend.rs      object-store backend construction
  config.rs       CLI configuration and validation
  config/file.rs  TOML configuration loading
  key.rs          nonempty storage keys: prefix every key with byte 0
  server/mod.rs   TCP requests -> Store calls -> responses
  storage.rs      Store facade dispatching to writer or reader
  storage/
    memory.rs     per-engine memory configuration
    ttl.rs        shared expiry arithmetic and value filtering
```

[Key mapping](src/key.rs) keeps the existing zero-byte prefix on every accepted
nonempty key, preserving the representation of existing records.
All CRUD entrypoints require nonempty keys; PUT also requires a nonempty value.
The public Client rejects these inputs locally; gateway and node request paths
reject raw invalid requests before forwarding or storage. The storage mapping
remains unchanged, but new empty-key operations are rejected. Legacy stored
empty values remain decodable. Opaque bytes and the message layout remain
unchanged on the public wire.

Read in this order:

1. [Writer main](../../apps/rockbee-writer/src/main.rs) or
   [reader main](../../apps/rockbee-reader/src/main.rs): the executable entrypoint.
2. [Writer](src/writer/mod.rs) or [reader](src/reader/mod.rs): `run` selects the
   role; the same module owns that role's actual SlateDB operations.
3. [Runtime](src/runtime.rs), [configuration](src/config.rs), and
   [server](src/server/mod.rs): startup and TCP request dispatch.
4. [Store facade](src/storage.rs): role dispatch and read-only mutation rejection.
   Follow its calls back into the role module to see storage behavior.

```text
Client -> Gateway -- PUT/DELETE -> Writer -> SlateDB -> S3 / LocalStack
                 `-- GET ------> Reader -> SlateDB -> S3 / LocalStack
```

Writer mutations wait for `await_durable()` before success. Readers have their
own SlateDB RAM caches and can lag durable writer ACKs; gateway GETs do not
promise immediate read-after-write visibility. The gateway has no key/value
cache. See [mutation completion](../rockbee-client/README.md) and
[reader visibility](../../README.md#how-it-works).

## Reader refresh interval

`[reader].poll_interval_ms` in a reader TOML file controls how often SlateDB's
read-only engine checks both the durable manifest and WAL for writer updates.
The default is the pinned SlateDB 0.17 default of `10000` ms. Lowering it can
reduce eventual-read visibility delay, but creates more object-store requests.
It must be positive and no greater than `300000` ms because Rockbee uses
SlateDB's default 600-second managed-checkpoint lifetime. Confirm the resolved
value without opening storage:

```sh
rockbee-reader --config configs/reader.toml --check-config
```

[The legacy rockbee-server entrypoint](src/bin/rockbee-server.rs) remains a
compatibility binary for scripts and tests. It uses the same library and
selects its role through configuration.
