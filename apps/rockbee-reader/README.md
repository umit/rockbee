# Reader

Package and binary: `rockbee-reader`.

This entrypoint always opens the read-only role. A conflicting role in CLI
arguments or TOML is an error. The [main function](src/main.rs) calls `rockbee_node::reader::run`. Reader
SlateDB operations live in [the reader module](../../crates/rockbee-node/src/reader);
configuration, startup and TCP handling are shared in the
[rockbee-node library](../../crates/rockbee-node/README.md).

This binary is its own process, listening on port `7102` by default. Its RAM
is separate from the gateway and writer processes.

From the repository root, after starting the writer on the same bucket/prefix:

```sh
make reader
```

Each reader has its own SlateDB RAM cache. Reads can lag durable writer ACKs;
readiness does not promise immediate read-after-write visibility. See
[reader configuration](../../configs/reader.toml) and
[how reads become visible](../../README.md#how-it-works).
