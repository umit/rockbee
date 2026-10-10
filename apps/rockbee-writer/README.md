# Writer

Package and binary: `rockbee-writer`.

This entrypoint always opens the writer role. A conflicting role in CLI arguments
or TOML is an error. The [main function](src/main.rs) calls `rockbee_node::writer::run`. Writer
SlateDB operations live in [the writer module](../../crates/rockbee-node/src/writer);
configuration, startup and TCP handling are shared in the
[rockbee-node library](../../crates/rockbee-node/README.md).

This binary is its own process, listening on port `7101` by default. Its RAM
is separate from the gateway and reader processes.

From the repository root:

```sh
make writer
```

One active writer owns a database prefix. Successful mutations wait for SlateDB
durability. See [writer configuration](../../configs/writer.toml) and
[mutation completion semantics](../../crates/rockbee-client/README.md).
