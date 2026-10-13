# Gateway

Package and binary: `rockbee-gateway`.

The gateway accepts the public byte protocol and forwards `PUT`/`DELETE` to the
configured writer and `GET` to the configured readers. It owns routing, bounded
admission and upstream links. It has no key/value cache or SlateDB instance.

From the repository root, after starting writer and reader:

```sh
make gateway
```

The topology and limits can live in TOML. The checked-in local example matches
the writer and reader examples:

```sh
cargo run -p rockbee-gateway -- --config configs/gateway.toml
```

Validate the merged settings and print them as JSON:

```sh
cargo run -p rockbee-gateway -- --config configs/gateway.toml --check-config
```

`--check-config` exits without binding a listener, resolving DNS, or connecting
to a backend. It checks address syntax, distinct backend address strings, and
the supported limits; it does not establish whether those servers are reachable
or have the correct roles. Omitted flags keep their normal defaults. The default
gateway address is `127.0.0.1:7100`, also used by the command-line client.

Every command-line value overrides TOML. Repeated `--reader` values replace the
entire `readers` array from the file, so a deployment can change its read
topology without retaining file entries. Unknown TOML fields, including fields
inside `[upstream]` and `[rpc]`, are rejected.

Read [how it works](../../README.md#how-it-works) and
[the Client contract](../../crates/rockbee-client/README.md). Package integration tests
live in `tests/`; the server transport is shared through `rockbee-rpc`.
