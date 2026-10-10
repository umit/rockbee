# Gateway

Package and binary: `rockbee-gateway`.

The gateway accepts the public byte protocol and forwards `PUT`/`DELETE` to the
configured writer and `GET` to the configured readers. It owns routing, bounded
admission and upstream links. It has no key/value cache or SlateDB instance.

From the repository root, after starting writer and reader:

```sh
make gateway
```

Validate the addresses and limits and print the effective settings as JSON:

```sh
cargo run -p rockbee-gateway -- --check-config \
  --writer 127.0.0.1:7101 --reader 127.0.0.1:7102
```

`--check-config` exits without binding a listener, resolving DNS, or connecting
to a backend. It checks address syntax, distinct backend address strings, and
the supported limits; it does not establish whether those servers are reachable
or have the correct roles. Omitted flags keep their normal defaults. The default
gateway address is `127.0.0.1:7100`, also used by the command-line client.

Read [how it works](../../README.md#how-it-works) and
[the Client contract](../../crates/rockbee-client/README.md). Package integration tests
live in `tests/`; the server transport is shared through `rockbee-rpc`.
