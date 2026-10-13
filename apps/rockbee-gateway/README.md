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

`[upstream].request_timeout_ms` sets the total routing deadline after request
validation, covering upstream queueing, connection, and reply time. It defaults
to 5000 ms. `reader_attempt_timeout_ms` defaults to 1000 ms and limits each
intermediate reader attempt for a `GET`; it may be larger than the total budget,
but is capped by the time remaining. The last (or only) reader receives the
entire remaining total deadline. Both values must be positive milliseconds.
Their command-line forms are `--upstream-request-timeout-ms` and
`--upstream-reader-attempt-timeout-ms`.

These are routing deadlines, distinct from the RPC frame read and write
timeouts. A client timeout independently caps how long that client observes a
request. The deadlines do not guarantee read freshness: readers remain
eventually consistent. A timeout does not guarantee cancellation of a mutation
already sent to the writer; mutations are never automatically retried.

Read [how it works](../../README.md#how-it-works) and
[the Client contract](../../crates/rockbee-client/README.md). Package integration tests
live in `tests/`; the server transport is shared through `rockbee-rpc`.
