# Local development

[Back to Rockbee](README.md)

Setup, configuration, diagnostics, and verification for a local Rockbee workspace.

## Try it locally

You need Rust through rustup, a C++ toolchain for bundled DuckDB, and Docker
Desktop for LocalStack. The repository pins Rust 1.98.1 with Clippy and Rustfmt
in `rust-toolchain.toml`. On macOS, install Apple's Command Line Tools; if Clang
cannot find standard C++ headers, set this in the terminal running Cargo:

```sh
export CXXFLAGS="-isystem $(xcrun --sdk macosx --show-sdk-path)/usr/include/c++/v1"
```

For RustRover, see [IDE setup](#rustrover-setup). Rockbee's integration is Rust;
the bundled DuckDB engine is C++.

Use an existing compatible
LocalStack instance with bucket `rockbee-test`. Only if no compatible instance is
running **and port 4566 is free**, start the repository's Compose service:

```sh
docker compose up -d --wait
```

If another service occupies that port, use your existing compatible LocalStack
endpoint and bucket with `make local ENDPOINT=http://HOST:PORT BUCKET=NAME`.
For the default endpoint, start a disposable topology:

```sh
make local
```

This builds the release binaries and starts a writer, reader, and gateway with a
unique storage prefix. It prints **dynamic loopback addresses**. Copy the printed
gateway address into a second terminal; it is not necessarily port 7100:

```sh
export GATEWAY_ADDR='127.0.0.1:PORT_FROM_READY_OUTPUT'
./target/release/rockbee-client --addr "$GATEWAY_ADDR" put demo:key demo-value
./target/release/rockbee-client --addr "$GATEWAY_ADDR" get demo:key
./target/release/rockbee-client --addr "$GATEWAY_ADDR" delete demo:key
```

The first GET may be missing while the reader catches up. To observe convergence,
repeat GET before deleting the key. Stop traffic and await outstanding PUT
handles before Ctrl-C. The runner stops only its owned processes and retains
storage data and logs; it does not manage or remove existing LocalStack
containers. Shutdown allows a shared 30-second grace period before force-stopping
remaining owned children; a forced stop does not guarantee pending writes.

For fixed addresses and a reusable prefix, run these in separate terminals:

```sh
make writer PREFIX=demo
make reader PREFIX=demo
make gateway
```

Start the writer first, then the reader, then the gateway. This workflow uses
ports `7101`, `7102`, and `7100`, respectively. Both storage nodes must use the
same bucket and prefix. Run `make help` for endpoint and address overrides.
The command-line client defaults to `127.0.0.1:7100` when `--addr` is omitted.

Check configuration before starting the services:

```sh
./target/release/rockbee-writer --config configs/writer.toml --check-config
./target/release/rockbee-reader --config configs/reader.toml --check-config
./target/release/rockbee-gateway --config configs/gateway.toml --check-config
```

These commands print effective settings as JSON and exit without starting the
services. Gateway validation checks addresses and limits locally; it does not
check DNS, backend reachability, or backend roles.

`make gateway` reads [configs/gateway.toml](configs/gateway.toml) for its
listener, writer, readers, and existing upstream/RPC limits. Override the file
with `GATEWAY_CONFIG=path/to/gateway.toml`. Explicit CLI flags take precedence
over TOML, then built-in defaults; CLI `--reader` entries replace the entire
file reader list. Unknown TOML fields are rejected. Explicit Make overrides
such as `make gateway READER_ADDR=127.0.0.1:7202` still work and replace that
list with the supplied reader. Configuration is loaded once at startup.

Gateway `[upstream]` routing uses a 5000 ms total request deadline after local
validation, including upstream queueing, connection, and reply time. Intermediate
reader attempts for `GET` are capped at 1000 ms; an attempt never exceeds the
time left, while the last or only reader gets the full remaining deadline. Set
positive millisecond values with `request_timeout_ms` and
`reader_attempt_timeout_ms`, or `--upstream-request-timeout-ms` and
`--upstream-reader-attempt-timeout-ms`.
These routing deadlines are separate from RPC frame timeouts, and a client
timeout independently limits its observed wait. They do not promise fresh reads;
a timeout does not guarantee cancellation of a mutation already sent to the
writer; mutations are never automatically retried.

Query one running service with the command-line client:

```sh
./target/release/rockbee-client --addr 127.0.0.1:7100 status --expect-role gateway
./target/release/rockbee-client --addr 127.0.0.1:7101 status --expect-role writer
./target/release/rockbee-client --addr 127.0.0.1:7102 status --expect-role reader --timeout-ms 2000
```

`status` prints one JSON record with the endpoint's role and RPC counters. It
exits unsuccessfully on an unreachable endpoint, timeout, malformed reply, or
role mismatch. `--expect-role` is optional; the default timeout is two seconds.
This checks the named process only: a gateway's Status response does not prove
its backends work, and a reader's response does not prove it has caught up.
Use `make probe` for all configured roles and `make visibility` to observe
write visibility. Status does not issue a KV mutation or retry a failed request.

Gateway, writer, and reader emit newline-delimited JSON lifecycle events on
stdout: `ready` once listening, then `stopped` after a successful graceful
shutdown or `failed` if startup, serving, or shutdown returns an error. Terminal
events include the role, address, `uptime_ms`, and a shutdown reason or error.
Uptime includes startup time after configuration validation. Invalid arguments
and configuration errors exit before this lifecycle begins; abrupt process
termination such as SIGKILL cannot emit a final event.

An ASCII Rockbee banner appears on stderr when starting in a terminal, including
with `make local`. It is omitted from captured stderr, `--help`, and
`--check-config`, keeping machine-readable stdout intact.

Diagnostic logs use `tracing` and go to stderr as timestamped text. By default,
Rockbee emits `INFO` and above; SlateDB and other dependencies emit `WARN` and
above (`warn,rockbee=info`). Set `RUST_LOG` before starting a service to replace
that filter:

```sh
RUST_LOG=warn,rockbee=debug,slatedb=info make local
RUST_LOG=off make gateway
```

The first command includes TCP connection diagnostics and SlateDB's informational
events. The second disables diagnostic logs; stdout lifecycle events and the
terminal banner remain. Invalid filters fail before the service binds or opens
storage. `--help` and `--check-config` do not initialize logging. The local
launcher saves each child service's stderr in its printed log directory.
Successful GET/PUT/DELETE requests are not logged individually, and Rockbee's
diagnostics do not record their keys or values. The public Rust RockBeeClient emits
diagnostic events but leaves subscriber setup to the application that uses it.

The reader's `[reader].poll_interval_ms` setting in
[configs/reader.toml](configs/reader.toml) controls how often SlateDB checks for
durable updates. It defaults to `10000` (10 seconds). A shorter interval adds
object-store requests and can reduce visibility delay; it is not a freshness
guarantee and does not change durable write acknowledgements.

### Continuing an existing local setup

Cargo packages, Rust imports, executables, environment variables, and the
Protobuf package use `rockbee` / `ROCKBEE_*`. When upgrading an earlier checkout,
rebuild the binaries and update scripts to these names. Protobuf field numbers
and the wire protocol version are unchanged by the branding update.

Stored objects and CDC cursor files are not renamed or migrated. Local defaults
use bucket `rockbee-test` and, for the fixed-address Make targets, prefix
`rockbee-dev`. To reopen an existing dataset, substitute its actual bucket and
prefix below and run these commands in separate terminals:

```sh
make writer BUCKET=EXISTING_BUCKET PREFIX=EXISTING_PREFIX
make reader BUCKET=EXISTING_BUCKET PREFIX=EXISTING_PREFIX
make gateway
```

Keep the original endpoint as well. There must still be only one active writer
for that prefix. To run an isolated local demo on the existing bucket instead,
use `make local BUCKET=EXISTING_BUCKET`.

Keep an existing LocalStack container running; do not start another Compose
project on its port or recreate it just to change its name. The new Compose
project is named `rockbee-localstack`; it does not adopt the old container.

If you use the optional CDC exporter, preserve the original backend, endpoint,
bucket, prefix, and cursor path with explicit CLI flags, including
`--cursor /path/to/existing-cursor.json`. The default cursor is
`./rockbee-cdc.cursor.json`; using a different cursor starts a separate export
instead of resuming the previous one.

## RustRover setup

- Open the root `Cargo.toml` as one workspace. Let rustup select the pinned
  toolchain; **Settings > Rust** should show version **1.98.1**.
- Keep macro expansion enabled and the toolchain's standard library sources
  available for navigation and type inference.
- Use **Rust > External Linters > Clippy**, with `--locked` as an additional
  argument. Keep the channel at its default so it follows `rust-toolchain.toml`.
- Keep **Rust > Rustfmt > Use Rustfmt instead of the built-in formatter** enabled
  so formatting follows this repository's Rustfmt configuration.
- If the macOS header issue occurs, add `CXXFLAGS` under **Rust > Environment
  variables**. Its value is `-isystem /actual/sdk/path/usr/include/c++/v1`, using
  the output of `xcrun --sdk macosx --show-sdk-path`. IDE environment fields do not
  execute `$(...)`. Refresh Cargo projects after changing it. No compiler wrapper
  is required.

IDE settings remain local; command-line builds need their own environment.

## Develop and verify

```sh
make fmt        # Format all workspace Rust code
make fmt-check  # Check formatting without changing files
make test       # Rust workspace tests and documentation examples; no Docker required
make verify     # Formatting, strict Clippy, and Rust tests
make test-local ENDPOINT=http://127.0.0.1:4566 BUCKET=rockbee-test
```

The opt-in LocalStack tests require an existing compatible instance and bucket;
they use unique prefixes and stop only their own children. With the services
running, use `make benchmark`, `make benchmark-read`, `make soak`, and
`make visibility`; run `make help` for duration, concurrency, and address options.
Keep Tokio, TCP, and SlateDB defaults unless evidence justifies a change.

To measure durable ACK-to-visibility on **each reader**, use its direct address:

```sh
make visibility GATEWAY_ADDR=127.0.0.1:7100 VISIBILITY_SAMPLES=4 \
  VISIBILITY_READERS="127.0.0.1:7102 127.0.0.1:7103"
```

Mutations still go through the gateway. The JSON report contains a separate
observation for every named reader after each ACK, including failures; a failed
observation makes the command exit unsuccessfully. DELETE is measured only
after every named reader has observed that key's PUT. Without
`VISIBILITY_READERS`, the probe measures the first matching GET through the
gateway; it does not prove that every reader has caught up. This is a low-load
visibility sample, not a throughput benchmark or a strict staleness bound.
