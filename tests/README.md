# Test layout

Rust integration tests stay beside the Cargo package that owns their public
behavior. `make test` runs the workspace Rust suite, including the public
Client examples' unit tests, and does not need Docker or LocalStack.

`make verify` adds `cargo fmt --check` and Clippy to that suite.

`crates/rockbee-node/tests/localstack.rs` is the opt-in real-process integration
test. Run `make test-local ENDPOINT=http://127.0.0.1:4566 BUCKET=rockbee-test`.
The target builds release binaries, starts an isolated writer, reader, and
gateway with its own S3 prefix, and stops only those child processes. It requires
an existing LocalStack endpoint and bucket; it is not part of `make test`.

The normal application names are `rockbee-writer`, `rockbee-reader`, and
`rockbee-gateway`. Benchmarks and visibility studies run through the Rust client
examples with `make benchmark`, `make benchmark-read`, `make soak`, and
`make visibility` after the services are running.
