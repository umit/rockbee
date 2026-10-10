#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p crates/rockbee-protocol/src/generated
cargo run --locked -p rockbee-protocol --example generate_protocol
