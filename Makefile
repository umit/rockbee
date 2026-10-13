# Simple local workflow. `local` owns a disposable three-process topology;
# individual service targets remain available for separate terminals.

CARGO ?= cargo
CARGO_TARGET_DIR ?= target
export CARGO_TARGET_DIR

WRITER := $(CARGO_TARGET_DIR)/release/rockbee-writer
READER := $(CARGO_TARGET_DIR)/release/rockbee-reader
GATEWAY := $(CARGO_TARGET_DIR)/release/rockbee-gateway
LOCAL := $(CARGO_TARGET_DIR)/release/rockbee-local
PUT_BENCH := $(CARGO_TARGET_DIR)/release/examples/put_bench
GET_BENCH := $(CARGO_TARGET_DIR)/release/examples/get_bench
MIXED_SOAK := $(CARGO_TARGET_DIR)/release/examples/mixed_soak
VISIBILITY_PROBE := $(CARGO_TARGET_DIR)/release/examples/visibility_probe

ENDPOINT ?= http://127.0.0.1:4566
BUCKET ?= rockbee-test
PREFIX ?= rockbee-dev
WRITER_ADDR ?= 127.0.0.1:7101
READER_ADDR ?= 127.0.0.1:7102
GATEWAY_ADDR ?= 127.0.0.1:7100
WRITER_CONFIG ?= configs/writer.toml
READER_CONFIG ?= configs/reader.toml
GATEWAY_CONFIG ?= configs/gateway.toml

# The gateway file owns its topology unless an address is explicitly supplied
# through make's command line or environment. A reader override replaces the list.
GATEWAY_OVERRIDES :=
ifneq ($(origin GATEWAY_ADDR),file)
GATEWAY_OVERRIDES += --addr "$(GATEWAY_ADDR)"
endif
ifneq ($(origin WRITER_ADDR),file)
GATEWAY_OVERRIDES += --writer "$(WRITER_ADDR)"
endif
ifneq ($(origin READER_ADDR),file)
GATEWAY_OVERRIDES += --reader "$(READER_ADDR)"
endif

BENCH_ADDR ?= $(GATEWAY_ADDR)
BENCH_SECONDS ?= 60
BENCH_CONCURRENCY ?= 128
BENCH_VALUE_BYTES ?= 1024
BENCH_KEYS ?= 1024
SOAK_SECONDS ?= 60
SOAK_LANES ?= 8
VISIBILITY_SAMPLES ?= 20
# Optional space-separated direct reader addresses; empty measures the gateway path.
VISIBILITY_READERS ?=
# Leave empty to let put_bench generate a distinct key namespace.
BENCH_PREFIX ?=

ifneq ($(strip $(BENCH_PREFIX)),)
BENCH_PREFIX_ARG := --prefix "$(BENCH_PREFIX)"
endif

.PHONY: help build local probe writer reader gateway fmt fmt-check test test-local verify benchmark benchmark-read soak visibility

help:
	@printf '%s\n' \
	  'make build                                  Build release binaries and examples.' \
	  'make local                                  Run owned writer/reader/gateway; dynamic ports and unique prefix.' \
	  'make probe                                  Check roles at GATEWAY_ADDR, WRITER_ADDR, READER_ADDR; no CRUD.' \
	  'make writer PREFIX=demo                     Start the LocalStack-backed writer in the foreground.' \
	  'make reader PREFIX=demo                     Start the matching eventual reader in the foreground.' \
	  'make gateway                                Start the gateway in the foreground.' \
	  'make benchmark                              Run the async public Client PUT benchmark (60s, 128 pipeline).' \
	  'make benchmark BENCH_SECONDS=10 BENCH_CONCURRENCY=32' \
	  'make benchmark-read                         Prepare byte fixtures, warm readers, then measure GETs for 60s.' \
	  'make benchmark-read BENCH_SECONDS=10 BENCH_CONCURRENCY=32 BENCH_KEYS=1024' \
	  'make soak SOAK_SECONDS=1800                  Check mixed CRUD with one shared Client and eight lanes.' \
	  'make visibility VISIBILITY_SAMPLES=20        Measure durable ACK to first matching GET through the gateway.' \
	  'make visibility VISIBILITY_READERS="127.0.0.1:7102 127.0.0.1:7103"  Measure every named reader directly.' \
	  'make fmt                                    Format all workspace Rust code.' \
	  'make fmt-check                              Check Rust formatting without changing files.' \
	  'make test                                   Run workspace Rust tests, including documentation examples.' \
	  'make test-local                             Test real role binaries on an existing LocalStack (isolated prefix).' \
	  'make verify                                 Check format, lint all targets, then run Rust tests.' \
	  '' \
	  'Services use LocalStack at ENDPOINT and BUCKET; make local always generates a unique prefix.' \
	  'For make local, use the addresses printed in its ready JSON with make probe or benchmark.' \
	  'WRITER_CONFIG / READER_CONFIG select TOML; explicit Makefile CLI values override the file.' \
	  'GATEWAY_CONFIG selects gateway TOML; explicit GATEWAY_ADDR / WRITER_ADDR / READER_ADDR override it.' \
	  'Override CARGO_TARGET_DIR to reuse another Cargo build directory.'

build:
	$(CARGO) build --release --workspace --bins --examples --locked

local: build
	"$(LOCAL)" run --writer-config "$(WRITER_CONFIG)" --reader-config "$(READER_CONFIG)" --endpoint "$(ENDPOINT)" --bucket "$(BUCKET)"

probe: build
	"$(LOCAL)" probe --gateway "$(GATEWAY_ADDR)" --writer "$(WRITER_ADDR)" --reader "$(READER_ADDR)"

writer: build
	"$(WRITER)" --config "$(WRITER_CONFIG)" --role writer --addr "$(WRITER_ADDR)" --backend localstack --endpoint "$(ENDPOINT)" --bucket "$(BUCKET)" --prefix "$(PREFIX)"

reader: build
	"$(READER)" --config "$(READER_CONFIG)" --role reader --addr "$(READER_ADDR)" --backend localstack --endpoint "$(ENDPOINT)" --bucket "$(BUCKET)" --prefix "$(PREFIX)"

gateway: build
	"$(GATEWAY)" --config "$(GATEWAY_CONFIG)" $(GATEWAY_OVERRIDES)

fmt:
	$(CARGO) fmt --all

fmt-check:
	$(CARGO) fmt --all -- --check

test:
	$(CARGO) test --workspace --all-targets --locked
	$(CARGO) test --workspace --doc --locked

test-local: build
	ROCKBEE_BIN_DIR="$(abspath $(CARGO_TARGET_DIR))/release" ROCKBEE_S3_ENDPOINT="$(ENDPOINT)" ROCKBEE_S3_BUCKET="$(BUCKET)" $(CARGO) test --locked -p rockbee-node --test localstack -- --ignored --nocapture
	ROCKBEE_BIN_DIR="$(abspath $(CARGO_TARGET_DIR))/release" ROCKBEE_S3_ENDPOINT="$(ENDPOINT)" ROCKBEE_S3_BUCKET="$(BUCKET)" $(CARGO) test --locked -p rockbee-cli --test localstack -- --ignored --nocapture

verify: fmt-check
	$(CARGO) clippy --workspace --all-targets --locked -- -D warnings
	$(MAKE) test

benchmark: build
	"$(PUT_BENCH)" --addr "$(BENCH_ADDR)" --duration-seconds "$(BENCH_SECONDS)" --concurrency "$(BENCH_CONCURRENCY)" --value-bytes "$(BENCH_VALUE_BYTES)" $(BENCH_PREFIX_ARG)

benchmark-read: build
	"$(GET_BENCH)" --addr "$(BENCH_ADDR)" --duration-seconds "$(BENCH_SECONDS)" --concurrency "$(BENCH_CONCURRENCY)" --keys "$(BENCH_KEYS)" --value-bytes "$(BENCH_VALUE_BYTES)"

soak: build
	"$(MIXED_SOAK)" --addr "$(BENCH_ADDR)" --duration-seconds "$(SOAK_SECONDS)" --lanes "$(SOAK_LANES)"

visibility: build
	"$(VISIBILITY_PROBE)" "$(BENCH_ADDR)" "$(VISIBILITY_SAMPLES)" $(foreach reader,$(VISIBILITY_READERS),--reader "$(reader)")
