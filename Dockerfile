FROM rust:1.98.1-slim-bookworm AS builder

ARG BUILD_JOBS=2

# aws-lc-sys builds its native library with CMake; SlateDB also brings native
# compression libraries through its locked dependency graph.
RUN apt-get update \
    && apt-get install --no-install-recommends -y \
        build-essential \
        ca-certificates \
        cmake \
        perl \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY apps ./apps
COPY tools ./tools
COPY crates ./crates

RUN cargo build --release --locked -p rockbee-reader --bin rockbee-reader --jobs "${BUILD_JOBS}"

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install --no-install-recommends -y \
        ca-certificates \
        libssl3 \
        libstdc++6 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home --home-dir /home/rockbee rockbee

COPY --from=builder /app/target/release/rockbee-reader /usr/local/bin/rockbee-reader

USER rockbee

ENTRYPOINT ["/usr/local/bin/rockbee-reader"]
