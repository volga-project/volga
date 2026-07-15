# syntax=docker/dockerfile:1.7

FROM rust:1.88-bookworm AS chef
WORKDIR /app
RUN cargo install cargo-chef --locked

FROM chef AS planner
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
COPY src ./src
COPY config ./config
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    pkg-config \
    cmake \
    clang \
    protobuf-compiler \
    libprotobuf-dev \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=planner /app/recipe.json recipe.json
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo chef cook --release --recipe-path recipe.json

COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
COPY src ./src
COPY config ./config
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo build --release --bin volga-master --bin volga-worker --bin volga-test-storage

FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/volga-master /usr/local/bin/volga-master
COPY --from=builder /app/target/release/volga-worker /usr/local/bin/volga-worker
COPY --from=builder /app/target/release/volga-test-storage /usr/local/bin/volga-test-storage
# Runtime-tunable timeouts (override without rebuilding the binary; or set
# VOLGA_RUNTIME_CONSTS_PATH / VOLGA_RUNTIME_CONSTS_DIR).
COPY config/runtime_consts.production.json config/runtime_consts.test.json /etc/volga/

ENV RUST_LOG=info
