# syntax=docker/dockerfile:1.4
# Angzarr sidecar images - unified dev/release build
#
# Dev (fast, ~2min):     podman build --target angzarr-aggregate-dev -t angzarr-aggregate .
# Release (slow, ~8min): podman build --target angzarr-aggregate -t angzarr-aggregate .
#
# Dev uses native glibc target + debian runtime (no cross-compilation)
# Release uses musl static target + distroless runtime (smallest images)
#
# Multi-arch release: podman build --platform linux/amd64,linux/arm64 --target angzarr-aggregate ...
#
# Base images from ghcr.io/angzarr (see build/images/)

# RUST_IMAGE must be provided via build arg (no default to avoid registry lookups)
ARG RUST_IMAGE

# =============================================================================
# Proto generation stage - runs build.rs to generate proto code
# This stage is cached unless proto files or build.rs change
# =============================================================================
FROM ${RUST_IMAGE} AS proto-gen

WORKDIR /app

# Copy only what's needed for proto generation
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto/ ./proto/
COPY angzarr-project/ ./angzarr-project/
COPY sererr/ ./sererr/
COPY crates/ ./crates/
COPY xtask/ ./xtask/

# Create minimal stubs - just enough for cargo to run build.rs
RUN mkdir -p src/bin tests/integration tests/interfaces migrations && \
    echo "fn main() {}" > src/main.rs && \
    echo "pub fn stub() {}" > src/lib.rs && \
    for bin in aggregate projector saga process_manager log stream upcaster event_projector; do \
      echo "fn main() {}" > src/bin/angzarr_$bin.rs; \
    done && \
    for f in acceptance container_integration mongodb_debug \
             storage_mongodb storage_redis storage_sqlite storage_postgres \
             storage_immudb storage_nats \
             bus_nats bus_amqp bus_kafka bus_pubsub bus_sns_sqs; do \
      echo "fn main() {}" > tests/$f.rs; \
    done && \
    echo "fn main() {}" > tests/interfaces/main.rs && \
    echo "fn main() {}" > tests/integration/query_test.rs && \
    touch migrations/.keep

# Run cargo build to execute build.rs and generate proto code
# The build will fail on actual compilation but build.rs runs first
RUN cargo build --profile container-dev --features otel,postgres,amqp 2>&1 || true

# Extract generated proto files to a known location
RUN mkdir -p /proto-out && \
    cp -r target/container-dev/build/angzarr-*/out/* /proto-out/ 2>/dev/null || true

# =============================================================================
# Dev builder - deps stage (cached until Cargo.toml/Cargo.lock change)
# =============================================================================
FROM ${RUST_IMAGE} AS builder-dev-deps

WORKDIR /app

# Copy dependency manifests
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto/ ./proto/
COPY angzarr-project/ ./angzarr-project/
COPY sererr/ ./sererr/
COPY crates/ ./crates/
COPY xtask/ ./xtask/

# Copy pre-generated proto files from proto-gen stage
COPY --from=proto-gen /proto-out/ /proto-cache/

# Create stubs for dependency compilation
RUN mkdir -p src/bin tests/integration tests/interfaces migrations && \
    echo "fn main() {}" > src/main.rs && \
    echo "pub fn stub() {}" > src/lib.rs && \
    for bin in aggregate projector saga process_manager log stream upcaster event_projector; do \
      echo "fn main() {}" > src/bin/angzarr_$bin.rs; \
    done && \
    for f in acceptance container_integration mongodb_debug \
             storage_mongodb storage_redis storage_sqlite storage_postgres \
             storage_immudb storage_nats \
             bus_nats bus_amqp bus_kafka bus_pubsub bus_sns_sqs; do \
      echo "fn main() {}" > tests/$f.rs; \
    done && \
    echo "fn main() {}" > tests/interfaces/main.rs && \
    echo "fn main() {}" > tests/integration/query_test.rs && \
    touch migrations/.keep

# Build dependencies (will fail on angzarr crate, but deps compile)
RUN cargo build --profile container-dev --features otel,postgres,amqp \
    --bin angzarr-aggregate 2>&1 || true

# =============================================================================
# Dev builder - source build (invalidates when src/ changes)
# =============================================================================
FROM builder-dev-deps AS builder-dev

# Remove stub source to avoid conflicts
RUN rm -rf src/ tests/ migrations/

# Copy real source
COPY src/ ./src/
COPY migrations/ ./migrations/
COPY tests/ ./tests/

# Inject pre-generated proto files into cargo's expected location
# This makes build.rs a no-op (files already exist)
RUN BUILD_DIR=$(ls -d target/container-dev/build/angzarr-*/out 2>/dev/null | head -1) && \
    if [ -n "$BUILD_DIR" ]; then \
        cp -r /proto-cache/* "$BUILD_DIR/" 2>/dev/null || true; \
    fi

# Clean angzarr artifacts to force rebuild with real source
RUN rm -rf target/container-dev/.fingerprint/angzarr-* \
    target/container-dev/deps/libangzarr* \
    target/container-dev/deps/angzarr-* \
    target/container-dev/angzarr-*

# Build with real source
RUN cargo build --profile container-dev --features otel,postgres,amqp \
    --bin angzarr-aggregate \
    --bin angzarr-projector \
    --bin angzarr-saga \
    --bin angzarr-process-manager && \
    cp target/container-dev/angzarr-* /tmp/

# =============================================================================
# Release builder - deps stage
# =============================================================================
FROM ${RUST_IMAGE} AS builder-release-deps

ARG TARGETARCH

ENV RUSTFLAGS="-C target-feature=+crt-static"

WORKDIR /app

# Copy dependency manifests
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto/ ./proto/
COPY angzarr-project/ ./angzarr-project/
COPY sererr/ ./sererr/
COPY crates/ ./crates/
COPY xtask/ ./xtask/

# Copy pre-generated proto files
COPY --from=proto-gen /proto-out/ /proto-cache/

# Create stubs
RUN mkdir -p src/bin tests/integration tests/interfaces migrations && \
    echo "fn main() {}" > src/main.rs && \
    echo "pub fn stub() {}" > src/lib.rs && \
    for bin in aggregate projector saga process_manager log stream upcaster event_projector; do \
      echo "fn main() {}" > src/bin/angzarr_$bin.rs; \
    done && \
    for f in acceptance container_integration mongodb_debug \
             storage_mongodb storage_redis storage_sqlite storage_postgres \
             storage_immudb storage_nats \
             bus_nats bus_amqp bus_kafka bus_pubsub bus_sns_sqs; do \
      echo "fn main() {}" > tests/$f.rs; \
    done && \
    echo "fn main() {}" > tests/interfaces/main.rs && \
    echo "fn main() {}" > tests/integration/query_test.rs && \
    touch migrations/.keep

# Build dependencies
RUN if [ "$TARGETARCH" = "arm64" ]; then \
        TARGET="aarch64-unknown-linux-musl"; \
    else \
        TARGET="x86_64-unknown-linux-musl"; \
    fi && \
    cargo build --profile production --target $TARGET --features full-musl \
    --bin angzarr-aggregate 2>&1 || true

# =============================================================================
# Release builder - source build
# =============================================================================
FROM builder-release-deps AS builder-release

ARG TARGETARCH

# Remove stub source to avoid conflicts
RUN rm -rf src/ tests/ migrations/

# Copy real source
COPY src/ ./src/
COPY migrations/ ./migrations/
COPY tests/ ./tests/

# Determine target
RUN if [ "$TARGETARCH" = "arm64" ]; then \
        echo "aarch64-unknown-linux-musl" > /tmp/target; \
    else \
        echo "x86_64-unknown-linux-musl" > /tmp/target; \
    fi

# Inject pre-generated proto files
RUN TARGET=$(cat /tmp/target) && \
    BUILD_DIR=$(ls -d target/$TARGET/production/build/angzarr-*/out 2>/dev/null | head -1) && \
    if [ -n "$BUILD_DIR" ]; then \
        cp -r /proto-cache/* "$BUILD_DIR/" 2>/dev/null || true; \
    fi

# Clean angzarr artifacts
RUN TARGET=$(cat /tmp/target) && \
    rm -rf target/$TARGET/production/.fingerprint/angzarr-* \
    target/$TARGET/production/deps/libangzarr* \
    target/$TARGET/production/deps/angzarr-* \
    target/$TARGET/production/angzarr-*

# Build with real source
RUN TARGET=$(cat /tmp/target) && \
    cargo build --profile production --target $TARGET --features full-musl \
    --bin angzarr-aggregate \
    --bin angzarr-projector \
    --bin angzarr-saga \
    --bin angzarr-process-manager && \
    cp target/$TARGET/production/angzarr-* /tmp/

# =============================================================================
# Runtime bases
# =============================================================================
FROM docker.io/library/debian:trixie-slim AS runtime-dev-base
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    gdb \
    gdbserver \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
ENV ANGZARR_LOG=info

FROM gcr.io/distroless/static-debian12:nonroot AS runtime-release-base
WORKDIR /app
USER nonroot:nonroot
ENV ANGZARR_LOG=info

# =============================================================================
# Dev images (fast builds, larger runtime)
# =============================================================================
FROM runtime-dev-base AS angzarr-aggregate-dev
COPY --from=builder-dev /tmp/angzarr-aggregate ./server
EXPOSE 1313 1314
ENTRYPOINT ["./server"]

FROM runtime-dev-base AS angzarr-projector-dev
COPY --from=builder-dev /tmp/angzarr-projector ./server
ENTRYPOINT ["./server"]

FROM runtime-dev-base AS angzarr-saga-dev
COPY --from=builder-dev /tmp/angzarr-saga ./server
EXPOSE 1313 1314
ENTRYPOINT ["./server"]

FROM runtime-dev-base AS angzarr-process-manager-dev
COPY --from=builder-dev /tmp/angzarr-process-manager ./server
EXPOSE 1313 1314
ENTRYPOINT ["./server"]

# =============================================================================
# Release images (slow builds, minimal runtime, all features)
# =============================================================================
FROM runtime-release-base AS angzarr-aggregate
COPY --from=builder-release /tmp/angzarr-aggregate ./server
EXPOSE 1313 1314
ENTRYPOINT ["./server"]

FROM runtime-release-base AS angzarr-projector
COPY --from=builder-release /tmp/angzarr-projector ./server
ENTRYPOINT ["./server"]

FROM runtime-release-base AS angzarr-saga
COPY --from=builder-release /tmp/angzarr-saga ./server
EXPOSE 1313 1314
ENTRYPOINT ["./server"]

FROM runtime-release-base AS angzarr-process-manager
COPY --from=builder-release /tmp/angzarr-process-manager ./server
EXPOSE 1313 1314
ENTRYPOINT ["./server"]

