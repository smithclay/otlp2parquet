# Multi-stage build for otlp2parquet
# Produces multi-arch images (amd64/arm64) optimized for size

# Build stage
# NOTE: Using nightly temporarily due to transitive dependency (home-0.5.12) requiring edition2024
# This can be switched back to stable once edition2024 is stabilized or the dependency is updated
FROM rustlang/rust:nightly-slim AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy manifests and source
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

# Build the binary
RUN cargo build --release -p otlp2parquet --bin otlp2parquet

# Strip binary for smaller size
RUN strip target/release/otlp2parquet

# Optional prebuilt binary stage (expects dist/linux-${TARGETARCH}/otlp2parquet in build context)
FROM scratch AS prebuilt
ARG TARGETARCH
COPY dist/linux-${TARGETARCH}/otlp2parquet /usr/local/bin/otlp2parquet

# Shared runtime configuration
FROM gcr.io/distroless/cc-debian13:latest AS runtime-base

# Environment defaults (can be overridden)
ENV STORAGE_BACKEND=filesystem \
    FILESYSTEM_ROOT=/data \
    HTTP_PORT=4318 \
    HTTP_HOST=0.0.0.0

WORKDIR /data

# Expose OTLP HTTP port
EXPOSE 4318

# Run as non-root (distroless provides nonroot user)
USER nonroot:nonroot

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/usr/local/bin/otlp2parquet", "--health-check"]

ENTRYPOINT ["/usr/local/bin/otlp2parquet"]

# Runtime stage using prebuilt binaries (selected with --target runtime-prebuilt)
FROM runtime-base AS runtime-prebuilt
ARG TARGETARCH
COPY --from=prebuilt /usr/local/bin/otlp2parquet /usr/local/bin/otlp2parquet

# Runtime stage - builds binary from source (default)
FROM runtime-base AS runtime

# Copy binary from builder
COPY --from=builder /build/target/release/otlp2parquet /usr/local/bin/otlp2parquet
