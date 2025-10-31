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

# Copy manifests
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

# Build dependencies first (better caching)
RUN mkdir src && echo "fn main() {}" > src/main.rs && echo "" > src/lib.rs
RUN cargo build --release --features server
RUN rm -rf src target/release/otlp2parquet* target/release/deps/otlp2parquet*

# Copy source and build
COPY src ./src

RUN cargo build --release --features server

# Strip binary for smaller size
RUN strip target/release/otlp2parquet

# Runtime stage - minimal distroless image
FROM gcr.io/distroless/cc-debian12:latest

# Copy binary from builder
COPY --from=builder /build/target/release/otlp2parquet /usr/local/bin/otlp2parquet

# Environment defaults (can be overridden)
ENV STORAGE_BACKEND=filesystem
ENV FILESYSTEM_ROOT=/data
ENV HTTP_PORT=8080
ENV HTTP_HOST=0.0.0.0

# Create data directory
WORKDIR /data

# Expose HTTP port
EXPOSE 8080

# Run as non-root (distroless provides nonroot user)
USER nonroot:nonroot

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/usr/local/bin/otlp2parquet", "--health-check"]

ENTRYPOINT ["/usr/local/bin/otlp2parquet"]
