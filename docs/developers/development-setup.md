# Development Setup

This guide provides instructions for setting up your development environment, building the project, and understanding key development practices.

## Prerequisites

To get started with `otlp2parquet` development, you'll need the following tools:

```bash
# Install Rust toolchain
rustup toolchain install stable
rustup component add rustfmt clippy
rustup target add wasm32-unknown-unknown

# Install wasm-opt (required for WASM optimization)
# macOS:
brew install binaryen

# Linux (Ubuntu/Debian):
wget https://github.com/WebAssembly/binaryen/releases/latest/download/binaryen-*-x86_64-linux.tar.gz
tar xzf binaryen-*-x86_64-linux.tar.gz
sudo cp binaryen-*/bin/wasm-opt /usr/local/bin/

# Install development tools (optional but recommended)
cargo install twiggy          # WASM binary profiler
curl -LsSf https://astral.sh/uv/install.sh | sh  # uv for Python tools

# Setup pre-commit hooks
uvx pre-commit install
```

## Dependency Configuration (Critical for Size Optimization)

`otlp2parquet` employs aggressive size optimization, especially for Cloudflare Workers (WASM) deployments. This is reflected in the `Cargo.toml` configuration:

### `Cargo.toml` - Aggressive Size Optimization

```toml
[workspace]
members = ["crates/*"]

[profile.release]
opt-level = "z"           # Optimize for size
lto = true                # Link-time optimization
codegen-units = 1         # Better optimization
strip = true              # Strip symbols
panic = "abort"           # Smaller panic handler

[profile.release.package."*"]
opt-level = "z"

[dependencies]
# Arrow/Parquet - MINIMAL features only
arrow = { version = "53", default-features = false, features = [
    "ipc"                 # Arrow IPC format (smaller than full)
] }
parquet = { version = "53", default-features = false, features = [
    "arrow",              # Arrow integration
    "snap",               # Snappy only (smallest compressor)
] }

# OTLP Protocol - minimal
prost = { version = "0.13", default-features = false, features = ["std"] }

# Platform-specific (behind features)
worker = { version = "0.4", optional = true }
aws-lambda-runtime = { version = "0.13", optional = true }

# Core utilities - minimal
tokio = { version = "1", default-features = false, features = ["rt", "macros"] }
serde = { version = "1", default-features = false, features = ["derive"] }
anyhow = "1"
uuid = { version = "1", default-features = false, features = ["v4", "fast-rng"] }

[features]
default = []
cloudflare = ["worker"]
lambda = ["aws-lambda-runtime"]

[build-dependencies]
prost-build = "0.13"
```

### OpenDAL Dependency Configuration

```toml
# Cargo.toml - workspace dependencies
opendal = { version = "0.54", default-features = false }

# Platform-specific features (crates/otlp2parquet-runtime/Cargo.toml)
[features]
cloudflare = ["worker", "opendal/services-s3"]
lambda = ["lambda_runtime", "opendal/services-s3"]
server = ["opendal/services-fs", "opendal/services-s3", "axum", "tracing"]
```

## Building

### Using Makefile (Recommended)

The project includes a comprehensive `Makefile` with Rust-idiomatic targets for all common development tasks.

```bash
# Show all available commands
make help

# Quick development check (fast)
make dev

# Format and lint
make fmt
make clippy

# Run tests
make test

# Build for specific platform
make build-server
make build-lambda
make build-cloudflare

# Full WASM pipeline: build → optimize → compress → profile
make wasm-full

# Run pre-commit checks before committing
make pre-commit

# Run full CI locally
make ci
```

### Manual Build Commands

**Cloudflare Workers (WASM):**

```bash
# Build with minimal features
cargo build --release \
  --target wasm32-unknown-unknown \
  --no-default-features \
  --features cloudflare

# Optimize
wasm-opt -Oz --enable-bulk-memory --enable-nontrapping-float-to-int \
  -o optimized.wasm target/wasm32-unknown-unknown/release/otlp2parquet.wasm

# Compress and check size (must be <3MB)
gzip -9 optimized.wasm
l s -lh optimized.wasm.gz
```

**AWS Lambda:**

```bash
# Install cargo-lambda (optional, for local testing)
cargo install cargo-lambda

# Build
cargo build --release --no-default-features --features lambda
```

**Server Mode (Default - Docker/Kubernetes/Development):**

```bash
# Build
cargo build --release --no-default-features --features server

# Run with filesystem storage (default)
./target/release/otlp2parquet

# Run with S3 storage
STORAGE_BACKEND=s3 \
S3_BUCKET=my-logs-bucket \
S3_REGION=us-east-1 \
./target/release/otlp2parquet

# Run with R2 storage
STORAGE_BACKEND=r2 \
R2_BUCKET=my-r2-bucket \
R2_ACCOUNT_ID=your_account_id \
R2_ACCESS_KEY_ID=your_key_id \
R2_SECRET_ACCESS_KEY=your_secret \
./target/release/otlp2parquet

# Docker deployment example
docker build -t otlp2parquet .
docker run -p 4318:4318 \
  -e STORAGE_BACKEND=s3 \
  -e S3_BUCKET=my-logs-bucket \
  -e S3_REGION=us-east-1 \
  -e LOG_FORMAT=json \
  otlp2parquet
```

## Size Optimization Checklist

Target: <3MB compressed WASM

Current optimizations:
- `opt-level = "z"` (size optimization)
- LTO enabled
- `default-features = false` for all dependencies
- Minimal feature flags
- Snappy compression only
- Strip symbols

Profile with twiggy to identify bloat:
```bash
make wasm-profile
```

If over 3MB, consider these steps:
- Use Arrow IPC instead of Parquet for CF Workers.
- Strip more Arrow features.
- Consider a custom protobuf parser (without `prost`).

## Development Tooling

### `uv` - Fast Python Package Manager

This project uses [uv](https://github.com/astral.sh/uv) for managing Python-based development tools. `uv` is an extremely fast Python package installer and resolver written in Rust.

**Installation:**
```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Verify installation
uv --version
```

**Usage:**
```bash
# Run a tool without installing it globally (recommended)
uvx pre-commit run --all-files

# Install a package in a virtual environment
uv pip install pre-commit

# Run a Python script with automatic dependency management
uv run script.py
```

### Pre-commit Hooks

Pre-commit hooks ensure code quality and consistency before commits. The project uses:

**Rust Hooks:**
- `cargo fmt` - Format Rust code with rustfmt
- `cargo clippy` - Lint Rust code with clippy (zero warnings policy)

**General Hooks:**
- YAML/TOML validation
- Trailing whitespace removal
- End-of-file fixer
- Large file detection
- Merge conflict detection

**Setup:**
```bash
# Install pre-commit hooks (one-time setup)
uvx pre-commit install

# Run hooks manually on all files
uvx pre-commit run --all-files

# Run hooks on staged files only
uvx pre-commit run

# Update hook versions
uvx pre-commit autoupdate
```

**Note:** Hooks run automatically on `git commit`. If a hook fails:
1. Review the changes made by auto-fixers (fmt)
2. Fix any issues reported by linters (clippy)
3. Stage the changes and commit again

## Testing Strategy

`otlp2parquet` employs a comprehensive testing strategy:

1.  **Unit tests**: For OTLP parsing, Arrow conversion, and schema validation.
2.  **Integration tests**: End-to-end tests with mock storage.
3.  **Size tests**: Assert binary size < 3MB compressed for WASM targets.
4.  **Load tests**: Using `otel-cli` or custom generators.
5.  **Platform tests**: Utilizing Miniflare for Cloudflare Workers and `cargo-lambda` for AWS Lambda.

```
