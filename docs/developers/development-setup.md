# Development Setup

This guide shows how to set up a local development environment for building, testing, and contributing to `otlp2parquet`.

## Prerequisites & Initial Setup

First, set up the necessary tools and pre-commit hooks.

### Install Tools

```bash
# Install the Rust stable toolchain
rustup toolchain install stable

# Add components for formatting, linting, and WASM builds
rustup component add rustfmt clippy
rustup target add wasm32-unknown-unknown

# Install wasm-opt for WASM optimization (macOS example)
brew install binaryen

# Install other recommended development tools
cargo install twiggy # WASM binary profiler
curl -LsSf https://astral.sh/uv/install.sh | sh # uv for Python tools
```

### Configure Pre-commit Hooks

This project uses pre-commit hooks to automatically format and lint code. This is the recommended way to ensure contributions meet project standards.

```bash
# Install the hooks into your local git repository (one-time setup)
uvx pre-commit install
```

After installation, the hooks will run automatically on every `git commit`.

## Building the Project

The easiest way to build the project is with the provided `Makefile`.

### Using the Makefile (Recommended)

The `Makefile` contains targets for all common development tasks.

```bash
# Show all available commands
make help

# Run a quick format, lint, and test cycle
make dev

# Build for a specific platform
make build-server
make build-lambda
make build-cloudflare

# Build, optimize, and profile the WASM binary
make wasm-full
```

### Manual Build Commands

While the `Makefile` is recommended, you can also use `cargo` directly.

*   **Server**: `cargo build --release`
*   **AWS Lambda**: `cargo build --release -p otlp2parquet-lambda`
*   **Cloudflare Workers**: See the `make build-cloudflare` target for the full `worker-build` command.

## Testing Strategy

`otlp2parquet` uses a multi-layered testing strategy.

*   **Unit Tests**: Validate individual components like OTLP parsing and Arrow conversion.
*   **Integration Tests**: Provide end-to-end testing with mock storage.
*   **Platform Tests**: Use tools like Miniflare and `cargo-lambda` to test platform-specific behavior.

To run all tests for the default platform:

```bash
make test
```

## Size Optimization

For serverless platforms, especially Cloudflare Workers, binary size is critical. The release profile is already configured for size (`opt-level = "z"`, LTO, etc.).

To analyze the WASM binary size, use the `twiggy` profiler via the Makefile:

```bash
# Profile the WASM binary to identify size contributors
make wasm-profile
```

If the binary exceeds the size limit (3MB compressed), this output will help identify areas for optimization.
