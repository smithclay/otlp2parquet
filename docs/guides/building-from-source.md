# Building from Source

This guide shows how to set up a local development environment for building, testing, and running `otlp2parquet` from the latest source code.

If you are interested in contributing to the project, see the [Contributing Overview](../contributing/overview.md) after setting up your environment.

## 1. Install Tools

First, install the necessary tools and Rust components.

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

## 2. Configure Pre-commit Hooks (Optional)

If you plan to contribute, install the pre-commit hooks to automatically format and lint your code.

```bash
# Install the hooks into your local git repository
uvx pre-commit install
```

## 3. Build the Project

!!! tip "Use the Makefile"
    The easiest way to build and test the project is with the provided `Makefile`, which contains targets for all common development tasks. Run `make help` to see all available commands.

**Common `make` commands:**
```bash
# Run a quick format, lint, and test cycle
make dev

# Build for a specific platform
make build-server
make build-lambda
make build-cloudflare
```

While the `Makefile` is recommended, you can also use `cargo` directly.
*   **Server**: `cargo build --release`
*   **AWS Lambda**: `cargo build --release -p otlp2parquet-lambda`

## 4. Run Tests

`otlp2parquet` uses a multi-layered testing strategy. To run all tests for the default platform:
```bash
make test
```
