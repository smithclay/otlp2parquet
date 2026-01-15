# Contributing

## Setup

Clone and build:

```bash
git clone https://github.com/smithclay/otlp2parquet
cd otlp2parquet
make dev
```

Install development tools:

```bash
make install-tools
```

This installs:
- Rust toolchain with clippy and rustfmt
- DuckDB
- pre-commit hooks

## Build Commands

Use the Makefile. Avoid calling cargo directly.

```bash
# Build CLI binary
make build-cli

# Check all platforms
make check

# Run tests
make test

# Run server smoke tests
make smoke-server
```

## Code Style

Write production code that never panics.

- Use `tracing::info!`, `tracing::error!`, etc. No `println!` or `eprintln!`.
- Propagate errors. No `unwrap()` or `expect()` in production paths.
- Keep dependencies minimal. Use `default-features = false` everywhere.
- Keep allocations lean. Prefer `&str` over `String`.
- Run clippy. Fix all warnings. Zero tolerance.

## Testing

Run tests before committing:

```bash
make pre-commit
```

This runs:
1. `cargo fmt` - format code
2. `cargo clippy` - lint all platforms
3. `cargo test` - unit tests

For server smoke tests (requires Docker and DuckDB):

```bash
make smoke-server
```

## Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add histogram metric support
fix: correct timestamp parsing for traces
docs: update deployment guide
refactor: simplify Arrow conversion
test: add e2e test for batching
```

The pre-commit hook enforces this format.

## Pull Requests

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run `make pre-commit`
5. Commit with conventional format
6. Push and open a PR

Keep PRs focused. One feature or fix per PR.

## Questions

Open an issue on GitHub.
