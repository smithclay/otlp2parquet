# otlp2parquet Makefile
# Provides convenient targets for building, testing, and linting across all feature combinations

.PHONY: help
help: ## Show this help message
	@echo "Available targets:"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

#
# Standard Rust Development Commands
#

.PHONY: check
check: ## Run cargo check on all feature combinations
	@echo "==> Checking server..."
	@cargo check

.PHONY: fmt
fmt: ## Format all Rust code
	@cargo fmt --all

.PHONY: fmt-check
fmt-check: ## Check Rust code formatting
	@cargo fmt --all -- --check

.PHONY: clippy
clippy: ## Run clippy on all feature combinations
	@echo "==> Clippy server..."
	@cargo clippy --all-targets -- -D warnings

.PHONY: test
test: ## Run tests for all testable feature combinations
	@echo "==> Testing server..."
	@cargo test

.PHONY: test-verbose
test-verbose: ## Run tests with verbose output
	@cargo test -- --nocapture

#
# Build Commands by Feature
#

.PHONY: build
build: ## Build all feature combinations (debug mode)
	@echo "==> Building server..."
	@cargo build

.PHONY: build-release
build-release: ## Build all feature combinations (release mode)
	@echo "==> Building server (release)..."
	@cargo build --release

.PHONY: build-server
build-server: ## Build server binary only (default mode)
	@cargo build --release

.PHONY: build-cli
build-cli: ## Build CLI binary in release mode
	@echo "==> Building otlp2parquet CLI binary..."
	@cargo build --release --bin otlp2parquet
	@echo "==> Binary available at: target/release/otlp2parquet"

.PHONY: install-cli
install-cli: build-cli ## Install CLI binary to /usr/local/bin (requires sudo)
	@echo "==> Installing otlp2parquet to /usr/local/bin..."
	@cp target/release/otlp2parquet /usr/local/bin/
	@echo "==> Installed successfully. Run 'otlp2parquet --help' to get started."

.PHONY: run-cli
run-cli: ## Run CLI binary in development mode
	@cargo run --bin otlp2parquet

#
# Demo WASM Build
#

.PHONY: clean-wasm-demo
clean-wasm-demo: ## Remove demo WASM artifacts
	@rm -rf docs/query-demo/wasm
	@echo "Cleaned demo WASM artifacts"

#
# Clean Commands
#

.PHONY: clean
clean: ## Remove build artifacts
	@cargo clean
	@echo "Cleaned all build artifacts"

#
# Development Workflow Commands
#

.PHONY: pre-commit
pre-commit: fmt clippy test ## Run pre-commit checks (fmt, clippy, test)

.PHONY: ci
ci: fmt-check clippy test build-release ## Run full CI pipeline locally

.PHONY: dev
dev: check test ## Quick development check (fast)

#
# Documentation
#

.PHONY: doc
doc: ## Build documentation
	@cargo doc --no-deps

.PHONY: doc-open
doc-open: ## Build and open documentation in browser
	@cargo doc --no-deps --open

#
# Utility Commands
#

.PHONY: install-tools
install-tools: ## Install required development tools
	@echo "==> Installing Rust toolchain..."
	@rustup toolchain install stable
	@rustup component add rustfmt clippy
	@echo "==> Installing uv (Python package manager)..."
	@curl -LsSf https://astral.sh/uv/install.sh | sh
	@echo "==> Installing DuckDB (for smoke tests)..."
	@if command -v duckdb >/dev/null 2>&1; then \
		echo "duckdb already installed."; \
	elif command -v brew >/dev/null 2>&1; then \
		brew install duckdb; \
	else \
		echo "Please install DuckDB manually:"; \
		echo "  Linux: wget https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-amd64.zip && unzip duckdb_cli-linux-amd64.zip && sudo mv duckdb /usr/local/bin/"; \
		echo "  macOS: brew install duckdb"; \
	fi
	@echo "==> Setting up pre-commit hooks..."
	@uvx pre-commit install
	@echo "==> All tools installed!"

.PHONY: version
version: ## Show versions of build tools
	@echo "Rust toolchain:"
	@rustc --version
	@cargo --version
	@echo ""
	@echo "Components:"
	@rustfmt --version 2>/dev/null || echo "rustfmt: not installed"
	@cargo clippy --version 2>/dev/null || echo "clippy: not installed"
	@echo ""
	@echo "Data tools:"
	@duckdb --version 2>/dev/null || echo "duckdb: not installed"

.PHONY: audit
audit: ## Run security audit on dependencies
	@cargo audit || (echo "cargo-audit not installed. Install with: cargo install cargo-audit" && exit 1)

#
# Publishing Commands
#

.PHONY: publish-dry-run
publish-dry-run: ## Dry-run publish crate to crates.io
	@echo "==> Dry-run publishing crate to crates.io..."
	@cargo publish --dry-run

.PHONY: publish
publish: ## Publish crate to crates.io
	@echo "==> Publishing crate to crates.io..."
	@cargo publish

#
# Performance Profiling Commands
#

.PHONY: bloat
bloat: ## Analyze binary size with cargo-bloat
	@echo "==> Analyzing binary size (top 20)..."
	@if ! command -v cargo-bloat >/dev/null 2>&1; then \
		echo "Installing cargo-bloat..."; \
		cargo install cargo-bloat; \
	fi
	@cargo bloat --release -n 20 | tee bloat.txt
	@echo "==> Results saved to: bloat.txt"

.PHONY: llvm-lines
llvm-lines: ## Analyze LLVM IR line counts with cargo-llvm-lines
	@echo "==> Analyzing LLVM IR line counts..."
	@if ! command -v cargo-llvm-lines >/dev/null 2>&1; then \
		echo "Installing cargo-llvm-lines..."; \
		cargo install cargo-llvm-lines; \
	fi
	@cargo llvm-lines --release | head -50 | tee llvm_lines.txt
	@echo "==> Results saved to: llvm_lines.txt"

.PHONY: profile-all
profile-all: bloat llvm-lines ## Run all profiling tools
	@echo "==> All profiling complete!"
	@echo "    - Binary size: bloat.txt"
	@echo "    - LLVM lines: llvm_lines.txt"

#
# Smoke Tests (Unified Framework)
#

.PHONY: test-smoke
test-smoke: ## Run smoke tests for server only (requires Docker + DuckDB)
	@echo "==> Running server smoke tests..."
	@cargo test --test smoke --features smoke-server -- --test-threads=1

.PHONY: smoke-server
smoke-server: ## Run server smoke tests only (requires Docker + DuckDB)
	@echo "==> Checking for DuckDB..."
	@if ! command -v duckdb >/dev/null 2>&1; then \
		echo "ERROR: DuckDB not found. Install it:"; \
		echo "  macOS: brew install duckdb"; \
		echo "  Linux: wget https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-amd64.zip && unzip duckdb_cli-linux-amd64.zip && sudo mv duckdb /usr/local/bin/"; \
		exit 1; \
	fi
	@echo "==> Running server smoke tests (plain Parquet)..."
	@cargo test --test smoke --features smoke-server -- --test-threads=1

.PHONY: smoke-server-verbose
smoke-server-verbose: ## Run server smoke tests with verbose output
	@echo "==> Running server smoke tests (verbose mode)..."
	cargo test --test smoke --features smoke-server -- --nocapture

.PHONY: test-all
test-all: test smoke-server ## Run unit tests + server smoke tests

.PHONY: test-full
test-full: test smoke-server ## Run unit tests + server smoke tests (no cloud required)
