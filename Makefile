# otlp2parquet Makefile
# Provides convenient targets for building, testing, and linting across all feature combinations

.PHONY: help
help: ## Show this help message
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

#
# Standard Rust Development Commands
#

.PHONY: check
check: ## Run cargo check on all feature combinations
	@echo "==> Checking server..."
	@cargo check --no-default-features --features server
	@echo "==> Checking lambda..."
	@cargo check --no-default-features --features lambda
	@echo "==> Checking lambda+grpc..."
	@cargo check --no-default-features --features lambda,grpc
	@echo "==> Checking cloudflare..."
	@cargo check --no-default-features --features cloudflare --target wasm32-unknown-unknown

.PHONY: fmt
fmt: ## Format all Rust code
	@cargo fmt --all

.PHONY: fmt-check
fmt-check: ## Check Rust code formatting
	@cargo fmt --all -- --check

.PHONY: clippy
clippy: ## Run clippy on all feature combinations
	@echo "==> Clippy server..."
	@cargo clippy --all-targets --no-default-features --features server -- -D warnings
	@echo "==> Clippy lambda..."
	@cargo clippy --all-targets --no-default-features --features lambda -- -D warnings
	@echo "==> Clippy lambda+grpc..."
	@cargo clippy --all-targets --no-default-features --features lambda,grpc -- -D warnings
	@echo "==> Clippy cloudflare..."
	@cargo clippy --all-targets --no-default-features --features cloudflare --target wasm32-unknown-unknown -- -D warnings

.PHONY: test
test: ## Run tests for all testable feature combinations
	@echo "==> Testing server..."
	@cargo test --no-default-features --features server
	@echo "==> Testing lambda..."
	@cargo test --no-default-features --features lambda
	@echo "==> Testing lambda+grpc..."
	@cargo test --no-default-features --features lambda,grpc
	@echo "==> Testing core (no features)..."
	@cargo test -p otlp2parquet-core

.PHONY: test-verbose
test-verbose: ## Run tests with verbose output
	@cargo test --no-default-features --features server -- --nocapture

#
# Build Commands by Feature
#

.PHONY: build
build: ## Build all feature combinations (debug mode)
	@echo "==> Building server..."
	@cargo build --no-default-features --features server
	@echo "==> Building lambda..."
	@cargo build --no-default-features --features lambda
	@echo "==> Building lambda+grpc..."
	@cargo build --no-default-features --features lambda,grpc
	@echo "==> Building cloudflare..."
	@cargo build --no-default-features --features cloudflare --target wasm32-unknown-unknown

.PHONY: build-release
build-release: ## Build all feature combinations (release mode)
	@echo "==> Building server (release)..."
	@cargo build --release --no-default-features --features server
	@echo "==> Building lambda (release)..."
	@cargo build --release --no-default-features --features lambda
	@echo "==> Building lambda+grpc (release)..."
	@cargo build --release --no-default-features --features lambda,grpc
	@echo "==> Building cloudflare (release)..."
	@cargo build --release --no-default-features --features cloudflare --target wasm32-unknown-unknown

.PHONY: build-server
build-server: ## Build server binary only (default mode)
	@cargo build --release --no-default-features --features server

.PHONY: build-lambda
build-lambda: ## Build Lambda binary only
	@cargo build --release --no-default-features --features lambda

.PHONY: build-lambda-grpc
build-lambda-grpc: ## Build Lambda binary with gRPC support
	@cargo build --release --no-default-features --features lambda,grpc

.PHONY: build-cloudflare
build-cloudflare: ## Build Cloudflare Workers WASM binary
	@cargo build --release --no-default-features --features cloudflare --target wasm32-unknown-unknown

#
# WASM-Specific Commands
#

WASM_BINARY := target/wasm32-unknown-unknown/release/otlp2parquet.wasm
WASM_OPTIMIZED := target/wasm32-unknown-unknown/release/otlp2parquet-optimized.wasm
WASM_COMPRESSED := target/wasm32-unknown-unknown/release/otlp2parquet-optimized.wasm.gz

.PHONY: wasm
wasm: build-cloudflare ## Build WASM binary for Cloudflare Workers

.PHONY: wasm-opt
wasm-opt: wasm ## Optimize WASM binary with wasm-opt
	@echo "==> Optimizing WASM binary..."
	@if ! command -v wasm-opt >/dev/null 2>&1; then \
		echo "ERROR: wasm-opt not found. Install binaryen:"; \
		echo "  macOS: brew install binaryen"; \
		echo "  Linux: apt install binaryen or download from https://github.com/WebAssembly/binaryen/releases"; \
		exit 1; \
	fi
	@wasm-opt -Oz --enable-bulk-memory --enable-nontrapping-float-to-int \
		-o $(WASM_OPTIMIZED) $(WASM_BINARY)
	@echo "==> WASM optimization complete"
	@$(MAKE) wasm-size

.PHONY: wasm-compress
wasm-compress: wasm-opt ## Compress optimized WASM binary with gzip
	@echo "==> Compressing WASM binary..."
	@gzip -9 -f -k $(WASM_OPTIMIZED)
	@echo "==> Compression complete"
	@$(MAKE) wasm-size

.PHONY: wasm-size
wasm-size: ## Show WASM binary sizes
	@echo ""
	@echo "WASM Binary Sizes:"
	@echo "=================="
	@if [ -f $(WASM_BINARY) ]; then \
		SIZE=$$(stat -f%z $(WASM_BINARY) 2>/dev/null || stat -c%s $(WASM_BINARY) 2>/dev/null); \
		SIZE_KB=$$(echo "scale=1; $$SIZE / 1024" | bc); \
		SIZE_MB=$$(echo "scale=3; $$SIZE / 1024 / 1024" | bc); \
		echo "Original:  $$SIZE_KB KB ($$SIZE_MB MB)"; \
	fi
	@if [ -f $(WASM_OPTIMIZED) ]; then \
		SIZE=$$(stat -f%z $(WASM_OPTIMIZED) 2>/dev/null || stat -c%s $(WASM_OPTIMIZED) 2>/dev/null); \
		SIZE_KB=$$(echo "scale=1; $$SIZE / 1024" | bc); \
		SIZE_MB=$$(echo "scale=3; $$SIZE / 1024 / 1024" | bc); \
		echo "Optimized: $$SIZE_KB KB ($$SIZE_MB MB)"; \
	fi
	@if [ -f $(WASM_COMPRESSED) ]; then \
		SIZE=$$(stat -f%z $(WASM_COMPRESSED) 2>/dev/null || stat -c%s $(WASM_COMPRESSED) 2>/dev/null); \
		SIZE_KB=$$(echo "scale=1; $$SIZE / 1024" | bc); \
		SIZE_MB=$$(echo "scale=3; $$SIZE / 1024 / 1024" | bc); \
		MAX_SIZE=3145728; \
		PERCENT=$$(echo "scale=1; ($$SIZE * 100) / $$MAX_SIZE" | bc); \
		echo "Compressed: $$SIZE_KB KB ($$SIZE_MB MB) - $$PERCENT% of 3MB limit"; \
		if [ $$SIZE -gt $$MAX_SIZE ]; then \
			echo "WARNING: Exceeds 3MB Cloudflare Workers limit!"; \
		else \
			echo "✓ Within Cloudflare Workers 3MB limit"; \
		fi \
	fi
	@echo ""

.PHONY: wasm-profile
wasm-profile: wasm ## Profile WASM binary size with twiggy
	@if ! command -v twiggy >/dev/null 2>&1; then \
		echo "Installing twiggy..."; \
		cargo install twiggy; \
	fi
	@echo "==> Top 20 largest items in WASM binary:"
	@twiggy top -n 20 $(WASM_BINARY)

.PHONY: wasm-full
wasm-full: wasm-compress wasm-profile ## Build, optimize, compress, and profile WASM binary

#
# Clean Commands
#

.PHONY: clean
clean: ## Remove build artifacts
	@cargo clean

.PHONY: clean-wasm
clean-wasm: ## Remove WASM-specific artifacts
	@rm -f $(WASM_OPTIMIZED) $(WASM_COMPRESSED)
	@echo "Cleaned WASM artifacts"

#
# Development Workflow Commands
#

.PHONY: pre-commit
pre-commit: fmt clippy test ## Run pre-commit checks (fmt, clippy, test)

.PHONY: ci
ci: fmt-check clippy test build-release wasm-compress ## Run full CI pipeline locally

.PHONY: dev
dev: check test ## Quick development check (fast)

#
# Documentation
#

.PHONY: doc
doc: ## Build documentation
	@cargo doc --no-deps --features server,lambda,grpc

.PHONY: doc-open
doc-open: ## Build and open documentation in browser
	@cargo doc --no-deps --features server,lambda,grpc --open

#
# Utility Commands
#

.PHONY: install-tools
install-tools: ## Install required development tools
	@echo "==> Installing Rust toolchain..."
	@rustup toolchain install stable
	@rustup component add rustfmt clippy
	@rustup target add wasm32-unknown-unknown
	@echo "==> Installing wasm-opt (binaryen)..."
	@if command -v brew >/dev/null 2>&1; then \
		brew install binaryen; \
	else \
		echo "Please install binaryen manually: https://github.com/WebAssembly/binaryen/releases"; \
	fi
	@echo "==> Installing twiggy..."
	@cargo install twiggy
	@echo "==> Installing uv (Python package manager)..."
	@curl -LsSf https://astral.sh/uv/install.sh | sh
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
	@echo "WASM tools:"
	@wasm-opt --version 2>/dev/null || echo "wasm-opt: not installed"
	@twiggy --version 2>/dev/null || echo "twiggy: not installed"

.PHONY: audit
audit: ## Run security audit on dependencies
	@cargo audit || (echo "cargo-audit not installed. Install with: cargo install cargo-audit" && exit 1)
