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
	@echo "==> Checking lambda..."
	@cargo check -p otlp2parquet-lambda
	@echo "==> Checking cloudflare..."
	@cargo check -p otlp2parquet-cloudflare --target wasm32-unknown-unknown

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
	@echo "==> Clippy lambda..."
	@cargo clippy -p otlp2parquet-lambda --all-targets -- -D warnings
	@echo "==> Clippy cloudflare..."
	@cargo clippy -p otlp2parquet-cloudflare --all-targets --target wasm32-unknown-unknown -- -D warnings

.PHONY: test
test: ## Run tests for all testable feature combinations
	@echo "==> Testing server..."
	@cargo test
	@echo "==> Testing lambda..."
	@cargo test -p otlp2parquet-lambda
	@echo "==> Testing core (no features)..."
	@cargo test -p otlp2parquet-core

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
	@echo "==> Building lambda..."
	@cargo build -p otlp2parquet-lambda
	@echo "==> Building cloudflare..."
	@cargo build -p otlp2parquet-cloudflare --target wasm32-unknown-unknown

.PHONY: build-release
build-release: ## Build all feature combinations (release mode)
	@echo "==> Building server (release)..."
	@cargo build --release
	@echo "==> Building lambda (release)..."
	@cargo build --release -p otlp2parquet-lambda
	@echo "==> Building cloudflare (release)..."
	@cargo build --release -p otlp2parquet-cloudflare --target wasm32-unknown-unknown

.PHONY: build-server
build-server: ## Build server binary only (default mode)
	@cargo build --release

.PHONY: build-lambda
build-lambda: ## Build Lambda binary and create bootstrap-arm64.zip for deployment
	@echo "==> Building Lambda binary with cargo-lambda..."
	@cd crates/otlp2parquet-lambda && cargo lambda build --release --arm64
	@echo "==> Creating bootstrap-arm64.zip..."
	@cd target/lambda/bootstrap && zip -q ../bootstrap-arm64.zip bootstrap
	@echo "==> Lambda deployment package ready at: target/lambda/bootstrap-arm64.zip"
	@SIZE=$$(stat -f%z target/lambda/bootstrap-arm64.zip 2>/dev/null || stat -c%s target/lambda/bootstrap-arm64.zip 2>/dev/null); \
	python3 -c "import sys; size=int(sys.argv[1]); print(f\"==> Package size: {size/1024:.1f} KB ({size/1024/1024:.2f} MB)\")" $$SIZE

.PHONY: build-cloudflare
build-cloudflare: ## Build Cloudflare Workers with worker-build
	@echo "==> Building WASM for Cloudflare Workers..."
	@cd crates/otlp2parquet-cloudflare && cargo install -q worker-build && worker-build --release
	@echo "==> WASM ready at: crates/otlp2parquet-cloudflare/build/index_bg.wasm"
	@SIZE=$$(stat -f%z crates/otlp2parquet-cloudflare/build/index_bg.wasm 2>/dev/null || stat -c%s crates/otlp2parquet-cloudflare/build/index_bg.wasm 2>/dev/null); \
	python3 -c "import sys; size=int(sys.argv[1]); print(f\"==> WASM size: {size/1024:.1f} KB ({size/1024/1024:.3f} MB)\")" $$SIZE

.PHONY: build-cli
build-cli: ## Build CLI binary in release mode
	@echo "==> Building otlp2parquet CLI binary..."
	@cargo build --release -p otlp2parquet --bin otlp2parquet
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
# WASM-Specific Commands
#

WASM_BINARY := crates/otlp2parquet-cloudflare/build/index_bg.wasm
WASM_OPTIMIZED := crates/otlp2parquet-cloudflare/build/index_bg_optimized.wasm
WASM_COMPRESSED := crates/otlp2parquet-cloudflare/build/index_bg_optimized.wasm.gz

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
	@wasm-opt -Oz --enable-bulk-memory --enable-nontrapping-float-to-int --enable-sign-ext \
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
		python3 -c "import sys; size=int(sys.argv[1]); label=sys.argv[2]; print(f\"{label}:  {size/1024:.1f} KB ({size/1024/1024:.3f} MB)\")" $$SIZE Original; \
	fi
	@if [ -f $(WASM_OPTIMIZED) ]; then \
		SIZE=$$(stat -f%z $(WASM_OPTIMIZED) 2>/dev/null || stat -c%s $(WASM_OPTIMIZED) 2>/dev/null); \
		python3 -c "import sys; size=int(sys.argv[1]); label=sys.argv[2]; print(f\"{label}: {size/1024:.1f} KB ({size/1024/1024:.3f} MB)\")" $$SIZE Optimized; \
	fi
	@if [ -f $(WASM_COMPRESSED) ]; then \
		SIZE=$$(stat -f%z $(WASM_COMPRESSED) 2>/dev/null || stat -c%s $(WASM_COMPRESSED) 2>/dev/null); \
		python3 -c "import sys; size=int(sys.argv[1]); max_size=3145728; percent=size*100/max_size; print(f\"Compressed: {size/1024:.1f} KB ({size/1024/1024:.3f} MB) - {percent:.1f}% of 3MB limit\"); print(\"WARNING: Exceeds 3MB Cloudflare Workers limit!\" if size > max_size else \"✓ Within Cloudflare Workers 3MB limit\")" $$SIZE; \
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
	@rm -rf target/lambda

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
	@cargo doc --no-deps --workspace

.PHONY: doc-open
doc-open: ## Build and open documentation in browser
	@cargo doc --no-deps --workspace --open

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
	@echo "==> Installing Cloudflare Wrangler CLI..."
	@if command -v wrangler >/dev/null 2>&1; then \
		echo "wrangler already installed."; \
	elif command -v npm >/dev/null 2>&1; then \
		npm install -g wrangler; \
	else \
		echo "Please install wrangler manually: npm install -g wrangler"; \
	fi
	@echo "==> Installing AWS SAM CLI..."
	@if command -v sam >/dev/null 2>&1; then \
		echo "aws-sam-cli already installed."; \
	elif command -v brew >/dev/null 2>&1; then \
		brew install aws-sam-cli; \
	elif command -v pipx >/dev/null 2>&1; then \
		pipx install aws-sam-cli; \
	else \
		echo "Please install aws-sam-cli manually: https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html"; \
	fi
	@echo "==> Installing cargo-lambda..."
	@if command -v cargo-lambda >/dev/null 2>&1; then \
		cargo-lambda --version; \
	else \
		cargo install --locked cargo-lambda; \
	fi
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
	@echo "WASM tools:"
	@wasm-opt --version 2>/dev/null || echo "wasm-opt: not installed"
	@twiggy --version 2>/dev/null || echo "twiggy: not installed"
	@echo ""
	@echo "Serverless tooling:"
	@wrangler --version 2>/dev/null || echo "wrangler: not installed"
	@sam --version 2>/dev/null | head -n 1 || echo "aws-sam-cli: not installed"
	@cargo-lambda --version 2>/dev/null || echo "cargo-lambda: not installed"
	@echo ""
	@echo "Data tools:"
	@duckdb --version 2>/dev/null || echo "duckdb: not installed"

.PHONY: audit
audit: ## Run security audit on dependencies
	@cargo audit || (echo "cargo-audit not installed. Install with: cargo install cargo-audit" && exit 1)

#
# Performance Profiling & Benchmarking Commands
#

.PHONY: bench
bench: ## Run all benchmarks with Criterion
	@echo "==> Running benchmarks..."
	@cargo bench
	@echo "==> Benchmark results: target/criterion/report/index.html"

.PHONY: bench-baseline
bench-baseline: ## Save benchmark baseline for comparison
	@echo "==> Saving benchmark baseline..."
	@cargo bench -- --save-baseline base
	@echo "==> Baseline saved as 'base'"

.PHONY: bench-compare
bench-compare: ## Run benchmarks and compare against baseline
	@echo "==> Running benchmarks and comparing to baseline..."
	@cargo bench -- --baseline base
	@echo "==> Comparison results: target/criterion/report/index.html"

.PHONY: flamegraph
flamegraph: ## Generate CPU flamegraph for e2e_pipeline benchmark
	@echo "==> Generating flamegraph..."
	@if ! command -v cargo-flamegraph >/dev/null 2>&1; then \
		echo "Installing cargo-flamegraph..."; \
		cargo install flamegraph; \
	fi
	@cargo flamegraph --bench e2e_pipeline -- --bench
	@echo "==> Flamegraph saved to: flamegraph.svg"

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
profile-all: bloat llvm-lines flamegraph ## Run all profiling tools
	@echo "==> All profiling complete!"
	@echo "    - Binary size: bloat.txt"
	@echo "    - LLVM lines: llvm_lines.txt"
	@echo "    - CPU profile: flamegraph.svg"

#
# Smoke Tests (Unified Framework)
#

.PHONY: test-smoke
test-smoke: ## Run smoke tests for all platforms (requires Docker + cloud credentials)
	@echo "==> Running unified smoke tests for all platforms..."
	@cargo test --test smoke --features smoke-server,smoke-lambda,smoke-workers

.PHONY: smoke-server
smoke-server: ## Run server smoke tests only (requires Docker + DuckDB)
	@echo "==> Checking for DuckDB..."
	@if ! command -v duckdb >/dev/null 2>&1; then \
		echo "ERROR: DuckDB not found. Install it:"; \
		echo "  macOS: brew install duckdb"; \
		echo "  Linux: wget https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-amd64.zip && unzip duckdb_cli-linux-amd64.zip && sudo mv duckdb /usr/local/bin/"; \
		exit 1; \
	fi
	@echo "==> Running server smoke tests (both catalog modes)..."
	@cargo test --test smoke --features smoke-server -- --test-threads=1

.PHONY: smoke-server-verbose
smoke-server-verbose: ## Run server smoke tests with verbose output
	@echo "==> Running server smoke tests (verbose mode)..."
	cargo test --test smoke --features smoke-server -- --nocapture

.PHONY: test-all
test-all: test smoke-server ## Run unit tests + server smoke tests

#
# Platform Smoke Tests (requires cloud credentials)
#

.PHONY: smoke-lambda
smoke-lambda: build-lambda ## Run Lambda + S3 Tables smoke tests (requires AWS credentials)
	@echo "==> Running Lambda smoke tests..."
	@echo "Prerequisites: AWS credentials (deployment bucket auto-created)"
	@cargo test --test smoke --features smoke-lambda -- --test-threads=1

.PHONY: smoke-workers
smoke-workers: wasm-compress ## Run Workers + R2 Catalog smoke tests (requires Cloudflare credentials)
	@echo "==> Running Workers smoke tests..."
	@echo "Prerequisites: CLOUDFLARE_API_TOKEN, CLOUDFLARE_ACCOUNT_ID env vars"
	@if [ -f crates/otlp2parquet-cloudflare/.env ]; then \
		echo "Loading environment from crates/otlp2parquet-cloudflare/.env"; \
		set -a && . crates/otlp2parquet-cloudflare/.env && set +a && \
		PATH="$$PATH" cargo test --test smoke --features smoke-workers -- --test-threads=1; \
	else \
		PATH="$$PATH" cargo test --test smoke --features smoke-workers -- --test-threads=1; \
	fi

.PHONY: smoke-all
smoke-all: smoke-lambda smoke-workers ## Run all platform smoke tests (requires all cloud credentials)

.PHONY: test-full
test-full: test smoke-server ## Run unit tests + server smoke tests (no cloud required)

.PHONY: test-ci
test-ci: test-full smoke-all ## Run complete test suite including cloud smoke tests
