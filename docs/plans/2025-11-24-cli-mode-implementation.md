# CLI Mode Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add CLI binary target to otlp2parquet-server crate with ergonomic argument parsing and smart defaults for local development.

**Architecture:** Add `main.rs` binary that uses clap for CLI parsing, merges config sources (CLI args → env vars → config file → defaults), validates early, and calls existing `lib.rs::run()` function. Extend otlp2parquet-config with helpers for loading without required files.

**Tech Stack:** Rust, clap 4.5 (derive API), tokio async runtime, existing otlp2parquet-config crate

---

## Task 1: Add clap dependency and binary target configuration

**Files:**
- Modify: `crates/otlp2parquet-server/Cargo.toml`

**Step 1: Add clap dependency**

Add clap to the dependencies section in `crates/otlp2parquet-server/Cargo.toml`:

```toml
# Add after existing dependencies
clap = { version = "4.5", default-features = false, features = ["std", "derive", "help", "usage", "error-context"] }
```

**Step 2: Add binary target**

Add binary target configuration to `crates/otlp2parquet-server/Cargo.toml` after the `[package]` section:

```toml
[[bin]]
name = "otlp2parquet"
path = "src/main.rs"
```

**Step 3: Verify Cargo.toml changes**

Run: `cargo metadata --manifest-path crates/otlp2parquet-server/Cargo.toml --format-version 1 | grep -A 5 '"targets"'`

Expected: Should show both lib and bin targets

**Step 4: Commit**

```bash
git add crates/otlp2parquet-server/Cargo.toml
git commit -m "feat(server): add clap dependency and binary target config"
```

---

## Task 2: Add config loading helpers to otlp2parquet-config

**Files:**
- Modify: `crates/otlp2parquet-config/src/sources.rs`
- Test: Manual verification with cargo check

**Step 1: Add load_from_file_path public function**

In `crates/otlp2parquet-config/src/sources.rs`, after the existing `load_from_file` function (around line 57), add:

```rust
/// Load configuration from a specific file path (for CLI --config flag).
/// Returns error if file doesn't exist or can't be parsed.
pub fn load_from_file_path(path: impl AsRef<Path>) -> Result<RuntimeConfig> {
    let path = path.as_ref();
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read config file: {}", path.display()))?;
    let config: RuntimeConfig = toml::from_str(&content)
        .with_context(|| format!("Failed to parse config file: {}", path.display()))?;
    Ok(config)
}
```

**Step 2: Add load_or_default public function**

In `crates/otlp2parquet-config/src/sources.rs`, after the function you just added, add:

```rust
/// Load configuration with graceful fallback to defaults.
/// Tries standard config file locations, returns platform defaults if none found.
pub fn load_or_default(platform: Platform) -> Result<RuntimeConfig> {
    let mut config = RuntimeConfig::from_platform_defaults(platform);

    // Try to load from file, but don't fail if not found
    if let Ok(Some(file_config)) = load_from_file() {
        config.merge(file_config);
    }

    // Apply environment overrides
    let env_source = StdEnvSource;
    env_overrides::apply_env_overrides(&mut config, &env_source)?;

    config.validate()?;
    Ok(config)
}
```

**Step 3: Export new functions in lib.rs**

In `crates/otlp2parquet-config/src/lib.rs`, find the existing public API section and add new methods to RuntimeConfig impl block (around line 433):

```rust
    /// Load configuration from a specific file path (for CLI usage).
    #[cfg(not(target_arch = "wasm32"))]
    pub fn load_from_path(path: impl AsRef<std::path::Path>) -> Result<Self> {
        sources::load_from_file_path(path)
    }

    /// Load configuration with graceful fallback to defaults.
    /// Does not fail if config file is missing - uses platform defaults instead.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn load_or_default() -> Result<Self> {
        sources::load_or_default(Platform::detect())
    }
```

**Step 4: Verify compilation**

Run: `cargo check -p otlp2parquet-config`

Expected: No errors, clean compilation

**Step 5: Commit**

```bash
git add crates/otlp2parquet-config/src/sources.rs crates/otlp2parquet-config/src/lib.rs
git commit -m "feat(config): add load_from_path and load_or_default methods"
```

---

## Task 3: Refactor lib.rs to expose run_with_config

**Files:**
- Modify: `crates/otlp2parquet-server/src/lib.rs:127-218`

**Step 1: Extract current run() logic into run_with_config**

In `crates/otlp2parquet-server/src/lib.rs`, replace the existing `run()` function (lines 127-218) with:

```rust
/// Entry point for server mode (loads config automatically)
pub async fn run() -> Result<()> {
    let config = RuntimeConfig::load().context("Failed to load configuration")?;
    run_with_config(config).await
}

/// Entry point for server mode with pre-loaded configuration (for CLI usage)
pub async fn run_with_config(config: RuntimeConfig) -> Result<()> {
    // Initialize tracing with config
    init_tracing(&config);

    // Configure Parquet writer properties before first use
    set_parquet_row_group_size(config.storage.parquet_row_group_size);

    info!("Server mode - full-featured HTTP server with multi-backend storage");

    // Get listen address from config
    let addr = config
        .server
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("server config required"))?
        .listen_addr
        .clone();

    // Initialize catalog and namespace
    let (catalog, namespace) = init_writer(&config).await?;

    // Configure batching
    let batch_config = BatchConfig {
        max_rows: config.batch.max_rows,
        max_bytes: config.batch.max_bytes,
        max_age: Duration::from_secs(config.batch.max_age_secs),
    };

    let batcher = if !config.batch.enabled {
        info!("Batching disabled by configuration");
        None
    } else {
        info!(
            "Batching enabled (max_rows={} max_bytes={} max_age={}s)",
            batch_config.max_rows,
            batch_config.max_bytes,
            batch_config.max_age.as_secs()
        );
        Some(Arc::new(BatchManager::new(batch_config)))
    };

    let max_payload_bytes = config.request.max_payload_bytes;
    info!("Max payload size set to {} bytes", max_payload_bytes);

    // Create app state
    let state = AppState {
        catalog,
        namespace,
        batcher,
        passthrough: PassthroughBatcher::default(),
        max_payload_bytes,
    };

    let router_state = state.clone();

    // Build router
    let app = Router::new()
        .route("/v1/logs", post(handle_logs))
        .route("/v1/traces", post(handle_traces))
        .route("/v1/metrics", post(handle_metrics))
        .route("/health", get(health_check))
        .route("/ready", get(ready_check))
        .with_state(router_state);

    // Create TCP listener
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .context(format!("Failed to bind to {}", addr))?;

    info!("OTLP HTTP endpoint listening on http://{}", addr);
    info!("Routes:");
    info!("  POST http://{}/v1/logs    - OTLP log ingestion", addr);
    info!("  POST http://{}/v1/metrics - OTLP metrics ingestion", addr);
    info!("  POST http://{}/v1/traces  - OTLP trace ingestion", addr);
    info!("  GET  http://{}/health     - Health check", addr);
    info!("  GET  http://{}/ready      - Readiness check", addr);
    info!("Press Ctrl+C or send SIGTERM to stop");

    // Start server with graceful shutdown
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("Server error")?;

    flush_pending_batches(&state).await?;

    info!("Server shutdown complete");

    Ok(())
}
```

**Step 2: Verify compilation**

Run: `cargo check -p otlp2parquet-server`

Expected: No errors, clean compilation

**Step 3: Commit**

```bash
git add crates/otlp2parquet-server/src/lib.rs
git commit -m "refactor(server): extract run_with_config for CLI usage"
```

---

## Task 4: Create CLI argument parser in main.rs

**Files:**
- Create: `crates/otlp2parquet-server/src/main.rs`

**Step 1: Create main.rs with CLI struct**

Create `crates/otlp2parquet-server/src/main.rs` with:

```rust
use anyhow::{Context, Result};
use clap::Parser;
use otlp2parquet_config::RuntimeConfig;
use std::path::PathBuf;

/// OTLP HTTP server writing Parquet files to object storage
#[derive(Parser)]
#[command(name = "otlp2parquet")]
#[command(version)]
#[command(about = "OTLP HTTP server writing Parquet files to object storage", long_about = None)]
struct Cli {
    /// Path to configuration file
    #[arg(short, long, value_name = "FILE")]
    config: Option<PathBuf>,

    /// HTTP listen port (overrides config file)
    #[arg(short, long, value_name = "PORT")]
    port: Option<u16>,

    /// Output directory for Parquet files (filesystem backend only)
    #[arg(short, long, value_name = "DIR")]
    output: Option<PathBuf>,

    /// Log level: trace, debug, info, warn, error
    #[arg(short = 'v', long, value_name = "LEVEL")]
    log_level: Option<String>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Build tokio runtime and run async server
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("Failed to build tokio runtime")?
        .block_on(async_main(cli))
}

async fn async_main(cli: Cli) -> Result<()> {
    // TODO: Implement config loading and merging
    // For now, just test CLI parsing works
    println!("CLI args parsed successfully");
    if let Some(config) = &cli.config {
        println!("Config file: {}", config.display());
    }
    if let Some(port) = cli.port {
        println!("Port: {}", port);
    }
    if let Some(output) = &cli.output {
        println!("Output: {}", output.display());
    }
    if let Some(level) = &cli.log_level {
        println!("Log level: {}", level);
    }

    Ok(())
}
```

**Step 2: Test CLI parsing**

Run: `cargo run --bin otlp2parquet -- --help`

Expected: Should show help message with all flags documented

**Step 3: Test with arguments**

Run: `cargo run --bin otlp2parquet -- -p 8080 -o /tmp/test -v debug`

Expected: Should print parsed arguments

**Step 4: Commit**

```bash
git add crates/otlp2parquet-server/src/main.rs
git commit -m "feat(server): add CLI argument parser with clap"
```

---

## Task 5: Implement configuration loading and merging

**Files:**
- Modify: `crates/otlp2parquet-server/src/main.rs:29-42`

**Step 1: Replace async_main with full implementation**

In `crates/otlp2parquet-server/src/main.rs`, replace the `async_main` function (lines 29-42) with:

```rust
async fn async_main(cli: Cli) -> Result<()> {
    // Step 1: Load base configuration
    let mut config = if let Some(config_path) = &cli.config {
        // Explicit config file path provided
        RuntimeConfig::load_from_path(config_path)
            .with_context(|| format!("Failed to load config from {}", config_path.display()))?
    } else {
        // Try default locations, fall back to defaults
        RuntimeConfig::load_or_default()
            .context("Failed to load configuration")?
    };

    // Step 2: Apply CLI overrides (highest priority)
    apply_cli_overrides(&mut config, &cli)?;

    // Step 3: Apply desktop-friendly defaults
    apply_desktop_defaults(&mut config);

    // Step 4: Validate configuration early
    validate_config(&config).await?;

    // Step 5: Display startup info
    display_startup_info(&config);

    // Step 6: Run server with resolved config
    otlp2parquet_server::run_with_config(config).await
}
```

**Step 2: Add apply_cli_overrides function**

Add after `async_main`:

```rust
fn apply_cli_overrides(config: &mut RuntimeConfig, cli: &Cli) -> Result<()> {
    use otlp2parquet_config::{ServerConfig, StorageBackend};

    // Override port
    if let Some(port) = cli.port {
        let server = config.server.get_or_insert_with(ServerConfig::default);
        server.listen_addr = format!("0.0.0.0:{}", port);
    }

    // Override output directory (only valid for fs backend)
    if let Some(output) = &cli.output {
        if config.storage.backend != StorageBackend::Fs {
            anyhow::bail!(
                "--output flag only works with filesystem backend, but backend is '{}'.\n\
                Either remove --output flag or set backend to 'fs' in config file.",
                config.storage.backend
            );
        }

        let fs_config = config.storage.fs.get_or_insert_with(Default::default);
        fs_config.path = output.to_string_lossy().to_string();
    }

    // Override log level
    if let Some(level) = &cli.log_level {
        let server = config.server.get_or_insert_with(ServerConfig::default);
        server.log_level = level.clone();
    }

    Ok(())
}
```

**Step 3: Add apply_desktop_defaults function**

Add after `apply_cli_overrides`:

```rust
fn apply_desktop_defaults(config: &mut RuntimeConfig) {
    use otlp2parquet_config::{CatalogMode, LogFormat, ServerConfig, StorageBackend};

    // Ensure server config exists with defaults
    let server = config.server.get_or_insert_with(ServerConfig::default);

    // Desktop-friendly log format (human-readable)
    server.log_format = LogFormat::Text;

    // Default to filesystem backend for desktop
    if config.storage.backend == StorageBackend::Fs {
        config.storage.fs.get_or_insert_with(Default::default);
    }

    // Default to no Iceberg catalog (simpler for local dev)
    if config.catalog_mode == CatalogMode::Iceberg && config.iceberg.is_none() {
        config.catalog_mode = CatalogMode::None;
    }
}
```

**Step 4: Verify compilation**

Run: `cargo check -p otlp2parquet-server --bin otlp2parquet`

Expected: No errors

**Step 5: Commit**

```bash
git add crates/otlp2parquet-server/src/main.rs
git commit -m "feat(server): implement config loading and CLI overrides"
```

---

## Task 6: Implement early validation with directory creation

**Files:**
- Modify: `crates/otlp2parquet-server/src/main.rs` (add validate_config function)

**Step 1: Add validate_config function**

Add after `apply_desktop_defaults`:

```rust
async fn validate_config(config: &RuntimeConfig) -> Result<()> {
    use otlp2parquet_config::StorageBackend;
    use std::fs;
    use tracing::info;

    // Validate filesystem output directory if using fs backend
    if config.storage.backend == StorageBackend::Fs {
        let fs_config = config.storage.fs.as_ref()
            .ok_or_else(|| anyhow::anyhow!("filesystem backend requires storage.fs configuration"))?;

        let output_path = PathBuf::from(&fs_config.path);

        // Create directory if it doesn't exist
        if !output_path.exists() {
            info!("Creating output directory: {}", fs_config.path);
            fs::create_dir_all(&output_path)
                .with_context(|| format!("Failed to create output directory: {}", fs_config.path))?;
        }

        // Validate writability by creating a test file
        let test_file = output_path.join(".otlp2parquet-write-test");
        fs::write(&test_file, b"test")
            .with_context(|| format!(
                "Output directory '{}' is not writable. Check permissions.",
                fs_config.path
            ))?;
        fs::remove_file(&test_file)
            .context("Failed to remove test file")?;

        info!("Output directory validated: {}", fs_config.path);
    }

    // Validate server config exists
    config.server.as_ref()
        .ok_or_else(|| anyhow::anyhow!("server configuration required"))?;

    Ok(())
}
```

**Step 2: Add use statement for PathBuf at top of file**

At the top of `main.rs`, verify PathBuf is imported (should already be there from Step 4):

```rust
use std::path::PathBuf;
```

**Step 3: Test validation with valid path**

Run: `cargo run --bin otlp2parquet -- -o /tmp/otlp-test-data -p 9999`

Expected: Should create directory and validate it (won't start server yet, will fail during init_tracing)

**Step 4: Test validation with invalid path**

Run: `cargo run --bin otlp2parquet -- -o /root/forbidden 2>&1 | head -5`

Expected: Should fail with clear permission error

**Step 5: Commit**

```bash
git add crates/otlp2parquet-server/src/main.rs
git commit -m "feat(server): add early validation with directory creation"
```

---

## Task 7: Implement startup info display

**Files:**
- Modify: `crates/otlp2parquet-server/src/main.rs` (add display_startup_info function)

**Step 1: Add display_startup_info function**

Add after `validate_config`:

```rust
fn display_startup_info(config: &RuntimeConfig) {
    use otlp2parquet_config::StorageBackend;
    use tracing::info;

    let server = config.server.as_ref().expect("server config validated");

    info!("╭─────────────────────────────────────────────────");
    info!("│ otlp2parquet server starting");
    info!("├─────────────────────────────────────────────────");
    info!("│ Listen address: http://{}", server.listen_addr);
    info!("│ Storage backend: {}", config.storage.backend);

    if config.storage.backend == StorageBackend::Fs {
        if let Some(fs) = &config.storage.fs {
            info!("│ Output directory: {}", fs.path);
        }
    } else if config.storage.backend == StorageBackend::S3 {
        if let Some(s3) = &config.storage.s3 {
            info!("│ S3 bucket: {}", s3.bucket);
            info!("│ S3 region: {}", s3.region);
        }
    } else if config.storage.backend == StorageBackend::R2 {
        if let Some(r2) = &config.storage.r2 {
            info!("│ R2 bucket: {}", r2.bucket);
            info!("│ R2 account: {}", r2.account_id);
        }
    }

    info!("│ Log level: {}", server.log_level);
    info!("│ Catalog mode: {}", config.catalog_mode);
    info!("│ Batching: {}", if config.batch.enabled { "enabled" } else { "disabled" });

    if config.batch.enabled {
        info!("│   - Max rows: {}", config.batch.max_rows);
        info!("│   - Max bytes: {} MB", config.batch.max_bytes / 1_048_576);
        info!("│   - Max age: {}s", config.batch.max_age_secs);
    }

    info!("╰─────────────────────────────────────────────────");
}
```

**Step 2: Initialize tracing earlier in async_main**

Modify `async_main` to initialize tracing before validation so our info logs show up. Change the beginning of `async_main` to:

```rust
async fn async_main(cli: Cli) -> Result<()> {
    // Step 1: Load base configuration
    let mut config = if let Some(config_path) = &cli.config {
        RuntimeConfig::load_from_path(config_path)
            .with_context(|| format!("Failed to load config from {}", config_path.display()))?
    } else {
        RuntimeConfig::load_or_default()
            .context("Failed to load configuration")?
    };

    // Step 2: Apply CLI overrides (highest priority)
    apply_cli_overrides(&mut config, &cli)?;

    // Step 3: Apply desktop-friendly defaults
    apply_desktop_defaults(&mut config);

    // Step 4: Initialize tracing early so validation logs show up
    // Note: run_with_config will also call init_tracing, but that's idempotent-ish
    use otlp2parquet_server::init_tracing;
    init_tracing(&config);

    // Step 5: Validate configuration early
    validate_config(&config).await?;

    // Step 6: Display startup info
    display_startup_info(&config);

    // Step 7: Run server with resolved config
    otlp2parquet_server::run_with_config(config).await
}
```

**Step 3: Make init_tracing public in lib.rs**

In `crates/otlp2parquet-server/src/lib.rs`, find the `use` statement for `init` module (around line 36) and change it to make `init_tracing` public:

```rust
pub use init::init_tracing;
use init::init_writer;
```

**Step 4: Test startup info display**

Run: `RUST_LOG=info cargo run --bin otlp2parquet -- -p 4318 -o ./test-data`

Expected: Should show nice box-drawing startup info, then start server (Ctrl+C to stop)

**Step 5: Commit**

```bash
git add crates/otlp2parquet-server/src/main.rs crates/otlp2parquet-server/src/lib.rs
git commit -m "feat(server): add startup info display with config summary"
```

---

## Task 8: Add Makefile targets for CLI builds

**Files:**
- Modify: `Makefile`

**Step 1: Add build-cli target**

Add to the Makefile (find a good spot after existing build targets):

```makefile
# Build CLI binary in release mode
.PHONY: build-cli
build-cli:
	@echo "Building otlp2parquet CLI binary..."
	cargo build --release --bin otlp2parquet
	@echo "Binary available at: target/release/otlp2parquet"

# Install CLI binary to /usr/local/bin (requires sudo)
.PHONY: install-cli
install-cli: build-cli
	@echo "Installing otlp2parquet to /usr/local/bin..."
	cp target/release/otlp2parquet /usr/local/bin/
	@echo "Installed successfully. Run 'otlp2parquet --help' to get started."

# Run CLI binary in development mode
.PHONY: run-cli
run-cli:
	cargo run --bin otlp2parquet
```

**Step 2: Test make targets**

Run: `make build-cli`

Expected: Should build release binary successfully

**Step 3: Test binary directly**

Run: `./target/release/otlp2parquet --version`

Expected: Should show version

**Step 4: Test binary with args**

Run: `./target/release/otlp2parquet --help`

Expected: Should show help message

**Step 5: Commit**

```bash
git add Makefile
git commit -m "feat(build): add Makefile targets for CLI binary"
```

---

## Task 9: Add integration tests for CLI

**Files:**
- Create: `crates/otlp2parquet-server/tests/cli_integration_test.rs`

**Step 1: Create integration test file**

Create `crates/otlp2parquet-server/tests/cli_integration_test.rs`:

```rust
use anyhow::Result;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use tempfile::TempDir;

fn get_binary_path() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.pop(); // Go up to workspace root
    path.pop();
    path.push("target");
    path.push("debug");
    path.push("otlp2parquet");
    path
}

#[test]
fn test_cli_help() {
    let binary = get_binary_path();
    let output = Command::new(&binary)
        .arg("--help")
        .output()
        .expect("Failed to run binary");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("OTLP HTTP server"));
    assert!(stdout.contains("--port"));
    assert!(stdout.contains("--output"));
    assert!(stdout.contains("--log-level"));
    assert!(stdout.contains("--config"));
}

#[test]
fn test_cli_version() {
    let binary = get_binary_path();
    let output = Command::new(&binary)
        .arg("--version")
        .output()
        .expect("Failed to run binary");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("otlp2parquet"));
}

#[test]
fn test_cli_creates_output_directory() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path().join("test-output");

    // Verify directory doesn't exist yet
    assert!(!output_path.exists());

    // Note: We can't actually run the server in tests (it would block),
    // but we can test that validation creates the directory
    // This would need to be a manual test or spawn server and kill it

    Ok(())
}

#[test]
fn test_cli_invalid_output_path_fails() {
    let binary = get_binary_path();

    // Try to write to /root (should fail on most systems without sudo)
    let output = Command::new(&binary)
        .arg("--output")
        .arg("/root/forbidden-test-output")
        .env("RUST_LOG", "error")
        .output()
        .expect("Failed to run binary");

    // Expecting failure due to permissions
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("permission") || stderr.contains("Permission"));
}
```

**Step 2: Add tempfile dev dependency**

Add to `crates/otlp2parquet-server/Cargo.toml` in `[dev-dependencies]` section (create if doesn't exist):

```toml
[dev-dependencies]
tempfile = "3.13"
```

**Step 3: Build debug binary for tests**

Run: `cargo build --bin otlp2parquet`

Expected: Debug binary built successfully

**Step 4: Run integration tests**

Run: `cargo test --test cli_integration_test`

Expected: All tests pass (except possibly permission test depending on system)

**Step 5: Commit**

```bash
git add crates/otlp2parquet-server/tests/cli_integration_test.rs crates/otlp2parquet-server/Cargo.toml
git commit -m "test(server): add CLI integration tests"
```

---

## Task 10: Update documentation

**Files:**
- Modify: `README.md`
- Create: `docs/setup/cli.md`

**Step 1: Add CLI quick start to README**

In `README.md`, find the "How It Works" section (around line 54) and add a new "Quick Start" section before it:

```markdown
## Quick Start

### macOS/Linux CLI

**Install from source:**
```bash
git clone https://github.com/smithclay/otlp2parquet
cd otlp2parquet
make build-cli
./target/release/otlp2parquet
```

Server starts on http://localhost:4318 with data written to `./data`.

**Common options:**
```bash
# Custom port and output directory
otlp2parquet --port 8080 --output ./my-data

# Enable debug logging
otlp2parquet --log-level debug

# Use custom config file
otlp2parquet --config prod.toml
```

**Need help?** Run `otlp2parquet --help`

---
```

**Step 2: Create detailed CLI documentation**

Create `docs/setup/cli.md`:

```markdown
# CLI Deployment Guide

Run otlp2parquet as a native binary on macOS or Linux for local development.

## Installation

### From Source

```bash
git clone https://github.com/smithclay/otlp2parquet
cd otlp2parquet
make build-cli
```

Binary will be at `target/release/otlp2parquet`.

### Install to System

```bash
make build-cli
sudo make install-cli
```

Installs to `/usr/local/bin/otlp2parquet`.

## Usage

### Zero-Config Startup

The simplest way to run otlp2parquet:

```bash
otlp2parquet
```

This starts the server with sensible defaults:
- Listen on http://0.0.0.0:4318 (OTLP standard port)
- Write Parquet files to `./data`
- Log level: info
- Batching: enabled
- No Iceberg catalog (plain Parquet files)

### CLI Options

```bash
otlp2parquet [OPTIONS]

Options:
  -c, --config <FILE>      Path to configuration file
  -p, --port <PORT>        HTTP listen port [default: 4318]
  -o, --output <DIR>       Output directory for Parquet files [default: ./data]
  -v, --log-level <LEVEL>  Log level (trace, debug, info, warn, error) [default: info]
  -h, --help               Print help
  -V, --version            Print version
```

### Common Workflows

**Custom port and output:**
```bash
otlp2parquet --port 8080 --output /tmp/otlp-data
```

**Debug logging:**
```bash
otlp2parquet --log-level debug
```

**With config file:**
```bash
otlp2parquet --config ./config.toml
```

**All together:**
```bash
otlp2parquet -p 9000 -o ~/otlp-logs -v trace -c custom.toml
```

## Configuration Priority

Settings are loaded in this order (highest to lowest priority):

1. **CLI flags** - Explicit arguments like `--port 8080`
2. **Environment variables** - `OTLP2PARQUET_*` prefix
3. **Config file** - Specified via `--config` or default locations
4. **Platform defaults** - Built-in defaults for server mode

### Example

Given this config.toml:
```toml
[server]
listen_addr = "0.0.0.0:4318"
log_level = "info"
```

Running:
```bash
otlp2parquet --port 8080 --log-level debug
```

Results in:
- Port: 8080 (CLI flag wins)
- Log level: debug (CLI flag wins)
- Listen address: 0.0.0.0:8080 (port from CLI, host from default)

## Troubleshooting

### Permission Denied on Port

```
Error: Failed to bind to 0.0.0.0:80
Caused by: Permission denied (os error 13)
```

**Solution:** Ports below 1024 require root. Use a higher port:
```bash
otlp2parquet --port 8080
```

### Output Directory Not Writable

```
Error: Output directory '/restricted/path' is not writable
```

**Solution:** Choose a directory you have write access to:
```bash
otlp2parquet --output ~/otlp-data
```

### Config File Not Found

```
Error: Failed to load config from ./custom.toml
Caused by: No such file or directory
```

**Solution:** Check the path or omit `--config` to use defaults.

## Advanced Usage

### With Iceberg Catalog

Create config.toml:
```toml
catalog_mode = "iceberg"

[iceberg]
rest_uri = "http://localhost:19120/iceberg"
warehouse = "s3://my-bucket/warehouse"
namespace = "otel"
```

Run:
```bash
otlp2parquet --config config.toml
```

### With S3 Backend

Create config.toml:
```toml
[storage]
backend = "s3"

[storage.s3]
bucket = "my-otlp-bucket"
region = "us-east-1"
```

Set AWS credentials:
```bash
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
otlp2parquet --config config.toml
```

Note: `--output` flag only works with filesystem backend.

## Next Steps

- [Send data from applications](../guides/sending-data.md)
- [Configuration reference](../concepts/configuration.md)
- [Query data with DuckDB](../guides/querying-data.md)
```

**Step 3: Update platform matrix in README**

In `README.md`, find the Platform & Feature Matrix table (around line 22) and update the row headers to include CLI:

Change the table header row from:
```markdown
| Feature / Platform | Docker (Server) | Cloudflare Workers | AWS Lambda |
```

To:
```markdown
| Feature / Platform | CLI / Server | Cloudflare Workers | AWS Lambda |
```

**Step 4: Test documentation formatting**

Run: `cat docs/setup/cli.md | head -50`

Expected: Clean markdown rendering

**Step 5: Commit**

```bash
git add README.md docs/setup/cli.md
git commit -m "docs: add CLI quick start and detailed guide"
```

---

## Task 11: Final testing and validation

**Files:**
- Manual testing (no file changes)

**Step 1: Build release binary**

Run: `make build-cli`

Expected: Clean build, binary at target/release/otlp2parquet

**Step 2: Test zero-config startup**

Run: `./target/release/otlp2parquet`

Expected:
- Creates ./data directory
- Shows startup info box
- Server starts on port 4318
- Press Ctrl+C to stop

**Step 3: Test with custom args**

Run: `./target/release/otlp2parquet -p 9000 -o /tmp/otlp-cli-test -v debug`

Expected:
- Creates /tmp/otlp-cli-test directory
- Shows debug logs
- Server starts on port 9000
- Press Ctrl+C to stop

**Step 4: Test with config file**

Create test config:
```bash
cat > /tmp/test-config.toml << 'EOF'
[server]
listen_addr = "0.0.0.0:7000"
log_level = "trace"

[storage]
backend = "fs"

[storage.fs]
path = "/tmp/otlp-from-config"
EOF
```

Run: `./target/release/otlp2parquet --config /tmp/test-config.toml`

Expected:
- Creates /tmp/otlp-from-config directory
- Server starts on port 7000
- Trace level logging
- Press Ctrl+C to stop

**Step 5: Test CLI override of config**

Run: `./target/release/otlp2parquet --config /tmp/test-config.toml --port 8888`

Expected:
- Port 8888 (CLI overrides config)
- Other settings from config file
- Press Ctrl+C to stop

**Step 6: Test error handling**

Run: `./target/release/otlp2parquet --output /root/forbidden 2>&1 | head -10`

Expected: Clear error about permissions

**Step 7: Verify all tests pass**

Run: `cargo test -p otlp2parquet-server`

Expected: All tests pass

**Step 8: Clean up test artifacts**

Run: `rm -rf ./data /tmp/otlp-cli-test /tmp/otlp-from-config /tmp/test-config.toml ./test-data`

**Step 9: No commit (testing only)**

---

## Task 12: Update CLAUDE.md with CLI build instructions

**Files:**
- Modify: `CLAUDE.md`

**Step 1: Add CLI build note**

In `CLAUDE.md`, add after the first line:

```markdown
- use "make build-cli" to build the CLI binary for desktop use
```

**Step 2: Verify file**

Run: `head -10 CLAUDE.md`

Expected: Shows updated content

**Step 3: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: add CLI build instruction to CLAUDE.md"
```

---

## Summary

This plan implements CLI mode for otlp2parquet-server through 12 incremental tasks:

1. ✅ Add clap dependency and binary config
2. ✅ Extend config crate with load helpers
3. ✅ Refactor lib.rs to expose run_with_config
4. ✅ Create CLI argument parser
5. ✅ Implement config loading and merging
6. ✅ Add early validation with directory creation
7. ✅ Add startup info display
8. ✅ Add Makefile targets
9. ✅ Add integration tests
10. ✅ Update documentation
11. ✅ Final testing and validation
12. ✅ Update CLAUDE.md

**Total estimated time:** ~60-90 minutes for experienced Rust developer

**Testing strategy:**
- Unit tests for config merging
- Integration tests for CLI parsing
- Manual testing for end-to-end workflows
- Error path validation

**Deployment:**
- Binary name: `otlp2parquet`
- Installation: `make install-cli`
- Distribution: Homebrew formula (future)
