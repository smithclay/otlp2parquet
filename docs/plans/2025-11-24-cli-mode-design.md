# CLI Mode Design for otlp2parquet-server

**Date:** 2025-11-24
**Status:** Design Complete
**Goal:** Transform otlp2parquet-server into a CLI-friendly tool for macOS/Linux desktop use with Homebrew distribution

## Overview

Enable `otlp2parquet-server` to run as a native CLI binary with ergonomic defaults for local development, while preserving all existing server functionality and backward compatibility.

## Use Case

**Primary:** Long-running local OTLP server for development environments on macOS/Linux desktops.

**Target user:** Developers who want to quickly spin up a local OTLP endpoint without Docker or complex configuration. Should work with zero setup.

## Design Principles

1. **Zero-config startup** - `otlp2parquet` just works with sensible defaults
2. **CLI flags override config** - Common settings controllable via flags
3. **Fail fast with clear errors** - Validate early, give actionable messages
4. **Backward compatible** - No breaking changes to existing deployments
5. **Standard Unix conventions** - Follow established CLI patterns

## Architecture

### Current State
- Library crate (`lib.rs`) exposing server functionality
- Requires `config.toml` file and environment variables
- Used by Docker deployments

### Proposed State
- Add binary target (`main.rs`) to same crate
- Parse CLI arguments with `clap` (derive API)
- Merge CLI args → env vars → config file → defaults
- Call existing `run()` function with resolved config

### Configuration Priority
1. **CLI flags** (highest - explicit user intent)
2. **Environment variables** (session-specific)
3. **Config file** (project-specific)
4. **Smart defaults** (lowest - just works™)

## CLI Interface

### Command Structure
```bash
# Zero-config startup
otlp2parquet

# Common workflows
otlp2parquet --port 8080 --output ./my-data
otlp2parquet -p 9000 -v debug
otlp2parquet --config ./prod-config.toml
```

### Arguments

| Flag | Short | Type | Default | Description |
|------|-------|------|---------|-------------|
| `--config` | `-c` | Path | - | Path to configuration file |
| `--port` | `-p` | u16 | 4318 | HTTP listen port (OTLP standard) |
| `--output` | `-o` | Path | ./data | Output directory for Parquet files |
| `--log-level` | `-v` | String | info | Log verbosity (trace/debug/info/warn/error) |

### Smart Defaults
- **Port:** 4318 (OTLP HTTP standard)
- **Output:** ./data (current directory)
- **Log level:** info
- **Log format:** text (human-readable for desktop)
- **Backend:** fs (filesystem - only one that makes sense locally)
- **Batching:** enabled (good for local dev)
- **Catalog:** none (keep it simple)

### Help Output
```
$ otlp2parquet --help

OTLP HTTP server writing Parquet files to object storage

Usage: otlp2parquet [OPTIONS]

Options:
  -c, --config <FILE>      Path to configuration file
  -p, --port <PORT>        HTTP listen port [default: 4318]
  -o, --output <DIR>       Output directory for Parquet files [default: ./data]
  -v, --log-level <LEVEL>  Log level (trace, debug, info, warn, error) [default: info]
  -h, --help               Print help
  -V, --version            Print version
```

## Configuration Merging

### Flow
```rust
fn main() -> Result<()> {
    // 1. Parse CLI args
    let cli = Cli::parse();

    // 2. Load base config (file or defaults)
    let mut config = if let Some(path) = &cli.config {
        RuntimeConfig::load_from_file(path)?
    } else {
        RuntimeConfig::load_or_default()
    };

    // 3. Apply CLI overrides
    apply_cli_overrides(&mut config, &cli)?;

    // 4. Apply desktop defaults
    apply_desktop_defaults(&mut config);

    // 5. Validate and run
    validate_config(&config)?;
    display_startup_info(&config);
    run_with_config(config).await
}
```

### Override Logic
- **--port:** Overrides `server.listen_addr` (keeps 0.0.0.0, changes port)
- **--output:** Overrides `storage.fs.path` (only valid for fs backend)
- **--log-level:** Overrides `server.log_level`
- **--config:** Specifies config file path to load

### Error Handling
- Missing config file + no CLI args = use defaults ✅
- Invalid config file = fail fast with error ❌
- Invalid CLI args = clap handles automatically ❌
- Permission issues = caught early in validation ❌

## Startup Experience

### Validation Steps
1. **Create output directory** if missing (`mkdir -p`)
2. **Test write permissions** (create + delete test file)
3. **Validate port availability** (during bind)
4. **Display effective configuration**

### Startup Output
```
$ otlp2parquet
2025-11-24T10:30:15.123Z  INFO otlp2parquet_server: Creating output directory: ./data
2025-11-24T10:30:15.125Z  INFO otlp2parquet_server: Output directory validated: ./data
2025-11-24T10:30:15.126Z  INFO otlp2parquet_server: ╭─────────────────────────────────────────────────
2025-11-24T10:30:15.126Z  INFO otlp2parquet_server: │ otlp2parquet server starting
2025-11-24T10:30:15.126Z  INFO otlp2parquet_server: ├─────────────────────────────────────────────────
2025-11-24T10:30:15.126Z  INFO otlp2parquet_server: │ Listen address: http://0.0.0.0:4318
2025-11-24T10:30:15.126Z  INFO otlp2parquet_server: │ Storage backend: fs
2025-11-24T10:30:15.126Z  INFO otlp2parquet_server: │ Output directory: ./data
2025-11-24T10:30:15.126Z  INFO otlp2parquet_server: │ Log level: info
2025-11-24T10:30:15.126Z  INFO otlp2parquet_server: │ Catalog mode: none
2025-11-24T10:30:15.126Z  INFO otlp2parquet_server: │ Batching: enabled
2025-11-24T10:30:15.126Z  INFO otlp2parquet_server: │   - Max rows: 200000
2025-11-24T10:30:15.126Z  INFO otlp2parquet_server: │   - Max bytes: 128 MB
2025-11-24T10:30:15.126Z  INFO otlp2parquet_server: │   - Max age: 10s
2025-11-24T10:30:15.126Z  INFO otlp2parquet_server: ╰─────────────────────────────────────────────────
2025-11-24T10:30:15.130Z  INFO otlp2parquet_server: OTLP HTTP endpoint listening on http://0.0.0.0:4318
```

### Error Examples
```bash
# Permission denied
$ otlp2parquet --output /root/data
Error: Output directory '/root/data' is not writable. Check permissions.
Caused by: Permission denied (os error 13)

# Port requires root
$ otlp2parquet --port 80
Error: Failed to bind to 0.0.0.0:80
Caused by: Permission denied (os error 13)
Hint: Ports below 1024 require root privileges. Try a port >= 1024.
```

## Implementation Changes

### New Files
- **`crates/otlp2parquet-server/src/main.rs`** (new)
  - CLI argument parsing with clap
  - Configuration merging
  - Startup validation
  - Entry point

### Modified Files
- **`crates/otlp2parquet-server/src/lib.rs`**
  - Add `pub async fn run_with_config(config: RuntimeConfig)`
  - Keep existing logic unchanged

- **`crates/otlp2parquet-config/src/lib.rs`**
  - Add `RuntimeConfig::load_or_default()`
  - Add `RuntimeConfig::load_from_file(path: &Path)`
  - Ensure `Default` implementations
  - Handle missing files gracefully

- **`crates/otlp2parquet-server/Cargo.toml`**
  - Add `[[bin]]` section
  - Add `clap` dependency

### Dependencies
```toml
[dependencies]
# ... existing ...
clap = { version = "4.5", default-features = false, features = ["std", "derive", "help", "usage"] }
```

## Build & Distribution

### Build Configuration
```toml
[profile.release]
opt-level = 3
lto = true           # Link-time optimization
codegen-units = 1    # Better optimization
strip = true         # Strip debug symbols
panic = "abort"      # Smaller binary
```

### Binary Specifications
- **Name:** `otlp2parquet` (matches project)
- **Expected size:** 6-10 MB (stripped release build)
- **Platforms:** macOS (x86_64, ARM64), Linux (x86_64, ARM64)

### Makefile Targets
```makefile
.PHONY: build-cli
build-cli:
	cargo build --release --bin otlp2parquet

.PHONY: install-cli
install-cli: build-cli
	cp target/release/otlp2parquet /usr/local/bin/
```

### Installation Methods

**Via Homebrew (future):**
```bash
brew install smithclay/tap/otlp2parquet
```

**From source (immediate):**
```bash
git clone https://github.com/smithclay/otlp2parquet
cd otlp2parquet
make build-cli
sudo make install-cli
```

**Via cargo install (if published):**
```bash
cargo install otlp2parquet-server
```

### Homebrew Formula (Reference)
```ruby
class Otlp2parquet < Formula
  desc "OTLP HTTP server writing Parquet files to object storage"
  homepage "https://github.com/smithclay/otlp2parquet"
  url "https://github.com/smithclay/otlp2parquet/archive/refs/tags/v0.1.0.tar.gz"
  license "Apache-2.0"

  depends_on "rust" => :build

  def install
    system "cargo", "install", *std_cargo_args(path: "crates/otlp2parquet-server")
  end

  test do
    assert_match "otlp2parquet", shell_output("#{bin}/otlp2parquet --version")
  end
end
```

## Testing Strategy

### Unit Tests
- CLI argument parsing
- Configuration merging logic
- Default value generation
- Override precedence

### Integration Tests
```rust
// crates/otlp2parquet-server/tests/cli_tests.rs
#[test] fn test_cli_parsing() { ... }
#[test] fn test_default_config_generation() { ... }
#[test] fn test_config_file_override() { ... }
#[test] fn test_cli_overrides_config() { ... }
#[test] fn test_output_dir_creation() { ... }
#[test] fn test_permission_validation() { ... }
```

### Manual Testing Checklist
- [ ] Zero-config startup works
- [ ] `--port` overrides port
- [ ] `--output` creates directory
- [ ] `--config` loads custom file
- [ ] `--log-level` changes verbosity
- [ ] Multiple flags work together
- [ ] Permission errors caught early
- [ ] `--help` shows documentation
- [ ] `--version` shows version
- [ ] Docker deployments unchanged

## Backward Compatibility

### Unchanged Behavior
✅ Library API (lib.rs exports same functions)
✅ Docker deployments (still use env vars + config.toml)
✅ Lambda/Workers (different crates)
✅ Existing config.toml files
✅ Environment variable behavior

### Migration Path
- **Zero migration needed** - purely additive feature
- Existing users see no changes
- New users choose: config file or CLI flags
- CLI is opt-in via binary installation

## Documentation Updates

### README.md
Add quick start section:
```markdown
### Quick Start (macOS/Linux)

**Install via Homebrew:**
```bash
brew install smithclay/tap/otlp2parquet
otlp2parquet
```

**Common options:**
```bash
otlp2parquet --port 8080 --output ./my-data
otlp2parquet --log-level debug
otlp2parquet --config prod.toml
```
```

### New Files
- **`docs/setup/cli.md`** - Detailed CLI usage guide
  - All flags documented
  - Configuration precedence explained
  - Common workflows
  - Troubleshooting

### Updated Files
- **`docs/setup/overview.md`** - Add CLI as deployment option
- **Platform matrix** - Include CLI row

## Future Enhancements (Out of Scope)

### Subcommands
```bash
otlp2parquet serve          # Current behavior (default)
otlp2parquet convert <file> # Convert OTLP file to Parquet
otlp2parquet validate <file> # Validate OTLP payload
```

### Additional Flags
- `--no-batch` to disable batching
- `--catalog <uri>` for Iceberg endpoint
- `--s3-bucket <name>` for cloud storage

### Enhanced UX
- Interactive config generation (`otlp2parquet init`)
- Shell completions (bash/zsh/fish)
- Man pages
- `--dry-run` mode
- Prometheus metrics endpoint

## YAGNI - What We're NOT Doing

❌ Complex subcommand structure
❌ Database/state storage
❌ Built-in query functionality
❌ GUI/TUI interface
❌ Plugin system
❌ Multiple profiles/workspaces

## Summary

This design enables desktop CLI usage while maintaining:
- ✅ Zero breaking changes
- ✅ Library functionality unchanged
- ✅ Existing deployments unaffected
- ✅ Simple, ergonomic interface
- ✅ Standard Unix conventions
- ✅ Clear path to Homebrew distribution

**Key value:** Developers can run `otlp2parquet` with zero setup and immediately have a working local OTLP endpoint.
