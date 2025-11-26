# CLI Deploy Command Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add `otlp2parquet deploy cloudflare` and `otlp2parquet deploy aws` subcommands with interactive wizards.

**Architecture:** New `deploy/` module in server crate with platform-specific wizards. Templates embedded via `include_str!`. Uses `dialoguer` for interactive prompts.

**Tech Stack:** Rust, clap (subcommands), dialoguer (prompts), include_str! (template embedding)

---

## Task 1: Add dialoguer dependency

**Files:**
- Modify: `crates/otlp2parquet-server/Cargo.toml`

**Step 1: Add dialoguer to dependencies**

In `crates/otlp2parquet-server/Cargo.toml`, add after the `clap` line:

```toml
dialoguer = { version = "0.12", default-features = false, features = ["password"] }
rand = { version = "0.8", default-features = false, features = ["std", "std_rng"] }
```

**Step 2: Verify it compiles**

Run: `cargo check -p otlp2parquet-server`
Expected: Compiles successfully

**Step 3: Commit**

```bash
git add crates/otlp2parquet-server/Cargo.toml
git commit -m "feat(cli): add dialoguer and rand dependencies for deploy wizard"
```

---

## Task 2: Create deploy module structure

**Files:**
- Create: `crates/otlp2parquet-server/src/deploy/mod.rs`
- Create: `crates/otlp2parquet-server/src/deploy/names.rs`
- Modify: `crates/otlp2parquet-server/src/lib.rs`

**Step 1: Create deploy/mod.rs**

Create `crates/otlp2parquet-server/src/deploy/mod.rs`:

```rust
//! Deploy command - generates platform-specific deployment configs

mod names;

pub mod cloudflare;
pub mod aws;

use clap::Subcommand;

#[derive(Subcommand)]
pub enum DeployCommand {
    /// Generate wrangler.toml for Cloudflare Workers + R2
    Cloudflare(cloudflare::CloudflareArgs),
    /// Generate template.yaml for AWS Lambda + S3/S3 Tables
    Aws(aws::AwsArgs),
}

impl DeployCommand {
    pub fn run(self) -> anyhow::Result<()> {
        match self {
            DeployCommand::Cloudflare(args) => cloudflare::run(args),
            DeployCommand::Aws(args) => aws::run(args),
        }
    }
}
```

**Step 2: Create deploy/names.rs**

Create `crates/otlp2parquet-server/src/deploy/names.rs`:

```rust
//! Fun default name generation for stacks and workers

use rand::Rng;

const ADJECTIVES: &[&str] = &[
    "swift", "eager", "bright", "cosmic", "dapper",
    "fluent", "golden", "humble", "jovial", "keen",
    "lively", "mellow", "nimble", "plucky", "quick",
    "rustic", "snappy", "trusty", "vivid", "witty",
];

const NOUNS: &[&str] = &[
    "arrow", "beacon", "conduit", "depot", "emitter",
    "funnel", "gauge", "harbor", "inlet", "journal",
    "keeper", "ledger", "metric", "nexus", "outlet",
    "parquet", "queue", "relay", "signal", "tracer",
];

/// Generate a fun default name like "nimble-relay-2847"
pub fn generate() -> String {
    let mut rng = rand::thread_rng();
    let adjective = ADJECTIVES[rng.gen_range(0..ADJECTIVES.len())];
    let noun = NOUNS[rng.gen_range(0..NOUNS.len())];
    let number: u16 = rng.gen_range(1000..10000);
    format!("{}-{}-{}", adjective, noun, number)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_name_format() {
        let name = generate();
        let parts: Vec<&str> = name.split('-').collect();
        assert_eq!(parts.len(), 3);
        assert!(ADJECTIVES.contains(&parts[0]));
        assert!(NOUNS.contains(&parts[1]));
        let number: u16 = parts[2].parse().unwrap();
        assert!(number >= 1000 && number < 10000);
    }
}
```

**Step 3: Create placeholder cloudflare.rs**

Create `crates/otlp2parquet-server/src/deploy/cloudflare.rs`:

```rust
//! Cloudflare Workers deployment config generator

use clap::Args;

#[derive(Args)]
pub struct CloudflareArgs {
    /// Worker name
    #[arg(long)]
    pub worker_name: Option<String>,

    /// R2 bucket name
    #[arg(long)]
    pub bucket: Option<String>,

    /// Cloudflare Account ID
    #[arg(long)]
    pub account_id: Option<String>,

    /// Catalog mode: "iceberg" or "none"
    #[arg(long)]
    pub catalog: Option<String>,

    /// Release version to use (default: current CLI version)
    #[arg(long)]
    pub release: Option<String>,

    /// Overwrite existing file without asking
    #[arg(long)]
    pub force: bool,
}

pub fn run(_args: CloudflareArgs) -> anyhow::Result<()> {
    todo!("Cloudflare wizard not yet implemented")
}
```

**Step 4: Create placeholder aws.rs**

Create `crates/otlp2parquet-server/src/deploy/aws.rs`:

```rust
//! AWS CloudFormation deployment config generator

use clap::Args;

#[derive(Args)]
pub struct AwsArgs {
    /// S3 URI of Lambda binary (e.g., s3://my-bucket/lambda.zip)
    #[arg(long)]
    pub lambda_s3_uri: Option<String>,

    /// CloudFormation stack name
    #[arg(long)]
    pub stack_name: Option<String>,

    /// S3 bucket name for data storage
    #[arg(long)]
    pub bucket: Option<String>,

    /// Catalog mode: "iceberg" or "none"
    #[arg(long)]
    pub catalog: Option<String>,

    /// CloudWatch log retention in days
    #[arg(long, default_value = "7")]
    pub retention: u16,

    /// Overwrite existing file without asking
    #[arg(long)]
    pub force: bool,
}

pub fn run(_args: AwsArgs) -> anyhow::Result<()> {
    todo!("AWS wizard not yet implemented")
}
```

**Step 5: Add deploy module to lib.rs**

In `crates/otlp2parquet-server/src/lib.rs`, add near the top with other module declarations:

```rust
pub mod deploy;
```

**Step 6: Verify it compiles**

Run: `cargo check -p otlp2parquet-server`
Expected: Compiles (with unused warnings, that's fine)

**Step 7: Run the test**

Run: `cargo test -p otlp2parquet-server deploy::names`
Expected: test passes

**Step 8: Commit**

```bash
git add crates/otlp2parquet-server/src/deploy/
git add crates/otlp2parquet-server/src/lib.rs
git commit -m "feat(cli): add deploy module structure with name generator"
```

---

## Task 3: Wire deploy subcommand into main CLI

**Files:**
- Modify: `crates/otlp2parquet-server/src/main.rs`

**Step 1: Update CLI struct to use subcommands**

Replace the entire `crates/otlp2parquet-server/src/main.rs` with:

```rust
use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use otlp2parquet_config::RuntimeConfig;
use std::path::PathBuf;

/// OTLP HTTP server writing Parquet files to object storage
#[derive(Parser)]
#[command(name = "otlp2parquet")]
#[command(version)]
#[command(about = "OTLP HTTP server writing Parquet files to object storage", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    /// Path to configuration file
    #[arg(short, long, value_name = "FILE", global = true)]
    config: Option<PathBuf>,

    /// HTTP listen port (overrides config file)
    #[arg(short, long, value_name = "PORT", global = true)]
    port: Option<u16>,

    /// Output directory for Parquet files (filesystem backend only)
    #[arg(short, long, value_name = "DIR", global = true)]
    output: Option<PathBuf>,

    /// Log level: trace, debug, info, warn, error
    #[arg(short = 'v', long, value_name = "LEVEL", global = true)]
    log_level: Option<String>,
}

#[derive(Subcommand)]
enum Commands {
    /// Generate deployment configuration for cloud platforms
    Deploy {
        #[command(subcommand)]
        platform: otlp2parquet_server::deploy::DeployCommand,
    },
    /// Start the HTTP server (default if no subcommand given)
    Serve,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Deploy { platform }) => platform.run(),
        Some(Commands::Serve) | None => run_server(cli),
    }
}

fn run_server(cli: Cli) -> Result<()> {
    // Build tokio runtime and run async server
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("Failed to build tokio runtime")?
        .block_on(async_main(cli))
}

async fn async_main(cli: Cli) -> Result<()> {
    // Step 1: Load base configuration
    let mut config = if let Some(config_path) = &cli.config {
        // Explicit config file path provided
        RuntimeConfig::load_from_path(config_path)
            .with_context(|| format!("Failed to load config from {}", config_path.display()))?
    } else {
        // Try default locations, fall back to defaults
        RuntimeConfig::load_or_default().context("Failed to load configuration")?
    };

    // Step 2: Apply CLI overrides (highest priority)
    apply_cli_overrides(&mut config, &cli)?;

    // Step 3: Apply desktop-friendly defaults
    apply_desktop_defaults(&mut config);

    // Step 4: Initialize tracing early so validation logs show up
    // Note: run_with_config will also call init_tracing, but that's idempotent
    use otlp2parquet_server::init_tracing;
    init_tracing(&config);

    // Step 5: Validate configuration early (creates directories, tests write permissions)
    validate_config(&config).await?;

    // Step 6: Display startup info
    display_startup_info(&config);

    // Step 7: Run server with resolved config
    otlp2parquet_server::run_with_config(config).await
}

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

fn display_startup_info(config: &RuntimeConfig) {
    use otlp2parquet_config::StorageBackend;
    use tracing::info;

    let server = config.server.as_ref().expect("server config validated");

    info!("╭─────────────────────────────────────────────────");
    info!("│ otlp2parquet v{}", env!("CARGO_PKG_VERSION"));
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
    info!(
        "│ Batching: {}",
        if config.batch.enabled {
            "enabled"
        } else {
            "disabled"
        }
    );

    if config.batch.enabled {
        info!("│   - Max rows: {}", config.batch.max_rows);
        info!("│   - Max bytes: {} MB", config.batch.max_bytes / 1_048_576);
        info!("│   - Max age: {}s", config.batch.max_age_secs);
    }

    info!("╰─────────────────────────────────────────────────");
}

async fn validate_config(config: &RuntimeConfig) -> Result<()> {
    use otlp2parquet_config::StorageBackend;
    use std::fs;
    use tracing::info;

    // Validate filesystem output directory if using fs backend
    if config.storage.backend == StorageBackend::Fs {
        let fs_config = config.storage.fs.as_ref().ok_or_else(|| {
            anyhow::anyhow!("filesystem backend requires storage.fs configuration")
        })?;

        let output_path = PathBuf::from(&fs_config.path);

        // Create directory if it doesn't exist
        if !output_path.exists() {
            info!("Creating output directory: {}", fs_config.path);
            fs::create_dir_all(&output_path).with_context(|| {
                format!("Failed to create output directory: {}", fs_config.path)
            })?;
        }

        // Validate writability by creating a test file
        let test_file = output_path.join(".otlp2parquet-write-test");
        fs::write(&test_file, b"test").with_context(|| {
            format!(
                "Output directory '{}' is not writable. Check permissions.",
                fs_config.path
            )
        })?;
        fs::remove_file(&test_file).context("Failed to remove test file")?;
        info!("Output directory validated: {}", fs_config.path);
    }

    // Validate server config exists
    config
        .server
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("server configuration required"))?;

    Ok(())
}
```

**Step 2: Verify it compiles**

Run: `cargo check -p otlp2parquet-server`
Expected: Compiles successfully

**Step 3: Test help output**

Run: `cargo run -p otlp2parquet-server -- --help`
Expected: Shows `deploy` and `serve` subcommands

Run: `cargo run -p otlp2parquet-server -- deploy --help`
Expected: Shows `cloudflare` and `aws` subcommands

**Step 4: Commit**

```bash
git add crates/otlp2parquet-server/src/main.rs
git commit -m "feat(cli): wire deploy subcommand into main CLI"
```

---

## Task 4: Create Cloudflare wrangler template

**Files:**
- Create: `crates/otlp2parquet-server/templates/wrangler.toml`

**Step 1: Create templates directory and wrangler.toml**

Create `crates/otlp2parquet-server/templates/wrangler.toml`:

```toml
# Generated by otlp2parquet deploy cloudflare
# See: https://github.com/smithclay/otlp2parquet

name = "{{WORKER_NAME}}"
main = "build/worker/shim.mjs"
compatibility_date = "2025-01-15"

workers_dev = true

[build]
command = "curl -sL https://github.com/smithclay/otlp2parquet/releases/download/{{VERSION}}/otlp2parquet-worker.zip -o otlp2parquet-worker.zip && unzip -o otlp2parquet-worker.zip -d build"

[limits]
cpu_ms = 2000

[[r2_buckets]]
binding = "LOGS_BUCKET"
bucket_name = "{{BUCKET_NAME}}"

[vars]
OTLP2PARQUET_STORAGE_BACKEND = "r2"
OTLP2PARQUET_R2_BUCKET = "{{BUCKET_NAME}}"
CLOUDFLARE_ACCOUNT_ID = "{{ACCOUNT_ID}}"
OTLP2PARQUET_R2_ACCOUNT_ID = "{{ACCOUNT_ID}}"
AWS_REGION = "auto"
OTLP2PARQUET_CATALOG_MODE = "{{CATALOG_MODE}}"
{{#ICEBERG}}
CLOUDFLARE_BUCKET_NAME = "{{BUCKET_NAME}}"
{{/ICEBERG}}

[observability]
[observability.logs]
enabled = true
head_sampling_rate = 1
invocation_logs = true
persist = true
```

**Step 2: Commit**

```bash
git add crates/otlp2parquet-server/templates/wrangler.toml
git commit -m "feat(cli): add cloudflare wrangler template"
```

---

## Task 5: Implement Cloudflare wizard

**Files:**
- Modify: `crates/otlp2parquet-server/src/deploy/cloudflare.rs`

**Step 1: Implement the wizard**

Replace `crates/otlp2parquet-server/src/deploy/cloudflare.rs` with:

```rust
//! Cloudflare Workers deployment config generator

use anyhow::{bail, Context, Result};
use clap::Args;
use dialoguer::{Confirm, Input, Select};
use std::fs;
use std::path::Path;

use super::names;

const TEMPLATE: &str = include_str!("../../templates/wrangler.toml");

#[derive(Args)]
pub struct CloudflareArgs {
    /// Worker name
    #[arg(long)]
    pub worker_name: Option<String>,

    /// R2 bucket name
    #[arg(long)]
    pub bucket: Option<String>,

    /// Cloudflare Account ID
    #[arg(long)]
    pub account_id: Option<String>,

    /// Catalog mode: "iceberg" or "none"
    #[arg(long)]
    pub catalog: Option<String>,

    /// Release version to use (default: current CLI version)
    #[arg(long)]
    pub release: Option<String>,

    /// Overwrite existing file without asking
    #[arg(long)]
    pub force: bool,
}

pub fn run(args: CloudflareArgs) -> Result<()> {
    println!();
    println!("otlp2parquet deploy - Cloudflare Workers + R2");
    println!();

    // Collect values via wizard or flags
    let default_name = names::generate();
    let worker_name = match args.worker_name {
        Some(name) => name,
        None => Input::new()
            .with_prompt("Worker name")
            .default(default_name)
            .interact_text()?,
    };

    let bucket_name = match args.bucket {
        Some(bucket) => bucket,
        None => Input::new()
            .with_prompt("R2 bucket name")
            .validate_with(validate_bucket_name)
            .interact_text()?,
    };

    let account_id = match args.account_id {
        Some(id) => id,
        None => Input::new()
            .with_prompt("Cloudflare Account ID")
            .validate_with(validate_account_id)
            .interact_text()?,
    };

    let catalog_mode = match args.catalog {
        Some(cat) => {
            if cat != "iceberg" && cat != "none" {
                bail!("Invalid catalog mode '{}'. Must be 'iceberg' or 'none'.", cat);
            }
            cat
        }
        None => {
            let options = &["No  - Plain Parquet files (simpler)", "Yes - R2 Data Catalog (queryable tables)"];
            let selection = Select::new()
                .with_prompt("Enable Iceberg catalog?")
                .items(options)
                .default(0)
                .interact()?;
            if selection == 0 { "none".to_string() } else { "iceberg".to_string() }
        }
    };

    let version = args.release.unwrap_or_else(|| format!("v{}", env!("CARGO_PKG_VERSION")));

    // Check if file exists
    let output_path = Path::new("wrangler.toml");
    if output_path.exists() && !args.force {
        let overwrite = Confirm::new()
            .with_prompt("wrangler.toml already exists. Overwrite?")
            .default(false)
            .interact()?;
        if !overwrite {
            println!("Aborted.");
            return Ok(());
        }
    }

    // Render template
    let use_iceberg = catalog_mode == "iceberg";
    let mut content = TEMPLATE
        .replace("{{WORKER_NAME}}", &worker_name)
        .replace("{{BUCKET_NAME}}", &bucket_name)
        .replace("{{ACCOUNT_ID}}", &account_id)
        .replace("{{CATALOG_MODE}}", &catalog_mode)
        .replace("{{VERSION}}", &version);

    // Handle conditional iceberg section
    if use_iceberg {
        content = content
            .replace("{{#ICEBERG}}", "")
            .replace("{{/ICEBERG}}", "");
    } else {
        // Remove iceberg-only lines
        let lines: Vec<&str> = content.lines().collect();
        let mut filtered = Vec::new();
        let mut in_iceberg_block = false;
        for line in lines {
            if line.contains("{{#ICEBERG}}") {
                in_iceberg_block = true;
                continue;
            }
            if line.contains("{{/ICEBERG}}") {
                in_iceberg_block = false;
                continue;
            }
            if !in_iceberg_block {
                filtered.push(line);
            }
        }
        content = filtered.join("\n");
    }

    // Write file
    fs::write(output_path, &content).context("Failed to write wrangler.toml")?;

    println!();
    println!("Created wrangler.toml");
    println!();
    println!("Next steps:");
    println!("  1. Create R2 bucket (if needed):");
    println!("     wrangler r2 bucket create {}", bucket_name);
    println!();
    println!("  2. Set secrets:");
    println!("     wrangler secret put AWS_ACCESS_KEY_ID");
    println!("     wrangler secret put AWS_SECRET_ACCESS_KEY");
    if use_iceberg {
        println!("     wrangler secret put CLOUDFLARE_API_TOKEN");
    }
    println!();
    println!("  3. Deploy:");
    println!("     wrangler deploy");
    println!();

    Ok(())
}

fn validate_bucket_name(input: &String) -> Result<(), String> {
    if input.is_empty() {
        return Err("Bucket name cannot be empty".to_string());
    }
    if input.len() < 3 || input.len() > 63 {
        return Err("Bucket name must be 3-63 characters".to_string());
    }
    if !input.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-') {
        return Err("Bucket name must contain only lowercase letters, numbers, and hyphens".to_string());
    }
    if input.starts_with('-') || input.ends_with('-') {
        return Err("Bucket name cannot start or end with a hyphen".to_string());
    }
    Ok(())
}

fn validate_account_id(input: &String) -> Result<(), String> {
    if input.is_empty() {
        return Err("Account ID cannot be empty".to_string());
    }
    if !input.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err("Account ID must be a hex string".to_string());
    }
    if input.len() != 32 {
        return Err("Account ID must be 32 characters".to_string());
    }
    Ok(())
}
```

**Step 2: Verify it compiles**

Run: `cargo check -p otlp2parquet-server`
Expected: Compiles successfully

**Step 3: Test the wizard (interactive)**

Run: `cargo run -p otlp2parquet-server -- deploy cloudflare --force --worker-name test-worker --bucket my-test-bucket --account-id 0123456789abcdef0123456789abcdef --catalog none`
Expected: Creates wrangler.toml, prints next steps

**Step 4: Verify generated file**

Run: `cat wrangler.toml`
Expected: Contains worker name, bucket name, account ID, catalog mode

**Step 5: Clean up and commit**

```bash
rm -f wrangler.toml
git add crates/otlp2parquet-server/src/deploy/cloudflare.rs
git commit -m "feat(cli): implement cloudflare deploy wizard"
```

---

## Task 6: Create AWS CloudFormation template

**Files:**
- Create: `crates/otlp2parquet-server/templates/cloudformation.yaml`

**Step 1: Create CloudFormation template**

Create `crates/otlp2parquet-server/templates/cloudformation.yaml`:

```yaml
# Generated by otlp2parquet deploy aws
# See: https://github.com/smithclay/otlp2parquet
AWSTemplateFormatVersion: '2010-09-09'
Description: otlp2parquet - OTLP to Parquet on AWS Lambda

Parameters:
  CatalogMode:
    Type: String
    Default: "{{CATALOG_MODE}}"
    AllowedValues:
      - iceberg
      - none

  BucketName:
    Type: String
    Default: "{{BUCKET_NAME}}"

  LogRetentionDays:
    Type: Number
    Default: {{LOG_RETENTION}}
    AllowedValues: [1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1827, 3653]

Conditions:
  UseIcebergCatalog: !Equals [!Ref CatalogMode, iceberg]
  UsePlainParquet: !Equals [!Ref CatalogMode, none]

Resources:
  OtlpToParquetFunction:
    Type: AWS::Lambda::Function
    Properties:
      FunctionName: {{STACK_NAME}}
      Description: Ingests OTLP logs and writes Parquet to S3
      Runtime: provided.al2023
      Architectures:
        - arm64
      Handler: bootstrap
      Code:
        S3Bucket: {{LAMBDA_S3_BUCKET}}
        S3Key: {{LAMBDA_S3_KEY}}
      MemorySize: 512
      Timeout: 30
      Environment:
        Variables:
          RUST_LOG: info
          OTLP2PARQUET_CATALOG_MODE: !Ref CatalogMode
          OTLP2PARQUET_STORAGE_S3_BUCKET: !If [UsePlainParquet, !Ref DataBucket, !Ref AWS::NoValue]
          OTLP2PARQUET_STORAGE_S3_REGION: !If [UsePlainParquet, !Ref AWS::Region, !Ref AWS::NoValue]
          OTLP2PARQUET_ICEBERG_BUCKET_ARN: !If [UseIcebergCatalog, !GetAtt S3TablesBucket.Arn, !Ref AWS::NoValue]
          OTLP2PARQUET_ICEBERG_NAMESPACE: otlp
      Role: !GetAtt LambdaExecutionRole.Arn

  LambdaExecutionRole:
    Type: AWS::IAM::Role
    Properties:
      AssumeRolePolicyDocument:
        Version: '2012-10-17'
        Statement:
          - Effect: Allow
            Principal:
              Service: lambda.amazonaws.com
            Action: sts:AssumeRole
      ManagedPolicyArns:
        - arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
      Policies:
        - !If
          - UsePlainParquet
          - PolicyName: S3Access
            PolicyDocument:
              Version: '2012-10-17'
              Statement:
                - Effect: Allow
                  Action:
                    - s3:PutObject
                    - s3:GetObject
                    - s3:DeleteObject
                    - s3:ListBucket
                  Resource:
                    - !Sub 'arn:aws:s3:::${DataBucket}'
                    - !Sub 'arn:aws:s3:::${DataBucket}/*'
          - !Ref AWS::NoValue
        - !If
          - UseIcebergCatalog
          - PolicyName: S3TablesAccess
            PolicyDocument:
              Version: '2012-10-17'
              Statement:
                - Effect: Allow
                  Action:
                    - s3tables:GetTable
                    - s3tables:GetTableMetadataLocation
                    - s3tables:CreateTable
                    - s3tables:UpdateTableMetadataLocation
                    - s3tables:GetNamespace
                    - s3tables:CreateNamespace
                  Resource:
                    - !Sub '${S3TablesBucket.Arn}/*'
                    - !GetAtt S3TablesBucket.Arn
                - Effect: Allow
                  Action:
                    - s3:PutObject
                    - s3:GetObject
                    - s3:DeleteObject
                    - s3:ListBucket
                  Resource:
                    - !Sub '${S3TablesBucket.Arn}/*'
                    - !GetAtt S3TablesBucket.Arn
          - !Ref AWS::NoValue

  FunctionUrl:
    Type: AWS::Lambda::Url
    Properties:
      AuthType: AWS_IAM
      TargetFunctionArn: !GetAtt OtlpToParquetFunction.Arn
      Cors:
        AllowOrigins:
          - "*"
        AllowMethods:
          - POST
        AllowHeaders:
          - content-type
          - authorization
          - x-amz-date
          - x-amz-security-token
        MaxAge: 300

  FunctionUrlPermission:
    Type: AWS::Lambda::Permission
    Properties:
      FunctionName: !Ref OtlpToParquetFunction
      Action: lambda:InvokeFunctionUrl
      Principal: "*"
      FunctionUrlAuthType: AWS_IAM

  FunctionLogGroup:
    Type: AWS::Logs::LogGroup
    Properties:
      LogGroupName: !Sub /aws/lambda/${OtlpToParquetFunction}
      RetentionInDays: !Ref LogRetentionDays

  DataBucket:
    Type: AWS::S3::Bucket
    Condition: UsePlainParquet
    Properties:
      BucketName: !Ref BucketName
      PublicAccessBlockConfiguration:
        BlockPublicAcls: true
        BlockPublicPolicy: true
        IgnorePublicAcls: true
        RestrictPublicBuckets: true

  S3TablesBucket:
    Type: AWS::S3Tables::TableBucket
    Condition: UseIcebergCatalog
    Properties:
      TableBucketName: !Ref BucketName

Outputs:
  FunctionUrl:
    Description: OTLP HTTP endpoint URL
    Value: !GetAtt FunctionUrl.FunctionUrl

  FunctionArn:
    Description: Lambda function ARN
    Value: !GetAtt OtlpToParquetFunction.Arn

  DataLocation:
    Description: Data storage location
    Value: !If
      - UsePlainParquet
      - !Sub 's3://${DataBucket}'
      - !GetAtt S3TablesBucket.Arn
```

**Step 2: Commit**

```bash
git add crates/otlp2parquet-server/templates/cloudformation.yaml
git commit -m "feat(cli): add aws cloudformation template"
```

---

## Task 7: Implement AWS wizard

**Files:**
- Modify: `crates/otlp2parquet-server/src/deploy/aws.rs`

**Step 1: Implement the wizard**

Replace `crates/otlp2parquet-server/src/deploy/aws.rs` with:

```rust
//! AWS CloudFormation deployment config generator

use anyhow::{bail, Context, Result};
use clap::Args;
use dialoguer::{Confirm, Input, Select};
use std::fs;
use std::path::Path;

use super::names;

const TEMPLATE: &str = include_str!("../../templates/cloudformation.yaml");
const GITHUB_RELEASES_URL: &str = "https://github.com/smithclay/otlp2parquet/releases/latest";

#[derive(Args)]
pub struct AwsArgs {
    /// S3 URI of Lambda binary (e.g., s3://my-bucket/otlp2parquet-lambda-arm64.zip)
    #[arg(long)]
    pub lambda_s3_uri: Option<String>,

    /// CloudFormation stack name
    #[arg(long)]
    pub stack_name: Option<String>,

    /// S3 bucket name for data storage
    #[arg(long)]
    pub bucket: Option<String>,

    /// Catalog mode: "iceberg" or "none"
    #[arg(long)]
    pub catalog: Option<String>,

    /// CloudWatch log retention in days
    #[arg(long, default_value = "7")]
    pub retention: u16,

    /// Overwrite existing file without asking
    #[arg(long)]
    pub force: bool,
}

pub fn run(args: AwsArgs) -> Result<()> {
    println!();
    println!("otlp2parquet deploy - AWS Lambda + S3");
    println!();

    // Collect values via wizard or flags
    let lambda_s3_uri = match args.lambda_s3_uri {
        Some(uri) => uri,
        None => {
            println!("Download the Lambda binary from:");
            println!("  {}", GITHUB_RELEASES_URL);
            println!();
            Input::new()
                .with_prompt("S3 URI of Lambda binary")
                .validate_with(validate_s3_uri)
                .interact_text()?
        }
    };

    let (lambda_bucket, lambda_key) = parse_s3_uri(&lambda_s3_uri)?;

    let default_name = names::generate();
    let stack_name = match args.stack_name {
        Some(name) => name,
        None => Input::new()
            .with_prompt("Stack name")
            .default(default_name)
            .interact_text()?,
    };

    let bucket_name = match args.bucket {
        Some(bucket) => bucket,
        None => Input::new()
            .with_prompt("S3 bucket name for data")
            .validate_with(validate_bucket_name)
            .interact_text()?,
    };

    let catalog_mode = match args.catalog {
        Some(cat) => {
            if cat != "iceberg" && cat != "none" {
                bail!("Invalid catalog mode '{}'. Must be 'iceberg' or 'none'.", cat);
            }
            cat
        }
        None => {
            let options = &["No  - Plain Parquet to S3", "Yes - S3 Tables (Iceberg)"];
            let selection = Select::new()
                .with_prompt("Enable Iceberg catalog?")
                .items(options)
                .default(0)
                .interact()?;
            if selection == 0 { "none".to_string() } else { "iceberg".to_string() }
        }
    };

    let retention = args.retention;

    // Check if file exists
    let output_path = Path::new("template.yaml");
    if output_path.exists() && !args.force {
        let overwrite = Confirm::new()
            .with_prompt("template.yaml already exists. Overwrite?")
            .default(false)
            .interact()?;
        if !overwrite {
            println!("Aborted.");
            return Ok(());
        }
    }

    // Render template
    let content = TEMPLATE
        .replace("{{STACK_NAME}}", &stack_name)
        .replace("{{BUCKET_NAME}}", &bucket_name)
        .replace("{{CATALOG_MODE}}", &catalog_mode)
        .replace("{{LOG_RETENTION}}", &retention.to_string())
        .replace("{{LAMBDA_S3_BUCKET}}", &lambda_bucket)
        .replace("{{LAMBDA_S3_KEY}}", &lambda_key);

    // Write file
    fs::write(output_path, &content).context("Failed to write template.yaml")?;

    println!();
    println!("Created template.yaml");
    println!();
    println!("Next steps:");
    println!("  1. Deploy:");
    println!("     aws cloudformation deploy \\");
    println!("       --template-file template.yaml \\");
    println!("       --stack-name {} \\", stack_name);
    println!("       --capabilities CAPABILITY_IAM");
    println!();

    Ok(())
}

fn validate_s3_uri(input: &String) -> Result<(), String> {
    if !input.starts_with("s3://") {
        return Err("S3 URI must start with 's3://'".to_string());
    }
    let path = input.strip_prefix("s3://").unwrap();
    if !path.contains('/') {
        return Err("S3 URI must include both bucket and key (e.g., s3://bucket/key.zip)".to_string());
    }
    Ok(())
}

fn parse_s3_uri(uri: &str) -> Result<(String, String)> {
    let path = uri.strip_prefix("s3://").context("Invalid S3 URI")?;
    let (bucket, key) = path.split_once('/').context("Invalid S3 URI format")?;
    Ok((bucket.to_string(), key.to_string()))
}

fn validate_bucket_name(input: &String) -> Result<(), String> {
    if input.is_empty() {
        return Err("Bucket name cannot be empty".to_string());
    }
    if input.len() < 3 || input.len() > 63 {
        return Err("Bucket name must be 3-63 characters".to_string());
    }
    if !input.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-') {
        return Err("Bucket name must contain only lowercase letters, numbers, and hyphens".to_string());
    }
    if input.starts_with('-') || input.ends_with('-') {
        return Err("Bucket name cannot start or end with a hyphen".to_string());
    }
    Ok(())
}
```

**Step 2: Verify it compiles**

Run: `cargo check -p otlp2parquet-server`
Expected: Compiles successfully

**Step 3: Test the wizard (non-interactive)**

Run: `cargo run -p otlp2parquet-server -- deploy aws --force --lambda-s3-uri s3://my-bucket/lambda.zip --stack-name test-stack --bucket my-data-bucket --catalog none`
Expected: Creates template.yaml, prints next steps

**Step 4: Verify generated file**

Run: `cat template.yaml | head -30`
Expected: Contains stack name, bucket name, lambda S3 location

**Step 5: Clean up and commit**

```bash
rm -f template.yaml
git add crates/otlp2parquet-server/src/deploy/aws.rs
git commit -m "feat(cli): implement aws deploy wizard"
```

---

## Task 8: Run clippy and fix any warnings

**Files:**
- Potentially: any files with warnings

**Step 1: Run clippy**

Run: `cargo clippy -p otlp2parquet-server -- -D warnings`
Expected: No warnings (fix any that appear)

**Step 2: Run tests**

Run: `cargo test -p otlp2parquet-server`
Expected: All tests pass

**Step 3: Commit fixes if any**

```bash
git add -u
git commit -m "fix(cli): address clippy warnings"
```

---

## Task 9: Update design doc and create final commit

**Files:**
- Modify: `docs/plans/2025-01-25-cli-deploy-command-design.md`

**Step 1: Mark design as implemented**

Add at the top of `docs/plans/2025-01-25-cli-deploy-command-design.md`:

```markdown
> **Status:** Implemented
```

**Step 2: Final commit**

```bash
git add docs/plans/2025-01-25-cli-deploy-command-design.md
git commit -m "docs: mark cli deploy design as implemented"
```

---

## Summary

After completing all tasks, you will have:

1. `otlp2parquet deploy cloudflare` - Interactive wizard generating `wrangler.toml`
2. `otlp2parquet deploy aws` - Interactive wizard generating `template.yaml`
3. Fun default names like `nimble-relay-2847`
4. Pre-built artifact support (no Rust toolchain needed for deployment)
5. Flags for CI/CD non-interactive usage
