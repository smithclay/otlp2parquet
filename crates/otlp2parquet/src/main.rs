use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use otlp2parquet_common::config::RuntimeConfig;
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
    #[command(alias = "deploy")]
    Create {
        #[command(subcommand)]
        platform: otlp2parquet::deploy::DeployCommand,
    },
    /// Generate configuration for connecting external services
    Connect {
        #[command(subcommand)]
        service: otlp2parquet::connect::ConnectCommand,
    },
    /// Start the HTTP server (default if no subcommand given)
    Serve,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Create { platform }) => platform.run(),
        Some(Commands::Connect { service }) => run_connect(service),
        Some(Commands::Serve) | None => run_server(cli),
    }
}

fn run_connect(service: otlp2parquet::connect::ConnectCommand) -> Result<()> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("Failed to build tokio runtime")?
        .block_on(service.run())
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
    otlp2parquet::init_tracing(&config);

    // Step 5: Validate configuration early (creates directories, tests write permissions)
    validate_config(&config).await?;

    // Step 6: Display startup info
    display_startup_info(&config);

    // Step 7: Run server with resolved config
    otlp2parquet::run_with_config(config).await
}

fn apply_cli_overrides(config: &mut RuntimeConfig, cli: &Cli) -> Result<()> {
    use otlp2parquet_common::config::{ServerConfig, StorageBackend};

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
    use otlp2parquet_common::config::{LogFormat, ServerConfig, StorageBackend};

    // Ensure server config exists with defaults
    let server = config.server.get_or_insert_with(ServerConfig::default);

    // Desktop-friendly log format (human-readable)
    server.log_format = LogFormat::Text;

    // Default to filesystem backend for desktop
    if config.storage.backend == StorageBackend::Fs {
        config.storage.fs.get_or_insert_with(Default::default);
    }
}

fn display_startup_info(config: &RuntimeConfig) {
    use otlp2parquet_common::config::StorageBackend;
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
    use otlp2parquet_common::config::StorageBackend;
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
