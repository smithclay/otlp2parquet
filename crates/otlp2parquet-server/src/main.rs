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

    // Step 4: Validate configuration early (creates directories, tests write permissions)
    validate_config(&config).await?;

    // Step 5: Run server with resolved config
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

async fn validate_config(config: &RuntimeConfig) -> Result<()> {
    use otlp2parquet_config::StorageBackend;
    use std::fs;

    // Validate filesystem output directory if using fs backend
    if config.storage.backend == StorageBackend::Fs {
        let fs_config = config.storage.fs.as_ref().ok_or_else(|| {
            anyhow::anyhow!("filesystem backend requires storage.fs configuration")
        })?;

        let output_path = PathBuf::from(&fs_config.path);

        // Create directory if it doesn't exist
        if !output_path.exists() {
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
    }

    // Validate server config exists
    config
        .server
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("server configuration required"))?;

    Ok(())
}
