use anyhow::{Context, Result};
use clap::Parser;
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
