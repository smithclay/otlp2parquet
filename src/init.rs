// Initialization utilities for server mode
//
// Storage backend and logging/tracing setup

use crate::config::{LogFormat, RuntimeConfig, StorageBackend};
use anyhow::Result;
use tracing::info;

/// Initialize storage from RuntimeConfig
pub(crate) fn init_writer(config: &RuntimeConfig) -> Result<()> {
    info!(
        "Initializing writer with storage backend: {}",
        config.storage.backend
    );

    match config.storage.backend {
        StorageBackend::Fs => {
            if let Some(fs) = config.storage.fs.as_ref() {
                info!("Using filesystem storage at: {}", fs.path);
            } else {
                info!("Using filesystem storage");
            }
        }
        StorageBackend::S3 => {
            if let Some(s3) = config.storage.s3.as_ref() {
                info!(
                    "Using S3 storage: bucket={}, region={}",
                    s3.bucket, s3.region
                );
            } else {
                info!("Using S3 storage");
            }
        }
        StorageBackend::R2 => {
            if let Some(r2) = config.storage.r2.as_ref() {
                info!(
                    "Using R2 storage: account={}, bucket={}",
                    r2.account_id, r2.bucket
                );
            } else {
                info!("Using R2 storage");
            }
        }
    }
    // Initialize storage for direct writes
    crate::writer::initialize_storage(config)
        .map_err(|e| anyhow::anyhow!("Failed to initialize storage: {}", e))?;

    Ok(())
}

/// Initialize tracing/logging from RuntimeConfig
pub fn init_tracing(config: &RuntimeConfig) {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};

    let Some(server) = config.server.as_ref() else {
        eprintln!("ERROR: server config required for tracing initialization");
        return;
    };

    // Parse log level from config
    let env_filter =
        EnvFilter::try_new(&server.log_level).unwrap_or_else(|_| EnvFilter::new("info"));

    let registry = tracing_subscriber::registry().with(env_filter);

    // Try to set the global subscriber; ignore error if already set (idempotent)
    let _ = match server.log_format {
        LogFormat::Json => {
            tracing::subscriber::set_global_default(registry.with(fmt::layer().json()))
        }
        LogFormat::Text => tracing::subscriber::set_global_default(registry.with(fmt::layer())),
    };
}
