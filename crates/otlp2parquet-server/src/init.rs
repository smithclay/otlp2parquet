// Initialization utilities for server mode
//
// Storage backend and logging/tracing setup

use anyhow::{Context, Result};
use otlp2parquet_storage::opendal_storage::OpenDalStorage;
use std::sync::Arc;
use tracing::info;

/// Initialize storage backend based on STORAGE_BACKEND env var
pub(crate) fn init_storage() -> Result<Arc<OpenDalStorage>> {
    let backend = std::env::var("STORAGE_BACKEND").unwrap_or_else(|_| "fs".to_string());

    info!("Initializing storage backend: {}", backend);

    let storage = match backend.as_str() {
        "fs" => {
            let path = std::env::var("STORAGE_PATH").unwrap_or_else(|_| "./data".to_string());
            info!("Using filesystem storage at: {}", path);
            OpenDalStorage::new_fs(&path)?
        }
        "s3" => {
            let bucket =
                std::env::var("S3_BUCKET").context("S3_BUCKET env var required for s3 backend")?;
            let region =
                std::env::var("S3_REGION").context("S3_REGION env var required for s3 backend")?;
            let endpoint = std::env::var("S3_ENDPOINT").ok();

            // Credentials auto-discovered from environment (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
            // or IAM role (recommended for production)
            info!("Using S3 storage: bucket={}, region={}", bucket, region);
            OpenDalStorage::new_s3(&bucket, &region, endpoint.as_deref(), None, None)?
        }
        "r2" => {
            let bucket =
                std::env::var("R2_BUCKET").context("R2_BUCKET env var required for r2 backend")?;
            let account_id = std::env::var("R2_ACCOUNT_ID")
                .context("R2_ACCOUNT_ID env var required for r2 backend")?;
            let access_key_id = std::env::var("R2_ACCESS_KEY_ID")
                .context("R2_ACCESS_KEY_ID env var required for r2 backend")?;
            let secret_access_key = std::env::var("R2_SECRET_ACCESS_KEY")
                .context("R2_SECRET_ACCESS_KEY env var required for r2 backend")?;

            info!(
                "Using R2 storage: account={}, bucket={}",
                account_id, bucket
            );
            OpenDalStorage::new_r2(&bucket, &account_id, &access_key_id, &secret_access_key)?
        }
        _ => {
            anyhow::bail!(
                "Unsupported storage backend: {}. Supported: fs, s3, r2",
                backend
            );
        }
    };

    Ok(Arc::new(storage))
}

/// Initialize tracing/logging
pub(crate) fn init_tracing() {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};

    // Default to INFO level, override with LOG_LEVEL env var
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    // Check if we should use JSON output (for structured logging in production)
    let json_logs = std::env::var("LOG_FORMAT")
        .map(|f| f == "json")
        .unwrap_or(false);

    let registry = tracing_subscriber::registry().with(env_filter);

    if json_logs {
        registry.with(fmt::layer().json()).init();
    } else {
        registry.with(fmt::layer()).init();
    }
}

/// Platform-specific helper: Read max payload bytes from environment
pub(crate) fn max_payload_bytes_from_env(default: usize) -> usize {
    std::env::var("MAX_PAYLOAD_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default)
}
