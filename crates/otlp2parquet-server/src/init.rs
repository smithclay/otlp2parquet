// Initialization utilities for server mode
//
// Storage backend and logging/tracing setup

use anyhow::Result;
use otlp2parquet_config::{LogFormat, RuntimeConfig, StorageBackend};
use otlp2parquet_storage::opendal_storage::OpenDalStorage;
use std::sync::Arc;
use tracing::info;

/// Initialize storage backend from RuntimeConfig
pub(crate) fn init_storage(config: &RuntimeConfig) -> Result<Arc<OpenDalStorage>> {
    info!("Initializing storage backend: {}", config.storage.backend);

    let storage = match config.storage.backend {
        StorageBackend::Fs => {
            let fs = config
                .storage
                .fs
                .as_ref()
                .expect("fs config required for filesystem backend");
            info!("Using filesystem storage at: {}", fs.path);
            OpenDalStorage::new_fs(&fs.path)?
        }
        StorageBackend::S3 => {
            let s3 = config
                .storage
                .s3
                .as_ref()
                .expect("s3 config required for S3 backend");
            // Credentials auto-discovered from environment (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
            // or IAM role (recommended for production)
            info!(
                "Using S3 storage: bucket={}, region={}",
                s3.bucket, s3.region
            );
            OpenDalStorage::new_s3(&s3.bucket, &s3.region, s3.endpoint.as_deref(), None, None)?
        }
        StorageBackend::R2 => {
            let r2 = config
                .storage
                .r2
                .as_ref()
                .expect("r2 config required for R2 backend");
            info!(
                "Using R2 storage: account={}, bucket={}",
                r2.account_id, r2.bucket
            );
            OpenDalStorage::new_r2(
                &r2.bucket,
                &r2.account_id,
                &r2.access_key_id,
                &r2.secret_access_key,
            )?
        }
    };

    Ok(Arc::new(storage))
}

/// Initialize tracing/logging from RuntimeConfig
pub(crate) fn init_tracing(config: &RuntimeConfig) {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};

    let server = config.server.as_ref().expect("server config required");

    // Parse log level from config
    let env_filter =
        EnvFilter::try_new(&server.log_level).unwrap_or_else(|_| EnvFilter::new("info"));

    let registry = tracing_subscriber::registry().with(env_filter);

    match server.log_format {
        LogFormat::Json => {
            registry.with(fmt::layer().json()).init();
        }
        LogFormat::Text => {
            registry.with(fmt::layer()).init();
        }
    }
}
