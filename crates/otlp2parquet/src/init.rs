// Initialization utilities for server mode
//
// Storage backend and logging/tracing setup

use anyhow::Result;
use otlp2parquet_config::{LogFormat, RuntimeConfig, StorageBackend};
use std::sync::Arc;
use tracing::info;

/// Initialize catalog from RuntimeConfig
pub(crate) async fn init_writer(
    config: &RuntimeConfig,
) -> Result<(
    Option<Arc<dyn otlp2parquet_writer::icepick::catalog::Catalog>>,
    String,
)> {
    use otlp2parquet_config::CatalogMode;

    info!(
        "Initializing writer with storage backend: {} (catalog_mode: {})",
        config.storage.backend, config.catalog_mode
    );

    // Create OpenDAL operator based on storage backend
    let operator = match config.storage.backend {
        StorageBackend::Fs => {
            let fs = config
                .storage
                .fs
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("fs config required for filesystem backend"))?;
            info!("Using filesystem storage at: {}", fs.path);

            let fs_builder = opendal::services::Fs::default().root(&fs.path);
            opendal::Operator::new(fs_builder)?.finish()
        }
        StorageBackend::S3 => {
            let s3 = config
                .storage
                .s3
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("s3 config required for S3 backend"))?;
            info!(
                "Using S3 storage: bucket={}, region={}",
                s3.bucket, s3.region
            );

            let mut s3_builder = opendal::services::S3::default()
                .bucket(&s3.bucket)
                .region(&s3.region);

            if let Some(endpoint) = &s3.endpoint {
                s3_builder = s3_builder.endpoint(endpoint);
            }

            opendal::Operator::new(s3_builder)?.finish()
        }
        StorageBackend::R2 => {
            let r2 = config
                .storage
                .r2
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("r2 config required for R2 backend"))?;
            info!(
                "Using R2 storage: account={}, bucket={}",
                r2.account_id, r2.bucket
            );

            let r2_builder = opendal::services::S3::default()
                .bucket(&r2.bucket)
                .region("auto")
                .endpoint(&format!(
                    "https://{}.r2.cloudflarestorage.com",
                    r2.account_id
                ))
                .access_key_id(&r2.access_key_id)
                .secret_access_key(&r2.secret_access_key);

            opendal::Operator::new(r2_builder)?.finish()
        }
    };

    // Check catalog_mode to determine whether to initialize Iceberg catalog
    if config.catalog_mode == CatalogMode::None {
        // Plain Parquet mode - skip catalog entirely
        info!("Catalog mode: none - writing plain Parquet files without Iceberg catalog");

        // Initialize storage for direct writes
        otlp2parquet_writer::initialize_storage(config)
            .map_err(|e| anyhow::anyhow!("Failed to initialize storage: {}", e))?;

        return Ok((None, "otlp".to_string()));
    }

    // Iceberg mode - check if Iceberg catalog is configured
    let iceberg_cfg = config.iceberg.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "Catalog mode is 'iceberg' but no Iceberg catalog configuration found. \
            Either:\n\
            1. Add [iceberg] section to config.toml with:\n\
               - bucket_arn = \"arn:aws:s3tables:...:bucket/name\" for S3 Tables\n\
               - rest_uri = \"https://catalog.example.com\" and warehouse = \"s3://bucket/path\" for REST catalog\n\
            2. Or set OTLP2PARQUET_CATALOG_MODE=none to write plain Parquet files"
        )
    })?;

    let catalog_config = if let Some(bucket_arn) = &iceberg_cfg.bucket_arn {
        // S3 Tables catalog (AWS managed catalog via ARN)
        info!("Initializing with S3 Tables catalog: {}", bucket_arn);

        let namespace = iceberg_cfg
            .namespace
            .clone()
            .unwrap_or_else(|| "otlp".to_string());

        otlp2parquet_writer::CatalogConfig {
            namespace,
            catalog_type: otlp2parquet_writer::CatalogType::S3Tables {
                bucket_arn: bucket_arn.clone(),
            },
        }
    } else if !iceberg_cfg.rest_uri.is_empty() {
        // Generic REST catalog (Nessie, Glue REST, etc.)
        info!(
            "Initializing with REST catalog: endpoint={}, warehouse={:?}",
            iceberg_cfg.rest_uri, iceberg_cfg.warehouse
        );

        // Get warehouse location (required for REST catalog)
        let warehouse = iceberg_cfg
            .warehouse
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("warehouse is required for REST catalog"))?
            .clone();

        // Get namespace from config or use default
        let namespace = iceberg_cfg
            .namespace
            .clone()
            .unwrap_or_else(|| "otlp".to_string());

        // Get auth token if available from env
        let token = std::env::var("ICEBERG_REST_TOKEN").ok();

        otlp2parquet_writer::CatalogConfig {
            namespace,
            catalog_type: otlp2parquet_writer::CatalogType::Rest {
                uri: iceberg_cfg.rest_uri.clone(),
                warehouse,
                operator,
                token,
            },
        }
    } else {
        anyhow::bail!("Server requires either bucket_arn or rest_uri in iceberg configuration")
    };

    let namespace = catalog_config.namespace.clone();
    let catalog = otlp2parquet_writer::initialize_catalog(catalog_config).await?;

    // Ensure namespace exists
    otlp2parquet_writer::ensure_namespace(catalog.as_ref(), &namespace).await?;

    Ok((Some(catalog), namespace))
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
