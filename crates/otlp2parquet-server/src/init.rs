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
    Arc<dyn otlp2parquet_writer::icepick::catalog::Catalog>,
    String,
)> {
    info!(
        "Initializing writer with storage backend: {}",
        config.storage.backend
    );

    // Create OpenDAL operator based on storage backend
    let operator = match config.storage.backend {
        StorageBackend::Fs => {
            let fs = config
                .storage
                .fs
                .as_ref()
                .expect("fs config required for filesystem backend");
            info!("Using filesystem storage at: {}", fs.path);

            let fs_builder = opendal::services::Fs::default().root(&fs.path);
            opendal::Operator::new(fs_builder)?.finish()
        }
        StorageBackend::S3 => {
            let s3 = config
                .storage
                .s3
                .as_ref()
                .expect("s3 config required for S3 backend");
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
                .expect("r2 config required for R2 backend");
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

    // Check if Iceberg catalog is configured
    let iceberg_cfg = config.iceberg.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "Server requires Iceberg catalog configuration. \
            Add [iceberg] section to config.toml with either:\n\
            - bucket_arn = \"arn:aws:s3tables:...:bucket/name\" for S3 Tables\n\
            - rest_uri = \"https://catalog.example.com\" and warehouse = \"s3://bucket/path\" for REST catalog"
        )
    })?;

    let catalog_config = if let Some(bucket_arn) = &iceberg_cfg.bucket_arn {
        // S3 Tables catalog (AWS managed catalog via ARN)
        info!("Initializing with S3 Tables catalog: {}", bucket_arn);

        otlp2parquet_writer::CatalogConfig {
            namespace: "otlp".to_string(),
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

    Ok((catalog, namespace))
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
