// Initialization utilities for server mode
//
// Storage backend and logging/tracing setup

use anyhow::Result;
use icepick::catalog::Catalog;
use otlp2parquet_config::{LogFormat, RuntimeConfig, StorageBackend};
use otlp2parquet_writer::IcepickWriter;
use std::sync::Arc;
use tracing::info;

/// Initialize writer from RuntimeConfig
pub(crate) async fn init_writer(config: &RuntimeConfig) -> Result<IcepickWriter> {
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

    let file_io = icepick::FileIO::new(operator);

    // Check if Iceberg catalog is configured
    let catalog: Option<Arc<dyn Catalog>> = if let Some(iceberg_cfg) = &config.iceberg {
        if let Some(bucket_arn) = &iceberg_cfg.bucket_arn {
            // S3 Tables catalog (AWS managed catalog via ARN)
            info!("Initializing S3 Tables catalog from ARN: {}", bucket_arn);
            match icepick::S3TablesCatalog::from_arn("otlp2parquet", bucket_arn).await {
                Ok(catalog) => Some(Arc::new(catalog)),
                Err(e) => {
                    info!(
                        "Failed to create S3 Tables catalog: {} - continuing without catalog",
                        e
                    );
                    None
                }
            }
        } else if !iceberg_cfg.rest_uri.is_empty() {
            // Generic REST catalog (Nessie, Glue REST, etc.)
            info!(
                "Initializing REST catalog: endpoint={}, warehouse={:?}, namespace={:?}",
                iceberg_cfg.rest_uri, iceberg_cfg.warehouse, iceberg_cfg.namespace
            );

            let mut builder = icepick::RestCatalog::builder("otlp2parquet", &iceberg_cfg.rest_uri);

            // Nessie uses branch name as prefix (e.g., "main")
            // Other catalogs may use warehouse or empty prefix
            builder = builder.with_prefix("main");

            // Build catalog with file_io
            match builder
                .with_file_io(file_io.clone())
                .with_bearer_token("") // Anonymous/no auth for Nessie
                .build()
            {
                Ok(catalog) => Some(Arc::new(catalog)),
                Err(e) => {
                    info!(
                        "Failed to create REST catalog: {} - continuing without catalog",
                        e
                    );
                    None
                }
            }
        } else {
            info!("No bucket_arn or rest_uri configured - Iceberg catalog disabled");
            None
        }
    } else {
        info!("No Iceberg configuration - catalog disabled");
        None
    };

    // Get namespace from config (default to "otlp")
    let namespace = config
        .iceberg
        .as_ref()
        .and_then(|cfg| cfg.namespace.clone())
        .unwrap_or_else(|| "otlp".to_string());

    // Create namespace if catalog is configured
    if let Some(ref catalog) = catalog {
        let namespace_ident = icepick::NamespaceIdent::new(vec![namespace.clone()]);

        // Try to create namespace (ignore error if it already exists)
        match catalog
            .create_namespace(&namespace_ident, Default::default())
            .await
        {
            Ok(_) => info!("Created namespace: {}", namespace),
            Err(e) => {
                // Namespace might already exist, which is fine
                info!(
                    "Namespace creation result for '{}': {} (may already exist)",
                    namespace, e
                );
            }
        }
    }

    IcepickWriter::new_with_namespace(file_io, catalog, String::new(), namespace)
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
