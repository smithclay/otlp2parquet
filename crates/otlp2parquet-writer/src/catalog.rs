//! Catalog initialization and management

use crate::error::{Result, WriterError};
use icepick::catalog::Catalog;
use icepick::NamespaceIdent;
use std::collections::HashMap;
use std::sync::Arc;

/// Configuration for catalog initialization
pub struct CatalogConfig {
    /// Namespace for tables
    pub namespace: String,
    /// Platform-specific catalog type
    pub catalog_type: CatalogType,
}

/// Catalog type variants for different platforms
pub enum CatalogType {
    /// AWS S3 Tables catalog (Lambda)
    S3Tables {
        /// S3 Tables bucket ARN
        bucket_arn: String,
    },
    /// REST catalog (Server - Nessie, Glue, etc.)
    Rest {
        /// REST catalog URI
        uri: String,
        /// Warehouse location (for logging)
        warehouse: String,
        /// OpenDAL operator for storage access
        operator: opendal::Operator,
        /// Optional authentication token
        token: Option<String>,
    },
    /// R2 Data Catalog (Cloudflare Workers)
    R2DataCatalog {
        /// Cloudflare account ID
        account_id: String,
        /// R2 bucket name
        bucket_name: String,
        /// Cloudflare API token
        api_token: String,
    },
}

/// Initialize a catalog based on configuration
///
/// # Arguments
/// * `config` - Catalog configuration with platform-specific details
///
/// # Returns
/// Arc-wrapped catalog implementation ready for use
///
/// # Errors
/// Returns `WriterError::CatalogInit` if catalog creation fails
/// Returns `WriterError::UnsupportedPlatform` if platform incompatible with build
pub async fn initialize_catalog(config: CatalogConfig) -> Result<Arc<dyn Catalog>> {
    match config.catalog_type {
        CatalogType::S3Tables { bucket_arn } => {
            #[cfg(target_family = "wasm")]
            {
                let _ = bucket_arn;
                Err(WriterError::UnsupportedPlatform(
                    "S3 Tables not available on WASM".into(),
                ))
            }

            #[cfg(not(target_family = "wasm"))]
            {
                tracing::debug!("Initializing S3 Tables catalog with ARN: {}", bucket_arn);

                let catalog = icepick::S3TablesCatalog::from_arn("otlp2parquet", &bucket_arn)
                    .await
                    .map_err(|e| {
                        WriterError::CatalogInit(format!("S3 Tables ARN '{}': {}", bucket_arn, e))
                    })?;

                tracing::info!("✓ Connected to S3 Tables catalog: {}", bucket_arn);
                Ok(Arc::new(catalog))
            }
        }

        CatalogType::Rest {
            uri,
            warehouse,
            operator,
            token,
        } => {
            tracing::debug!(
                "Initializing REST catalog: uri={}, warehouse={}",
                uri,
                warehouse
            );

            let mut builder = icepick::RestCatalog::builder("otlp2parquet", &uri);
            builder = builder.with_file_io(icepick::FileIO::new(operator));

            if let Some(token) = token {
                tracing::debug!("Using Bearer token authentication");
                builder = builder.with_bearer_token(&token);
            } else {
                tracing::debug!("Using anonymous authentication");
                builder = builder.with_bearer_token("");
            }

            let catalog = builder
                .build()
                .map_err(|e| WriterError::CatalogInit(format!("REST catalog '{}': {}", uri, e)))?;

            tracing::info!(
                "✓ Connected to REST catalog: {} (warehouse: {})",
                uri,
                warehouse
            );
            Ok(Arc::new(catalog))
        }

        CatalogType::R2DataCatalog {
            account_id,
            bucket_name,
            api_token,
        } => {
            tracing::debug!(
                "Initializing R2 Data Catalog: account={}, bucket={}",
                account_id,
                bucket_name
            );

            let catalog =
                icepick::R2Catalog::new("otlp2parquet", &account_id, &bucket_name, &api_token)
                    .await
                    .map_err(|e| {
                        WriterError::CatalogInit(format!(
                            "R2 catalog account={}, bucket={}: {}",
                            account_id, bucket_name, e
                        ))
                    })?;

            tracing::info!(
                "✓ Connected to R2 Data Catalog: {}/{}",
                account_id,
                bucket_name
            );
            Ok(Arc::new(catalog))
        }
    }
}

/// Ensure namespace exists in catalog
///
/// Creates namespace if it doesn't exist. Ignores errors if namespace already exists.
///
/// # Arguments
/// * `catalog` - Catalog instance
/// * `namespace` - Namespace name to create
///
/// # Returns
/// Ok(()) if namespace exists or was created successfully
pub async fn ensure_namespace(catalog: &dyn Catalog, namespace: &str) -> Result<()> {
    let namespace_ident = NamespaceIdent::new(vec![namespace.to_string()]);

    match catalog
        .create_namespace(&namespace_ident, HashMap::new())
        .await
    {
        Ok(_) => {
            tracing::info!("Created namespace: {}", namespace);
            Ok(())
        }
        Err(e) => {
            tracing::debug!(
                "Namespace '{}' may already exist: {} (ignoring)",
                namespace,
                e
            );
            Ok(())
        }
    }
}
