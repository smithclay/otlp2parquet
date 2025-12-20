//! Catalog initialization and management

use crate::error::{redact_secret, Result, WriterError};
use icepick::catalog::{Catalog, CatalogOptions};
use icepick::error::Error as CatalogError;
use icepick::NamespaceIdent;
use std::collections::HashMap;
use std::sync::Arc;

// Only import retry/timeout types on non-WASM platforms where they're used
#[cfg(not(target_family = "wasm"))]
use icepick::catalog::{BackoffStrategy, RetryConfig};
#[cfg(not(target_family = "wasm"))]
use std::time::Duration;

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
        /// R2 access key ID (AWS-compatible)
        access_key_id: String,
        /// R2 secret access key (AWS-compatible)
        secret_access_key: String,
    },
}

/// Initialize a catalog based on configuration
///
/// Delegates to icepick catalog implementations which handle connection management,
/// authentication, and basic error handling. This function focuses on configuration
/// mapping and error translation.
///
/// # Resilience & Timeouts
///
/// Connection resilience is handled by icepick catalog implementations:
/// - **S3 Tables**: Uses AWS SDK defaults (connection timeout ~60s, retries via SDK)
/// - **REST Catalog**: Uses reqwest defaults (connection timeout ~30s, no automatic retries)
/// - **R2 Data Catalog**: Uses Cloudflare API defaults (timeout ~30s, no automatic retries)
///
/// If a catalog connection fails, this function returns immediately with an error.
/// Callers should handle retries at the application level if needed.
///
/// # Future Configuration
///
/// Once icepick exposes builder patterns for timeout/retry configuration, this function
/// will be updated to accept and forward those settings via `CatalogConfig`.
///
/// # Arguments
/// * `config` - Catalog configuration with platform-specific details
///
/// # Returns
/// Arc-wrapped catalog implementation ready for use
///
/// # Errors
/// Returns `WriterError::CatalogInit` if catalog creation fails (connection, auth, etc.)
/// Returns `WriterError::UnsupportedPlatform` if platform incompatible with build target
pub async fn initialize_catalog(config: CatalogConfig) -> Result<Arc<dyn Catalog>> {
    match config.catalog_type {
        CatalogType::S3Tables { bucket_arn } => {
            #[cfg(target_family = "wasm")]
            {
                let _ = bucket_arn;
                Err(WriterError::unsupported_platform(
                    "S3 Tables not available on WASM".to_string(),
                    "Server, Lambda (native only)".to_string(),
                ))
            }

            #[cfg(not(target_family = "wasm"))]
            {
                tracing::debug!("Initializing S3 Tables catalog with ARN: {}", bucket_arn);

                let catalog = icepick::S3TablesCatalog::from_arn("otlp2parquet", &bucket_arn)
                    .await
                    .map_err(|e| {
                        WriterError::catalog_init(
                            "S3 Tables".to_string(),
                            bucket_arn.to_string(),
                            e.to_string(),
                        )
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

            let mut builder = icepick::RestCatalog::builder("otlp2parquet", &uri)
                .with_file_io(icepick::FileIO::new(operator));

            // Configure retry and timeout (not available on WASM)
            #[cfg(not(target_family = "wasm"))]
            {
                let retry_config = RetryConfig::new(
                    3,
                    BackoffStrategy::Exponential {
                        initial_delay: Duration::from_millis(100),
                        max_delay: Duration::from_secs(30),
                        multiplier: 2.0,
                    },
                )
                .with_max_elapsed_time(Duration::from_secs(120));

                builder = builder
                    .with_retry_config(retry_config)
                    .with_timeout(Duration::from_secs(60));
            }

            // Capture redacted token for error messages before moving
            let redacted_token = token
                .as_ref()
                .map(|t| redact_secret(t))
                .unwrap_or_else(|| "(anonymous)".to_string());

            if let Some(token) = token {
                tracing::debug!("Using Bearer token authentication");
                builder = builder.with_bearer_token(&token);
            } else {
                tracing::debug!("Using anonymous authentication");
                builder = builder.with_bearer_token("");
            }

            let catalog = builder.build().map_err(|e| {
                let credentials_used = format!("  • bearer_token: {}", redacted_token);
                WriterError::catalog_init_with_credentials(
                    "REST".to_string(),
                    uri.to_string(),
                    e.to_string(),
                    credentials_used,
                )
            })?;

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
            access_key_id,
            secret_access_key,
        } => {
            tracing::debug!(
                "Initializing R2 Data Catalog: account={}, bucket={}",
                account_id,
                bucket_name
            );

            let catalog = icepick::R2Catalog::with_credentials(
                "otlp2parquet",
                &account_id,
                &bucket_name,
                &api_token,
                &access_key_id,
                &secret_access_key,
                CatalogOptions::default(),
            )
            .await
            .map_err(|e| {
                // Format redacted credentials for debugging
                let credentials_used = format!(
                    "  • api_token: {}\n  • access_key_id: {}\n  • secret_access_key: {}",
                    redact_secret(&api_token),
                    redact_secret(&access_key_id),
                    redact_secret(&secret_access_key)
                );
                WriterError::catalog_init_with_credentials(
                    "R2 Data Catalog".to_string(),
                    format!("account={}, bucket={}", account_id, bucket_name),
                    e.to_string(),
                    credentials_used,
                )
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
/// Attempts to create the namespace. If creation fails because it already exists (HTTP 409
/// Conflict), this is expected and logged at debug level. Other failures are logged at warn
/// level but still return Ok for best-effort availability.
///
/// # Observability
///
/// - Success: Logs at `info` level with namespace name
/// - Already exists (Conflict): Logs at `debug` level
/// - Other failures: Logs at `warn` level (continues anyway)
///
/// # Arguments
/// * `catalog` - Catalog instance
/// * `namespace` - Namespace name to create
///
/// # Returns
/// Always returns `Ok(())` - namespace creation failures are logged but not propagated
/// to maintain availability for the write path
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
        Err(CatalogError::Conflict { .. }) => {
            // HTTP 409 - namespace already exists, this is expected
            tracing::debug!("Namespace '{}' already exists", namespace);
            Ok(())
        }
        Err(e) => {
            // Other errors (network, permission, etc.) - log at warn level but continue
            // to maintain availability. The subsequent table operations will fail if
            // the namespace truly doesn't exist.
            tracing::warn!(
                namespace = %namespace,
                error = %e,
                "Failed to create namespace (continuing anyway)"
            );
            Ok(())
        }
    }
}
