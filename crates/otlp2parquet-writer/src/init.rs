//! Writer initialization and catalog setup

use anyhow::Result;

use crate::{IcepickWriter, Platform};

/// Initialize a writer for AWS Lambda with S3 Tables catalog
///
/// # Arguments
/// * `bucket_arn` - S3 Tables bucket ARN (e.g., "arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket")
/// * `base_path` - Optional base path prefix for files (empty string for none)
pub async fn initialize_lambda_writer(
    bucket_arn: &str,
    base_path: String,
) -> Result<IcepickWriter> {
    #[cfg(target_family = "wasm")]
    {
        let _ = (bucket_arn, base_path);
        anyhow::bail!("S3TablesCatalog not supported on WASM")
    }

    #[cfg(not(target_family = "wasm"))]
    {
        initialize_lambda_writer_impl(bucket_arn, base_path).await
    }
}

#[cfg(not(target_family = "wasm"))]
async fn initialize_lambda_writer_impl(
    bucket_arn: &str,
    _base_path: String, // Unused - icepick handles paths
) -> Result<IcepickWriter> {
    use std::sync::Arc;

    tracing::debug!(
        "Initializing Lambda writer with S3 Tables ARN: {}",
        bucket_arn
    );

    // Create S3 Tables catalog - icepick will:
    // 1. Create namespace if it doesn't exist
    // 2. Create tables if they don't exist (deriving Iceberg from Arrow)
    // 3. Write Parquet files with statistics
    // 4. Commit transactions atomically

    // TODO(error-handling): Add connection timeout
    // TODO(error-handling): Validate ARN format before attempting connection
    let catalog = icepick::S3TablesCatalog::from_arn("otlp2parquet", bucket_arn)
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "Failed to create S3 Tables catalog from ARN '{}': {}",
                bucket_arn,
                e
            )
        })?;

    tracing::info!("✓ Connected to S3 Tables catalog: {}", bucket_arn);

    // Default namespace for OTLP tables
    let namespace = "otlp".to_string();

    IcepickWriter::new(Arc::new(catalog), namespace)
}

/// Initialize a writer for Server with configurable REST catalog
///
/// # Arguments
/// * `rest_uri` - REST catalog endpoint (e.g., `https://nessie.example.com/iceberg`)
/// * `warehouse` - Warehouse location (e.g., `s3://bucket/warehouse`)
/// * `operator` - OpenDAL operator for accessing warehouse storage
/// * `catalog_config` - Optional catalog-specific configuration (auth tokens, etc.)
///
/// # Configuration
/// Expected keys in `catalog_config`:
/// - `token`: Bearer token for authentication (optional)
/// - `credential`: OAuth2 credential (optional)
/// - `namespace`: Iceberg namespace (defaults to "otlp")
///
/// # Examples
/// ```ignore
/// // Nessie REST catalog
/// let config = HashMap::from([
///     ("token".to_string(), "my-bearer-token".to_string()),
///     ("namespace".to_string(), "production".to_string()),
/// ]);
/// let operator = opendal::Operator::new(s3_builder)?.finish();
/// let writer = initialize_server_writer(
///     "https://nessie.example.com/iceberg",
///     "s3://my-bucket/warehouse",
///     operator,
///     Some(config)
/// ).await?;
/// ```
///
/// TODO(error-handling): Add connection timeout
/// TODO(error-handling): Add retry logic for transient failures
/// TODO(error-handling): Validate REST endpoint is reachable before proceeding
pub async fn initialize_server_writer(
    rest_uri: &str,
    warehouse: &str,
    operator: opendal::Operator,
    catalog_config: Option<std::collections::HashMap<String, String>>,
) -> Result<IcepickWriter> {
    use std::sync::Arc;

    tracing::debug!(
        "Initializing Server writer with REST catalog: uri={}, warehouse={}",
        rest_uri,
        warehouse
    );

    // Extract configuration
    let config = catalog_config.unwrap_or_default();
    let namespace = config
        .get("namespace")
        .cloned()
        .unwrap_or_else(|| "otlp".to_string());

    // Build REST catalog
    let mut builder = icepick::RestCatalog::builder("otlp2parquet", rest_uri);

    // Create FileIO from operator
    let file_io = icepick::FileIO::new(operator);
    builder = builder.with_file_io(file_io);

    // Add authentication if provided
    if let Some(token) = config.get("token") {
        tracing::debug!("Using Bearer token authentication");
        builder = builder.with_bearer_token(token);
    } else {
        // Use empty bearer token for anonymous access (Nessie default)
        tracing::debug!("Using anonymous authentication");
        builder = builder.with_bearer_token("");
    }

    // TODO(error-handling): Add connection timeout via builder
    // TODO(error-handling): Add retry configuration via builder

    let catalog = builder.build().map_err(|e| {
        anyhow::anyhow!(
            "Failed to create REST catalog (uri: {}, warehouse: {}): {}",
            rest_uri,
            warehouse,
            e
        )
    })?;

    tracing::info!(
        "✓ Connected to REST catalog: {} (namespace: {})",
        rest_uri,
        namespace
    );

    IcepickWriter::new(Arc::new(catalog), namespace)
}

/// Initialize a writer for Cloudflare Workers with R2 Data Catalog
///
/// # Arguments
/// * `account_id` - Cloudflare account ID
/// * `bucket_name` - R2 bucket name
/// * `api_token` - Cloudflare API token with R2 permissions
/// * `namespace` - Iceberg namespace for tables (e.g., "otlp")
///
/// # Returns
/// IcepickWriter configured with R2 Data Catalog
///
/// TODO(error-handling): Add validation for credentials format
/// TODO(error-handling): Add connection timeout
/// TODO(error-handling): Handle token expiration gracefully
pub async fn initialize_cloudflare_writer(
    account_id: &str,
    bucket_name: &str,
    api_token: &str,
    namespace: Option<String>,
) -> Result<IcepickWriter> {
    use std::sync::Arc;

    tracing::debug!(
        "Initializing Cloudflare worker with R2 Data Catalog: account={}, bucket={}",
        account_id,
        bucket_name
    );

    // Create R2 Data Catalog - icepick will:
    // 1. Create namespace if it doesn't exist
    // 2. Create tables if they don't exist (deriving Iceberg from Arrow)
    // 3. Write Parquet files with statistics
    // 4. Commit transactions atomically

    // TODO(error-handling): Add connection timeout
    // TODO(error-handling): Validate credentials before attempting connection
    let catalog = icepick::R2Catalog::new("otlp2parquet", account_id, bucket_name, api_token)
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "Failed to create R2 Data Catalog (account: {}, bucket: {}): {}",
                account_id,
                bucket_name,
                e
            )
        })?;

    tracing::info!(
        "✓ Connected to R2 Data Catalog: {}/{}",
        account_id,
        bucket_name
    );

    // Default namespace for OTLP tables
    let ns = namespace.unwrap_or_else(|| "otlp".to_string());

    IcepickWriter::new(Arc::new(catalog), ns)
}

/// Initialize a writer based on platform detection
pub async fn initialize_writer(platform: Platform, bucket_arn: &str) -> Result<IcepickWriter> {
    match platform {
        Platform::Lambda => initialize_lambda_writer(bucket_arn, String::new()).await,
        Platform::Server => {
            anyhow::bail!("Server platform requires initialize_server_writer()")
        }
        Platform::CloudflareWorkers => {
            anyhow::bail!("CloudflareWorkers requires initialize_cloudflare_writer()")
        }
    }
}
