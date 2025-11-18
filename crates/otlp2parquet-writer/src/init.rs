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
    base_path: String,
) -> Result<IcepickWriter> {
    use std::sync::Arc;

    tracing::debug!(
        "Initializing Lambda writer with S3 Tables ARN: {}",
        bucket_arn
    );

    // Create S3TablesCatalog - it manages its own FileIO internally
    let catalog = icepick::S3TablesCatalog::from_arn("otlp2parquet", bucket_arn)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create S3Tables catalog: {}", e))?;

    tracing::info!("Successfully created S3TablesCatalog from ARN");

    // S3TablesCatalog manages its own FileIO with correct region/credentials
    // We'll get the FileIO from tables as needed
    // For now, we need a placeholder FileIO for the writer initialization
    // The actual FileIO will come from table.file_io() when we load/create tables

    // Extract bucket info for FileIO placeholder
    let parts: Vec<&str> = bucket_arn.split(':').collect();
    if parts.len() < 6 {
        anyhow::bail!("Invalid S3 Tables ARN format: {}", bucket_arn);
    }

    let region = parts[3];
    let bucket_name = parts[5]
        .split('/')
        .next_back()
        .ok_or_else(|| anyhow::anyhow!("Invalid bucket name in ARN"))?;

    tracing::debug!(
        "Extracted from ARN - region: {}, bucket: {}",
        region,
        bucket_name
    );

    // Create FileIO - this will be replaced by table.file_io() when we have a table
    let s3_builder = opendal::services::S3::default()
        .bucket(bucket_name)
        .region(region);

    let operator = opendal::Operator::new(s3_builder)
        .map_err(|e| anyhow::anyhow!("Failed to build S3 operator: {}", e))?
        .finish();

    let file_io = icepick::FileIO::new(operator);

    IcepickWriter::new(file_io, Some(Arc::new(catalog)), base_path)
}

/// Initialize a writer for Server with configurable storage
///
/// # Arguments
/// * `storage_uri` - Storage URI (e.g., "s3://bucket", "file:///path")
/// * `catalog_type` - Optional catalog type ("s3-tables", "r2", or None for plain Parquet)
/// * `catalog_config` - Catalog-specific configuration
pub async fn initialize_server_writer(
    _storage_uri: &str,
    _catalog_type: Option<&str>,
    _catalog_config: Option<std::collections::HashMap<String, String>>,
) -> Result<IcepickWriter> {
    // TODO: Implement server initialization with multiple storage backends
    anyhow::bail!("Server initialization not yet implemented")
}

/// Initialize a writer for Cloudflare Workers with R2 storage
///
/// # Arguments
/// * `bucket` - R2 bucket name
/// * `region` - R2 region (usually "auto")
/// * `endpoint` - Optional custom S3-compatible endpoint
/// * `access_key_id` - Optional R2 access key ID
/// * `secret_access_key` - Optional R2 secret access key
/// * `base_path` - Optional base path prefix for files (empty string for none)
///
/// Note: For now, this creates a plain Parquet writer without R2 catalog integration.
/// R2 Data Catalog support will be added in a future update.
pub async fn initialize_cloudflare_writer(
    bucket: &str,
    region: &str,
    endpoint: Option<&str>,
    access_key_id: Option<&str>,
    secret_access_key: Option<&str>,
    base_path: String,
) -> Result<IcepickWriter> {
    // Create OpenDAL S3 operator for R2
    let mut s3_builder = opendal::services::S3::default()
        .bucket(bucket)
        .region(region);

    if let Some(ep) = endpoint {
        s3_builder = s3_builder.endpoint(ep);
    }

    if let Some(key) = access_key_id {
        s3_builder = s3_builder.access_key_id(key);
    }

    if let Some(secret) = secret_access_key {
        s3_builder = s3_builder.secret_access_key(secret);
    }

    let operator = opendal::Operator::new(s3_builder)
        .map_err(|e| anyhow::anyhow!("Failed to build R2 operator: {}", e))?
        .finish();

    let file_io = icepick::FileIO::new(operator);

    // Create writer without catalog (plain Parquet mode)
    IcepickWriter::new(file_io, None, base_path)
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
