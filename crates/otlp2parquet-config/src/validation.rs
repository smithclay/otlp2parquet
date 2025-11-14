// Configuration validation
//
// Validates that required fields are present and values are sensible

use crate::*;
use anyhow::{bail, Result};
use tracing::warn;

pub fn validate_config(config: &RuntimeConfig) -> Result<()> {
    // Validate batch config
    validate_batch_config(&config.batch)?;

    // Validate request config
    validate_request_config(&config.request)?;

    // Validate storage config
    validate_storage_config(&config.storage)?;

    // Validate platform-specific configs
    if let Some(ref server) = config.server {
        validate_server_config(server)?;
    }

    Ok(())
}

fn validate_batch_config(config: &BatchConfig) -> Result<()> {
    if config.max_rows == 0 {
        bail!("batch.max_rows must be greater than 0");
    }

    if config.max_bytes == 0 {
        bail!("batch.max_bytes must be greater than 0");
    }

    if config.max_age_secs == 0 {
        bail!("batch.max_age_secs must be greater than 0");
    }

    // Warn about very large batch sizes
    if config.max_rows > 10_000_000 {
        warn!(
            max_rows = config.max_rows,
            "batch.max_rows is very large; may cause memory issues"
        );
    }

    if config.max_bytes > 1024 * 1024 * 1024 {
        // 1 GB
        warn!(
            max_bytes = config.max_bytes,
            "batch.max_bytes is very large; may cause memory issues"
        );
    }

    Ok(())
}

fn validate_request_config(config: &RequestConfig) -> Result<()> {
    if config.max_payload_bytes == 0 {
        bail!("request.max_payload_bytes must be greater than 0");
    }

    // Warn about very large payloads
    if config.max_payload_bytes > 100 * 1024 * 1024 {
        // 100 MB
        warn!(
            max_payload_bytes = config.max_payload_bytes,
            "request.max_payload_bytes is very large; may cause issues"
        );
    }

    Ok(())
}

fn validate_storage_config(config: &StorageConfig) -> Result<()> {
    if config.parquet_row_group_size == 0 {
        bail!("storage.parquet_row_group_size must be greater than 0");
    }

    match config.backend {
        StorageBackend::Fs => {
            let fs = config
                .fs
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("fs storage backend requires 'fs' configuration"))?;

            if fs.path.is_empty() {
                bail!("storage.fs.path must not be empty");
            }
        }
        StorageBackend::S3 => {
            let s3 = config
                .s3
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("s3 storage backend requires 's3' configuration"))?;

            if s3.bucket.is_empty() {
                bail!("storage.s3.bucket is required for S3 backend");
            }

            if s3.region.is_empty() {
                bail!("storage.s3.region is required for S3 backend");
            }
        }
        StorageBackend::R2 => {
            let r2 = config
                .r2
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("r2 storage backend requires 'r2' configuration"))?;

            if r2.bucket.is_empty() {
                bail!("storage.r2.bucket is required for R2 backend");
            }

            if r2.account_id.is_empty() {
                bail!("storage.r2.account_id is required for R2 backend");
            }

            if r2.access_key_id.is_empty() {
                bail!("storage.r2.access_key_id is required for R2 backend");
            }

            if r2.secret_access_key.is_empty() {
                bail!("storage.r2.secret_access_key is required for R2 backend");
            }
        }
    }

    Ok(())
}

fn validate_server_config(config: &ServerConfig) -> Result<()> {
    if config.listen_addr.is_empty() {
        bail!("server.listen_addr must not be empty");
    }

    // Basic validation that it looks like an address
    if !config.listen_addr.contains(':') {
        bail!("server.listen_addr must be in format 'host:port'");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_batch_config() {
        let valid = BatchConfig {
            max_rows: 100,
            max_bytes: 1024,
            max_age_secs: 10,
            enabled: true,
        };
        assert!(validate_batch_config(&valid).is_ok());

        let invalid_rows = BatchConfig {
            max_rows: 0,
            max_bytes: 1024,
            max_age_secs: 10,
            enabled: true,
        };
        assert!(validate_batch_config(&invalid_rows).is_err());
    }

    #[test]
    fn test_validate_storage_config() {
        // Valid S3 config
        let s3_config = StorageConfig {
            backend: StorageBackend::S3,
            parquet_row_group_size: default_parquet_row_group_size(),
            fs: None,
            s3: Some(S3Config {
                bucket: "test-bucket".to_string(),
                region: "us-east-1".to_string(),
                endpoint: None,
            }),
            r2: None,
        };
        assert!(validate_storage_config(&s3_config).is_ok());

        // Invalid S3 config (missing bucket)
        let invalid_s3 = StorageConfig {
            backend: StorageBackend::S3,
            parquet_row_group_size: default_parquet_row_group_size(),
            fs: None,
            s3: Some(S3Config {
                bucket: String::new(),
                region: "us-east-1".to_string(),
                endpoint: None,
            }),
            r2: None,
        };
        assert!(validate_storage_config(&invalid_s3).is_err());
    }
}
