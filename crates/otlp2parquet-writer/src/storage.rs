//! Storage operator initialization and management
//!
//! Provides a global operator for direct Parquet writes.

use once_cell::sync::OnceCell;
use otlp2parquet_common::config::{RuntimeConfig, StorageBackend};

static OPERATOR: OnceCell<opendal::Operator> = OnceCell::new();
static STORAGE_PREFIX: OnceCell<Option<String>> = OnceCell::new();

/// Initialize storage operator from RuntimeConfig
///
/// This creates an OpenDAL operator that can be used for direct Parquet writes.
/// The operator is stored globally and reused.
pub fn initialize_storage(config: &RuntimeConfig) -> crate::Result<()> {
    if OPERATOR.get().is_some() {
        return Ok(()); // Already initialized
    }

    let operator = match config.storage.backend {
        #[cfg(not(target_arch = "wasm32"))]
        StorageBackend::Fs => {
            let fs = config.storage.fs.as_ref().ok_or_else(|| {
                crate::WriterError::write_failure(
                    "fs config required for filesystem backend".to_string(),
                )
            })?;

            let fs_builder = opendal::services::Fs::default().root(&fs.path);
            opendal::Operator::new(fs_builder)
                .map_err(|e| {
                    crate::WriterError::write_failure(format!(
                        "Failed to create filesystem operator: {}",
                        e
                    ))
                })?
                .finish()
        }
        #[cfg(target_arch = "wasm32")]
        StorageBackend::Fs => {
            return Err(crate::WriterError::write_failure(
                "Filesystem storage backend not supported in WASM environments".to_string(),
            ));
        }
        StorageBackend::S3 => {
            let s3 = config.storage.s3.as_ref().ok_or_else(|| {
                crate::WriterError::write_failure("s3 config required for S3 backend".to_string())
            })?;

            // Store the prefix for path generation
            let _ = STORAGE_PREFIX.set(s3.prefix.clone());

            let mut s3_builder = opendal::services::S3::default()
                .bucket(&s3.bucket)
                .region(&s3.region);

            if let Some(endpoint) = &s3.endpoint {
                s3_builder = s3_builder.endpoint(endpoint);
            }

            opendal::Operator::new(s3_builder)
                .map_err(|e| {
                    crate::WriterError::write_failure(format!(
                        "Failed to create S3 operator: {}",
                        e
                    ))
                })?
                .finish()
        }
        StorageBackend::R2 => {
            let r2 = config.storage.r2.as_ref().ok_or_else(|| {
                crate::WriterError::write_failure("r2 config required for R2 backend".to_string())
            })?;

            // Store the prefix for path generation
            let _ = STORAGE_PREFIX.set(r2.prefix.clone());

            // Use endpoint from config if provided, otherwise construct from account_id
            let endpoint = r2
                .endpoint
                .clone()
                .unwrap_or_else(|| format!("https://{}.r2.cloudflarestorage.com", r2.account_id));

            let r2_builder = opendal::services::S3::default()
                .bucket(&r2.bucket)
                .region("auto")
                .endpoint(&endpoint)
                .access_key_id(&r2.access_key_id)
                .secret_access_key(&r2.secret_access_key);

            opendal::Operator::new(r2_builder)
                .map_err(|e| {
                    crate::WriterError::write_failure(format!(
                        "Failed to create R2 operator: {}",
                        e
                    ))
                })?
                .finish()
        }
    };

    match OPERATOR.set(operator) {
        Ok(_) => {
            tracing::debug!("Storage operator initialized");
            Ok(())
        }
        Err(_) => {
            tracing::debug!("Storage operator already initialized by another call");
            Ok(())
        }
    }
}

/// Get the global storage operator
///
/// Returns None if operator has not been initialized via `initialize_storage`.
pub(crate) fn get_operator() -> Option<&'static opendal::Operator> {
    OPERATOR.get()
}

/// Get the configured storage prefix (e.g., "smoke-abc123/")
///
/// Returns None if no prefix is configured. The prefix always ends with "/" if present.
pub(crate) fn get_storage_prefix() -> Option<&'static str> {
    STORAGE_PREFIX
        .get()
        .and_then(|opt| opt.as_ref())
        .map(|s| s.as_str())
}

/// Clone the global storage operator for external users.
pub fn get_operator_clone() -> Option<opendal::Operator> {
    OPERATOR.get().cloned()
}
