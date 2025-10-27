// Storage trait for writing Parquet files to various backends
//
// Implementations:
// - R2Storage (Cloudflare Workers)
// - S3Storage (AWS Lambda)
// - LocalStorage (standalone/testing)

use anyhow::Result;
use async_trait::async_trait;

/// Storage abstraction for writing Parquet files
///
/// Note: For WASM/Cloudflare Workers, Send + Sync are not required
/// due to single-threaded JS runtime
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait Storage {
    /// Write data to the storage backend at the specified path
    async fn write(&self, path: &str, data: &[u8]) -> Result<()>;

    /// Optional: Read data from storage (for testing/verification)
    async fn read(&self, _path: &str) -> Result<Vec<u8>> {
        Err(anyhow::anyhow!(
            "Read not implemented for this storage backend"
        ))
    }

    /// Optional: List objects with a prefix
    async fn list(&self, _prefix: &str) -> Result<Vec<String>> {
        Err(anyhow::anyhow!(
            "List not implemented for this storage backend"
        ))
    }
}
