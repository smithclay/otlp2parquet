// Storage trait for writing Parquet files to various backends
//
// Implementations:
// - R2Storage (Cloudflare Workers)
// - S3Storage (AWS Lambda)
// - LocalStorage (standalone/testing)

use anyhow::Result;
use async_trait::async_trait;

/// Storage abstraction for writing Parquet files
#[async_trait]
pub trait Storage: Send + Sync {
    /// Write data to the storage backend at the specified path
    async fn write(&self, path: &str, data: &[u8]) -> Result<()>;

    /// Optional: Read data from storage (for testing/verification)
    async fn read(&self, path: &str) -> Result<Vec<u8>> {
        Err(anyhow::anyhow!("Read not implemented for this storage backend"))
    }

    /// Optional: List objects with a prefix
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        Err(anyhow::anyhow!("List not implemented for this storage backend"))
    }
}
