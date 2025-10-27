// Standalone runtime for local development and testing
//
// Uses local filesystem for storage

use anyhow::Result;
use otlp2parquet_core::Storage;
use std::path::PathBuf;

pub struct LocalStorage {
    base_path: PathBuf,
}

impl LocalStorage {
    pub fn new(base_path: PathBuf) -> Self {
        Self { base_path }
    }
}

#[async_trait::async_trait]
impl Storage for LocalStorage {
    async fn write(&self, path: &str, data: &[u8]) -> Result<()> {
        let full_path = self.base_path.join(path);

        // Create parent directories
        if let Some(parent) = full_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // Write file
        tokio::fs::write(&full_path, data).await?;

        Ok(())
    }

    async fn read(&self, path: &str) -> Result<Vec<u8>> {
        let full_path = self.base_path.join(path);
        let data = tokio::fs::read(&full_path).await?;
        Ok(data)
    }
}

/// Entry point for standalone mode
pub async fn run() -> Result<()> {
    println!("Running in standalone mode");
    // TODO: Start HTTP server for OTLP endpoint
    // TODO: Initialize LocalStorage
    Ok(())
}
