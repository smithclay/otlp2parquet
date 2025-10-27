// Standalone runtime for local development and testing
//
// Uses blocking I/O and local filesystem
//
// Philosophy: Simple, single-threaded, blocking I/O
// No tokio needed - just std::fs and std::net

use anyhow::Result;
use std::fs;
use std::path::PathBuf;

pub struct LocalStorage {
    base_path: PathBuf,
}

impl LocalStorage {
    pub fn new(base_path: PathBuf) -> Self {
        Self { base_path }
    }

    /// Write data to local filesystem (blocking)
    pub fn write(&self, path: &str, data: &[u8]) -> Result<()> {
        let full_path = self.base_path.join(path);

        // Create parent directories
        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Write file (blocking)
        fs::write(&full_path, data)?;

        Ok(())
    }

    /// Read data from local filesystem (blocking)
    pub fn read(&self, path: &str) -> Result<Vec<u8>> {
        let full_path = self.base_path.join(path);
        let data = fs::read(&full_path)?;
        Ok(data)
    }
}

/// Entry point for standalone mode
pub fn run() -> Result<()> {
    println!("Running in standalone mode");
    println!("Using blocking I/O - no tokio, simple and direct");

    // TODO: Start simple HTTP server for OTLP endpoint
    // Could use tiny_http or just std::net::TcpListener
    // TODO: Initialize LocalStorage

    println!("Standalone server not yet implemented");
    println!("This will be a simple blocking HTTP server");

    Ok(())
}
