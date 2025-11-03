// otlp2parquet-config - Unified configuration for all runtimes
//
// Supports configuration from multiple sources:
// 1. Environment variables (highest priority)
// 2. Config file path from OTLP2PARQUET_CONFIG env var
// 3. Config file contents from OTLP2PARQUET_CONFIG_CONTENT env var
// 4. Default config file locations (./config.toml, ./.otlp2parquet.toml)
// 5. Platform-specific defaults (lowest priority)

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::time::Duration;

mod platform;
mod sources;
mod validation;

pub use platform::Platform;

/// Main runtime configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeConfig {
    #[serde(default)]
    pub batch: BatchConfig,

    #[serde(default)]
    pub request: RequestConfig,

    pub storage: StorageConfig,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server: Option<ServerConfig>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lambda: Option<LambdaConfig>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cloudflare: Option<CloudflareConfig>,
}

/// Batch configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchConfig {
    pub max_rows: usize,
    pub max_bytes: usize,
    pub max_age_secs: u64,
    #[serde(default = "default_batching_enabled")]
    pub enabled: bool,
}

fn default_batching_enabled() -> bool {
    true
}

impl BatchConfig {
    pub fn max_age(&self) -> Duration {
        Duration::from_secs(self.max_age_secs)
    }
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_rows: 200_000,
            max_bytes: 128 * 1024 * 1024,
            max_age_secs: 10,
            enabled: true,
        }
    }
}

/// Request handling configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestConfig {
    pub max_payload_bytes: usize,
}

impl Default for RequestConfig {
    fn default() -> Self {
        Self {
            max_payload_bytes: 8 * 1024 * 1024,
        }
    }
}

/// Storage backend configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub backend: StorageBackend,
    #[serde(default = "default_parquet_row_group_size")]
    pub parquet_row_group_size: usize,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fs: Option<FsConfig>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub s3: Option<S3Config>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub r2: Option<R2Config>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageBackend {
    Fs,
    S3,
    R2,
}

impl std::fmt::Display for StorageBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageBackend::Fs => write!(f, "fs"),
            StorageBackend::S3 => write!(f, "s3"),
            StorageBackend::R2 => write!(f, "r2"),
        }
    }
}

fn default_parquet_row_group_size() -> usize {
    32 * 1024
}

impl std::str::FromStr for StorageBackend {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "fs" | "filesystem" => Ok(StorageBackend::Fs),
            "s3" | "aws" => Ok(StorageBackend::S3),
            "r2" | "cloudflare" => Ok(StorageBackend::R2),
            _ => anyhow::bail!("Unsupported storage backend: {}. Supported: fs, s3, r2", s),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsConfig {
    pub path: String,
}

impl Default for FsConfig {
    fn default() -> Self {
        Self {
            path: "./data".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct R2Config {
    pub bucket: String,
    pub account_id: String,
    pub access_key_id: String,
    pub secret_access_key: String,
}

/// Server-specific configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub listen_addr: String,
    pub log_level: String,
    pub log_format: LogFormat,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:4318".to_string(),
            log_level: "info".to_string(),
            log_format: LogFormat::Text,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    Text,
    Json,
}

/// Lambda-specific configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LambdaConfig {
    // Future lambda-specific config can go here
}

/// Cloudflare Workers-specific configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CloudflareConfig {
    // Future cloudflare-specific config can go here
}

impl RuntimeConfig {
    /// Load configuration from all sources with priority
    pub fn load() -> Result<Self> {
        let platform = Platform::detect();
        sources::load_config(platform)
    }

    /// Load configuration for a specific platform (useful for testing)
    pub fn load_for_platform(platform: Platform) -> Result<Self> {
        sources::load_config(platform)
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<()> {
        validation::validate_config(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_backend_from_str() {
        assert_eq!("fs".parse::<StorageBackend>().unwrap(), StorageBackend::Fs);
        assert_eq!("s3".parse::<StorageBackend>().unwrap(), StorageBackend::S3);
        assert_eq!("r2".parse::<StorageBackend>().unwrap(), StorageBackend::R2);
        assert_eq!(
            "filesystem".parse::<StorageBackend>().unwrap(),
            StorageBackend::Fs
        );
        assert_eq!("aws".parse::<StorageBackend>().unwrap(), StorageBackend::S3);
        assert_eq!(
            "cloudflare".parse::<StorageBackend>().unwrap(),
            StorageBackend::R2
        );
    }

    #[test]
    fn test_default_configs() {
        let batch = BatchConfig::default();
        assert_eq!(batch.max_rows, 200_000);
        assert!(batch.enabled);

        let server = ServerConfig::default();
        assert_eq!(server.listen_addr, "0.0.0.0:4318");
        assert_eq!(server.log_format, LogFormat::Text);
    }
}
