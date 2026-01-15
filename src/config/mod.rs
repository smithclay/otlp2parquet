// otlp2parquet-config - Unified configuration for all runtimes
//
// Supports configuration from multiple sources:
// 1. Environment variables (highest priority)
// 2. Config file path from OTLP2PARQUET_CONFIG env var
// 3. Config file contents from OTLP2PARQUET_CONFIG_CONTENT env var
// 4. Default config file locations (./config.toml, ./.otlp2parquet.toml)
// 5. Platform-specific defaults (lowest priority)

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

mod env_overrides;
mod platform;
#[cfg(not(target_arch = "wasm32"))]
mod sources;
mod validation;

pub use env_overrides::{EnvSource, ENV_PREFIX};
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

impl BatchConfig {}

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

impl std::str::FromStr for StorageBackend {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "fs" | "filesystem" => Ok(StorageBackend::Fs),
            "s3" | "aws" => Ok(StorageBackend::S3),
            "r2" => Ok(StorageBackend::R2),
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
    /// Optional path prefix for all stored files (e.g., "smoke-abc123/")
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct R2Config {
    pub bucket: String,
    pub account_id: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub endpoint: Option<String>,
    /// Optional path prefix for all stored files (e.g., "smoke-abc123/")
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
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

impl RuntimeConfig {
    /// Load configuration from all sources with priority
    #[cfg(not(target_arch = "wasm32"))]
    pub fn load() -> Result<Self> {
        let platform = Platform::detect();
        sources::load_config(platform)
    }

    /// `wasm32` builds cannot touch host env or filesystem.
    /// Return platform defaults so the runtime can layer worker-specific overrides.
    #[cfg(target_arch = "wasm32")]
    pub fn load() -> Result<Self> {
        Ok(RuntimeConfig::from_platform_defaults(Platform::detect()))
    }

    /// Load configuration for a specific platform (useful for testing)
    #[cfg(not(target_arch = "wasm32"))]
    pub fn load_for_platform(platform: Platform) -> Result<Self> {
        sources::load_config(platform)
    }

    /// Load configuration from a specific file path (for CLI usage).
    #[cfg(not(target_arch = "wasm32"))]
    pub fn load_from_path(path: impl AsRef<std::path::Path>) -> Result<Self> {
        sources::load_from_file_path(path)
    }

    /// Load configuration with graceful fallback to defaults.
    /// Does not fail if config file is missing - uses platform defaults instead.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn load_or_default() -> Result<Self> {
        sources::load_or_default(Platform::detect())
    }

    /// Construct a config that contains only platform defaults (no env or files).
    pub fn from_platform_defaults(platform: Platform) -> Self {
        platform_defaults(platform)
    }

    /// Merge another config into this one (used for TOML layering).
    pub fn merge(&mut self, other: RuntimeConfig) {
        self.batch = other.batch;
        self.request = other.request;
        self.storage = other.storage;

        if other.server.is_some() {
            self.server = other.server;
        }
    }

    /// Apply environment overrides from a custom source (e.g., WASM env).
    pub fn apply_env_overrides_from<E: EnvSource>(&mut self, env: &E) -> Result<()> {
        env_overrides::apply_env_overrides(self, env)
    }

    /// Build a configuration for the given platform using inline config content
    /// plus overrides supplied by an `EnvSource`. Intended for runtimes that
    /// cannot read files or host environment variables.
    pub fn load_for_platform_with_env<E: EnvSource>(
        platform: Platform,
        inline_config: Option<&str>,
        env: &E,
    ) -> Result<Self> {
        let mut config = RuntimeConfig::from_platform_defaults(platform);

        if let Some(inline) = inline_config {
            let file_config: RuntimeConfig =
                toml::from_str(inline).context("Failed to parse inline config content")?;
            config.merge(file_config);
        }

        config.apply_env_overrides_from(env)?;
        config.validate()?;
        Ok(config)
    }

    /// Convenience helper for WASM targets to load configuration with a custom env source.
    #[cfg(target_arch = "wasm32")]
    pub fn load_with_env_source<E: EnvSource>(
        env: &E,
        inline_config: Option<&str>,
    ) -> Result<Self> {
        Self::load_for_platform_with_env(Platform::detect(), inline_config, env)
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<()> {
        validation::validate_config(self)
    }
}

fn platform_defaults(platform: Platform) -> RuntimeConfig {
    let defaults = platform.defaults();

    let storage_backend = defaults
        .storage_backend
        .parse::<StorageBackend>()
        .unwrap_or(StorageBackend::Fs);

    let storage = match storage_backend {
        StorageBackend::Fs => StorageConfig {
            backend: StorageBackend::Fs,
            fs: Some(FsConfig::default()),
            s3: None,
            r2: None,
        },
        StorageBackend::S3 => StorageConfig {
            backend: StorageBackend::S3,
            fs: None,
            s3: Some(S3Config {
                bucket: "otlp-logs".to_string(),
                region: "us-east-1".to_string(),
                endpoint: None,
                prefix: None,
            }),
            r2: None,
        },
        StorageBackend::R2 => StorageConfig {
            backend: StorageBackend::R2,
            fs: None,
            s3: None,
            r2: Some(R2Config {
                bucket: String::new(),
                account_id: String::new(),
                access_key_id: String::new(),
                secret_access_key: String::new(),
                endpoint: None,
                prefix: None,
            }),
        },
    };

    RuntimeConfig {
        batch: BatchConfig {
            max_rows: defaults.batch_max_rows,
            max_bytes: defaults.batch_max_bytes,
            max_age_secs: defaults.batch_max_age_secs,
            enabled: true,
        },
        request: RequestConfig {
            max_payload_bytes: defaults.max_payload_bytes,
        },
        storage,
        server: Some(ServerConfig::default()),
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
