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
use std::collections::HashMap;
use std::time::Duration;

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

    #[serde(default)]
    pub catalog_mode: CatalogMode,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server: Option<ServerConfig>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lambda: Option<LambdaConfig>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cloudflare: Option<CloudflareConfig>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iceberg: Option<IcebergConfig>,
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

/// Catalog mode for Iceberg integration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum CatalogMode {
    /// Use Iceberg catalog (R2 Data Catalog, S3 Tables, Nessie, etc.)
    #[default]
    Iceberg,
    /// Write plain Parquet files without catalog integration
    None,
}

impl std::fmt::Display for CatalogMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogMode::Iceberg => write!(f, "iceberg"),
            CatalogMode::None => write!(f, "none"),
        }
    }
}

impl std::str::FromStr for CatalogMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "iceberg" => Ok(CatalogMode::Iceberg),
            "none" => Ok(CatalogMode::None),
            _ => anyhow::bail!("Unsupported catalog mode: {}. Supported: iceberg, none", s),
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

/// Lambda-specific configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LambdaConfig {
    #[serde(default)]
    pub integrated_iceberg: bool,
}

/// Cloudflare Workers-specific configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CloudflareConfig {
    // Future cloudflare-specific config can go here
}

/// Apache Iceberg configuration (Lambda and Server only)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergConfig {
    /// REST catalog endpoint URI
    pub rest_uri: String,

    /// Warehouse location (optional, depends on catalog)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warehouse: Option<String>,

    /// Namespace for tables (dot-separated, e.g., "otel.production")
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,

    /// Catalog name (defaults to "rest")
    #[serde(default = "default_iceberg_catalog_name")]
    pub catalog_name: String,

    /// Staging prefix for data files
    #[serde(default = "default_iceberg_staging_prefix")]
    pub staging_prefix: String,

    /// Target file size in bytes
    #[serde(default = "default_iceberg_target_file_size")]
    pub target_file_size_bytes: u64,

    /// Iceberg format version
    #[serde(default = "default_iceberg_format_version")]
    pub format_version: i32,

    /// Custom table names per signal type
    #[serde(default)]
    pub tables: IcebergTableNames,

    /// Optional fully-qualified data location (e.g., s3://bucket/path)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_location: Option<String>,

    /// AWS S3 Tables bucket ARN (for Lambda with S3 Tables)
    /// Format: arn:aws:s3tables:region:account:bucket/bucket-name
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket_arn: Option<String>,
}

impl Default for IcebergConfig {
    fn default() -> Self {
        Self {
            rest_uri: String::new(),
            warehouse: None,
            namespace: None,
            catalog_name: default_iceberg_catalog_name(),
            staging_prefix: default_iceberg_staging_prefix(),
            target_file_size_bytes: default_iceberg_target_file_size(),
            format_version: default_iceberg_format_version(),
            tables: IcebergTableNames::default(),
            data_location: None,
            bucket_arn: None,
        }
    }
}

/// Iceberg table names configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergTableNames {
    #[serde(default = "default_table_logs")]
    pub logs: String,

    #[serde(default = "default_table_traces")]
    pub traces: String,

    #[serde(default = "default_table_metrics_gauge")]
    pub metrics_gauge: String,

    #[serde(default = "default_table_metrics_sum")]
    pub metrics_sum: String,

    #[serde(default = "default_table_metrics_histogram")]
    pub metrics_histogram: String,

    #[serde(default = "default_table_metrics_exponential_histogram")]
    pub metrics_exponential_histogram: String,

    #[serde(default = "default_table_metrics_summary")]
    pub metrics_summary: String,
}

impl Default for IcebergTableNames {
    fn default() -> Self {
        Self {
            logs: default_table_logs(),
            traces: default_table_traces(),
            metrics_gauge: default_table_metrics_gauge(),
            metrics_sum: default_table_metrics_sum(),
            metrics_histogram: default_table_metrics_histogram(),
            metrics_exponential_histogram: default_table_metrics_exponential_histogram(),
            metrics_summary: default_table_metrics_summary(),
        }
    }
}

fn default_iceberg_catalog_name() -> String {
    "rest".to_string()
}

fn default_iceberg_staging_prefix() -> String {
    "data/incoming".to_string()
}

fn default_iceberg_target_file_size() -> u64 {
    512 * 1024 * 1024 // 512 MB
}

fn default_iceberg_format_version() -> i32 {
    2
}

fn default_table_logs() -> String {
    "otel_logs".to_string()
}

fn default_table_traces() -> String {
    "otel_traces".to_string()
}

fn default_table_metrics_gauge() -> String {
    "otel_metrics_gauge".to_string()
}

fn default_table_metrics_sum() -> String {
    "otel_metrics_sum".to_string()
}

fn default_table_metrics_histogram() -> String {
    "otel_metrics_histogram".to_string()
}

fn default_table_metrics_exponential_histogram() -> String {
    "otel_metrics_exponential_histogram".to_string()
}

fn default_table_metrics_summary() -> String {
    "otel_metrics_summary".to_string()
}

impl IcebergConfig {
    /// Convert to the HashMap format expected by otlp2parquet-writer crate
    pub fn to_tables_map(&self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        map.insert("logs".to_string(), self.tables.logs.clone());
        map.insert("traces".to_string(), self.tables.traces.clone());
        map.insert(
            "metrics:gauge".to_string(),
            self.tables.metrics_gauge.clone(),
        );
        map.insert("metrics:sum".to_string(), self.tables.metrics_sum.clone());
        map.insert(
            "metrics:histogram".to_string(),
            self.tables.metrics_histogram.clone(),
        );
        map.insert(
            "metrics:exponential_histogram".to_string(),
            self.tables.metrics_exponential_histogram.clone(),
        );
        map.insert(
            "metrics:summary".to_string(),
            self.tables.metrics_summary.clone(),
        );
        map
    }

    /// Parse namespace string into `Vec<String>`
    pub fn namespace_vec(&self) -> Vec<String> {
        self.namespace
            .as_ref()
            .map(|ns| {
                ns.split('.')
                    .filter(|s| !s.trim().is_empty())
                    .map(|s| s.trim().to_string())
                    .collect()
            })
            .unwrap_or_default()
    }
}

impl RuntimeConfig {
    /// Namespace used for catalog operations (defaults to "otlp")
    pub fn catalog_namespace(&self) -> String {
        self.iceberg
            .as_ref()
            .and_then(|c| c.namespace.clone())
            .unwrap_or_else(|| "otlp".to_string())
    }
    /// Load configuration from all sources with priority
    #[cfg(not(target_arch = "wasm32"))]
    pub fn load() -> Result<Self> {
        let platform = Platform::detect();
        sources::load_config(platform)
    }

    /// `wasm32` (Cloudflare Workers) builds cannot touch host env or filesystem.
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
        self.catalog_mode = other.catalog_mode;

        if other.server.is_some() {
            self.server = other.server;
        }
        if other.lambda.is_some() {
            self.lambda = other.lambda;
        }
        if other.cloudflare.is_some() {
            self.cloudflare = other.cloudflare;
        }
        if other.iceberg.is_some() {
            self.iceberg = other.iceberg;
        }
    }

    /// Apply environment overrides from a custom source (e.g., Workers Env).
    pub fn apply_env_overrides_from<E: EnvSource>(&mut self, env: &E) -> Result<()> {
        env_overrides::apply_env_overrides(self, env)
    }

    /// Build a configuration for the given platform using inline config content
    /// plus overrides supplied by an `EnvSource`. Intended for runtimes that
    /// cannot read files or host environment variables (Cloudflare Workers).
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
        .expect("invalid default storage backend");

    let storage = match storage_backend {
        StorageBackend::Fs => StorageConfig {
            backend: StorageBackend::Fs,
            parquet_row_group_size: default_parquet_row_group_size(),
            fs: Some(FsConfig::default()),
            s3: None,
            r2: None,
        },
        StorageBackend::S3 => StorageConfig {
            backend: StorageBackend::S3,
            parquet_row_group_size: default_parquet_row_group_size(),
            fs: None,
            s3: Some(S3Config {
                bucket: "otlp-logs".to_string(),
                region: "us-east-1".to_string(),
                endpoint: None,
            }),
            r2: None,
        },
        StorageBackend::R2 => StorageConfig {
            backend: StorageBackend::R2,
            parquet_row_group_size: default_parquet_row_group_size(),
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
        catalog_mode: CatalogMode::default(),
        server: if platform == Platform::Server {
            Some(ServerConfig::default())
        } else {
            None
        },
        lambda: if platform == Platform::Lambda {
            Some(LambdaConfig::default())
        } else {
            None
        },
        cloudflare: if platform == Platform::CloudflareWorkers {
            Some(CloudflareConfig::default())
        } else {
            None
        },
        iceberg: None,
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
