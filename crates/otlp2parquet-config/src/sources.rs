// Configuration source loading
//
// Loads configuration from multiple sources with priority:
// 1. Environment variables (highest)
// 2. Config file from OTLP2PARQUET_CONFIG path
// 3. Inline config from OTLP2PARQUET_CONFIG_CONTENT
// 4. Default config files (./config.toml, ./.otlp2parquet.toml)
// 5. Platform defaults (lowest)

use crate::platform::Platform;
use crate::*;
use anyhow::{Context, Result};
use std::env;
use std::path::Path;

const ENV_PREFIX: &str = "OTLP2PARQUET_";

/// Load configuration from all sources
pub fn load_config(platform: Platform) -> Result<RuntimeConfig> {
    // Start with platform-specific defaults
    let mut config = platform_defaults(platform);

    // Try to load from config file (if available)
    if let Some(file_config) = load_from_file()? {
        merge_config(&mut config, file_config);
    }

    // Override with environment variables (highest priority)
    apply_env_overrides(&mut config, platform)?;

    // Validate final configuration
    config.validate()?;

    Ok(config)
}

/// Get platform-specific default configuration
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
                bucket: "otlp-logs".to_string(), // Lambda default
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
        iceberg: None, // Iceberg is optional, loaded from env vars or config file
    }
}

/// Load configuration from file
fn load_from_file() -> Result<Option<RuntimeConfig>> {
    // Check for explicit config file path
    if let Ok(path) = env::var("OTLP2PARQUET_CONFIG") {
        let content = std::fs::read_to_string(&path)
            .with_context(|| format!("Failed to read config file: {}", path))?;
        let config: RuntimeConfig = toml::from_str(&content)
            .with_context(|| format!("Failed to parse config file: {}", path))?;
        return Ok(Some(config));
    }

    // Check for inline config content
    if let Ok(content) = env::var("OTLP2PARQUET_CONFIG_CONTENT") {
        let config: RuntimeConfig = toml::from_str(&content)
            .context("Failed to parse inline config from OTLP2PARQUET_CONFIG_CONTENT")?;
        return Ok(Some(config));
    }

    // Try default config file locations
    for path in &["./config.toml", "./.otlp2parquet.toml"] {
        if Path::new(path).exists() {
            let content = std::fs::read_to_string(path)
                .with_context(|| format!("Failed to read config file: {}", path))?;
            let config: RuntimeConfig = toml::from_str(&content)
                .with_context(|| format!("Failed to parse config file: {}", path))?;
            return Ok(Some(config));
        }
    }

    Ok(None)
}

/// Merge file-based config into base config
fn merge_config(base: &mut RuntimeConfig, file: RuntimeConfig) {
    // Batch config
    base.batch = file.batch;

    // Request config
    base.request = file.request;

    // Storage config
    base.storage = file.storage;

    // Platform-specific configs
    if file.server.is_some() {
        base.server = file.server;
    }
    if file.lambda.is_some() {
        base.lambda = file.lambda;
    }
    if file.cloudflare.is_some() {
        base.cloudflare = file.cloudflare;
    }

    // Iceberg config
    if file.iceberg.is_some() {
        base.iceberg = file.iceberg;
    }
}

/// Apply environment variable overrides (highest priority)
fn apply_env_overrides(config: &mut RuntimeConfig, platform: Platform) -> Result<()> {
    // Batch configuration
    if let Some(val) = get_env_usize("BATCH_MAX_ROWS")? {
        config.batch.max_rows = val;
    }
    if let Some(val) = get_env_usize("BATCH_MAX_BYTES")? {
        config.batch.max_bytes = val;
    }
    if let Some(val) = get_env_u64("BATCH_MAX_AGE_SECS")? {
        config.batch.max_age_secs = val;
    }
    if let Some(val) = get_env_bool("BATCHING_ENABLED")? {
        config.batch.enabled = val;
    }

    // Request configuration
    if let Some(val) = get_env_usize("MAX_PAYLOAD_BYTES")? {
        config.request.max_payload_bytes = val;
    }

    // Storage backend
    if let Some(backend) = get_env_string("STORAGE_BACKEND")? {
        config.storage.backend = backend
            .parse::<StorageBackend>()
            .context("Invalid OTLP2PARQUET_STORAGE_BACKEND value")?;
    }
    if let Some(row_group_size) = get_env_usize("PARQUET_ROW_GROUP_SIZE")? {
        config.storage.parquet_row_group_size = row_group_size;
    }

    // Filesystem storage
    if let Some(path) = get_env_string("STORAGE_PATH")? {
        if config.storage.fs.is_none() {
            config.storage.fs = Some(FsConfig::default());
        }
        if let Some(ref mut fs) = config.storage.fs {
            fs.path = path;
        }
    }

    // S3 storage
    if let Some(bucket) = get_env_string("S3_BUCKET")? {
        if config.storage.s3.is_none() {
            config.storage.s3 = Some(S3Config {
                bucket: bucket.clone(),
                region: "us-east-1".to_string(),
                endpoint: None,
            });
        }
        if let Some(ref mut s3) = config.storage.s3 {
            s3.bucket = bucket;
        }
    }
    if let Some(region) = get_env_string("S3_REGION")? {
        if config.storage.s3.is_none() {
            config.storage.s3 = Some(S3Config {
                bucket: String::new(),
                region: region.clone(),
                endpoint: None,
            });
        }
        if let Some(ref mut s3) = config.storage.s3 {
            s3.region = region;
        }
    }
    if let Some(endpoint) = get_env_string("S3_ENDPOINT")? {
        if config.storage.s3.is_none() {
            config.storage.s3 = Some(S3Config {
                bucket: String::new(),
                region: String::new(),
                endpoint: Some(endpoint.clone()),
            });
        }
        if let Some(ref mut s3) = config.storage.s3 {
            s3.endpoint = Some(endpoint);
        }
    }

    // R2 storage
    if let Some(bucket) = get_env_string("R2_BUCKET")? {
        if config.storage.r2.is_none() {
            config.storage.r2 = Some(R2Config {
                bucket: bucket.clone(),
                account_id: String::new(),
                access_key_id: String::new(),
                secret_access_key: String::new(),
            });
        }
        if let Some(ref mut r2) = config.storage.r2 {
            r2.bucket = bucket;
        }
    }
    if let Some(account_id) = get_env_string("R2_ACCOUNT_ID")? {
        if config.storage.r2.is_none() {
            config.storage.r2 = Some(R2Config {
                bucket: String::new(),
                account_id: account_id.clone(),
                access_key_id: String::new(),
                secret_access_key: String::new(),
            });
        }
        if let Some(ref mut r2) = config.storage.r2 {
            r2.account_id = account_id;
        }
    }
    if let Some(access_key_id) = get_env_string("R2_ACCESS_KEY_ID")? {
        if config.storage.r2.is_none() {
            config.storage.r2 = Some(R2Config {
                bucket: String::new(),
                account_id: String::new(),
                access_key_id: access_key_id.clone(),
                secret_access_key: String::new(),
            });
        }
        if let Some(ref mut r2) = config.storage.r2 {
            r2.access_key_id = access_key_id;
        }
    }
    if let Some(secret_key) = get_env_string("R2_SECRET_ACCESS_KEY")? {
        if config.storage.r2.is_none() {
            config.storage.r2 = Some(R2Config {
                bucket: String::new(),
                account_id: String::new(),
                access_key_id: String::new(),
                secret_access_key: secret_key.clone(),
            });
        }
        if let Some(ref mut r2) = config.storage.r2 {
            r2.secret_access_key = secret_key;
        }
    }

    // Server-specific configuration
    if platform == Platform::Server {
        if config.server.is_none() {
            config.server = Some(ServerConfig::default());
        }

        if let Some(ref mut server) = config.server {
            if let Some(addr) = get_env_string("LISTEN_ADDR")? {
                server.listen_addr = addr;
            }
            if let Some(level) = get_env_string("LOG_LEVEL")? {
                server.log_level = level;
            }
            if let Some(format) = get_env_string("LOG_FORMAT")? {
                server.log_format = match format.to_lowercase().as_str() {
                    "json" => LogFormat::Json,
                    _ => LogFormat::Text,
                };
            }
        }
    }

    // Iceberg configuration (only for Lambda and Server)
    if platform == Platform::Lambda || platform == Platform::Server {
        // Check if any Iceberg env vars are set - if so, initialize config
        if let Some(rest_uri) = get_env_string("ICEBERG_REST_URI")? {
            let mut iceberg = config.iceberg.take().unwrap_or_else(|| IcebergConfig {
                rest_uri: rest_uri.clone(),
                warehouse: None,
                namespace: None,
                catalog_name: default_iceberg_catalog_name(),
                staging_prefix: default_iceberg_staging_prefix(),
                target_file_size_bytes: default_iceberg_target_file_size(),
                format_version: default_iceberg_format_version(),
                tables: IcebergTableNames::default(),
            });

            iceberg.rest_uri = rest_uri;

            if let Some(warehouse) = get_env_string("ICEBERG_WAREHOUSE")? {
                iceberg.warehouse = Some(warehouse);
            }
            if let Some(namespace) = get_env_string("ICEBERG_NAMESPACE")? {
                iceberg.namespace = Some(namespace);
            }
            if let Some(catalog_name) = get_env_string("ICEBERG_CATALOG_NAME")? {
                iceberg.catalog_name = catalog_name;
            }
            if let Some(staging_prefix) = get_env_string("ICEBERG_STAGING_PREFIX")? {
                iceberg.staging_prefix = staging_prefix;
            }
            if let Some(target_size) = get_env_u64("ICEBERG_TARGET_FILE_SIZE_BYTES")? {
                iceberg.target_file_size_bytes = target_size;
            }
            if let Some(format_version) = get_env_i32("ICEBERG_FORMAT_VERSION")? {
                iceberg.format_version = format_version;
            }

            // Table name overrides
            if let Some(logs) = get_env_string("ICEBERG_TABLE_LOGS")? {
                iceberg.tables.logs = logs;
            }
            if let Some(traces) = get_env_string("ICEBERG_TABLE_TRACES")? {
                iceberg.tables.traces = traces;
            }
            if let Some(gauge) = get_env_string("ICEBERG_TABLE_METRICS_GAUGE")? {
                iceberg.tables.metrics_gauge = gauge;
            }
            if let Some(sum) = get_env_string("ICEBERG_TABLE_METRICS_SUM")? {
                iceberg.tables.metrics_sum = sum;
            }
            if let Some(histogram) = get_env_string("ICEBERG_TABLE_METRICS_HISTOGRAM")? {
                iceberg.tables.metrics_histogram = histogram;
            }
            if let Some(exp_histogram) =
                get_env_string("ICEBERG_TABLE_METRICS_EXPONENTIAL_HISTOGRAM")?
            {
                iceberg.tables.metrics_exponential_histogram = exp_histogram;
            }
            if let Some(summary) = get_env_string("ICEBERG_TABLE_METRICS_SUMMARY")? {
                iceberg.tables.metrics_summary = summary;
            }

            config.iceberg = Some(iceberg);
        }
    }

    Ok(())
}

/// Helper: Get environment variable as string
fn get_env_string(key: &str) -> Result<Option<String>> {
    let full_key = format!("{}{}", ENV_PREFIX, key);
    match env::var(&full_key) {
        Ok(val) if !val.is_empty() => Ok(Some(val)),
        Ok(_) => Ok(None),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(e) => Err(e).with_context(|| format!("Failed to read env var {}", full_key)),
    }
}

/// Helper: Get environment variable as usize
fn get_env_usize(key: &str) -> Result<Option<usize>> {
    let full_key = format!("{}{}", ENV_PREFIX, key);
    match get_env_string(key)? {
        Some(val) => {
            let parsed = val
                .parse::<usize>()
                .with_context(|| format!("{} must be a valid number", full_key))?;
            Ok(Some(parsed))
        }
        None => Ok(None),
    }
}

/// Helper: Get environment variable as u64
fn get_env_u64(key: &str) -> Result<Option<u64>> {
    let full_key = format!("{}{}", ENV_PREFIX, key);
    match get_env_string(key)? {
        Some(val) => {
            let parsed = val
                .parse::<u64>()
                .with_context(|| format!("{} must be a valid number", full_key))?;
            Ok(Some(parsed))
        }
        None => Ok(None),
    }
}

/// Helper: Get environment variable as i32
fn get_env_i32(key: &str) -> Result<Option<i32>> {
    let full_key = format!("{}{}", ENV_PREFIX, key);
    match get_env_string(key)? {
        Some(val) => {
            let parsed = val
                .parse::<i32>()
                .with_context(|| format!("{} must be a valid number", full_key))?;
            Ok(Some(parsed))
        }
        None => Ok(None),
    }
}

/// Helper: Get environment variable as bool
fn get_env_bool(key: &str) -> Result<Option<bool>> {
    let full_key = format!("{}{}", ENV_PREFIX, key);
    match get_env_string(key)? {
        Some(val) => {
            let parsed = match val.to_lowercase().as_str() {
                "true" | "1" | "yes" | "on" => true,
                "false" | "0" | "no" | "off" => false,
                _ => anyhow::bail!("{} must be true or false", full_key),
            };
            Ok(Some(parsed))
        }
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_platform_defaults() {
        let server_config = platform_defaults(Platform::Server);
        assert_eq!(server_config.storage.backend, StorageBackend::Fs);
        assert!(server_config.server.is_some());

        let lambda_config = platform_defaults(Platform::Lambda);
        assert_eq!(lambda_config.storage.backend, StorageBackend::S3);
        assert!(lambda_config.lambda.is_some());

        let cf_config = platform_defaults(Platform::CloudflareWorkers);
        assert_eq!(cf_config.storage.backend, StorageBackend::R2);
        assert!(cf_config.cloudflare.is_some());
    }
}
