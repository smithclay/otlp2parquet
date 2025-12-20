use super::{
    CatalogMode, FsConfig, LambdaConfig, LogFormat, R2Config, RuntimeConfig, S3Config,
    ServerConfig, StorageBackend,
};
use anyhow::{anyhow, Context, Result};

pub const ENV_PREFIX: &str = "OTLP2PARQUET_";

/// Abstraction over environment-variable lookups so runtimes without `std::env`
/// (e.g., Cloudflare Workers) can supply their own source of overrides.
pub trait EnvSource {
    fn get(&self, key: &str) -> Option<String>;

    /// Get an environment variable WITHOUT the OTLP2PARQUET_ prefix
    /// Used for AWS standard variables (AWS_ACCESS_KEY_ID, etc.)
    fn get_raw(&self, key: &str) -> Option<String>;
}

/// Apply environment-variable overrides (highest priority) to the runtime config.
pub fn apply_env_overrides<E: EnvSource>(config: &mut RuntimeConfig, env: &E) -> Result<()> {
    // Batch configuration
    if let Some(val) = get_env_usize(env, "BATCH_MAX_ROWS")? {
        config.batch.max_rows = val;
    }

    // Server configuration (listen addr, log level/format)
    if let Some(addr) = get_env_string(env, "LISTEN_ADDR")? {
        ensure_server(config).listen_addr = addr;
    }
    if let Some(level) = get_env_string(env, "LOG_LEVEL")? {
        ensure_server(config).log_level = level;
    }
    if let Some(format) = get_env_string(env, "LOG_FORMAT")? {
        let parsed = match format.to_lowercase().as_str() {
            "json" => LogFormat::Json,
            _ => LogFormat::Text,
        };
        ensure_server(config).log_format = parsed;
    }

    // Lambda-specific toggles
    if let Some(val) = get_env_bool(env, "LAMBDA_INTEGRATED_ICEBERG")? {
        ensure_lambda(config).integrated_iceberg = val;
    }
    if let Some(val) = get_env_usize(env, "BATCH_MAX_BYTES")? {
        config.batch.max_bytes = val;
    }
    if let Some(val) = get_env_u64(env, "BATCH_MAX_AGE_SECS")? {
        config.batch.max_age_secs = val;
    }
    // Support both BATCH_ENABLED (canonical) and BATCHING_ENABLED (legacy)
    if let Some(val) = get_env_bool(env, "BATCH_ENABLED")? {
        config.batch.enabled = val;
    } else if let Some(val) = get_env_bool(env, "BATCHING_ENABLED")? {
        config.batch.enabled = val;
    }

    // Request configuration
    if let Some(val) = get_env_usize(env, "MAX_PAYLOAD_BYTES")? {
        config.request.max_payload_bytes = val;
    }

    // Storage backend
    if let Some(backend) = get_env_string(env, "STORAGE_BACKEND")? {
        config.storage.backend = backend
            .parse::<StorageBackend>()
            .context("Invalid OTLP2PARQUET_STORAGE_BACKEND value")?;
    }
    if let Some(row_group_size) = get_env_usize(env, "PARQUET_ROW_GROUP_SIZE")? {
        config.storage.parquet_row_group_size = row_group_size;
    }

    // Catalog mode
    if let Some(mode) = get_env_string(env, "CATALOG_MODE")? {
        config.catalog_mode = mode
            .parse::<CatalogMode>()
            .context("Invalid OTLP2PARQUET_CATALOG_MODE value")?;
    }

    // Filesystem storage
    if let Some(path) = get_env_string(env, "STORAGE_PATH")? {
        if config.storage.fs.is_none() {
            config.storage.fs = Some(FsConfig::default());
        }
        if let Some(ref mut fs) = config.storage.fs {
            fs.path = path;
        }
    }

    // S3 storage
    if let Some(bucket) = get_env_string(env, "S3_BUCKET")? {
        ensure_s3(config).bucket = bucket;
    }
    if let Some(region) = get_env_string(env, "S3_REGION")? {
        ensure_s3(config).region = region;
    }
    if let Some(endpoint) = get_env_string(env, "S3_ENDPOINT")? {
        ensure_s3(config).endpoint = Some(endpoint);
    }

    // R2 storage
    if let Some(bucket) = get_env_string(env, "R2_BUCKET")? {
        ensure_r2(config).bucket = bucket;
    }
    if let Some(account_id) = get_env_string(env, "R2_ACCOUNT_ID")? {
        ensure_r2(config).account_id = account_id;
    }
    // AWS standard credentials (without OTLP2PARQUET_ prefix for compatibility)
    if let Some(access_key_id) = get_raw_env_string(env, "AWS_ACCESS_KEY_ID")? {
        ensure_r2(config).access_key_id = access_key_id;
    }
    if let Some(secret_access_key) = get_raw_env_string(env, "AWS_SECRET_ACCESS_KEY")? {
        ensure_r2(config).secret_access_key = secret_access_key;
    }
    if let Some(endpoint) = get_raw_env_string(env, "AWS_ENDPOINT_URL")? {
        ensure_r2(config).endpoint = Some(endpoint);
    }
    if let Some(prefix) = get_env_string(env, "R2_PREFIX")? {
        // Normalize prefix: ensure it ends with "/" if non-empty
        let normalized = if prefix.is_empty() {
            None
        } else if prefix.ends_with('/') {
            Some(prefix)
        } else {
            Some(format!("{}/", prefix))
        };
        ensure_r2(config).prefix = normalized;
    }

    // Note: AWS_REGION should be set directly in wrangler.toml [vars] for Cloudflare Workers
    // WASM cannot use std::env::set_var, so OpenDAL reads it from worker::Env via get_raw_env_string above

    // Iceberg configuration
    let iceberg_keys = &[
        "ICEBERG_REST_URI",
        "ICEBERG_NAMESPACE",
        "ICEBERG_WAREHOUSE",
        "ICEBERG_BUCKET_ARN",
    ];
    let has_iceberg_env = has_any(env, iceberg_keys);

    // Debug: log each key check individually
    for key in iceberg_keys {
        let val = env.get(key);
        tracing::info!(
            key = %key,
            has_value = val.is_some(),
            value = ?val,
            "Checking iceberg env var"
        );
    }

    tracing::info!(
        has_iceberg_env = has_iceberg_env,
        iceberg_is_none = config.iceberg.is_none(),
        "Checking for iceberg env vars"
    );

    if config.iceberg.is_none() && has_iceberg_env {
        tracing::info!("Creating default iceberg config from env vars");
        config.iceberg = Some(Default::default());
    }

    if let Some(ref mut iceberg) = config.iceberg {
        if let Some(bucket_arn) = get_env_string(env, "ICEBERG_BUCKET_ARN")? {
            iceberg.bucket_arn = Some(bucket_arn);
        }
        if let Some(rest_uri) = get_env_string(env, "ICEBERG_REST_URI")? {
            iceberg.rest_uri = rest_uri;
        }
        if let Some(warehouse) = get_env_string(env, "ICEBERG_WAREHOUSE")? {
            iceberg.warehouse = Some(warehouse);
        }
        if let Some(namespace) = get_env_string(env, "ICEBERG_NAMESPACE")? {
            tracing::info!(namespace = %namespace, "Setting iceberg namespace from env");
            iceberg.namespace = Some(namespace);
        }
        if let Some(catalog_name) = get_env_string(env, "ICEBERG_CATALOG_NAME")? {
            iceberg.catalog_name = catalog_name;
        }
        if let Some(staging_prefix) = get_env_string(env, "ICEBERG_STAGING_PREFIX")? {
            iceberg.staging_prefix = staging_prefix;
        }
        if let Some(target_file_size) = get_env_u64(env, "ICEBERG_TARGET_FILE_SIZE_BYTES")? {
            iceberg.target_file_size_bytes = target_file_size;
        }
        if let Some(format_version) = get_env_i32(env, "ICEBERG_FORMAT_VERSION")? {
            iceberg.format_version = format_version;
        }
        if let Some(data_location) = get_env_string(env, "ICEBERG_DATA_LOCATION")? {
            iceberg.data_location = Some(data_location);
        }

        if let Some(table_logs) = get_env_string(env, "ICEBERG_TABLE_LOGS")? {
            iceberg.tables.logs = table_logs;
        }
        if let Some(table_traces) = get_env_string(env, "ICEBERG_TABLE_TRACES")? {
            iceberg.tables.traces = table_traces;
        }
        if let Some(table_gauge) = get_env_string(env, "ICEBERG_TABLE_METRICS_GAUGE")? {
            iceberg.tables.metrics_gauge = table_gauge;
        }
        if let Some(table_sum) = get_env_string(env, "ICEBERG_TABLE_METRICS_SUM")? {
            iceberg.tables.metrics_sum = table_sum;
        }
        if let Some(table_hist) = get_env_string(env, "ICEBERG_TABLE_METRICS_HISTOGRAM")? {
            iceberg.tables.metrics_histogram = table_hist;
        }
        if let Some(table_exp) = get_env_string(env, "ICEBERG_TABLE_METRICS_EXPONENTIAL_HISTOGRAM")?
        {
            iceberg.tables.metrics_exponential_histogram = table_exp;
        }
        if let Some(table_summary) = get_env_string(env, "ICEBERG_TABLE_METRICS_SUMMARY")? {
            iceberg.tables.metrics_summary = table_summary;
        }
    }

    Ok(())
}

fn ensure_s3(config: &mut RuntimeConfig) -> &mut S3Config {
    config.storage.s3.get_or_insert_with(|| S3Config {
        bucket: String::new(),
        region: String::new(),
        endpoint: None,
    })
}

fn ensure_r2(config: &mut RuntimeConfig) -> &mut R2Config {
    config.storage.r2.get_or_insert_with(|| R2Config {
        bucket: String::new(),
        account_id: String::new(),
        access_key_id: String::new(),
        secret_access_key: String::new(),
        endpoint: None,
        prefix: None,
    })
}

fn ensure_server(config: &mut RuntimeConfig) -> &mut ServerConfig {
    config.server.get_or_insert_with(ServerConfig::default)
}

fn ensure_lambda(config: &mut RuntimeConfig) -> &mut LambdaConfig {
    config.lambda.get_or_insert_with(LambdaConfig::default)
}

fn get_env_string<E: EnvSource>(env: &E, key: &str) -> Result<Option<String>> {
    Ok(env.get(key))
}

/// Get a raw environment variable without the OTLP2PARQUET_ prefix
/// Used for AWS standard variables like AWS_ACCESS_KEY_ID
fn get_raw_env_string<E: EnvSource>(env: &E, key: &str) -> Result<Option<String>> {
    Ok(env.get_raw(key))
}

fn get_env_usize<E: EnvSource>(env: &E, key: &str) -> Result<Option<usize>> {
    match get_env_string(env, key)? {
        Some(val) => {
            let parsed = val
                .parse::<usize>()
                .map_err(|e| anyhow!("Failed to parse {}{}: {}", ENV_PREFIX, key, e))?;
            Ok(Some(parsed))
        }
        None => Ok(None),
    }
}

fn get_env_u64<E: EnvSource>(env: &E, key: &str) -> Result<Option<u64>> {
    match get_env_string(env, key)? {
        Some(val) => {
            let parsed = val
                .parse::<u64>()
                .map_err(|e| anyhow!("Failed to parse {}{}: {}", ENV_PREFIX, key, e))?;
            Ok(Some(parsed))
        }
        None => Ok(None),
    }
}

fn get_env_i32<E: EnvSource>(env: &E, key: &str) -> Result<Option<i32>> {
    match get_env_string(env, key)? {
        Some(val) => {
            let parsed = val
                .parse::<i32>()
                .map_err(|e| anyhow!("Failed to parse {}{}: {}", ENV_PREFIX, key, e))?;
            Ok(Some(parsed))
        }
        None => Ok(None),
    }
}

fn get_env_bool<E: EnvSource>(env: &E, key: &str) -> Result<Option<bool>> {
    match get_env_string(env, key)? {
        Some(val) => {
            let parsed = val.parse::<bool>().map_err(|e| {
                anyhow!(
                    "Failed to parse {}{} (expected bool): {}",
                    ENV_PREFIX,
                    key,
                    e
                )
            })?;
            Ok(Some(parsed))
        }
        None => Ok(None),
    }
}

fn has_any<E: EnvSource>(env: &E, keys: &[&str]) -> bool {
    keys.iter().any(|key| env.get(key).is_some())
}
