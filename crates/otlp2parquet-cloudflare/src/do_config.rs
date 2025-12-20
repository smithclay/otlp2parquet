//! Configuration loading for Cloudflare Workers.
//!
//! This module provides the environment variable accessor and storage initialization
//! logic used by both the main Worker and Durable Objects.

use otlp2parquet_core::config::{EnvSource, Platform, RuntimeConfig, ENV_PREFIX};
use otlp2parquet_writer::set_table_name_overrides;
use worker::{Env, Result};

/// Environment source for Cloudflare Workers.
///
/// Implements the `EnvSource` trait to provide configuration values from
/// Cloudflare Workers environment variables and secrets. Used by both
/// the main Worker fetch handler and Durable Objects.
pub(crate) struct WorkerEnvSource<'a> {
    pub env: &'a Env,
}

/// Ensure the Iceberg namespace is populated from env even if config parsing missed it.
/// This guards against cases where OTLP2PARQUET_ICEBERG_NAMESPACE is set but the
/// config didn't materialize an Iceberg block (e.g., missing other Iceberg envs).
pub(crate) fn apply_namespace_fallback(env: &Env, config: &mut RuntimeConfig) {
    // If already set, nothing to do.
    if config
        .iceberg
        .as_ref()
        .and_then(|c| c.namespace.as_ref())
        .is_some()
    {
        return;
    }

    let provider = WorkerEnvSource { env };
    if let Some(ns) = provider.get("ICEBERG_NAMESPACE") {
        tracing::info!(
            namespace = %ns,
            "Applying Iceberg namespace from env fallback"
        );
        if config.iceberg.is_none() {
            config.iceberg = Some(Default::default());
        }
        if let Some(ref mut iceberg) = config.iceberg {
            iceberg.namespace = Some(ns);
        }
    }
}

/// Check if a key is likely to be a sensitive value that might be stored as a secret.
fn is_sensitive_key(key: &str) -> bool {
    key.contains("SECRET")
        || key.contains("TOKEN")
        || key.contains("PASSWORD")
        || key.contains("KEY")
        || key.contains("CREDENTIAL")
}

impl EnvSource for WorkerEnvSource<'_> {
    fn get(&self, key: &str) -> Option<String> {
        let binding = format!("{}{}", ENV_PREFIX, key);

        // Try var first, then secret for sensitive keys
        if let Ok(val) = self.env.var(&binding) {
            tracing::trace!(binding = %binding, "EnvSource: found var");
            return Some(val.to_string());
        }
        if is_sensitive_key(key) {
            if let Ok(secret) = self.env.secret(&binding) {
                tracing::trace!(binding = %binding, "EnvSource: found secret");
                return Some(secret.to_string());
            }
        }
        tracing::trace!(binding = %binding, "EnvSource: not set");
        None
    }

    fn get_raw(&self, key: &str) -> Option<String> {
        // Get variable without prefix (for AWS_* vars, etc.)
        match self.env.var(key) {
            Ok(val) => {
                let value = val.to_string();
                tracing::info!(key = %key, len = value.len(), "EnvSource (raw): found var");
                return Some(value);
            }
            Err(e) => {
                tracing::debug!(key = %key, error = %e, "EnvSource (raw): var not found, checking secret");
            }
        }
        // Check secrets for sensitive keys (TOKEN, SECRET, KEY, PASSWORD, CREDENTIAL)
        let is_sensitive = is_sensitive_key(key);
        tracing::info!(key = %key, is_sensitive = %is_sensitive, "EnvSource (raw): checking if sensitive");
        if is_sensitive {
            match self.env.secret(key) {
                Ok(secret) => {
                    let value = secret.to_string();
                    tracing::info!(key = %key, len = value.len(), "EnvSource (raw): found secret");
                    return Some(value);
                }
                Err(e) => {
                    tracing::error!(key = %key, error = %e, "EnvSource (raw): secret lookup FAILED");
                }
            }
        }
        tracing::warn!(key = %key, "EnvSource (raw): not found anywhere");
        None
    }
}

/// Load config and initialize storage for the Durable Object context.
///
/// This must be called before any write operations since DOs run in their own isolate.
/// Returns the RuntimeConfig for use in catalog initialization or other configuration needs.
///
/// # Errors
///
/// Returns an error if configuration loading or storage initialization fails.
pub(crate) fn ensure_storage_initialized(env: &Env) -> Result<RuntimeConfig> {
    let provider = WorkerEnvSource { env };
    let inline = provider.get("CONFIG_CONTENT");

    let config = RuntimeConfig::load_for_platform_with_env(
        Platform::CloudflareWorkers,
        inline.as_deref(),
        &provider,
    )
    .map_err(|e| worker::Error::RustError(format!("DO config error: {}", e)))?;

    // Ensure namespace is propagated even if Iceberg envs were partially present
    let mut config = config;
    apply_namespace_fallback(env, &mut config);

    tracing::info!(
        has_iceberg = config.iceberg.is_some(),
        namespace = %config.catalog_namespace(),
        "Config loaded in DO context"
    );

    if let Some(iceberg) = config.iceberg.as_ref() {
        tracing::info!(
            iceberg_namespace = ?iceberg.namespace,
            "Iceberg config present"
        );
        set_table_name_overrides(iceberg.to_tables_map());
    }

    otlp2parquet_writer::initialize_storage(&config)
        .map_err(|e| worker::Error::RustError(format!("DO storage init error: {}", e)))?;

    Ok(config)
}
