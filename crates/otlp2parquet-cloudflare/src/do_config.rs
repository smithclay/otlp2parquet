//! Configuration loading for Cloudflare Workers.
//!
//! This module provides the environment variable accessor for Cloudflare Workers.

use otlp2parquet_common::config::{EnvSource, ENV_PREFIX};
use worker::Env;

/// Environment source for Cloudflare Workers.
///
/// Implements the `EnvSource` trait to provide configuration values from
/// Cloudflare Workers environment variables and secrets. Used by both
/// the main Worker fetch handler and Durable Objects.
pub(crate) struct WorkerEnvSource<'a> {
    pub env: &'a Env,
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
