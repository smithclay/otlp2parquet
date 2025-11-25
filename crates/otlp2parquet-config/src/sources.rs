// Configuration source loading for native targets (non-WASM).
//
// Priority order:
// 1. Environment variables (OTLP2PARQUET_* prefix)
// 2. Config file path from OTLP2PARQUET_CONFIG
// 3. Inline config content from OTLP2PARQUET_CONFIG_CONTENT
// 4. Default config files (./config.toml, ./.otlp2parquet.toml)
// 5. Platform defaults (based on auto-detected Platform)

use crate::env_overrides::{self, EnvSource, ENV_PREFIX};
use crate::platform::Platform;
use crate::*;
use anyhow::{Context, Result};
use std::env;
use std::path::Path;

/// Load configuration for the detected platform using native environment/file access.
pub fn load_config(platform: Platform) -> Result<RuntimeConfig> {
    let mut config = RuntimeConfig::from_platform_defaults(platform);

    if let Some(file_config) = load_from_file()? {
        config.merge(file_config);
    }

    let env_source = StdEnvSource;
    env_overrides::apply_env_overrides(&mut config, &env_source)?;
    config.validate()?;
    Ok(config)
}

fn load_from_file() -> Result<Option<RuntimeConfig>> {
    if let Ok(path) = env::var("OTLP2PARQUET_CONFIG") {
        let content = std::fs::read_to_string(&path)
            .with_context(|| format!("Failed to read config file: {}", path))?;
        let config: RuntimeConfig = toml::from_str(&content)
            .with_context(|| format!("Failed to parse config file: {}", path))?;
        return Ok(Some(config));
    }

    if let Ok(content) = env::var("OTLP2PARQUET_CONFIG_CONTENT") {
        let config: RuntimeConfig = toml::from_str(&content)
            .context("Failed to parse inline config from OTLP2PARQUET_CONFIG_CONTENT")?;
        return Ok(Some(config));
    }

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

/// Load configuration from a specific file path (for CLI --config flag).
/// Returns error if file doesn't exist or can't be parsed.
/// Unlike load_config(), this starts with the file content and then applies
/// platform defaults and environment overrides.
pub fn load_from_file_path(path: impl AsRef<Path>) -> Result<RuntimeConfig> {
    let path = path.as_ref();
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read config file: {}", path.display()))?;
    let file_config: RuntimeConfig = toml::from_str(&content)
        .with_context(|| format!("Failed to parse config file: {}", path.display()))?;

    // Start with platform defaults, then merge file config
    let platform = Platform::detect();
    let mut config = RuntimeConfig::from_platform_defaults(platform);
    config.merge(file_config);

    // Apply environment overrides
    let env_source = StdEnvSource;
    env_overrides::apply_env_overrides(&mut config, &env_source)?;

    config.validate()?;
    Ok(config)
}

/// Load configuration with graceful fallback to defaults.
/// Tries standard config file locations, returns platform defaults if none found.
pub fn load_or_default(platform: Platform) -> Result<RuntimeConfig> {
    let mut config = RuntimeConfig::from_platform_defaults(platform);

    // Try to load from file, but don't fail if not found
    if let Ok(Some(file_config)) = load_from_file() {
        config.merge(file_config);
    }

    // Apply environment overrides
    let env_source = StdEnvSource;
    env_overrides::apply_env_overrides(&mut config, &env_source)?;

    config.validate()?;
    Ok(config)
}

struct StdEnvSource;

impl EnvSource for StdEnvSource {
    fn get(&self, key: &str) -> Option<String> {
        env::var(format!("{}{}", ENV_PREFIX, key)).ok()
    }

    fn get_raw(&self, key: &str) -> Option<String> {
        env::var(key).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn platform_defaults_match_expectations() {
        let server = RuntimeConfig::from_platform_defaults(Platform::Server);
        assert_eq!(server.storage.backend, StorageBackend::Fs);
        assert!(server.server.is_some());

        let lambda = RuntimeConfig::from_platform_defaults(Platform::Lambda);
        assert_eq!(lambda.storage.backend, StorageBackend::S3);
        assert!(lambda.lambda.is_some());

        let workers = RuntimeConfig::from_platform_defaults(Platform::CloudflareWorkers);
        assert_eq!(workers.storage.backend, StorageBackend::R2);
        assert!(workers.cloudflare.is_some());
    }
}
