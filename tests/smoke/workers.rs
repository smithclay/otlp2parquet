//! Cloudflare Workers + R2 smoke test harness
//!
//! Tests otlp2parquet Cloudflare Workers with both R2 plain Parquet and R2 Data Catalog (Iceberg).
//!
//! ## Architecture
//! - Setup: Create unique R2 bucket per test (e.g., "smoke-workers-{uuid}")
//! - Optional: Enable R2 Data Catalog for Iceberg tests
//! - Deploy: Wrangler deploy WASM Workers script using generated config
//! - Send signals: POST to Workers URL with OTLP payloads
//! - Verify execution: Query Workers logs via Cloudflare API
//! - Verify data: DuckDB queries R2 Parquet files (with or without catalog)
//! - Cleanup: Delete Workers script, all R2 objects, and R2 bucket
//!
//! ## Prerequisites
//! - Cloudflare API token with Workers and R2 permissions
//! - WASM binary built (make wasm-compress)
//! - R2 API credentials for S3-compatible access
//!
//! ## Environment Variables
//! - `CLOUDFLARE_API_TOKEN`: Cloudflare API token
//! - `CLOUDFLARE_ACCOUNT_ID`: Cloudflare account ID
//! - `AWS_ACCESS_KEY_ID`: R2 S3 API access key
//! - `AWS_SECRET_ACCESS_KEY`: R2 S3 API secret key
//! - `SMOKE_TEST_WORKER_PREFIX`: Worker name prefix (default: "smoke-workers")

use super::{
    CatalogType, DeploymentInfo, DuckDBVerifier, ExecutionStatus, R2Credentials, SmokeTestHarness,
    StorageBackend, StorageConfig, TestDataSet,
};
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

/// Catalog mode for Workers tests
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum CatalogMode {
    /// Enable R2 Data Catalog (Iceberg)
    Enabled,
    /// Disable catalog (plain Parquet to R2)
    Disabled,
}

/// Cloudflare Workers smoke test harness
pub struct WorkersHarness {
    /// Workers script name (unique per test)
    worker_name: String,
    /// R2 bucket name (pre-created, shared across tests)
    bucket_name: String,
    /// Unique test prefix within bucket (e.g., "smoke-abc123/")
    test_prefix: String,
    /// Cloudflare account ID
    account_id: String,
    /// Cloudflare API token
    api_token: String,
    /// R2 S3 API credentials (from .env)
    r2_access_key_id: String,
    r2_secret_access_key: String,
    /// Catalog mode
    catalog_mode: CatalogMode,
}

impl WorkersHarness {
    /// Get wrangler command with proper environment setup
    fn wrangler_command(&self) -> Command {
        let mut cmd = Command::new("sh");
        cmd.arg("-c")
            .env("CLOUDFLARE_API_TOKEN", &self.api_token)
            .env("CLOUDFLARE_ACCOUNT_ID", &self.account_id);

        // Add catalog env vars if enabled
        if self.catalog_mode == CatalogMode::Enabled {
            cmd.env("CLOUDFLARE_BUCKET_NAME", &self.bucket_name);
        }

        cmd
    }

    /// Create harness from environment variables
    pub fn from_env(catalog_mode: CatalogMode) -> Result<Self> {
        let worker_prefix = std::env::var("SMOKE_TEST_WORKER_PREFIX")
            .unwrap_or_else(|_| "smoke-workers".to_string());
        let account_id =
            std::env::var("CLOUDFLARE_ACCOUNT_ID").context("CLOUDFLARE_ACCOUNT_ID not set")?;
        let api_token =
            std::env::var("CLOUDFLARE_API_TOKEN").context("CLOUDFLARE_API_TOKEN not set")?;

        // R2 bucket (pre-created, shared across tests)
        let bucket_name = std::env::var("CLOUDFLARE_BUCKET_NAME")
            .unwrap_or_else(|_| "otlp2parquet-smoke".to_string());

        // R2 S3 API credentials
        let r2_access_key_id =
            std::env::var("AWS_ACCESS_KEY_ID").context("AWS_ACCESS_KEY_ID not set")?;
        let r2_secret_access_key =
            std::env::var("AWS_SECRET_ACCESS_KEY").context("AWS_SECRET_ACCESS_KEY not set")?;

        // Generate unique test ID for isolation
        let test_id = uuid::Uuid::new_v4()
            .to_string()
            .split('-')
            .next()
            .unwrap()
            .to_string();

        let worker_name = format!("{}-{}", worker_prefix, test_id);
        let test_prefix = format!("smoke-{}/", test_id); // Unique prefix within shared bucket

        Ok(Self {
            worker_name,
            bucket_name,
            test_prefix,
            account_id,
            api_token,
            r2_access_key_id,
            r2_secret_access_key,
            catalog_mode,
        })
    }

    /// Generate wrangler config from template
    fn generate_config(&self) -> Result<PathBuf> {
        let wrangler_dir =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("crates/otlp2parquet-cloudflare");
        let template_path = wrangler_dir.join("wrangler.template.toml");
        let template =
            fs::read_to_string(&template_path).context("Failed to read wrangler.template.toml")?;

        // Replace placeholders
        let mut config_content = template
            .replace("{{WORKER_NAME}}", &self.worker_name)
            .replace("{{BUCKET_NAME}}", &self.bucket_name)
            .replace("{{CLOUDFLARE_ACCOUNT_ID}}", &self.account_id)
            .replace("{{R2_ACCESS_KEY_ID}}", &self.r2_access_key_id)
            .replace("{{R2_SECRET_ACCESS_KEY}}", &self.r2_secret_access_key);

        // Add catalog configuration if enabled
        if self.catalog_mode == CatalogMode::Enabled {
            let catalog_vars = format!(
                "CLOUDFLARE_BUCKET_NAME = \"{}\"\nCLOUDFLARE_ACCOUNT_ID = \"{}\"\nCLOUDFLARE_API_TOKEN = \"{}\"",
                self.bucket_name, self.account_id, self.api_token
            );
            config_content = config_content.replace("# {{CATALOG_VARS}}", &catalog_vars);
        } else {
            config_content = config_content.replace("# {{CATALOG_VARS}}", "");
        }

        // Write to wrangler directory (so paths in config resolve correctly)
        let config_path = wrangler_dir.join(format!("wrangler-{}.toml", self.worker_name));
        fs::write(&config_path, config_content)
            .context("Failed to write generated wrangler config")?;

        tracing::info!("Generated wrangler config: {}", config_path.display());
        Ok(config_path)
    }

    /// List all objects in R2 bucket with test prefix
    fn list_r2_objects(&self) -> Result<Vec<String>> {
        let list_cmd = format!(
            "npx wrangler r2 object list {} --prefix {} --json",
            self.bucket_name, self.test_prefix
        );
        let output = self
            .wrangler_command()
            .arg(&list_cmd)
            .output()
            .context("Failed to list R2 objects")?;

        if !output.status.success() {
            // If bucket doesn't exist or is empty, return empty list
            return Ok(Vec::new());
        }

        let output_str = String::from_utf8_lossy(&output.stdout);

        // Parse JSON output to extract object keys
        // wrangler outputs: [{"key": "...", ...}, ...]
        let mut keys = Vec::new();
        for line in output_str.lines() {
            if line.contains("\"key\"") {
                // Simple extraction: find "key": "value"
                if let Some(start) = line.find("\"key\":") {
                    let rest = &line[start + 6..].trim_start();
                    if let Some(stripped) = rest.strip_prefix('\"') {
                        if let Some(end) = stripped.find('\"') {
                            let key = &stripped[..end];
                            keys.push(key.to_string());
                        }
                    }
                }
            }
        }

        Ok(keys)
    }

    /// Delete a single object from R2 bucket
    fn delete_r2_object(&self, key: &str) -> Result<()> {
        let delete_cmd = format!("npx wrangler r2 object delete {}/{}", self.bucket_name, key);
        let _ = self.wrangler_command().arg(&delete_cmd).output();
        Ok(()) // Ignore errors - best effort
    }

    /// Extract Workers URL from wrangler deploy output
    fn extract_workers_url(&self, output: &str) -> Result<String> {
        // wrangler outputs: "Published {worker_name} (1.23 sec)"
        // "  https://{worker_name}.{account_subdomain}.workers.dev"
        for line in output.lines() {
            if line.contains("https://") && line.contains(".workers.dev") {
                let url = line.trim();
                if url.starts_with("https://") {
                    return Ok(url.to_string());
                }
            }
        }

        // Fallback: construct URL from worker name
        Ok(format!("https://{}.workers.dev", self.worker_name))
    }
}

#[async_trait::async_trait]
impl SmokeTestHarness for WorkersHarness {
    async fn deploy(&self) -> Result<DeploymentInfo> {
        tracing::info!(
            "Deploying Workers smoke test: {} (bucket: {}, prefix: {}, catalog: {})",
            self.worker_name,
            self.bucket_name,
            self.test_prefix,
            if self.catalog_mode == CatalogMode::Enabled {
                "enabled"
            } else {
                "disabled"
            }
        );

        // Generate wrangler config from template
        let config_path = self.generate_config()?;

        // Enable R2 Data Catalog if requested
        if self.catalog_mode == CatalogMode::Enabled {
            tracing::info!("Enabling R2 Data Catalog for bucket: {}", self.bucket_name);
            let enable_catalog_cmd =
                format!("npx wrangler r2 bucket catalog enable {}", self.bucket_name);
            let output = self
                .wrangler_command()
                .arg(&enable_catalog_cmd)
                .output()
                .context("Failed to enable R2 catalog")?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                tracing::warn!("Failed to enable R2 catalog: {}", stderr);
                // Continue anyway - catalog might already be enabled
            }
        }

        // Deploy using wrangler CLI with generated config
        let wrangler_dir =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("crates/otlp2parquet-cloudflare");
        let deploy_cmd = format!(
            "cd {} && npx wrangler deploy --config {}",
            wrangler_dir.display(),
            config_path.display()
        );

        let output = self
            .wrangler_command()
            .arg(&deploy_cmd)
            .output()
            .context("Failed to run wrangler deploy")?;

        if !output.status.success() {
            anyhow::bail!(
                "Wrangler deploy failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        // Extract Workers URL from output
        let output_str = String::from_utf8_lossy(&output.stdout);
        let endpoint = self.extract_workers_url(&output_str)?;

        tracing::info!("Workers deployed successfully: {}", endpoint);

        Ok(DeploymentInfo {
            endpoint: endpoint.clone(),
            catalog_endpoint: if self.catalog_mode == CatalogMode::Enabled {
                format!("r2-catalog://{}", self.bucket_name)
            } else {
                "r2-plain-parquet".to_string()
            },
            bucket: self.bucket_name.clone(),
            resource_ids: HashMap::from([
                ("worker_name".to_string(), self.worker_name.clone()),
                ("bucket_name".to_string(), self.bucket_name.clone()),
                ("account_id".to_string(), self.account_id.clone()),
            ]),
        })
    }

    async fn send_signals(&self, endpoint: &str) -> Result<()> {
        tracing::info!("Sending OTLP signals to Workers");

        let testdata = TestDataSet::load();
        let client = reqwest::Client::new();

        // Send logs (protobuf)
        client
            .post(format!("{}/v1/logs", endpoint))
            .header("Content-Type", "application/x-protobuf")
            .body(testdata.logs_pb.to_vec())
            .send()
            .await
            .context("Failed to send logs protobuf")?
            .error_for_status()
            .context("Logs protobuf request failed")?;

        tracing::info!("Sent logs (protobuf)");

        // Send metrics (gauge)
        client
            .post(format!("{}/v1/metrics", endpoint))
            .header("Content-Type", "application/x-protobuf")
            .body(testdata.metrics_gauge_pb.to_vec())
            .send()
            .await
            .context("Failed to send metrics gauge")?
            .error_for_status()
            .context("Metrics gauge request failed")?;

        tracing::info!("Sent metrics (gauge)");

        // Send traces (protobuf)
        client
            .post(format!("{}/v1/traces", endpoint))
            .header("Content-Type", "application/x-protobuf")
            .body(testdata.traces_pb.to_vec())
            .send()
            .await
            .context("Failed to send traces protobuf")?
            .error_for_status()
            .context("Traces protobuf request failed")?;

        tracing::info!("Sent traces (protobuf)");

        Ok(())
    }

    async fn verify_execution(&self) -> Result<ExecutionStatus> {
        tracing::info!("Verifying Workers execution via tail logs");

        // Wait for logs to be available
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Query Workers tail logs via wrangler
        let tail_cmd = format!(
            "npx wrangler tail {} --format json --once",
            self.worker_name
        );

        let output = self
            .wrangler_command()
            .arg(&tail_cmd)
            .output()
            .context("Failed to run wrangler tail")?;

        let log_output = String::from_utf8_lossy(&output.stdout);

        // Count error/warning messages
        let error_count = log_output
            .lines()
            .filter(|line| {
                line.contains("\"error\"")
                    || line.contains("\"ERROR\"")
                    || line.contains("\"warn\"")
            })
            .count();

        let sample_errors: Vec<String> = log_output
            .lines()
            .filter(|line| {
                line.contains("\"error\"")
                    || line.contains("\"ERROR\"")
                    || line.contains("\"warn\"")
            })
            .take(5)
            .map(|s| s.to_string())
            .collect();

        tracing::info!(
            "Found {} potential errors/warnings in Workers logs",
            error_count
        );

        Ok(ExecutionStatus {
            error_count,
            sample_errors,
            metrics: HashMap::new(),
        })
    }

    fn duckdb_verifier(&self) -> DuckDBVerifier {
        DuckDBVerifier {
            catalog_type: CatalogType::R2Catalog,
            catalog_endpoint: if self.catalog_mode == CatalogMode::Enabled {
                format!("r2-catalog://{}", self.bucket_name)
            } else {
                "r2-plain-parquet".to_string()
            },
            storage_config: StorageConfig {
                backend: StorageBackend::R2 {
                    account_id: self.account_id.clone(),
                    bucket: self.bucket_name.clone(),
                    credentials: R2Credentials {
                        access_key_id: self.r2_access_key_id.clone(),
                        secret_access_key: self.r2_secret_access_key.clone(),
                    },
                },
            },
        }
    }

    async fn cleanup(&self) -> Result<()> {
        tracing::info!("Cleaning up Workers smoke test (fail-safe mode)");

        // 1. Delete Workers script
        let delete_cmd = format!("npx wrangler delete {} --force", self.worker_name);

        let output = self.wrangler_command().arg(&delete_cmd).output();

        match output {
            Ok(out) if out.status.success() => {
                tracing::info!("Workers script deleted successfully");
            }
            Ok(out) => {
                tracing::warn!(
                    "Failed to delete Workers script: {}",
                    String::from_utf8_lossy(&out.stderr)
                );
            }
            Err(e) => {
                tracing::warn!("Failed to run wrangler delete: {}", e);
            }
        }

        // Cleanup generated config file (from wrangler directory)
        let wrangler_dir =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("crates/otlp2parquet-cloudflare");
        let config_path = wrangler_dir.join(format!("wrangler-{}.toml", self.worker_name));
        let _ = fs::remove_file(config_path);

        // 2. Delete all R2 objects with test prefix
        tracing::info!(
            "Deleting test objects from bucket {} with prefix {}",
            self.bucket_name,
            self.test_prefix
        );
        match self.list_r2_objects() {
            Ok(objects) => {
                tracing::info!("Found {} objects to delete", objects.len());
                for key in objects {
                    let _ = self.delete_r2_object(&key);
                }
            }
            Err(e) => {
                tracing::warn!("Failed to list R2 objects: {}", e);
            }
        }

        // Always return Ok - cleanup failures are logged but don't fail the test
        Ok(())
    }
}
