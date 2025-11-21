//! Cloudflare Workers + R2 Data Catalog smoke test harness
//!
//! Tests otlp2parquet Cloudflare Workers with R2 Data Catalog for Iceberg.
//!
//! ## Architecture
//! - Deploy: Wrangler deploy WASM Workers script with R2 binding
//! - Send signals: POST to Workers URL with OTLP payloads
//! - Verify execution: Query Workers logs via Cloudflare API
//! - Verify data: DuckDB queries R2 Data Catalog + R2 Parquet files
//! - Cleanup: Delete Workers script and R2 objects
//!
//! ## Prerequisites
//! - Cloudflare API token with Workers and R2 permissions
//! - WASM binary built (make wasm-compress)
//! - R2 bucket created
//!
//! ## Environment Variables
//! - `CLOUDFLARE_API_TOKEN`: Cloudflare API token
//! - `CLOUDFLARE_ACCOUNT_ID`: Cloudflare account ID
//! - `SMOKE_TEST_WORKER_PREFIX`: Worker name prefix (default: "smoke-workers")
//! - `SMOKE_TEST_R2_BUCKET`: R2 bucket name (default: "otlp-smoke")

use super::{
    CatalogType, DeploymentInfo, DuckDBVerifier, ExecutionStatus, R2Credentials, SmokeTestHarness,
    StorageBackend, StorageConfig, TestDataSet,
};
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

/// Cloudflare Workers smoke test harness
pub struct WorkersHarness {
    /// Workers script name
    worker_name: String,
    /// Cloudflare account ID
    account_id: String,
    /// R2 bucket name
    r2_bucket: String,
    /// R2 Data Catalog endpoint (if available)
    r2_catalog_endpoint: Option<String>,
    /// Cloudflare API token
    api_token: String,
    /// WASM binary path
    wasm_path: PathBuf,
}

impl WorkersHarness {
    /// Create harness from environment variables
    pub fn from_env() -> Result<Self> {
        let worker_prefix = std::env::var("SMOKE_TEST_WORKER_PREFIX")
            .unwrap_or_else(|_| "smoke-workers".to_string());
        let account_id =
            std::env::var("CLOUDFLARE_ACCOUNT_ID").context("CLOUDFLARE_ACCOUNT_ID not set")?;
        let api_token =
            std::env::var("CLOUDFLARE_API_TOKEN").context("CLOUDFLARE_API_TOKEN not set")?;
        let r2_bucket =
            std::env::var("SMOKE_TEST_R2_BUCKET").unwrap_or_else(|_| "otlp-smoke".to_string());

        // Generate unique worker name for test isolation
        let test_id = uuid::Uuid::new_v4()
            .to_string()
            .split('-')
            .next()
            .unwrap()
            .to_string();
        let worker_name = format!("{}-{}", worker_prefix, test_id);

        // R2 Data Catalog endpoint (if configured)
        let r2_catalog_endpoint = std::env::var("SMOKE_TEST_R2_CATALOG_ENDPOINT").ok();

        let wasm_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../crates/otlp2parquet-cloudflare/build/index_bg_optimized.wasm.gz");

        Ok(Self {
            worker_name,
            account_id,
            r2_bucket,
            r2_catalog_endpoint,
            api_token,
            wasm_path,
        })
    }
}

#[async_trait::async_trait]
impl SmokeTestHarness for WorkersHarness {
    async fn deploy(&self) -> Result<DeploymentInfo> {
        tracing::info!("Deploying Workers smoke test: {}", self.worker_name);

        // Deploy using wrangler CLI
        let output = Command::new("wrangler")
            .args([
                "deploy",
                "--name",
                &self.worker_name,
                "--compatibility-date",
                "2024-01-01",
            ])
            .env("CLOUDFLARE_API_TOKEN", &self.api_token)
            .env("CLOUDFLARE_ACCOUNT_ID", &self.account_id)
            .current_dir(
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../crates/otlp2parquet-cloudflare"),
            )
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
            catalog_endpoint: self
                .r2_catalog_endpoint
                .clone()
                .unwrap_or_else(|| "r2-catalog-not-configured".to_string()),
            bucket: self.r2_bucket.clone(),
            resource_ids: HashMap::from([
                ("worker_name".to_string(), self.worker_name.clone()),
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
        let output = Command::new("wrangler")
            .args([
                "tail",
                &self.worker_name,
                "--format",
                "json",
                "--once", // Single batch of logs
            ])
            .env("CLOUDFLARE_API_TOKEN", &self.api_token)
            .env("CLOUDFLARE_ACCOUNT_ID", &self.account_id)
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
        // R2 credentials from environment
        let r2_access_key = std::env::var("CF_R2_ACCESS_KEY_ID").unwrap_or_default();
        let r2_secret_key = std::env::var("CF_R2_SECRET_ACCESS_KEY").unwrap_or_default();

        DuckDBVerifier {
            catalog_type: CatalogType::R2Catalog,
            catalog_endpoint: self
                .r2_catalog_endpoint
                .clone()
                .unwrap_or_else(|| "r2-catalog-not-configured".to_string()),
            storage_config: StorageConfig {
                backend: StorageBackend::R2 {
                    account_id: self.account_id.clone(),
                    bucket: self.r2_bucket.clone(),
                    credentials: R2Credentials {
                        access_key_id: r2_access_key,
                        secret_access_key: r2_secret_key,
                    },
                },
            },
        }
    }

    async fn cleanup(&self) -> Result<()> {
        tracing::info!("Cleaning up Workers smoke test");

        // Delete Workers script
        let output = Command::new("wrangler")
            .args(["delete", &self.worker_name, "--force"])
            .env("CLOUDFLARE_API_TOKEN", &self.api_token)
            .env("CLOUDFLARE_ACCOUNT_ID", &self.account_id)
            .output()
            .context("Failed to run wrangler delete")?;

        if !output.status.success() {
            tracing::warn!(
                "Failed to delete Workers script: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        } else {
            tracing::info!("Workers script deleted successfully");
        }

        // Note: R2 bucket cleanup is not automatic (could delete test objects)
        // For safety, we leave R2 objects for manual cleanup

        Ok(())
    }
}

impl WorkersHarness {
    /// Extract Workers URL from wrangler deploy output
    fn extract_workers_url(&self, output: &str) -> Result<String> {
        // wrangler outputs: "Published smoke-workers-abc123 (1.23 sec)"
        // "  https://smoke-workers-abc123.workers.dev"
        for line in output.lines() {
            if line.contains("https://") && line.contains(".workers.dev") {
                let url = line.trim();
                if url.starts_with("https://") {
                    return Ok(url.to_string());
                }
            }
        }

        anyhow::bail!("Failed to extract Workers URL from wrangler output")
    }
}
