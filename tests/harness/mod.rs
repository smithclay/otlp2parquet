//! Smoke test harness for platform-specific integration tests
//!
//! Provides a unified interface for testing otlp2parquet on the server:
//! - Local Server + MinIO
//!
//! Each platform implements the `SmokeTestHarness` trait, providing:
//! 1. deploy() - Deploy infrastructure
//! 2. send_signals() - Send OTLP test data (logs, metrics, traces)
//! 3. verify_execution() - Check for errors in platform logs
//! 4. duckdb_verifier() - Get configured DuckDB verifier for Parquet validation
//! 5. cleanup() - Remove all deployed resources
//!
//! ## DuckDB Verification
//!
//! DuckDB queries Parquet files directly from storage:
//! - Docker/Server: MinIO S3

#![cfg_attr(not(feature = "smoke-server"), allow(dead_code))]

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Shared test harness trait for platform smoke tests
#[async_trait::async_trait]
pub trait SmokeTestHarness {
    /// Deploy infrastructure (Lambda function, Workers script, etc.)
    ///
    /// Returns deployment information including endpoint URL and catalog config
    async fn deploy(&self) -> Result<DeploymentInfo>;

    /// Send OTLP signals (logs, metrics, traces) to deployed endpoint
    ///
    /// Uses canonical testdata/ fixtures
    async fn send_signals(&self, endpoint: &str) -> Result<()>;

    /// Verify execution succeeded (check platform logs for errors)
    async fn verify_execution(&self) -> Result<ExecutionStatus>;

    /// Get DuckDB verifier configured for this platform's catalog
    ///
    /// Takes deployment info to access catalog endpoints and bucket names
    /// that are only known after deployment
    fn duckdb_verifier(&self, info: &DeploymentInfo) -> DuckDBVerifier;

    /// Cleanup all deployed resources
    async fn cleanup(&self) -> Result<()>;
}

/// Information about deployed infrastructure
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct DeploymentInfo {
    /// OTLP endpoint URL (HTTP)
    pub endpoint: String,
    /// Storage bucket name
    pub bucket: String,
    /// Storage prefix for test isolation
    pub prefix: String,
    /// Deployed resource identifiers for cleanup
    pub resource_ids: HashMap<String, String>,
}

/// Execution status from platform logs
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ExecutionStatus {
    /// Number of errors found in logs
    pub error_count: usize,
    /// Sample error messages
    pub sample_errors: Vec<String>,
    /// Platform-specific metrics (invocation count, duration, etc.)
    pub metrics: HashMap<String, f64>,
}

impl ExecutionStatus {
    pub fn is_healthy(&self) -> bool {
        self.error_count == 0
    }
}

/// DuckDB verifier for Parquet file validation
pub struct DuckDBVerifier {
    pub storage_config: StorageConfig,
}

/// Storage backend configuration for DuckDB
#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub backend: StorageBackend,
    /// Optional prefix for smoke test isolation (e.g., "smoke-abc123/")
    pub prefix: Option<String>,
}

/// Storage backend types
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum StorageBackend {
    /// S3-compatible (MinIO, local testing)
    S3 {
        region: String,
        bucket: String,
        endpoint: Option<String>, // For MinIO
        credentials: S3Credentials,
    },
}

/// S3 credentials configuration
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum S3Credentials {
    /// Use AWS credential provider chain (IAM, env vars, etc.)
    FromEnvironment,
    /// Static credentials (MinIO, local testing)
    Static {
        access_key: String,
        secret_key: String,
    },
}

/// Validation report from DuckDB verification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationReport {
    /// Tables found in catalog
    pub tables: Vec<String>,
    /// Row counts per table
    pub row_counts: HashMap<String, usize>,
    /// Schema validation results
    pub schemas_valid: bool,
    /// Sample data retrieved successfully
    pub samples_valid: bool,
}

impl DuckDBVerifier {
    /// Verify Parquet files contain expected tables and data
    ///
    /// Steps:
    /// 1. Connect to DuckDB with storage credentials
    /// 2. Query Parquet files directly using glob patterns
    /// 3. Count rows for each signal type
    /// 4. Retrieve sample data for sanity checks
    #[allow(unused_variables)]
    pub async fn verify(&self, prefix: &str) -> Result<ValidationReport> {
        let script = self.generate_verification_script(prefix)?;
        let output = self.execute_duckdb_script(&script).await?;
        let report = self.parse_verification_output(&output)?;

        Ok(report)
    }

    /// Generate DuckDB SQL script for verification
    fn generate_verification_script(&self, _prefix: &str) -> Result<String> {
        let mut script = String::new();

        // Install extensions for S3 access
        script.push_str("INSTALL httpfs;\n");
        script.push_str("LOAD httpfs;\n\n");

        // Create storage secret based on backend
        match &self.storage_config.backend {
            StorageBackend::S3 {
                region,
                bucket: _,
                endpoint,
                credentials,
            } => {
                match credentials {
                    S3Credentials::FromEnvironment => {
                        script.push_str(&format!(
                            "CREATE SECRET aws_secret (
    TYPE s3,
    PROVIDER credential_chain,
    REGION '{}'
);\n\n",
                            region
                        ));
                    }
                    S3Credentials::Static {
                        access_key,
                        secret_key,
                    } => {
                        let endpoint_config = endpoint
                            .as_ref()
                            .map(|e| {
                                // Strip http:// or https:// protocol - DuckDB expects just hostname:port
                                let stripped = e
                                    .strip_prefix("http://")
                                    .or_else(|| e.strip_prefix("https://"))
                                    .unwrap_or(e);
                                format!("ENDPOINT '{}',\n    URL_STYLE 'path',\n    USE_SSL false,\n    ", stripped)
                            })
                            .unwrap_or_default();
                        script.push_str(&format!(
                            "CREATE SECRET s3_secret (
    TYPE S3,
    KEY_ID '{}',
    SECRET '{}',
    {}REGION '{}'
);\n\n",
                            access_key, secret_key, endpoint_config, region
                        ));
                    }
                }
            }
        }

        // Direct Parquet file scanning with glob patterns
        let logs_path = self.get_parquet_scan_path("logs")?;
        let traces_path = self.get_parquet_scan_path("traces")?;
        let metrics_gauge_path = self.get_parquet_scan_path("metrics/gauge")?;

        script.push_str(&format!(
            "SELECT 'otel_logs' AS table_name, COUNT(*) AS row_count FROM read_parquet('{}');\n",
            logs_path
        ));
        script.push_str(&format!(
            "-- OPTIONAL_TABLE: otel_traces\nSELECT 'otel_traces' AS table_name, COUNT(*) AS row_count FROM read_parquet('{}');\n",
            traces_path
        ));
        script.push_str(&format!(
            "-- OPTIONAL_TABLE: otel_metrics_gauge\nSELECT 'otel_metrics_gauge' AS table_name, COUNT(*) AS row_count FROM read_parquet('{}');\n",
            metrics_gauge_path
        ));

        Ok(script)
    }

    /// Get path pattern for direct Parquet scanning
    fn get_parquet_scan_path(&self, signal_type: &str) -> Result<String> {
        // Combine storage prefix (for test isolation) with signal type
        let full_prefix = match &self.storage_config.prefix {
            Some(p) => format!("{}{}", p, signal_type),
            None => signal_type.to_string(),
        };

        match &self.storage_config.backend {
            StorageBackend::S3 {
                bucket, endpoint, ..
            } => {
                if endpoint.is_some() {
                    // MinIO: s3://bucket/prefix/**/*.parquet
                    Ok(format!("s3://{}/{}/**/*.parquet", bucket, full_prefix))
                } else {
                    // Real S3
                    Ok(format!("s3://{}/{}/**/*.parquet", bucket, full_prefix))
                }
            }
        }
    }

    /// Execute DuckDB script and return output
    async fn execute_duckdb_script(&self, script: &str) -> Result<String> {
        use tokio::process::Command;

        // Write script to temp file
        let script_path =
            std::env::temp_dir().join(format!("duckdb_verify_{}.sql", uuid::Uuid::new_v4()));
        tokio::fs::write(&script_path, script)
            .await
            .context("Failed to write DuckDB script")?;

        tracing::info!("DuckDB script written to: {:?}", script_path);

        // Execute DuckDB with script file as argument (not stdin)
        // Using -f FILENAME reads file and exits, -markdown sets output format
        let child = Command::new("duckdb")
            .arg("-markdown") // Markdown output for parsing
            .arg("-f")
            .arg(&script_path)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .context("Failed to spawn duckdb process")?;

        // Add timeout to prevent hanging on connection issues
        let output = tokio::time::timeout(
            std::time::Duration::from_secs(120),
            child.wait_with_output(),
        )
        .await
        .context("DuckDB execution timed out after 120s")?
        .context("Failed to execute duckdb")?;

        // Keep temp file for debugging if test fails
        if !output.status.success() {
            tracing::warn!("DuckDB script saved for debugging: {:?}", script_path);
        } else {
            let _ = tokio::fs::remove_file(&script_path).await;
        }

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);

            // Check if the error is about missing files (optional tables)
            // "No files found that match the pattern" is expected for tests that don't generate all signal types
            if stderr.contains("No files found that match the pattern") {
                // This is acceptable - we'll return what we have
                // The test assertions will determine if required data is missing
                tracing::warn!(
                    "Some parquet files not found (expected for partial tests): {}",
                    stderr
                );
                // Return the successful portion of stdout
                return Ok(
                    String::from_utf8(output.stdout).context("Invalid UTF-8 in DuckDB output")?
                );
            }

            anyhow::bail!(
                "DuckDB verification failed:\nSTDERR: {}\nSTDOUT: {}",
                stderr,
                stdout
            );
        }

        let stdout_str =
            String::from_utf8(output.stdout).context("Invalid UTF-8 in DuckDB output")?;
        tracing::debug!("DuckDB output: {}", stdout_str);
        Ok(stdout_str)
    }

    /// Parse DuckDB output into validation report
    fn parse_verification_output(&self, output: &str) -> Result<ValidationReport> {
        let mut tables = Vec::new();
        let mut row_counts = HashMap::new();

        // Simple parsing of markdown table output
        // This is a placeholder - real implementation would parse markdown tables
        for line in output.lines() {
            if line.contains("otel_") {
                // Extract table names and row counts from output
                // Format: | table_name | row_count |
                let parts: Vec<&str> = line.split('|').collect();
                if parts.len() >= 3 {
                    let table_name = parts[1].trim();
                    if table_name.starts_with("otel_") {
                        tables.push(table_name.to_string());

                        if parts.len() >= 4 {
                            if let Ok(count) = parts[2].trim().parse::<usize>() {
                                row_counts.insert(table_name.to_string(), count);
                            }
                        }
                    }
                }
            }
        }

        Ok(ValidationReport {
            tables,
            row_counts,
            schemas_valid: true, // Placeholder
            samples_valid: true, // Placeholder
        })
    }
}

/// Load canonical test data from testdata/ directory
#[allow(dead_code)]
pub struct TestDataSet {
    pub logs_pb: &'static [u8],
    pub logs_json: &'static [u8],
    pub logs_jsonl: &'static [u8],
    pub metrics_gauge_pb: &'static [u8],
    pub metrics_gauge_json: &'static [u8],
    pub metrics_sum_pb: &'static [u8],
    pub metrics_histogram_pb: &'static [u8],
    pub metrics_exponential_histogram_pb: &'static [u8],
    pub metrics_summary_pb: &'static [u8],
    pub traces_pb: &'static [u8],
    pub traces_json: &'static [u8],
    pub traces_jsonl: &'static [u8],
}

impl TestDataSet {
    /// Load all test data fixtures
    pub fn load() -> Self {
        Self {
            logs_pb: include_bytes!("../../testdata/logs.pb"),
            logs_json: include_bytes!("../../testdata/log.json"),
            logs_jsonl: include_bytes!("../../testdata/logs.jsonl"),
            metrics_gauge_pb: include_bytes!("../../testdata/metrics_gauge.pb"),
            metrics_gauge_json: include_bytes!("../../testdata/metrics_gauge.json"),
            metrics_sum_pb: include_bytes!("../../testdata/metrics_sum.pb"),
            metrics_histogram_pb: include_bytes!("../../testdata/metrics_histogram.pb"),
            metrics_exponential_histogram_pb: include_bytes!(
                "../../testdata/metrics_exponential_histogram.pb"
            ),
            metrics_summary_pb: include_bytes!("../../testdata/metrics_summary.pb"),
            traces_pb: include_bytes!("../../testdata/traces.pb"),
            traces_json: include_bytes!("../../testdata/trace.json"),
            traces_jsonl: include_bytes!("../../testdata/traces.jsonl"),
        }
    }
}

#[cfg(feature = "smoke-server")]
pub mod server;
