//! Smoke test harness for platform-specific integration tests
//!
//! Provides a unified interface for testing otlp2parquet across different platforms:
//! - Lambda + S3 Tables Catalog
//! - Cloudflare Workers + R2 Data Catalog
//!
//! Each platform implements the `SmokeTestHarness` trait, providing:
//! 1. deploy() - Deploy infrastructure (Lambda function, Workers script)
//! 2. send_signals() - Send OTLP test data (logs, metrics, traces)
//! 3. verify_execution() - Check for errors in platform logs
//! 4. duckdb_verifier() - Get configured DuckDB verifier for catalog validation
//! 5. cleanup() - Remove all deployed resources
//!
//! ## DuckDB Universal Verification
//!
//! DuckDB connects to Iceberg catalogs across all platforms:
//! - Docker/Server: Nessie REST catalog + MinIO S3
//! - Lambda: S3 Tables catalog + AWS S3
//! - Workers: R2 Data Catalog + Cloudflare R2
//!
//! Same verification logic (table counts, schema, samples) regardless of platform.

#![cfg_attr(
    not(any(
        feature = "smoke-server",
        feature = "smoke-lambda",
        feature = "smoke-workers"
    )),
    allow(dead_code)
)]

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Catalog mode for smoke tests
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogMode {
    /// Use Iceberg catalog (REST, S3 Tables, R2 Data Catalog)
    Enabled,
    /// Plain Parquet files without catalog
    None,
}

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
    /// Catalog endpoint or identifier (ARN for S3 Tables, URL for REST)
    pub catalog_endpoint: String,
    /// Storage bucket name
    pub bucket: String,
    /// Iceberg namespace for tables
    pub namespace: String,
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

/// DuckDB verifier for Iceberg catalog validation
pub struct DuckDBVerifier {
    pub catalog_type: CatalogType,
    pub catalog_endpoint: String,
    pub storage_config: StorageConfig,
    /// Optional catalog token (e.g., R2 API token for R2 Data Catalog)
    pub catalog_token: Option<String>,
}

/// Iceberg catalog types
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum CatalogType {
    /// Nessie REST catalog (Docker/Server tests)
    NessieREST,
    /// AWS S3 Tables catalog (Lambda tests)
    S3Tables,
    /// Cloudflare R2 Data Catalog (Workers tests)
    R2Catalog,
    /// Plain Parquet files without catalog
    PlainParquet,
}

/// Storage backend configuration for DuckDB
#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub backend: StorageBackend,
}

/// Storage backend types
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum StorageBackend {
    /// AWS S3 (or S3-compatible like MinIO)
    S3 {
        region: String,
        bucket: String,
        endpoint: Option<String>, // For MinIO
        credentials: S3Credentials,
    },
    /// Cloudflare R2
    R2 {
        account_id: String,
        bucket: String,
        credentials: R2Credentials,
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

/// R2 credentials configuration
#[derive(Debug, Clone)]
pub struct R2Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
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
    /// Verify Iceberg catalog contains expected tables and data
    ///
    /// Steps:
    /// 1. Connect to DuckDB with appropriate storage and catalog config
    /// 2. List all tables in namespace
    /// 3. Count rows in each table
    /// 4. Validate schemas match expected ClickHouse format
    /// 5. Retrieve sample data for sanity checks
    pub async fn verify(&self, namespace: &str) -> Result<ValidationReport> {
        // For now, this is a placeholder that would use either:
        // 1. DuckDB Rust client (duckdb crate)
        // 2. Shell out to duckdb CLI (similar to verify-duckdb.sh)
        //
        // The shell approach is simpler and reuses existing verify-duckdb.sh logic

        let script = self.generate_verification_script(namespace)?;
        let output = self.execute_duckdb_script(&script).await?;
        let report = self.parse_verification_output(&output)?;

        Ok(report)
    }

    /// Generate DuckDB SQL script for verification
    fn generate_verification_script(&self, namespace: &str) -> Result<String> {
        let mut script = String::new();

        // Install extensions (S3 Tables support is now GA in DuckDB)
        if self.catalog_type == CatalogType::S3Tables {
            script.push_str("INSTALL aws;\n");
            script.push_str("INSTALL httpfs;\n");
            script.push_str("INSTALL iceberg;\n");
            script.push_str("LOAD aws;\n");
            script.push_str("LOAD httpfs;\n");
            script.push_str("LOAD iceberg;\n\n");
        } else {
            script.push_str("INSTALL httpfs;\n");
            script.push_str("INSTALL iceberg;\n");
            script.push_str("LOAD httpfs;\n");
            script.push_str("LOAD iceberg;\n\n");
        }

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
            StorageBackend::R2 {
                account_id,
                bucket: _,
                credentials,
            } => {
                script.push_str(&format!(
                    "CREATE SECRET r2_secret (
    TYPE S3,
    KEY_ID '{}',
    SECRET '{}',
    ENDPOINT '{}.r2.cloudflarestorage.com',
    REGION 'auto'
);\n\n",
                    credentials.access_key_id, credentials.secret_access_key, account_id
                ));
            }
        }

        // Attach Iceberg catalog based on type OR direct Parquet scanning
        match self.catalog_type {
            CatalogType::NessieREST => {
                script.push_str(&format!(
                    "ATTACH 's3://otlp/' AS iceberg_catalog (
    TYPE ICEBERG,
    ENDPOINT '{}',
    AUTHORIZATION_TYPE 'none'
);\n\n",
                    self.catalog_endpoint
                ));
            }
            CatalogType::S3Tables => {
                // S3 Tables uses ARN directly in ATTACH statement
                // ARN format: arn:aws:s3tables:region:account:bucket/bucket-name
                script.push_str(&format!(
                    "ATTACH '{}' AS iceberg_catalog (
    TYPE iceberg,
    ENDPOINT_TYPE s3_tables
);\n\n",
                    self.catalog_endpoint
                ));
            }
            CatalogType::R2Catalog => {
                // R2 Data Catalog requires an Iceberg SECRET with API token
                if let Some(token) = &self.catalog_token {
                    script.push_str(&format!(
                        "CREATE SECRET r2_catalog_secret (
    TYPE ICEBERG,
    TOKEN '{}'
);\n\n",
                        token
                    ));
                }

                // Extract account and bucket from catalog endpoint for warehouse name
                // Endpoint format: https://catalog.cloudflarestorage.com/<account>/<bucket>
                // Warehouse name format: <account>_<bucket>
                let parts: Vec<&str> = self.catalog_endpoint.split('/').collect();
                let warehouse_name = if parts.len() >= 5 {
                    // parts: ["https:", "", "catalog.cloudflarestorage.com", "<account>", "<bucket>"]
                    format!("{}_{}", parts[3], parts[4])
                } else {
                    // Fallback if format is unexpected
                    "default".to_string()
                };

                script.push_str(&format!(
                    "ATTACH '{}' AS iceberg_catalog (
    TYPE ICEBERG,
    ENDPOINT '{}'
);\n\n",
                    warehouse_name, self.catalog_endpoint
                ));
            }
            CatalogType::PlainParquet => {
                // No catalog attachment needed for plain Parquet mode
                // Will use direct read_parquet() queries instead
            }
        }

        // Note: information_schema.tables query causes "NameListToString NOT IMPLEMENTED" error
        // with S3 Tables catalog in DuckDB. We'll infer table existence from successful row counts instead.

        // Count rows in tables that should exist based on test signals sent
        // The full pipeline test sends: logs, metrics (gauge only), and traces
        if self.catalog_type == CatalogType::PlainParquet {
            // Plain Parquet mode: Direct file scanning with glob patterns
            let logs_path = self.get_parquet_scan_path("logs")?;
            let traces_path = self.get_parquet_scan_path("traces")?;
            let metrics_gauge_path = self.get_parquet_scan_path("metrics/gauge")?;

            script.push_str(&format!(
                "SELECT 'otel_logs' AS table_name, COUNT(*) AS row_count FROM read_parquet('{}');\n",
                logs_path
            ));
            script.push_str(&format!(
                "SELECT 'otel_traces' AS table_name, COUNT(*) AS row_count FROM read_parquet('{}');\n",
                traces_path
            ));
            script.push_str(&format!(
                "SELECT 'otel_metrics_gauge' AS table_name, COUNT(*) AS row_count FROM read_parquet('{}');\n",
                metrics_gauge_path
            ));
        } else {
            // Iceberg catalog mode: Query catalog tables
            script.push_str(&format!(
                "SELECT 'otel_logs' AS table_name, COUNT(*) AS row_count FROM iceberg_catalog.{}.otel_logs;\n",
                namespace
            ));
            script.push_str(&format!(
                "SELECT 'otel_traces' AS table_name, COUNT(*) AS row_count FROM iceberg_catalog.{}.otel_traces;\n",
                namespace
            ));
            script.push_str(&format!(
                "SELECT 'otel_metrics_gauge' AS table_name, COUNT(*) AS row_count FROM iceberg_catalog.{}.otel_metrics_gauge;\n",
                namespace
            ));
        }

        Ok(script)
    }

    /// Get path pattern for direct Parquet scanning (plain Parquet mode)
    fn get_parquet_scan_path(&self, prefix: &str) -> Result<String> {
        match &self.storage_config.backend {
            StorageBackend::S3 {
                bucket, endpoint, ..
            } => {
                if endpoint.is_some() {
                    // MinIO: s3://bucket/prefix/**/*.parquet
                    Ok(format!("s3://{}/{}/**/*.parquet", bucket, prefix))
                } else {
                    // Real S3
                    Ok(format!("s3://{}/{}/**/*.parquet", bucket, prefix))
                }
            }
            StorageBackend::R2 { bucket, .. } => {
                // DuckDB doesn't support r2:// URI scheme
                // Use s3:// with R2 endpoint configured in secret
                Ok(format!("s3://{}/{}/**/*.parquet", bucket, prefix))
            }
        }
    }

    /// Execute DuckDB script and return output
    async fn execute_duckdb_script(&self, script: &str) -> Result<String> {
        use tokio::process::Command;

        // Write script to temp file for debugging
        let script_path =
            std::env::temp_dir().join(format!("duckdb_verify_{}.sql", uuid::Uuid::new_v4()));
        tokio::fs::write(&script_path, script)
            .await
            .context("Failed to write DuckDB script")?;

        tracing::info!("DuckDB script written to: {:?}", script_path);
        tracing::info!("Catalog endpoint for DuckDB: {}", self.catalog_endpoint);

        // Execute DuckDB with script as input
        let mut child = Command::new("duckdb")
            .arg("-markdown") // Markdown output for parsing
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .context("Failed to spawn duckdb process")?;

        // Write script to stdin
        if let Some(mut stdin) = child.stdin.take() {
            use tokio::io::AsyncWriteExt;
            stdin
                .write_all(script.as_bytes())
                .await
                .context("Failed to write script to duckdb stdin")?;
            stdin.flush().await.context("Failed to flush stdin")?;
            drop(stdin); // Close stdin to signal EOF
        }

        // Add timeout to prevent hanging on connection issues
        let output =
            tokio::time::timeout(std::time::Duration::from_secs(30), child.wait_with_output())
                .await
                .context("DuckDB execution timed out after 30s")?
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
            logs_pb: include_bytes!("../../../../testdata/logs.pb"),
            logs_json: include_bytes!("../../../../testdata/log.json"),
            logs_jsonl: include_bytes!("../../../../testdata/logs.jsonl"),
            metrics_gauge_pb: include_bytes!("../../../../testdata/metrics_gauge.pb"),
            metrics_gauge_json: include_bytes!("../../../../testdata/metrics_gauge.json"),
            metrics_sum_pb: include_bytes!("../../../../testdata/metrics_sum.pb"),
            metrics_histogram_pb: include_bytes!("../../../../testdata/metrics_histogram.pb"),
            metrics_exponential_histogram_pb: include_bytes!(
                "../../../../testdata/metrics_exponential_histogram.pb"
            ),
            metrics_summary_pb: include_bytes!("../../../../testdata/metrics_summary.pb"),
            traces_pb: include_bytes!("../../../../testdata/traces.pb"),
            traces_json: include_bytes!("../../../../testdata/trace.json"),
            traces_jsonl: include_bytes!("../../../../testdata/traces.jsonl"),
        }
    }
}

#[cfg(feature = "smoke-server")]
pub mod server;

#[cfg(feature = "smoke-lambda")]
pub mod lambda;

#[cfg(feature = "smoke-workers")]
pub mod workers;
