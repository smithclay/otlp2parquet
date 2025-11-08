//! Apache Iceberg REST catalog integration
//!
//! Platform-agnostic catalog layer with pluggable HTTP auth

// Re-export main types
pub use catalog::{IcebergCatalog, NamespaceIdent};
pub use http::{HttpClient, HttpResponse, ReqwestHttpClient};
pub use otlp2parquet_core::{Blake3Hash, ParquetWriteResult};
pub use writer::{IcebergConfig, IcebergWriter};

// Module declarations
pub mod arrow_convert;
pub mod catalog;
pub mod datafile_convert;
pub mod http;
#[cfg(not(target_arch = "wasm32"))]
pub mod init;
#[cfg(not(target_arch = "wasm32"))]
pub mod manifest;
pub mod protocol;
pub mod types;
pub mod validation;
pub mod writer;

// AWS SigV4 client (feature-gated)
#[cfg(feature = "aws-sigv4")]
pub mod aws;

use std::collections::HashMap;
use std::env;

#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;

use anyhow::{Context, Result as AnyhowResult};
use serde::{Deserialize, Serialize};

#[cfg(not(target_arch = "wasm32"))]
use tracing::{debug, info, instrument, warn};

// Include IcebergRestConfig and IcebergCommitter from old mod.rs
// (This section comes from otlp2parquet-storage/src/iceberg/mod.rs lines 122-412)

/// Configuration for Iceberg REST catalog integration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IcebergRestConfig {
    pub rest_uri: String,
    pub warehouse: Option<String>,
    pub namespace: Vec<String>,
    pub tables: HashMap<String, String>,
    #[serde(default = "default_catalog_name")]
    pub catalog_name: String,
    #[serde(default = "default_format_version")]
    pub format_version: i32,
    #[serde(default = "default_target_file_size_bytes")]
    pub target_file_size_bytes: u64,
    #[serde(default = "default_staging_prefix")]
    pub staging_prefix: String,
}

const DEFAULT_CATALOG_NAME: &str = "rest";
const DEFAULT_STAGING_PREFIX: &str = "data/incoming";
const DEFAULT_TARGET_FILE_SIZE: u64 = 512 * 1024 * 1024;

fn default_catalog_name() -> String {
    DEFAULT_CATALOG_NAME.to_string()
}

fn default_format_version() -> i32 {
    2
}

fn default_target_file_size_bytes() -> u64 {
    DEFAULT_TARGET_FILE_SIZE
}

fn default_staging_prefix() -> String {
    DEFAULT_STAGING_PREFIX.to_string()
}

impl Default for IcebergRestConfig {
    fn default() -> Self {
        Self {
            rest_uri: String::new(),
            warehouse: None,
            namespace: Vec::new(),
            tables: HashMap::new(),
            catalog_name: default_catalog_name(),
            format_version: default_format_version(),
            target_file_size_bytes: default_target_file_size_bytes(),
            staging_prefix: default_staging_prefix(),
        }
    }
}

impl IcebergRestConfig {
    pub fn from_env() -> AnyhowResult<Self> {
        let rest_uri = env::var("OTLP2PARQUET_ICEBERG_REST_URI")
            .context("OTLP2PARQUET_ICEBERG_REST_URI must be set")?;

        let warehouse = env::var("OTLP2PARQUET_ICEBERG_WAREHOUSE").ok();

        let namespace = env::var("OTLP2PARQUET_ICEBERG_NAMESPACE")
            .unwrap_or_default()
            .split('.')
            .filter(|segment| !segment.trim().is_empty())
            .map(|segment| segment.trim().to_string())
            .collect::<Vec<_>>();

        let mut tables = HashMap::new();
        tables.insert(
            "logs".to_string(),
            env::var("OTLP2PARQUET_ICEBERG_TABLE_LOGS").unwrap_or_else(|_| "otel_logs".to_string()),
        );
        tables.insert(
            "traces".to_string(),
            env::var("OTLP2PARQUET_ICEBERG_TABLE_TRACES")
                .unwrap_or_else(|_| "otel_traces".to_string()),
        );
        tables.insert(
            "metrics:gauge".to_string(),
            env::var("OTLP2PARQUET_ICEBERG_TABLE_METRICS_GAUGE")
                .unwrap_or_else(|_| "otel_metrics_gauge".to_string()),
        );
        tables.insert(
            "metrics:sum".to_string(),
            env::var("OTLP2PARQUET_ICEBERG_TABLE_METRICS_SUM")
                .unwrap_or_else(|_| "otel_metrics_sum".to_string()),
        );
        tables.insert(
            "metrics:histogram".to_string(),
            env::var("OTLP2PARQUET_ICEBERG_TABLE_METRICS_HISTOGRAM")
                .unwrap_or_else(|_| "otel_metrics_histogram".to_string()),
        );
        tables.insert(
            "metrics:exponential_histogram".to_string(),
            env::var("OTLP2PARQUET_ICEBERG_TABLE_METRICS_EXPONENTIAL_HISTOGRAM")
                .unwrap_or_else(|_| "otel_metrics_exponential_histogram".to_string()),
        );
        tables.insert(
            "metrics:summary".to_string(),
            env::var("OTLP2PARQUET_ICEBERG_TABLE_METRICS_SUMMARY")
                .unwrap_or_else(|_| "otel_metrics_summary".to_string()),
        );

        let catalog_name = env::var("OTLP2PARQUET_ICEBERG_CATALOG_NAME")
            .unwrap_or_else(|_| DEFAULT_CATALOG_NAME.to_string());

        let staging_prefix = env::var("OTLP2PARQUET_ICEBERG_STAGING_PREFIX")
            .unwrap_or_else(|_| DEFAULT_STAGING_PREFIX.to_string());

        let target_file_size_bytes = env::var("OTLP2PARQUET_ICEBERG_TARGET_FILE_SIZE_BYTES")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(DEFAULT_TARGET_FILE_SIZE);

        Ok(Self {
            rest_uri,
            warehouse,
            namespace,
            tables,
            catalog_name,
            format_version: default_format_version(),
            target_file_size_bytes,
            staging_prefix,
        })
    }
}

/// Committer that appends Parquet files to an Iceberg table.
#[cfg(not(target_arch = "wasm32"))]
pub struct IcebergCommitter {
    catalog: Arc<IcebergCatalog<http::ReqwestHttpClient>>,
}

#[cfg(not(target_arch = "wasm32"))]
impl IcebergCommitter {
    pub fn new(catalog: Arc<IcebergCatalog<http::ReqwestHttpClient>>) -> Self {
        Self { catalog }
    }

    #[cfg_attr(not(target_arch = "wasm32"), instrument(skip(self, parquet_results, storage_operator), fields(num_files = parquet_results.len(), signal_type, metric_type)))]
    pub async fn commit_with_signal(
        &self,
        signal_type: &str,
        metric_type: Option<&str>,
        parquet_results: &[ParquetWriteResult],
        storage_operator: &opendal::Operator,
    ) -> AnyhowResult<()> {
        if parquet_results.is_empty() {
            debug!("No files to commit");
            return Ok(());
        }

        let mut data_files = Vec::with_capacity(parquet_results.len());
        for result in parquet_results {
            let arrow_schema = &result.arrow_schema;
            let iceberg_schema = match arrow_convert::arrow_to_iceberg_schema(arrow_schema) {
                Ok(schema) => schema,
                Err(e) => {
                    warn!(
                        path = %result.path,
                        error = %e,
                        "failed to convert Arrow schema to Iceberg - skipping file"
                    );
                    continue;
                }
            };

            let data_file = match datafile_convert::build_data_file(result, &iceberg_schema) {
                Ok(df) => df,
                Err(e) => {
                    warn!(
                        path = %result.path,
                        error = %e,
                        "failed to build DataFile from Parquet metadata - skipping file"
                    );
                    continue;
                }
            };
            data_files.push(data_file);
        }

        if data_files.is_empty() {
            warn!("No valid DataFiles to commit after conversion");
            return Ok(());
        }

        let total_rows: u64 = data_files.iter().map(|f| f.record_count).sum();
        let total_bytes: u64 = data_files.iter().map(|f| f.file_size_in_bytes).sum();

        debug!(
            signal_type = signal_type,
            metric_type = ?metric_type,
            num_files = data_files.len(),
            total_rows = total_rows,
            total_bytes = total_bytes,
            "committing files to Iceberg table"
        );

        if let Err(e) = self
            .catalog
            .commit_with_signal(signal_type, metric_type, data_files, storage_operator)
            .await
        {
            warn!(
                signal_type = signal_type,
                metric_type = ?metric_type,
                error = %e,
                "failed to commit to catalog - files written to storage but not cataloged"
            );
            return Ok(());
        }

        info!(
            signal_type = signal_type,
            metric_type = ?metric_type,
            num_files = parquet_results.len(),
            total_rows = total_rows,
            total_bytes = total_bytes,
            "successfully committed files to Iceberg table"
        );

        Ok(())
    }
}
