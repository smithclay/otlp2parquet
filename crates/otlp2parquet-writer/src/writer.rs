//! Core writer types and implementations
//!
//! Uses icepick's AppendOnlyTableWriter for high-level catalog integration.
//! Arrow schemas with field_id metadata are passed to icepick, which:
//! - Derives Iceberg schemas automatically
//! - Creates tables if they don't exist
//! - Writes Parquet files with statistics
//! - Commits to catalog atomically

use anyhow::Result;
use arrow::array::RecordBatch;
use async_trait::async_trait;
use icepick::catalog::Catalog;
use icepick::spec::NamespaceIdent;
use icepick::AppendOnlyTableWriter;
use otlp2parquet_core::SignalType;
use std::sync::Arc;

use crate::table_mapping::table_name_for_signal;

/// Result of a write operation
#[derive(Clone, Debug)]
pub struct WriteResult {
    /// Path where the file was written
    pub path: String,
    /// Number of rows written
    pub row_count: i64,
    /// Signal type
    pub signal_type: SignalType,
    /// Metric type (if applicable)
    pub metric_type: Option<String>,
    /// Timestamp when write completed
    pub completed_at: chrono::DateTime<chrono::Utc>,
}

/// Configuration for the writer
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct WriterConfig {
    /// Base path for writing files
    pub base_path: String,
    /// Optional catalog configuration
    pub catalog_config: Option<CatalogConfig>,
    /// Whether to enable table caching
    pub enable_table_cache: bool,
}

/// Catalog configuration
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct CatalogConfig {
    /// Catalog type (S3Tables, R2Catalog, etc.)
    pub catalog_type: String,
    /// Catalog-specific settings
    pub settings: std::collections::HashMap<String, String>,
}

/// Trait for writing OTLP data to Parquet files
#[cfg_attr(not(target_family = "wasm"), async_trait)]
#[cfg_attr(target_family = "wasm", async_trait(?Send))]
pub trait OtlpWriter: Send + Sync {
    /// Write a single RecordBatch to storage with optional catalog commit
    ///
    /// # Arguments
    /// * `batch` - Arrow RecordBatch to write
    /// * `signal_type` - Type of OTLP signal (logs, traces, metrics)
    /// * `metric_type` - Metric type if signal_type is Metrics (gauge, sum, etc.)
    /// * `service_name` - Service name for partitioning
    /// * `timestamp_nanos` - Timestamp for partitioning
    async fn write_batch(
        &self,
        batch: &RecordBatch,
        signal_type: SignalType,
        metric_type: Option<&str>,
        service_name: &str,
        timestamp_nanos: i64,
    ) -> Result<WriteResult>;
}

/// Icepick-based writer using AppendOnlyTableWriter
///
/// Delegates all Parquet writing and catalog operations to icepick.
/// icepick derives Iceberg schemas from Arrow batches (using field_id metadata).
pub struct IcepickWriter {
    /// Catalog for table operations (S3 Tables, Nessie, etc.)
    catalog: Arc<dyn Catalog>,
    /// Namespace for Iceberg tables (e.g., "otlp")
    namespace: String,
}

impl IcepickWriter {
    /// Create a new IcepickWriter with catalog and namespace
    pub fn new(catalog: Arc<dyn Catalog>, namespace: String) -> Result<Self> {
        Ok(Self { catalog, namespace })
    }
}

#[cfg_attr(not(target_family = "wasm"), async_trait)]
#[cfg_attr(target_family = "wasm", async_trait(?Send))]
impl OtlpWriter for IcepickWriter {
    async fn write_batch(
        &self,
        batch: &RecordBatch,
        signal_type: SignalType,
        metric_type: Option<&str>,
        service_name: &str,
        _timestamp_nanos: i64,
    ) -> Result<WriteResult> {
        let row_count = batch.num_rows() as i64;

        // Get table name based on signal type
        // e.g., "otel_logs", "otel_traces", "otel_metrics_gauge"
        let table_name = table_name_for_signal(signal_type, metric_type);

        tracing::debug!(
            "Writing {} rows to table '{}' (service: {}, signal: {:?})",
            row_count,
            table_name,
            service_name,
            signal_type
        );

        // Create AppendOnlyTableWriter for this table
        // icepick will:
        // 1. Create table if it doesn't exist (deriving Iceberg schema from Arrow field_id)
        // 2. Write Parquet file with statistics
        // 3. Commit to catalog atomically
        let namespace = NamespaceIdent::new(vec![self.namespace.clone()]);
        let writer =
            AppendOnlyTableWriter::new(self.catalog.as_ref(), namespace, table_name.clone());

        // TODO(error-handling): Add retry logic for transient failures
        // TODO(error-handling): Handle schema evolution (field additions/removals)
        // TODO(error-handling): Add timeout for slow catalog operations
        writer.append_batch(batch.clone()).await.map_err(|e| {
            anyhow::anyhow!("Failed to append batch to table '{}': {}", table_name, e)
        })?;

        let completed_at = chrono::Utc::now();

        tracing::info!(
            "✓ Wrote {} rows to '{}' via AppendOnlyTableWriter",
            row_count,
            table_name
        );

        // TODO(future): Get actual file path from icepick for logging
        Ok(WriteResult {
            path: format!("{}/{}", self.namespace, table_name),
            row_count,
            signal_type,
            metric_type: metric_type.map(String::from),
            completed_at,
        })
    }
}
