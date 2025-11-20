//! Core write operations using icepick
//!
//! Uses icepick's AppendOnlyTableWriter for high-level catalog integration.
//! Arrow schemas with field_id metadata are passed to icepick, which:
//! - Derives Iceberg schemas automatically
//! - Creates tables if they don't exist
//! - Writes Parquet files with statistics
//! - Commits to catalog atomically

use crate::error::{Result, WriterError};
use crate::table_mapping::table_name_for_signal;
use arrow::array::RecordBatch;
use icepick::catalog::Catalog;
use icepick::spec::NamespaceIdent;
use icepick::{AppendOnlyTableWriter, AppendResult};
use otlp2parquet_core::SignalType;

/// Write a RecordBatch to an Iceberg table via catalog
///
/// This function delegates all Parquet writing, schema management, and catalog operations
/// to icepick's `AppendOnlyTableWriter`. It focuses on table name resolution and error
/// translation.
///
/// # Behavior
///
/// 1. **Table creation**: If the table doesn't exist, icepick creates it automatically by
///    deriving the Iceberg schema from Arrow field_id metadata in the RecordBatch
/// 2. **Schema evolution**: Not currently supported - schema changes will fail the write
/// 3. **Atomicity**: Write and catalog commit are atomic via icepick
/// 4. **Retries**: None - failures return immediately. Callers should retry if needed
///
/// # Resilience
///
/// Write operations have no built-in retry logic. Failures propagate immediately as
/// `WriterError::WriteFailure`. Typical failure modes:
/// - Network errors (catalog unreachable)
/// - Schema mismatches (field type changes)
/// - Storage errors (quota exceeded, permissions)
/// - Catalog errors (transaction conflicts)
///
/// Application-level retry logic should handle transient failures based on error type.
///
/// # Arguments
/// * `catalog` - Catalog instance for table operations
/// * `namespace` - Namespace for tables (e.g., "otlp")
/// * `batch` - Arrow RecordBatch to write
/// * `signal_type` - Type of OTLP signal (logs, traces, metrics)
/// * `metric_type` - Metric type if signal_type is Metrics (gauge, sum, etc.)
/// * `service_name` - Service name for logging (not used for partitioning)
/// * `timestamp_nanos` - Timestamp for logging (not used for partitioning)
///
/// # Returns
/// Table path in format "{namespace}/{table_name}" for logging purposes only
///
/// # Errors
/// Returns `WriterError::InvalidTableName` if signal/metric type combination is invalid
/// Returns `WriterError::WriteFailure` if icepick append operation fails
pub async fn write_batch(
    catalog: &dyn Catalog,
    namespace: &str,
    batch: &RecordBatch,
    signal_type: SignalType,
    metric_type: Option<&str>,
    service_name: &str,
    _timestamp_nanos: i64,
) -> Result<String> {
    let row_count = batch.num_rows();

    // Get table name based on signal type
    // e.g., "otel_logs", "otel_traces", "otel_metrics_gauge"
    let table_name = table_name_for_signal(signal_type, metric_type)?;

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
    let namespace_ident = NamespaceIdent::new(vec![namespace.to_string()]);
    let writer = AppendOnlyTableWriter::new(catalog, namespace_ident, table_name.clone());

    let result = writer
        .append_batch(batch.clone())
        .await
        .map_err(|e| WriterError::WriteFailure(format!("table '{}': {}", table_name, e)))?;

    // Log different outcomes for observability
    match result {
        AppendResult::TableCreated {
            ref data_file,
            ref schema,
        } => {
            tracing::info!(
                "✓ Created table '{}' with {} fields and wrote {} rows",
                table_name,
                schema.fields().len(),
                data_file.record_count()
            );
        }
        AppendResult::SchemaEvolved {
            ref data_file,
            ref old_schema,
            ref new_schema,
        } => {
            tracing::warn!(
                "✓ Schema evolved for '{}' from {} to {} fields, wrote {} rows",
                table_name,
                old_schema.fields().len(),
                new_schema.fields().len(),
                data_file.record_count()
            );
        }
        AppendResult::Appended { ref data_file } => {
            tracing::info!(
                "✓ Wrote {} rows to '{}' via AppendOnlyTableWriter",
                data_file.record_count(),
                table_name
            );
        }
    }

    // Return table path for logging
    Ok(format!("{}/{}", namespace, table_name))
}
