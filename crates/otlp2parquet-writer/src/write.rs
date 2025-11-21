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
/// * `catalog` - Optional catalog instance for Iceberg table operations. If None, writes plain Parquet to storage.
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
/// Returns `WriterError::WriteFailure` if icepick append operation fails or direct write fails
/// Returns `WriterError::WriteFailure` if catalog is None and storage operator is not initialized
pub async fn write_batch(
    catalog: Option<&dyn Catalog>,
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
        "Writing {} rows to table '{}' (service: {}, signal: {:?}, catalog: {})",
        row_count,
        table_name,
        service_name,
        signal_type,
        if catalog.is_some() {
            "enabled"
        } else {
            "disabled"
        }
    );

    // If catalog is provided, use Iceberg. Otherwise, write plain Parquet to storage
    match catalog {
        Some(cat) => {
            // Create AppendOnlyTableWriter for this table
            // icepick will:
            // 1. Create table if it doesn't exist (deriving Iceberg schema from Arrow field_id)
            // 2. Write Parquet file with statistics
            // 3. Commit to catalog atomically
            let namespace_ident = NamespaceIdent::new(vec![namespace.to_string()]);
            let writer = AppendOnlyTableWriter::new(cat, namespace_ident, table_name.clone());

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
        }
        None => {
            // No catalog - write plain Parquet file to storage using global operator
            // This path is used by Cloudflare Workers when no R2 Data Catalog is configured
            let op = crate::storage::get_operator().ok_or_else(|| {
                WriterError::WriteFailure(
                    "Storage operator not initialized. Call initialize_storage() before writing without catalog.".to_string()
                )
            })?;

            // Generate timestamped file path with partitioning
            // Format: {signal}/{service}/year={year}/month={month}/day={day}/hour={hour}/{uuid}.parquet
            let file_path =
                generate_parquet_path(signal_type, metric_type, service_name, _timestamp_nanos)?;

            tracing::debug!("Writing plain Parquet to path: {}", file_path);

            // Convert RecordBatch to Parquet bytes
            let parquet_bytes = write_batch_to_parquet_bytes(batch)?;
            let bytes_written = parquet_bytes.len();

            // Write to storage using OpenDAL operator
            op.write(&file_path, parquet_bytes).await.map_err(|e| {
                WriterError::WriteFailure(format!("Failed to write to storage: {}", e))
            })?;

            tracing::info!(
                "✓ Wrote {} rows to '{}' (plain Parquet, no catalog, {} bytes)",
                row_count,
                file_path,
                bytes_written
            );
        }
    }

    // Return table path for logging
    Ok(format!("{}/{}", namespace, table_name))
}

/// Generate a partitioned file path for plain Parquet files
///
/// Format: {signal_type}/{service}/year={year}/month={month}/day={day}/hour={hour}/{timestamp}-{random}.parquet
/// Example: logs/my-service/year=2025/month=01/day=15/hour=10/1736938800000000000-a1b2c3d4.parquet
fn generate_parquet_path(
    signal_type: SignalType,
    metric_type: Option<&str>,
    service_name: &str,
    timestamp_nanos: i64,
) -> Result<String> {
    use chrono::{DateTime, Datelike, Timelike, Utc};

    // Convert nanoseconds to DateTime
    let dt = DateTime::from_timestamp(
        timestamp_nanos / 1_000_000_000,
        (timestamp_nanos % 1_000_000_000) as u32,
    )
    .unwrap_or_else(Utc::now);

    // Build signal prefix (e.g., "logs", "traces", "metrics/gauge")
    let signal_prefix = match signal_type {
        SignalType::Logs => "logs".to_string(),
        SignalType::Traces => "traces".to_string(),
        SignalType::Metrics => {
            if let Some(mtype) = metric_type {
                format!("metrics/{}", mtype)
            } else {
                "metrics".to_string()
            }
        }
    };

    // Sanitize service name (replace invalid path characters)
    let safe_service = service_name.replace(['/', '\\', ' '], "_");

    // Generate random suffix for uniqueness
    let random_suffix = blake3::hash(format!("{}{}", timestamp_nanos, service_name).as_bytes())
        .to_hex()
        .chars()
        .take(8)
        .collect::<String>();

    // Build partitioned path
    let path = format!(
        "{}/{}/year={}/month={:02}/day={:02}/hour={:02}/{}-{}.parquet",
        signal_prefix,
        safe_service,
        dt.year(),
        dt.month(),
        dt.day(),
        dt.hour(),
        timestamp_nanos,
        random_suffix
    );

    Ok(path)
}

/// Convert a RecordBatch to Parquet bytes
fn write_batch_to_parquet_bytes(batch: &RecordBatch) -> Result<Vec<u8>> {
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    // Create in-memory buffer
    let mut buffer = Vec::new();

    // Create Parquet writer with compression
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();

    let mut writer =
        ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props)).map_err(|e| {
            WriterError::WriteFailure(format!("Failed to create Parquet writer: {}", e))
        })?;

    // Write the batch
    writer
        .write(batch)
        .map_err(|e| WriterError::WriteFailure(format!("Failed to write RecordBatch: {}", e)))?;

    // Close writer to flush data
    writer
        .close()
        .map_err(|e| WriterError::WriteFailure(format!("Failed to close Parquet writer: {}", e)))?;

    Ok(buffer)
}
