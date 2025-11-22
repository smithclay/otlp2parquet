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
use icepick::{AppendOnlyTableWriter, AppendResult, TableWriterOptions};
use otlp2parquet_core::SignalType;

/// Request parameters for writing a batch to storage
pub struct WriteBatchRequest<'a> {
    /// Optional catalog instance for Iceberg table operations
    pub catalog: Option<&'a dyn Catalog>,
    /// Namespace for tables (e.g., "otlp")
    pub namespace: &'a str,
    /// Arrow RecordBatch to write
    pub batch: &'a RecordBatch,
    /// Type of OTLP signal (logs, traces, metrics)
    pub signal_type: SignalType,
    /// Metric type if signal_type is Metrics (gauge, sum, etc.)
    pub metric_type: Option<&'a str>,
    /// Service name for logging (not used for partitioning)
    pub service_name: &'a str,
    /// Timestamp in microseconds (from OTLP-to-Arrow nanos_to_micros conversion)
    pub timestamp_micros: i64,
    /// Optional Iceberg snapshot timestamp in milliseconds
    ///
    /// If provided, used for catalog commit timestamp. Should represent when the write
    /// occurs (current time), not when the data was generated. Use this on WASM platforms
    /// (Cloudflare Workers) where system time is unavailable.
    pub snapshot_timestamp_ms: Option<i64>,
}

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
/// # Returns
/// Table path in format "{namespace}/{table_name}" for logging purposes only
///
/// # Errors
/// Returns `WriterError::InvalidTableName` if signal/metric type combination is invalid
/// Returns `WriterError::WriteFailure` if icepick append operation fails or direct write fails
/// Returns `WriterError::WriteFailure` if catalog is None and storage operator is not initialized
pub async fn write_batch(req: WriteBatchRequest<'_>) -> Result<String> {
    let row_count = req.batch.num_rows();

    // Get table name based on signal type
    // e.g., "otel_logs", "otel_traces", "otel_metrics_gauge"
    let table_name = table_name_for_signal(req.signal_type, req.metric_type)?;

    tracing::debug!(
        "Writing {} rows to table '{}' (service: {}, signal: {:?}, catalog: {})",
        row_count,
        table_name,
        req.service_name,
        req.signal_type,
        if req.catalog.is_some() {
            "enabled"
        } else {
            "disabled"
        }
    );

    // If catalog is provided, use Iceberg. Otherwise, write plain Parquet to storage
    match req.catalog {
        Some(cat) => {
            // Create AppendOnlyTableWriter for this table
            // icepick will:
            // 1. Create table if it doesn't exist (deriving Iceberg schema from Arrow field_id)
            // 2. Write Parquet file with statistics
            // 3. Commit to catalog atomically
            let namespace_ident = NamespaceIdent::new(vec![req.namespace.to_string()]);

            // Create writer with options
            // Note: Iceberg snapshot timestamp should represent when the write occurs (current time),
            // not when the data was generated (timestamp_micros). The data timestamps are preserved
            // in the Parquet file columns.
            let mut writer = AppendOnlyTableWriter::new(cat, namespace_ident, table_name.clone());

            // If snapshot_timestamp_ms is provided (e.g., from Cloudflare Workers Date.now()),
            // use it for the catalog commit timestamp. Otherwise, icepick will use system time.
            if let Some(timestamp_ms) = req.snapshot_timestamp_ms {
                let options = TableWriterOptions::new().with_timestamp_ms(timestamp_ms);
                writer = writer.with_options(options);
            }

            let result = writer
                .append_batch(req.batch.clone())
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
            let file_path = generate_parquet_path(
                req.signal_type,
                req.metric_type,
                req.service_name,
                req.timestamp_micros,
            )?;

            tracing::debug!("Writing plain Parquet to path: {}", file_path);

            // Convert RecordBatch to Parquet bytes
            let parquet_bytes = write_batch_to_parquet_bytes(req.batch)?;
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
    Ok(format!("{}/{}", req.namespace, table_name))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_conversion_from_nanos_to_millis() {
        // Test typical OTLP timestamp (nanoseconds since Unix epoch)
        // Example: 2025-01-15 10:00:00 UTC = 1736938800 seconds
        let timestamp_nanos = 1736938800000000000i64; // 19 digits (nanoseconds)
        let timestamp_ms = timestamp_nanos / 1_000_000;

        // Should produce 13-digit milliseconds timestamp
        assert_eq!(timestamp_ms, 1736938800000);
        assert_eq!(
            timestamp_ms.to_string().len(),
            13,
            "Millisecond timestamp should have 13 digits"
        );
    }

    #[test]
    fn test_timestamp_from_error_message() {
        // These are the actual timestamps from the error message:
        // "Invalid snapshot timestamp 1760741572: before last updated timestamp 1763790139618"

        let invalid_timestamp = 1760741572i64; // 10 digits
        let last_updated = 1763790139618i64; // 13 digits

        println!(
            "Invalid timestamp: {} ({} digits)",
            invalid_timestamp,
            invalid_timestamp.to_string().len()
        );
        println!(
            "Last updated: {} ({} digits)",
            last_updated,
            last_updated.to_string().len()
        );

        // If this was supposed to be milliseconds, what was the original nanoseconds value?
        let reconstructed_nanos_from_invalid = invalid_timestamp * 1_000_000;
        println!(
            "If {} was the result of nanos/1_000_000, original nanos would be: {}",
            invalid_timestamp, reconstructed_nanos_from_invalid
        );

        // Check: 10 digits suggests this might be seconds, not milliseconds
        // If it was seconds, the original nanoseconds would be:
        let nanos_if_seconds = invalid_timestamp * 1_000_000_000;
        println!(
            "If {} was seconds, nanos would be: {} ({} digits)",
            invalid_timestamp,
            nanos_if_seconds,
            nanos_if_seconds.to_string().len()
        );

        // The issue: 1760741572 is 10 digits (seconds or short milliseconds)
        // Expected: 13 digits for milliseconds (like 1763790139618)
        assert_eq!(invalid_timestamp.to_string().len(), 10);
        assert_eq!(last_updated.to_string().len(), 13);
    }

    #[test]
    fn test_timestamp_scenarios() {
        // Scenario 1: Correct nanosecond input
        let correct_nanos = 1763790139618000000i64; // 19 digits
        let correct_ms = correct_nanos / 1_000_000;
        assert_eq!(correct_ms, 1763790139618); // 13 digits
        assert_eq!(correct_ms.to_string().len(), 13);

        // Scenario 2: What if input was already in milliseconds?
        let already_ms = 1763790139618i64; // 13 digits
        let double_converted = already_ms / 1_000_000;
        assert_eq!(double_converted, 1763790); // Oops! Now only 7 digits
        println!(
            "If input was already milliseconds ({}), dividing by 1_000_000 gives: {} ({} digits)",
            already_ms,
            double_converted,
            double_converted.to_string().len()
        );

        // Scenario 3: What if input was in seconds?
        let seconds = 1763790139i64; // 10 digits
        let seconds_to_ms_wrong = seconds / 1_000_000;
        println!(
            "If input was seconds ({}), dividing by 1_000_000 gives: {} ({} digits)",
            seconds,
            seconds_to_ms_wrong,
            seconds_to_ms_wrong.to_string().len()
        );

        // Scenario 4: What if input was in microseconds?
        let micros = 1763790139618000i64; // 16 digits
        let micros_to_ms = micros / 1_000_000;
        assert_eq!(micros_to_ms, 1763790139); // 10 digits - matches the error!
        println!(
            "If input was microseconds ({}), dividing by 1_000_000 gives: {} ({} digits)",
            micros,
            micros_to_ms,
            micros_to_ms.to_string().len()
        );
    }

    #[test]
    fn test_extract_timestamp_from_arrow_batch() {
        // This test would verify the actual extraction from RecordBatch
        // We need to check if extract_first_timestamp returns nanoseconds as expected
        use arrow::array::{ArrayRef, TimestampNanosecondArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        // Create a test RecordBatch with timestamp in nanoseconds
        let timestamp_nanos = 1736938800000000000i64; // 19 digits (nanoseconds)
        let timestamp_array = TimestampNanosecondArray::from(vec![timestamp_nanos]);
        let dummy_array = arrow::array::StringArray::from(vec!["test"]);

        let schema = Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("service", DataType::Utf8, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(timestamp_array) as ArrayRef,
                Arc::new(dummy_array) as ArrayRef,
            ],
        )
        .unwrap();

        // Extract timestamp using the same logic as handlers.rs
        if let Some(array) = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
        {
            let value = array.value(0);
            assert_eq!(value, timestamp_nanos);
            assert_eq!(
                value.to_string().len(),
                19,
                "Extracted timestamp should be 19 digits (nanoseconds)"
            );

            // Now test the conversion
            let timestamp_ms = value / 1_000_000;
            assert_eq!(timestamp_ms, 1736938800000);
            assert_eq!(
                timestamp_ms.to_string().len(),
                13,
                "Converted timestamp should be 13 digits (milliseconds)"
            );
        } else {
            panic!("Failed to extract timestamp from batch");
        }
    }

    #[test]
    fn test_real_otlp_timestamp_from_testdata() {
        // Parse actual OTLP logs from testdata to see what timestamp values we get
        use otlp2parquet_core::otlp;

        // Read the test data file
        let test_data =
            std::fs::read("../../testdata/logs.pb").expect("Failed to read testdata/logs.pb");

        // Parse OTLP request
        let request =
            otlp::parse_otlp_request(&test_data, otlp2parquet_core::InputFormat::Protobuf)
                .expect("Failed to parse OTLP request");

        println!("\n=== Real OTLP Test Data Analysis ===");
        println!("Resource logs count: {}", request.resource_logs.len());

        // Get first resource logs
        if let Some(resource_logs) = request.resource_logs.first() {
            if let Some(scope_logs) = resource_logs.scope_logs.first() {
                if let Some(log_record) = scope_logs.log_records.first() {
                    let time_unix_nano = log_record.time_unix_nano;

                    println!("Raw OTLP time_unix_nano: {}", time_unix_nano);
                    println!("Timestamp digits: {}", time_unix_nano.to_string().len());

                    // Test all three conversion scenarios
                    println!("\n--- Conversion Analysis ---");

                    // Scenario 1: If this is nanoseconds (expected)
                    let ms_from_nano = time_unix_nano / 1_000_000;
                    println!(
                        "If nanoseconds -> milliseconds (/ 1_000_000): {} ({} digits)",
                        ms_from_nano,
                        ms_from_nano.to_string().len()
                    );

                    // Scenario 2: If this is microseconds (hypothesis)
                    let ms_from_micro = time_unix_nano / 1_000;
                    println!(
                        "If microseconds -> milliseconds (/ 1_000): {} ({} digits)",
                        ms_from_micro,
                        ms_from_micro.to_string().len()
                    );

                    // Scenario 3: If this is already milliseconds
                    println!(
                        "If already milliseconds (/ 1): {} ({} digits)",
                        time_unix_nano,
                        time_unix_nano.to_string().len()
                    );

                    // Check if this matches the error pattern
                    if ms_from_nano.to_string().len() == 10 {
                        println!("\n⚠️  FOUND THE BUG!");
                        println!(
                            "   Current conversion (nanos / 1_000_000) produces 10 digits: {}",
                            ms_from_nano
                        );
                        println!(
                            "   This suggests the input '{}' is in MICROSECONDS, not nanoseconds",
                            time_unix_nano
                        );
                        println!(
                            "   Correct conversion should be: {} / 1_000 = {} (13 digits)",
                            time_unix_nano, ms_from_micro
                        );
                    } else if ms_from_nano.to_string().len() == 13 {
                        println!("\n✓ Conversion is correct");
                        println!("   Input '{}' is in nanoseconds", time_unix_nano);
                        println!(
                            "   Output '{}' is in milliseconds (13 digits)",
                            ms_from_nano
                        );
                    }
                }
            }
        }
    }
}
