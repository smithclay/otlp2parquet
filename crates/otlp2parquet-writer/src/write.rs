//! Core write operations for Parquet output
//!
//! Writes OTLP Arrow RecordBatch data to partitioned Parquet files using OpenDAL.

use crate::error::{Result, WriterError};
use arrow::array::RecordBatch;
#[cfg(not(target_family = "wasm"))]
use bytes::Bytes;
#[cfg(not(target_family = "wasm"))]
use futures::future::BoxFuture;
use otlp2parquet_common::SignalType;
#[cfg(target_family = "wasm")]
use parquet::arrow::ArrowWriter;
#[cfg(not(target_family = "wasm"))]
use parquet::arrow::{async_writer::AsyncFileWriter, AsyncArrowWriter};
#[cfg(not(target_family = "wasm"))]
use parquet::errors::{ParquetError, Result as ParquetResult};
use std::borrow::Cow;
use time::OffsetDateTime;
use uuid::Uuid;

#[cfg(not(target_family = "wasm"))]
struct OpendalAsyncWriter(opendal::Writer);

#[cfg(not(target_family = "wasm"))]
impl OpendalAsyncWriter {
    fn new(inner: opendal::Writer) -> Self {
        Self(inner)
    }
}

#[cfg(not(target_family = "wasm"))]
impl AsyncFileWriter for OpendalAsyncWriter {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, ParquetResult<()>> {
        Box::pin(async move {
            self.0
                .write(bs)
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))?;
            Ok(())
        })
    }

    fn complete(&mut self) -> BoxFuture<'_, ParquetResult<()>> {
        Box::pin(async move {
            self.0
                .close()
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))?;
            Ok(())
        })
    }
}

/// Request parameters for writing a batch to storage
pub struct WriteBatchRequest<'a> {
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
}

/// Request parameters for writing multiple batches as separate row groups (zero-copy on WASM)
///
/// Note: This struct is available on all platforms for compilation, but the actual
/// `write_multi_batch` function only works on WASM targets.
pub struct WriteMultiBatchRequest<'a> {
    /// Arrow RecordBatches to write as separate row groups
    pub batches: &'a [RecordBatch],
    /// Type of OTLP signal (logs, traces, metrics)
    pub signal_type: SignalType,
    /// Metric type if signal_type is Metrics (gauge, sum, etc.)
    pub metric_type: Option<&'a str>,
    /// Service name for logging (not used for partitioning)
    pub service_name: &'a str,
    /// Timestamp in microseconds (from OTLP-to-Arrow nanos_to_micros conversion)
    pub timestamp_micros: i64,
}

/// Write a batch as a Parquet file
#[cfg(not(target_family = "wasm"))]
async fn write_plain_parquet(
    signal_type: SignalType,
    metric_type: Option<&str>,
    service_name: &str,
    timestamp_micros: i64,
    batch: &RecordBatch,
) -> Result<String> {
    // Get global storage operator
    let op = crate::storage::get_operator().ok_or_else(|| {
        WriterError::write_failure(
            "Storage operator not initialized. Call initialize_storage() with RuntimeConfig before writing."
                .to_string(),
        )
    })?;

    // Generate timestamped file path with partitioning
    // Format: {signal}/{service}/year={year}/month={month}/day={day}/hour={hour}/{uuid}.parquet
    let file_path =
        generate_parquet_path(signal_type, metric_type, service_name, timestamp_micros)?;

    tracing::debug!("Writing plain Parquet to path: {}", file_path);

    // Initialize streaming writer
    let op_writer = op.writer(&file_path).await.map_err(|e| {
        WriterError::write_failure(format!("Failed to open writer for '{}': {}", file_path, e))
    })?;
    let async_writer = OpendalAsyncWriter::new(op_writer);

    let mut parquet_writer = AsyncArrowWriter::try_new(
        async_writer,
        batch.schema(),
        Some(crate::encoding::writer_properties().clone()),
    )
    .map_err(|e| WriterError::write_failure(format!("Failed to create Parquet writer: {}", e)))?;

    parquet_writer
        .write(batch)
        .await
        .map_err(|e| WriterError::write_failure(format!("Failed to write RecordBatch: {}", e)))?;

    parquet_writer.finish().await.map_err(|e| {
        WriterError::write_failure(format!("Failed to close Parquet writer: {}", e))
    })?;

    let bytes_written = parquet_writer.bytes_written();

    let row_count = batch.num_rows();
    tracing::info!(
        "✓ Wrote {} rows to '{}' (plain Parquet, {} bytes)",
        row_count,
        file_path,
        bytes_written
    );

    Ok(file_path)
}

/// 48MB - WASM environments typically have ~128MB memory limit.
/// Parquet writing requires buffer + Arrow arrays in memory simultaneously.
/// 48MB leaves ~80MB for Arrow buffers, runtime, and other allocations.
#[cfg(target_family = "wasm")]
const WASM_MAX_BUFFER_BYTES: usize = 48 * 1024 * 1024;

/// WASM plain Parquet path: write into an in-memory buffer synchronously to avoid Send bounds.
#[cfg(target_family = "wasm")]
async fn write_plain_parquet(
    signal_type: SignalType,
    metric_type: Option<&str>,
    service_name: &str,
    timestamp_micros: i64,
    batch: &RecordBatch,
) -> Result<String> {
    // Guard against OOM: estimate batch size and reject if too large for WASM buffer
    let estimated_size = batch.get_array_memory_size();
    if estimated_size > WASM_MAX_BUFFER_BYTES {
        return Err(WriterError::write_failure(format!(
            "Batch too large for WASM memory: {} bytes (limit: {} bytes, {} rows). \
             Reduce batch size or use an upstream OTel Collector to split requests.",
            estimated_size,
            WASM_MAX_BUFFER_BYTES,
            batch.num_rows()
        )));
    }

    let op = crate::storage::get_operator().ok_or_else(|| {
        WriterError::write_failure(
            "Storage operator not initialized. Call initialize_storage() with RuntimeConfig before writing."
                .to_string(),
        )
    })?;

    let file_path =
        generate_parquet_path(signal_type, metric_type, service_name, timestamp_micros)?;

    tracing::debug!("Writing plain Parquet (WASM) to path: {}", file_path);

    let mut buffer = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(
            &mut buffer,
            batch.schema(),
            Some(crate::encoding::writer_properties().clone()),
        )
        .map_err(|e| {
            WriterError::write_failure(format!("Failed to create Parquet writer: {}", e))
        })?;
        writer.write(batch).map_err(|e| {
            WriterError::write_failure(format!("Failed to write RecordBatch: {}", e))
        })?;
        writer.close().map_err(|e| {
            WriterError::write_failure(format!("Failed to close Parquet writer: {}", e))
        })?;
    }

    let bytes_written = buffer.len();
    op.write(&file_path, buffer).await.map_err(|e| {
        WriterError::write_failure(format!(
            "Failed to upload Parquet to '{}': {}",
            file_path, e
        ))
    })?;

    let row_count = batch.num_rows();
    tracing::info!(
        "✓ Wrote {} rows to '{}' (plain Parquet, {} bytes, wasm buffer)",
        row_count,
        file_path,
        bytes_written
    );

    Ok(file_path)
}

/// WASM plain Parquet path: write multiple batches as separate row groups.
///
/// This achieves zero-copy by writing each batch as a separate Parquet row group,
/// avoiding the full data copy that concat_batches performs.
#[cfg(target_family = "wasm")]
async fn write_plain_parquet_multi(
    signal_type: SignalType,
    metric_type: Option<&str>,
    service_name: &str,
    timestamp_micros: i64,
    batches: &[RecordBatch],
) -> Result<String> {
    if batches.is_empty() {
        return Err(WriterError::write_failure(
            "No batches to write".to_string(),
        ));
    }

    // Guard against OOM: estimate total size and reject if too large for WASM buffer
    let estimated_size: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
    if estimated_size > WASM_MAX_BUFFER_BYTES {
        return Err(WriterError::write_failure(format!(
            "Batches too large for WASM memory: {} bytes (limit: {} bytes, {} batches). \
             Reduce batch size or use an upstream OTel Collector to split requests.",
            estimated_size,
            WASM_MAX_BUFFER_BYTES,
            batches.len()
        )));
    }

    let op = crate::storage::get_operator().ok_or_else(|| {
        WriterError::write_failure(
            "Storage operator not initialized. Call initialize_storage() with RuntimeConfig before writing."
                .to_string(),
        )
    })?;

    let file_path =
        generate_parquet_path(signal_type, metric_type, service_name, timestamp_micros)?;

    tracing::debug!(
        "Writing {} batches as separate row groups (WASM) to path: {}",
        batches.len(),
        file_path
    );

    // Validate all batches have the same schema
    let expected_schema = batches[0].schema();
    for (i, batch) in batches.iter().enumerate().skip(1) {
        if batch.schema() != expected_schema {
            return Err(WriterError::write_failure(format!(
                "Schema mismatch in batch {}: expected {} fields, got {}. \
                 All batches must have identical schemas.",
                i,
                expected_schema.fields().len(),
                batch.schema().fields().len()
            )));
        }
    }

    let mut buffer = Vec::new();
    let total_rows: usize;
    {
        let mut writer = ArrowWriter::try_new(
            &mut buffer,
            expected_schema,
            Some(crate::encoding::writer_properties().clone()),
        )
        .map_err(|e| {
            WriterError::write_failure(format!("Failed to create Parquet writer: {}", e))
        })?;

        // Write each batch as a separate row group
        for batch in batches {
            writer.write(batch).map_err(|e| {
                WriterError::write_failure(format!("Failed to write RecordBatch: {}", e))
            })?;
        }

        total_rows = batches.iter().map(|b| b.num_rows()).sum();

        writer.close().map_err(|e| {
            WriterError::write_failure(format!("Failed to close Parquet writer: {}", e))
        })?;
    }

    let bytes_written = buffer.len();
    op.write(&file_path, buffer).await.map_err(|e| {
        WriterError::write_failure(format!(
            "Failed to upload Parquet to '{}': {}",
            file_path, e
        ))
    })?;

    tracing::info!(
        "✓ Wrote {} rows ({} batches, {} row groups) to '{}' (plain Parquet, {} bytes, wasm buffer)",
        total_rows,
        batches.len(),
        batches.len(),
        file_path,
        bytes_written
    );

    Ok(file_path)
}

pub async fn write_batch(req: WriteBatchRequest<'_>) -> Result<String> {
    let row_count = req.batch.num_rows();

    tracing::debug!(
        "Writing {} rows (service: {}, signal: {:?}, metric: {:?})",
        row_count,
        req.service_name,
        req.signal_type,
        req.metric_type
    );

    write_plain_parquet(
        req.signal_type,
        req.metric_type,
        req.service_name,
        req.timestamp_micros,
        req.batch,
    )
    .await
}

/// Write multiple batches to storage.
///
/// - On WASM: Writes each batch as a separate Parquet row group (zero-copy).
/// - On non-WASM: Returns an error (use `write_batch` with concatenated batches instead).
#[cfg(target_family = "wasm")]
pub async fn write_multi_batch(req: WriteMultiBatchRequest<'_>) -> Result<String> {
    write_plain_parquet_multi(
        req.signal_type,
        req.metric_type,
        req.service_name,
        req.timestamp_micros,
        req.batches,
    )
    .await
}

/// Non-WASM stub for write_multi_batch - returns an error.
/// Use `write_batch` with concatenated batches on non-WASM platforms.
#[cfg(not(target_family = "wasm"))]
pub async fn write_multi_batch(_req: WriteMultiBatchRequest<'_>) -> Result<String> {
    Err(WriterError::write_failure(
        "write_multi_batch is only supported on WASM targets. Use write_batch with concatenated batches instead."
            .to_string(),
    ))
}

/// Generate a partitioned file path for plain Parquet files
///
/// Format: {prefix?}{signal_type}/{service}/year={year}/month={month}/day={day}/hour={hour}/{timestamp}-{uuid}.parquet
/// Example: logs/my-service/year=2025/month=01/day=15/hour=10/1736938800000000-<uuid>.parquet
/// Example with prefix: smoke-abc123/logs/my-service/year=2025/month=01/day=15/hour=10/1736938800000000-<uuid>.parquet
fn generate_parquet_path(
    signal_type: SignalType,
    metric_type: Option<&str>,
    service_name: &str,
    timestamp_micros: i64,
) -> Result<String> {
    let (year, month, day, hour) = partition_from_timestamp(timestamp_micros);

    let signal_prefix: Cow<'_, str> = match signal_type {
        SignalType::Logs => Cow::Borrowed("logs"),
        SignalType::Traces => Cow::Borrowed("traces"),
        SignalType::Metrics => {
            if let Some(mtype) = metric_type {
                Cow::Owned(format!("metrics/{}", mtype))
            } else {
                Cow::Borrowed("metrics")
            }
        }
    };

    let safe_service = sanitize_service_name(service_name);
    let suffix = Uuid::new_v4().simple();

    // Get storage prefix if configured (e.g., "smoke-abc123/")
    let storage_prefix = crate::storage::get_storage_prefix().unwrap_or("");

    Ok(format!(
        "{}{}/{}/year={}/month={:02}/day={:02}/hour={:02}/{}-{}.parquet",
        storage_prefix,
        signal_prefix,
        safe_service,
        year,
        month,
        day,
        hour,
        timestamp_micros,
        suffix
    ))
}

fn sanitize_service_name(service_name: &str) -> Cow<'_, str> {
    // Characters that are invalid or problematic in object storage paths
    const INVALID: [char; 10] = ['/', '\\', ' ', ':', '*', '?', '"', '<', '>', '|'];

    if service_name.is_empty() {
        return Cow::Borrowed("unknown-service");
    }

    if service_name.chars().any(|c| INVALID.contains(&c)) {
        let sanitized = service_name
            .chars()
            .map(|c| if INVALID.contains(&c) { '_' } else { c })
            .collect::<String>();
        Cow::Owned(sanitized)
    } else {
        Cow::Borrowed(service_name)
    }
}

/// Get fallback timestamp partition values when timestamp_micros is invalid.
/// Uses platform-appropriate time source (std::time on native, js_sys on WASM).
#[cfg(not(target_family = "wasm"))]
fn fallback_partition() -> (i32, u8, u8, u8) {
    let now = OffsetDateTime::now_utc();
    (now.year(), u8::from(now.month()), now.day(), now.hour())
}

#[cfg(target_family = "wasm")]
fn fallback_partition() -> (i32, u8, u8, u8) {
    // js_sys::Date::now() returns milliseconds since epoch
    let now_ms = js_sys::Date::now() as i64;
    let nanos = i128::from(now_ms).saturating_mul(1_000_000);
    match OffsetDateTime::from_unix_timestamp_nanos(nanos) {
        Ok(dt) => (dt.year(), u8::from(dt.month()), dt.day(), dt.hour()),
        Err(_) => {
            // Absolute fallback if even JS time conversion fails
            (2025, 1, 1, 0)
        }
    }
}

fn partition_from_timestamp(timestamp_micros: i64) -> (i32, u8, u8, u8) {
    if timestamp_micros <= 0 {
        return fallback_partition();
    }

    let nanos = i128::from(timestamp_micros).saturating_mul(1_000);
    match OffsetDateTime::from_unix_timestamp_nanos(nanos) {
        Ok(dt) => (dt.year(), u8::from(dt.month()), dt.day(), dt.hour()),
        Err(_) => fallback_partition(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        // Parse actual OTLP logs from testdata and verify timestamp column values
        use arrow::array::TimestampMillisecondArray;
        use otlp2records::{transform_logs, InputFormat};

        // Read the test data file
        let test_data =
            std::fs::read("../../testdata/logs.pb").expect("Failed to read testdata/logs.pb");

        // Parse OTLP request to Arrow
        let batch =
            transform_logs(&test_data, InputFormat::Protobuf).expect("Failed to transform logs");

        println!("\n=== Real OTLP Test Data Analysis ===");
        println!("Batch row count: {}", batch.num_rows());

        // Extract the timestamp column (stored in milliseconds)
        if let Some(ts_col) = batch.column_by_name("timestamp") {
            if let Some(ts_array) = ts_col.as_any().downcast_ref::<TimestampMillisecondArray>() {
                let timestamp_ms = ts_array.value(0);
                println!("Timestamp (milliseconds): {}", timestamp_ms);
                println!("Timestamp digits: {}", timestamp_ms.to_string().len());

                assert_eq!(
                    timestamp_ms.to_string().len(),
                    13,
                    "Timestamp should be 13 digits (milliseconds since epoch)"
                );
            } else {
                panic!("timestamp column is not TimestampMillisecondArray");
            }
        } else {
            panic!("No timestamp column found");
        }
    }

    #[test]
    fn path_generation_sanitizes_service() {
        let path =
            generate_parquet_path(SignalType::Logs, None, "svc /name", 1_736_938_800_000_000)
                .unwrap();
        assert!(path.starts_with("logs/svc__name/year="));
        assert!(path.contains("/month="));
        assert!(path.ends_with(".parquet"));
        // UUID suffix should provide uniqueness; ensure it's present.
        assert!(path.split('-').next_back().unwrap().ends_with(".parquet"));
    }
}
