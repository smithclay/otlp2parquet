//! Core write operations for Parquet output.
//!
//! Writes OTLP Arrow RecordBatch data to partitioned Parquet files using OpenDAL.

use crate::SignalType;
use arrow::array::RecordBatch;
use otlp2records::output::to_parquet_bytes;
use std::borrow::Cow;
use time::OffsetDateTime;
use uuid::Uuid;

use super::error::{Result, WriterError};

/// Request parameters for writing a batch to storage.
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

/// Write a batch as a Parquet file.
async fn write_plain_parquet(
    signal_type: SignalType,
    metric_type: Option<&str>,
    service_name: &str,
    timestamp_micros: i64,
    batch: &RecordBatch,
) -> Result<String> {
    let op = super::storage::get_operator().ok_or_else(|| {
        WriterError::write_failure(
            "Storage operator not initialized. Call initialize_storage() with RuntimeConfig before writing."
                .to_string(),
        )
    })?;

    let file_path =
        generate_parquet_path(signal_type, metric_type, service_name, timestamp_micros)?;

    tracing::debug!("Writing plain Parquet to path: {}", file_path);

    let parquet_bytes = to_parquet_bytes(batch).map_err(|e| {
        WriterError::write_failure(format!("Failed to encode Parquet bytes: {}", e))
    })?;
    let bytes_written = parquet_bytes.len();

    op.write(&file_path, parquet_bytes).await.map_err(|e| {
        WriterError::write_failure(format!(
            "Failed to write parquet bytes to '{}': {}",
            file_path, e
        ))
    })?;

    let row_count = batch.num_rows();
    tracing::info!(
        "✓ Wrote {} rows to '{}' (plain Parquet, {} bytes)",
        row_count,
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

/// Generate a partitioned file path for plain Parquet files.
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

    let storage_prefix = super::storage::get_storage_prefix().unwrap_or("");

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

fn fallback_partition() -> (i32, u8, u8, u8) {
    let now = OffsetDateTime::now_utc();
    (now.year(), u8::from(now.month()), now.day(), now.hour())
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
        use arrow::array::{ArrayRef, TimestampNanosecondArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let timestamp_nanos = 1_736_938_800_000_000_000i64;
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

        if let Some(array) = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
        {
            let value = array.value(0);
            assert_eq!(value, timestamp_nanos);
            assert_eq!(value.to_string().len(), 19);

            let timestamp_ms = value / 1_000_000;
            assert_eq!(timestamp_ms, 1_736_938_800_000);
            assert_eq!(timestamp_ms.to_string().len(), 13);
        } else {
            panic!("Failed to extract timestamp from batch");
        }
    }

    #[test]
    fn test_real_otlp_timestamp_from_testdata() {
        use arrow::array::TimestampMillisecondArray;
        use otlp2records::{transform_logs, InputFormat};

        let test_data_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("testdata")
            .join("logs.pb");
        let test_data = std::fs::read(&test_data_path).expect("Failed to read testdata/logs.pb");

        let batch =
            transform_logs(&test_data, InputFormat::Protobuf).expect("Failed to transform logs");

        if let Some(ts_col) = batch.column_by_name("timestamp") {
            if let Some(ts_array) = ts_col.as_any().downcast_ref::<TimestampMillisecondArray>() {
                let timestamp_ms = ts_array.value(0);
                assert_eq!(timestamp_ms.to_string().len(), 13);
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
        assert!(path.split('-').next_back().unwrap().ends_with(".parquet"));
    }
}
