// otlp2parquet-core - Platform-agnostic core logic
//
// This crate contains the PURE processing logic for converting
// OTLP logs to Parquet files. No I/O, no async, no runtime dependencies.
//
// Philosophy (Fred Brooks): "Separate essence from accident"
// - Essence: OTLP bytes → Parquet bytes conversion
// - Accident: Storage, networking, runtime (platform-specific)

use anyhow::{anyhow, Result};
use arrow::array::{Array, RecordBatch, StringArray, TimestampNanosecondArray};

pub mod otlp;
pub mod parquet;
pub mod schema;

// Re-export commonly used types
pub use schema::otel_logs_schema;

/// Metadata extracted from OTLP logs for partitioning
#[derive(Debug, Clone)]
pub struct LogMetadata {
    pub service_name: String,
    pub timestamp_nanos: i64,
}

/// Extract metadata from the first log record in a RecordBatch
///
/// This is used to determine the partition path for storing the Parquet file.
/// We use the first log's service name and timestamp since logs in a single
/// batch are typically from the same service and time period.
pub fn extract_metadata(batch: &RecordBatch) -> Result<LogMetadata> {
    if batch.num_rows() == 0 {
        return Ok(LogMetadata {
            service_name: "unknown".to_string(),
            timestamp_nanos: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
        });
    }

    // Extract service name (column 8)
    let service_name_col = batch
        .column(8)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow!("ServiceName column is not a StringArray"))?;

    let service_name = service_name_col.value(0).to_string();

    // Extract timestamp (column 0)
    let timestamp_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .ok_or_else(|| anyhow!("Timestamp column is not a TimestampNanosecondArray"))?;

    let timestamp_nanos = timestamp_col.value(0);

    Ok(LogMetadata {
        service_name,
        timestamp_nanos,
    })
}

/// Process OTLP log data and convert to Parquet format
///
/// This is the main entry point for the core processing logic.
/// Takes OTLP protobuf bytes, converts to Arrow RecordBatch, then to Parquet.
///
/// # Arguments
/// * `otlp_bytes` - Raw OTLP ExportLogsServiceRequest protobuf bytes
///
/// # Returns
/// * `Ok(Vec<u8>)` - Parquet file as bytes
/// * `Err` - If parsing or conversion fails
pub fn process_otlp_logs(otlp_bytes: &[u8]) -> Result<Vec<u8>> {
    let (parquet_bytes, _metadata) = process_otlp_logs_with_metadata(otlp_bytes)?;
    Ok(parquet_bytes)
}

/// Process OTLP log data and convert to Parquet format with metadata
///
/// Similar to `process_otlp_logs` but also returns metadata for partitioning.
///
/// # Arguments
/// * `otlp_bytes` - Raw OTLP ExportLogsServiceRequest protobuf bytes
///
/// # Returns
/// * `Ok((Vec<u8>, LogMetadata))` - Parquet file as bytes and metadata
/// * `Err` - If parsing or conversion fails
pub fn process_otlp_logs_with_metadata(otlp_bytes: &[u8]) -> Result<(Vec<u8>, LogMetadata)> {
    // Parse OTLP and convert to Arrow
    let mut converter = otlp::ArrowConverter::new();
    converter.add_from_proto_bytes(otlp_bytes)?;
    let batch = converter.finish()?;

    // Extract metadata for partitioning
    let metadata = extract_metadata(&batch)?;

    // Convert Arrow to Parquet
    let parquet_bytes = parquet::write_parquet(&batch)?;

    Ok((parquet_bytes, metadata))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_empty_logs() {
        // Create minimal valid OTLP request with no logs
        use otlp2parquet_proto::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;
        use prost::Message;

        let request = ExportLogsServiceRequest {
            resource_logs: vec![],
        };

        let mut bytes = Vec::new();
        request.encode(&mut bytes).unwrap();

        // Should successfully create empty Parquet file
        let result = process_otlp_logs(&bytes);
        if let Err(ref e) = result {
            eprintln!("Error: {:?}", e);
        }
        assert!(result.is_ok());

        let parquet_bytes = result.unwrap();
        assert!(!parquet_bytes.is_empty());
        // Parquet files start with "PAR1" magic bytes
        assert_eq!(&parquet_bytes[0..4], b"PAR1");
    }
}
