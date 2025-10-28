// otlp2parquet-core - Platform-agnostic core logic
//
// This crate contains the PURE processing logic for converting
// OTLP logs to Parquet files. No I/O, no async, no runtime dependencies.
//
// Philosophy (Fred Brooks): "Separate essence from accident"
// - Essence: OTLP bytes → Parquet bytes conversion
// - Accident: Storage, networking, runtime (platform-specific)

use anyhow::Result;

pub mod otlp;
pub mod parquet;
pub mod schema;

// Re-export commonly used types
pub use schema::otel_logs_schema;

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
    // Parse OTLP and convert to Arrow
    let mut converter = otlp::ArrowConverter::new();
    converter.add_from_proto_bytes(otlp_bytes)?;
    let batch = converter.finish()?;

    // Convert Arrow to Parquet
    let parquet_bytes = parquet::write_parquet(&batch)?;

    Ok(parquet_bytes)
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
