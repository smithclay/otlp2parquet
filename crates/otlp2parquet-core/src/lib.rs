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
pub use otlp::LogMetadata;
pub use schema::otel_logs_schema;

/// Result of processing OTLP logs
///
/// Contains the Parquet file bytes and metadata extracted during parsing.
/// Metadata is needed by platform-specific storage layers for partitioning.
#[derive(Debug)]
pub struct ProcessingResult {
    pub parquet_bytes: Vec<u8>,
    pub service_name: String,
    pub timestamp_nanos: i64,
}

/// Process OTLP log data and convert to Parquet format
///
/// This is the PURE core processing logic: OTLP bytes → Parquet bytes + metadata.
/// No I/O, no side effects, deterministic for the same input.
///
/// # Arguments
/// * `otlp_bytes` - Raw OTLP ExportLogsServiceRequest protobuf bytes
///
/// # Returns
/// * `Ok(ProcessingResult)` - Parquet file bytes and metadata
/// * `Err` - If parsing or conversion fails
///
/// # Philosophy
/// "Show me your tables, and I won't usually need your flowcharts; they'll be obvious."
/// - Fred Brooks
///
/// This function preserves information flow: we extract metadata during parsing
/// and return it alongside the Parquet bytes. No information is lost and
/// re-extracted via brittle column indexing.
pub fn process_otlp_logs(otlp_bytes: &[u8]) -> Result<ProcessingResult> {
    // Parse OTLP and convert to Arrow (tracks metadata during parsing)
    let mut converter = otlp::ArrowConverter::new();
    converter.add_from_proto_bytes(otlp_bytes)?;
    let (batch, metadata) = converter.finish()?;

    // Convert Arrow to Parquet
    let parquet_bytes = parquet::write_parquet(&batch)?;

    Ok(ProcessingResult {
        parquet_bytes,
        service_name: metadata.service_name,
        timestamp_nanos: metadata.first_timestamp_nanos,
    })
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

        let processing_result = result.unwrap();
        assert!(!processing_result.parquet_bytes.is_empty());
        // Parquet files start with "PAR1" magic bytes
        assert_eq!(&processing_result.parquet_bytes[0..4], b"PAR1");
        assert_eq!(processing_result.service_name, "unknown");
    }
}
