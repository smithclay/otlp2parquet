// otlp2parquet-core - Platform-agnostic core logic
//
// This crate contains the PURE transformation logic for converting
// OTLP logs to Arrow RecordBatch. No I/O, no async, no serialization.
//
// Philosophy (Fred Brooks): "Separate essence from accident"
// - Essence: OTLP → Arrow transformation (THIS CRATE)
// - Accident: Serialization format (Parquet), storage, networking (OTHER CRATES)
//
// Serialization (Arrow → Parquet) moved to: otlp2parquet-writer
// Batching/optimization moved to: otlp2parquet-batch

use anyhow::Result;
use arrow::array::RecordBatch;

pub mod otlp;
pub mod parquet;
pub mod schema;
pub mod types;

// Re-export commonly used types
pub use otlp::{InputFormat, LogMetadata};
pub use schema::otel_logs_schema;
pub use types::{Blake3Hash, ParquetWriteResult, SignalType};

/// Parse OTLP log data and convert to Arrow RecordBatch
///
/// This is the PURE core transformation logic: OTLP bytes → Arrow RecordBatch + metadata.
/// No I/O, no side effects, deterministic for the same input.
///
/// # Arguments
/// * `otlp_bytes` - Raw OTLP data in the specified format
/// * `format` - Input format (Protobuf, JSON, or JSONL)
///
/// # Returns
/// * `Ok((RecordBatch, LogMetadata))` - Arrow batch and extracted metadata
/// * `Err` - If parsing or conversion fails
///
/// # Philosophy
/// "Show me your tables, and I won't usually need your flowcharts; they'll be obvious."
/// - Fred Brooks
///
/// This function preserves information flow: we extract metadata during parsing
/// and return it alongside the Arrow data. No information is lost.
///
/// # Example
/// ```ignore
/// use otlp2parquet_core::{parse_otlp_to_arrow, InputFormat};
///
/// // Protobuf input (most common)
/// let protobuf_bytes = /* ... */;
/// let (batch, metadata) = parse_otlp_to_arrow(protobuf_bytes, InputFormat::Protobuf)?;
///
/// // JSON input
/// let json_bytes = br#"{"resourceLogs":[...]}"#;
/// let (batch, metadata) = parse_otlp_to_arrow(json_bytes, InputFormat::Json)?;
/// ```
pub fn parse_otlp_to_arrow(
    otlp_bytes: &[u8],
    format: InputFormat,
) -> Result<(RecordBatch, LogMetadata)> {
    // Parse the input format into an ExportLogsServiceRequest
    let request = otlp::parse_otlp_request(otlp_bytes, format)?;
    convert_request_to_arrow(&request)
}

/// Convert a parsed OTLP request directly into Arrow structures.
///
/// Consumers that already decoded the OTLP payload (e.g. batching code) can call
/// this helper to avoid the encode/decode round-trip required by
/// `parse_otlp_to_arrow`.
pub fn convert_request_to_arrow(
    request: &otlp2parquet_proto::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest,
) -> Result<(RecordBatch, LogMetadata)> {
    let capacity_hint = estimate_request_row_count(request);
    convert_request_to_arrow_with_capacity(request, capacity_hint)
}

/// Variant of `convert_request_to_arrow` that accepts an explicit capacity hint for pre-allocation.
///
/// Callers that already know approximately how many log records will be produced can use this to
/// avoid recomputing the count inside the converter.
pub fn convert_request_to_arrow_with_capacity(
    request: &otlp2parquet_proto::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest,
    capacity_hint: usize,
) -> Result<(RecordBatch, LogMetadata)> {
    let mut converter = otlp::ArrowConverter::with_capacity(capacity_hint.max(1));
    converter.add_from_request(request)?;
    converter.finish()
}

/// Estimate the number of log records contained in an OTLP request.
///
/// This is used to size Arrow builders before conversion. The count is exact because the OTLP
/// schema exposes log record vectors directly.
pub fn estimate_request_row_count(
    request: &otlp2parquet_proto::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest,
) -> usize {
    request
        .resource_logs
        .iter()
        .map(|resource_logs| {
            resource_logs
                .scope_logs
                .iter()
                .map(|scope_logs| scope_logs.log_records.len())
                .sum::<usize>()
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_empty_logs() {
        // Create minimal valid OTLP request with no logs
        use otlp2parquet_proto::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;
        use prost::Message;

        let request = ExportLogsServiceRequest {
            resource_logs: vec![],
        };

        let mut bytes = Vec::new();
        request.encode(&mut bytes).unwrap();

        // Should successfully parse to Arrow
        let result = parse_otlp_to_arrow(&bytes, InputFormat::Protobuf);
        if let Err(ref e) = result {
            eprintln!("Error: {:?}", e);
        }
        assert!(result.is_ok());

        let (batch, metadata) = result.unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(metadata.service_name.as_ref(), "");
        assert_eq!(metadata.record_count, 0);
    }
}
