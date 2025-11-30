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

use anyhow::Result;
use arrow::array::RecordBatch;

pub mod batch;
pub mod config;
pub mod otlp;
pub mod parquet;
pub mod schema;
pub mod types;

// Re-export commonly used types
pub use otlp::{InputFormat, LogMetadata};
pub use schema::{otel_logs_schema, otel_metrics_gauge_schema};
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

// WASM bindings for browser usage
#[cfg(feature = "wasm")]
pub mod wasm {
    use super::*;
    use wasm_bindgen::prelude::*;

    /// Convert OTLP JSON logs to Arrow IPC bytes
    ///
    /// Input: JSON string in OTLP ExportLogsServiceRequest format
    /// Output: Arrow IPC stream bytes (can be loaded by DuckDB)
    #[wasm_bindgen]
    pub fn logs_json_to_arrow_ipc(otlp_json: &str) -> std::result::Result<Vec<u8>, JsError> {
        let (batch, _metadata) = parse_otlp_to_arrow(otlp_json.as_bytes(), InputFormat::Json)
            .map_err(|e| JsError::new(&e.to_string()))?;

        batch_to_ipc_bytes(&batch).map_err(|e| JsError::new(&e.to_string()))
    }

    /// Convert OTLP JSON traces to Arrow IPC bytes
    #[wasm_bindgen]
    pub fn traces_json_to_arrow_ipc(otlp_json: &str) -> std::result::Result<Vec<u8>, JsError> {
        use otlp::traces;

        let request = traces::parse_otlp_trace_request(otlp_json.as_bytes(), InputFormat::Json)
            .map_err(|e| JsError::new(&e.to_string()))?;

        let (batches, _metadata) = traces::TraceArrowConverter::convert(&request)
            .map_err(|e| JsError::new(&e.to_string()))?;

        // Return the first batch (demo assumes single batch)
        let batch = batches
            .into_iter()
            .next()
            .ok_or_else(|| JsError::new("No batches produced from trace conversion"))?;

        batch_to_ipc_bytes(&batch).map_err(|e| JsError::new(&e.to_string()))
    }

    /// Convert OTLP JSON metrics to Arrow IPC bytes (returns gauge data only for demo)
    #[wasm_bindgen]
    pub fn metrics_json_to_arrow_ipc(otlp_json: &str) -> std::result::Result<Vec<u8>, JsError> {
        use otlp::metrics;

        let request = metrics::parse_otlp_request(otlp_json.as_bytes(), InputFormat::Json)
            .map_err(|e| JsError::new(&e.to_string()))?;

        let converter = metrics::ArrowConverter::new();
        let (batches, _metadata) = converter
            .convert(request)
            .map_err(|e| JsError::new(&e.to_string()))?;

        // Return gauge batch if available, otherwise empty
        if let Some((_, batch)) = batches.into_iter().find(|(name, _)| name == "gauge") {
            batch_to_ipc_bytes(&batch).map_err(|e| JsError::new(&e.to_string()))
        } else {
            // Return empty batch with gauge schema
            let schema = crate::schema::otel_metrics_gauge_schema();
            let empty = RecordBatch::new_empty(std::sync::Arc::new(schema));
            batch_to_ipc_bytes(&empty).map_err(|e| JsError::new(&e.to_string()))
        }
    }

    fn batch_to_ipc_bytes(batch: &RecordBatch) -> Result<Vec<u8>> {
        use arrow::ipc::writer::StreamWriter;
        let mut buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())?;
            writer.write(batch)?;
            writer.finish()?;
        }
        Ok(buf)
    }
}
