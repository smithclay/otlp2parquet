// otlp2parquet-core - Platform-agnostic core logic
//
// This crate contains the PURE processing logic for converting
// OTLP logs to Parquet files. No I/O, no async, no runtime dependencies.
//
// Philosophy (Fred Brooks): "Separate essence from accident"
// - Essence: OTLP bytes → Parquet bytes conversion
// - Accident: Storage, networking, runtime (platform-specific)

use ::parquet::arrow::arrow_writer::ArrowWriter;
use anyhow::Result;
use arrow::array::RecordBatch;
use std::io::Write;

pub mod otlp;
pub mod parquet;
pub mod schema;

// Re-export commonly used types
pub use otlp::{InputFormat, LogMetadata};
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

/// Controls batching and streaming behaviour when encoding Parquet output.
#[derive(Debug, Clone)]
pub struct ProcessingOptions {
    /// Maximum number of rows to accumulate before flushing a record batch to Parquet.
    pub max_rows_per_batch: usize,
}

impl Default for ProcessingOptions {
    fn default() -> Self {
        Self {
            max_rows_per_batch: 32 * 1024, // 32k rows aligns with analytic best practices
        }
    }
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
    process_otlp_logs_with_options(
        otlp_bytes,
        InputFormat::Protobuf,
        &ProcessingOptions::default(),
    )
}

/// Process OTLP log data and stream the resulting Parquet bytes into `writer`
///
/// Returns log metadata alongside writing the Parquet file into the caller
/// provided sink.
pub fn process_otlp_logs_into<W>(otlp_bytes: &[u8], writer: &mut W) -> Result<otlp::LogMetadata>
where
    W: Write + Send,
{
    process_otlp_logs_into_with_options(
        otlp_bytes,
        InputFormat::Protobuf,
        writer,
        &ProcessingOptions::default(),
    )
}

/// Convert raw OTLP log protobuf bytes into an Arrow `RecordBatch` plus metadata.
///
/// Exposed for runtimes that need to inspect metadata before deciding where and
/// how to write the resulting Parquet bytes.
pub fn otlp_to_record_batch(otlp_bytes: &[u8]) -> Result<(RecordBatch, otlp::LogMetadata)> {
    let mut converter = otlp::ArrowConverter::new();
    converter.add_from_proto_bytes(otlp_bytes)?;
    converter.finish()
}

// ============================================================================
// Format-aware API (Phase 1: JSON/JSONL support)
// ============================================================================

/// Process OTLP log data in the specified format and convert to Parquet
///
/// This is the format-aware version of `process_otlp_logs()` that supports
/// multiple input formats: Protobuf (default), JSON, and JSONL.
///
/// # Arguments
/// * `otlp_bytes` - Raw OTLP data in the specified format
/// * `format` - Input format (Protobuf, JSON, or JSONL)
///
/// # Returns
/// * `Ok(ProcessingResult)` - Parquet file bytes and metadata
/// * `Err` - If parsing or conversion fails
///
/// # Example
/// ```ignore
/// use otlp2parquet_core::{process_otlp_logs_with_format, InputFormat};
///
/// // JSON input
/// let json_bytes = br#"{"resourceLogs":[...]}"#;
/// let result = process_otlp_logs_with_format(json_bytes, InputFormat::Json)?;
///
/// // JSONL input (newline-delimited)
/// let jsonl_bytes = b"{\"resourceLogs\":[...]}\n{\"resourceLogs\":[...]}";
/// let result = process_otlp_logs_with_format(jsonl_bytes, InputFormat::Jsonl)?;
/// ```
pub fn process_otlp_logs_with_format(
    otlp_bytes: &[u8],
    format: InputFormat,
) -> Result<ProcessingResult> {
    process_otlp_logs_with_options(otlp_bytes, format, &ProcessingOptions::default())
}

/// Process OTLP log data with format detection and stream the resulting Parquet bytes
///
/// Format-aware version of `process_otlp_logs_into()` that supports JSON and JSONL.
///
/// # Arguments
/// * `otlp_bytes` - Raw OTLP data in the specified format
/// * `format` - Input format (Protobuf, JSON, or JSONL)
/// * `writer` - Destination for Parquet bytes
///
/// # Returns
/// * `Ok(LogMetadata)` - Metadata extracted during parsing
/// * `Err` - If parsing or conversion fails
pub fn process_otlp_logs_into_with_format<W>(
    otlp_bytes: &[u8],
    format: InputFormat,
    writer: &mut W,
) -> Result<otlp::LogMetadata>
where
    W: Write + Send,
{
    process_otlp_logs_into_with_options(otlp_bytes, format, writer, &ProcessingOptions::default())
}

/// Convert OTLP log data in the specified format into an Arrow `RecordBatch` plus metadata
///
/// Format-aware version of `otlp_to_record_batch()` that supports JSON and JSONL.
///
/// # Arguments
/// * `otlp_bytes` - Raw OTLP data in the specified format
/// * `format` - Input format (Protobuf, JSON, or JSONL)
///
/// # Returns
/// * `Ok((RecordBatch, LogMetadata))` - Arrow batch and extracted metadata
/// * `Err` - If parsing or conversion fails
pub fn otlp_to_record_batch_with_format(
    otlp_bytes: &[u8],
    format: InputFormat,
) -> Result<(RecordBatch, otlp::LogMetadata)> {
    // Parse the input format into an ExportLogsServiceRequest
    let request = otlp::parse_otlp_request(otlp_bytes, format)?;

    // Convert to Arrow using the existing converter
    let mut converter = otlp::ArrowConverter::new();
    converter.add_from_request(&request)?;
    converter.finish()
}

/// Process OTLP log data with custom options, returning Parquet bytes and metadata.
pub fn process_otlp_logs_with_options(
    otlp_bytes: &[u8],
    format: InputFormat,
    options: &ProcessingOptions,
) -> Result<ProcessingResult> {
    let mut parquet_bytes = Vec::new();
    let metadata =
        process_otlp_logs_into_with_options(otlp_bytes, format, &mut parquet_bytes, options)?;

    Ok(ProcessingResult {
        parquet_bytes,
        service_name: metadata.service_name,
        timestamp_nanos: metadata.first_timestamp_nanos,
    })
}

/// Stream OTLP log data with custom options into an arbitrary writer.
pub fn process_otlp_logs_into_with_options<W>(
    otlp_bytes: &[u8],
    format: InputFormat,
    writer: &mut W,
    options: &ProcessingOptions,
) -> Result<otlp::LogMetadata>
where
    W: Write + Send,
{
    let request = otlp::parse_otlp_request(otlp_bytes, format)?;
    stream_request_into_writer(&request, writer, options)
}

struct MetadataAccumulator {
    service_name: Option<String>,
    first_timestamp_nanos: Option<i64>,
    record_count: usize,
}

impl MetadataAccumulator {
    fn new() -> Self {
        Self {
            service_name: None,
            first_timestamp_nanos: None,
            record_count: 0,
        }
    }

    fn update(&mut self, meta: &otlp::LogMetadata) {
        if let Some(ts) = (meta.first_timestamp_nanos != 0).then_some(meta.first_timestamp_nanos) {
            self.first_timestamp_nanos = match self.first_timestamp_nanos {
                Some(existing) if existing <= ts => Some(existing),
                _ => Some(ts),
            };
        }

        let incoming = meta.service_name.trim();
        let replace = match self.service_name.as_deref() {
            Some(existing) if existing == "unknown" && !incoming.is_empty() => true,
            Some(existing) => existing.trim().is_empty() && !incoming.is_empty(),
            None => true,
        };
        if replace {
            self.service_name = Some(meta.service_name.clone());
        }

        self.record_count += meta.record_count;
    }

    fn finish(self) -> otlp::LogMetadata {
        let service_name = self.service_name.unwrap_or_else(|| "unknown".to_string());
        let service_name = if service_name.trim().is_empty() {
            "unknown".to_string()
        } else {
            service_name
        };

        otlp::LogMetadata {
            service_name,
            first_timestamp_nanos: self.first_timestamp_nanos.unwrap_or(0),
            record_count: self.record_count,
        }
    }
}

fn stream_request_into_writer<W>(
    request: &otlp2parquet_proto::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest,
    writer: &mut W,
    options: &ProcessingOptions,
) -> Result<otlp::LogMetadata>
where
    W: Write + Send,
{
    let schema = schema::otel_logs_schema_arc();
    let mut arrow_writer = ArrowWriter::try_new(
        writer,
        schema,
        Some(crate::parquet::writer::writer_properties().clone()),
    )?;

    let mut accumulator = MetadataAccumulator::new();
    write_request_into_arrow_writer_internal(
        &mut arrow_writer,
        request,
        options,
        &mut accumulator,
    )?;

    arrow_writer.close()?;

    Ok(accumulator.finish())
}

/// Stream a pre-parsed OTLP request into an existing Arrow writer without closing it.
pub fn write_request_into_arrow_writer<W>(
    arrow_writer: &mut ArrowWriter<W>,
    request: &otlp2parquet_proto::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest,
    options: &ProcessingOptions,
) -> Result<otlp::LogMetadata>
where
    W: Write + Send,
{
    let mut accumulator = MetadataAccumulator::new();
    write_request_into_arrow_writer_internal(arrow_writer, request, options, &mut accumulator)?;
    Ok(accumulator.finish())
}

fn write_request_into_arrow_writer_internal<W>(
    arrow_writer: &mut ArrowWriter<W>,
    request: &otlp2parquet_proto::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest,
    options: &ProcessingOptions,
    accumulator: &mut MetadataAccumulator,
) -> Result<()>
where
    W: Write + Send,
{
    let mut converter = otlp::ArrowConverter::new();

    converter.add_from_request_with_flush(
        request,
        options.max_rows_per_batch,
        &mut |batch, meta| {
            accumulator.update(&meta);
            arrow_writer.write(&batch)?;
            Ok(())
        },
    )?;

    converter.flush(&mut |batch, meta| {
        accumulator.update(&meta);
        arrow_writer.write(&batch)?;
        Ok(())
    })?;

    Ok(())
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
