// Input format detection and parsing for OTLP logs
//
// Supports:
// - Protobuf (native OTLP format)
// - JSON (OTLP spec compliance)
// - JSONL (newline-delimited JSON, bonus feature)

use anyhow::{anyhow, Context, Result};
use otlp2parquet_proto::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;
use prost::Message;

/// Supported input formats for OTLP logs
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputFormat {
    /// Binary protobuf (default, most efficient)
    Protobuf,
    /// JSON (OTLP spec required)
    Json,
    /// Newline-delimited JSON (bonus feature for bulk ingestion)
    Jsonl,
}

impl InputFormat {
    /// Detect format from Content-Type header
    ///
    /// Defaults to Protobuf if header is missing or unrecognized for backward compatibility.
    pub fn from_content_type(content_type: Option<&str>) -> Self {
        match content_type {
            Some(ct) => {
                let ct_lower = ct.to_lowercase();
                // Check JSONL before JSON since "application/jsonl" contains "json"
                if ct_lower.contains("application/x-ndjson")
                    || ct_lower.contains("application/jsonl")
                {
                    Self::Jsonl
                } else if ct_lower.contains("application/json") {
                    Self::Json
                } else if ct_lower.contains("application/x-protobuf")
                    || ct_lower.contains("application/protobuf")
                {
                    Self::Protobuf
                } else {
                    // Default to protobuf for backward compatibility
                    Self::Protobuf
                }
            }
            None => Self::Protobuf, // Default to protobuf when no header present
        }
    }

    /// Get the canonical Content-Type string for this format
    pub fn content_type(&self) -> &'static str {
        match self {
            Self::Protobuf => "application/x-protobuf",
            Self::Json => "application/json",
            Self::Jsonl => "application/x-ndjson",
        }
    }
}

/// Parse OTLP logs from bytes in the specified format
pub fn parse_otlp_request(bytes: &[u8], format: InputFormat) -> Result<ExportLogsServiceRequest> {
    match format {
        InputFormat::Protobuf => parse_protobuf(bytes),
        InputFormat::Json => parse_json(bytes),
        InputFormat::Jsonl => parse_jsonl(bytes),
    }
}

/// Parse OTLP logs from protobuf bytes
fn parse_protobuf(bytes: &[u8]) -> Result<ExportLogsServiceRequest> {
    ExportLogsServiceRequest::decode(bytes).context("Failed to decode OTLP protobuf message")
}

/// Parse OTLP logs from JSON bytes
fn parse_json(bytes: &[u8]) -> Result<ExportLogsServiceRequest> {
    serde_json::from_slice(bytes).context("Failed to parse OTLP JSON message")
}

/// Parse OTLP logs from JSONL (newline-delimited JSON) bytes
///
/// Each line should be a valid JSON object. Lines are parsed individually
/// and merged into a single ExportLogsServiceRequest.
fn parse_jsonl(bytes: &[u8]) -> Result<ExportLogsServiceRequest> {
    let text = std::str::from_utf8(bytes).context("JSONL input is not valid UTF-8")?;

    let mut merged = ExportLogsServiceRequest {
        resource_logs: Vec::new(),
    };

    for (line_num, line) in text.lines().enumerate() {
        // Skip empty lines
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        // Parse each line as an ExportLogsServiceRequest
        let request: ExportLogsServiceRequest =
            serde_json::from_str(trimmed).with_context(|| {
                format!(
                    "Failed to parse JSONL line {} as OTLP message",
                    line_num + 1
                )
            })?;

        // Merge resource_logs into the accumulated result
        merged.resource_logs.extend(request.resource_logs);
    }

    // Validate that we parsed at least one log
    if merged.resource_logs.is_empty() {
        return Err(anyhow!("JSONL input contained no valid log records"));
    }

    Ok(merged)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_from_content_type() {
        assert_eq!(
            InputFormat::from_content_type(Some("application/x-protobuf")),
            InputFormat::Protobuf
        );
        assert_eq!(
            InputFormat::from_content_type(Some("application/protobuf")),
            InputFormat::Protobuf
        );
        assert_eq!(
            InputFormat::from_content_type(Some("application/json")),
            InputFormat::Json
        );
        assert_eq!(
            InputFormat::from_content_type(Some("application/x-ndjson")),
            InputFormat::Jsonl
        );
        assert_eq!(
            InputFormat::from_content_type(Some("application/jsonl")),
            InputFormat::Jsonl
        );
        // Unknown content types default to protobuf
        assert_eq!(
            InputFormat::from_content_type(Some("text/plain")),
            InputFormat::Protobuf
        );
        // Missing content type defaults to protobuf
        assert_eq!(InputFormat::from_content_type(None), InputFormat::Protobuf);
    }

    #[test]
    fn test_format_content_type() {
        assert_eq!(
            InputFormat::Protobuf.content_type(),
            "application/x-protobuf"
        );
        assert_eq!(InputFormat::Json.content_type(), "application/json");
        assert_eq!(InputFormat::Jsonl.content_type(), "application/x-ndjson");
    }

    #[test]
    fn test_parse_empty_jsonl() {
        // Empty JSONL should return an error
        let result = parse_jsonl(b"");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("no valid log records"));

        // JSONL with only whitespace should also error
        let result = parse_jsonl(b"\n\n  \n");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_invalid_utf8_jsonl() {
        // Invalid UTF-8 should return an error
        let invalid_utf8 = vec![0xFF, 0xFE, 0xFD];
        let result = parse_jsonl(&invalid_utf8);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not valid UTF-8"));
    }
}
