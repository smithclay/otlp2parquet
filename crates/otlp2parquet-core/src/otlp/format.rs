// Input format detection and parsing for OTLP logs
//
// Supports:
// - Protobuf (native OTLP format)
// - JSON (OTLP spec compliance)
// - JSONL (newline-delimited JSON, bonus feature)

use anyhow::{anyhow, Context, Result};
use hex::FromHex;
use otlp2parquet_proto::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;
use prost::Message;
use serde_json::{Map as JsonMap, Value as JsonValue};

// Canonical OTLP JSON uses camelCase field names and represents large integers as strings.
// We normalise incoming payloads so serde can deserialize into the prost-generated structs.
const U64_FIELDS: &[&str] = &["time_unix_nano", "observed_time_unix_nano"];
const U32_FIELDS: &[&str] = &["dropped_attributes_count", "flags", "trace_flags"];
const I64_FIELDS: &[&str] = &["int_value"];
const F64_FIELDS: &[&str] = &["double_value"];
const ANYVALUE_VARIANTS: &[&str] = &[
    "string_value",
    "bool_value",
    "int_value",
    "double_value",
    "array_value",
    "kvlist_value",
    "bytes_value",
];

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
    let value: JsonValue = serde_json::from_slice(bytes)
        .context("Failed to parse OTLP JSON message into serde_json::Value")?;
    canonical_json_to_request(value)
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
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let value: JsonValue = serde_json::from_str(trimmed).with_context(|| {
            format!("Failed to parse JSONL line {} as JSON value", line_num + 1)
        })?;

        let request = canonical_json_to_request(value).with_context(|| {
            format!(
                "Failed to convert JSONL line {} into ExportLogsServiceRequest",
                line_num + 1
            )
        })?;

        merged.resource_logs.extend(request.resource_logs);
    }

    if merged.resource_logs.is_empty() {
        return Err(anyhow!("JSONL input contained no valid log records"));
    }

    Ok(merged)
}

fn canonical_json_to_request(value: JsonValue) -> Result<ExportLogsServiceRequest> {
    let normalised = normalise_json_value(value, None)?;
    serde_json::from_value(normalised)
        .context("Failed to convert canonical OTLP JSON to protobuf struct")
}

fn normalise_json_value(value: JsonValue, key_hint: Option<&str>) -> Result<JsonValue> {
    match value {
        JsonValue::Object(map) => {
            let mut updated = JsonMap::new();
            for (key, val) in map {
                let snake_key = camel_to_snake_case(&key);
                let is_anyvalue_variant = ANYVALUE_VARIANTS.contains(&snake_key.as_str());
                let final_key = if is_anyvalue_variant {
                    snake_to_pascal_case(&snake_key)
                } else {
                    snake_key.clone()
                };
                let hint_owner = if is_anyvalue_variant {
                    snake_key.clone()
                } else {
                    final_key.clone()
                };
                let normalised = normalise_json_value(val, Some(&hint_owner))?;
                updated.insert(final_key, normalised);
            }

            if let Some("log_records") = key_hint {
                updated
                    .entry("dropped_attributes_count".to_string())
                    .or_insert_with(|| JsonValue::Number(serde_json::Number::from(0u32)));
                updated
                    .entry("flags".to_string())
                    .or_insert_with(|| JsonValue::Number(serde_json::Number::from(0u32)));
                updated
                    .entry("observed_time_unix_nano".to_string())
                    .or_insert_with(|| JsonValue::Number(serde_json::Number::from(0u64)));
                updated
                    .entry("time_unix_nano".to_string())
                    .or_insert_with(|| JsonValue::Number(serde_json::Number::from(0u64)));
                updated
                    .entry("severity_number".to_string())
                    .or_insert_with(|| JsonValue::Number(serde_json::Number::from(0i32)));
                updated
                    .entry("severity_text".to_string())
                    .or_insert_with(|| JsonValue::String(String::new()));
                updated
                    .entry("attributes".to_string())
                    .or_insert_with(|| JsonValue::Array(Vec::new()));
                updated
                    .entry("trace_id".to_string())
                    .or_insert_with(|| JsonValue::Array(Vec::new()));
                updated
                    .entry("span_id".to_string())
                    .or_insert_with(|| JsonValue::Array(Vec::new()));
            }

            if let Some("scope_logs") = key_hint {
                updated
                    .entry("schema_url".to_string())
                    .or_insert_with(|| JsonValue::String(String::new()));
            }

            if let Some("resource_logs") = key_hint {
                updated
                    .entry("schema_url".to_string())
                    .or_insert_with(|| JsonValue::String(String::new()));
            }

            if let Some("resource") = key_hint {
                updated
                    .entry("dropped_attributes_count".to_string())
                    .or_insert_with(|| JsonValue::Number(serde_json::Number::from(0u32)));
                updated
                    .entry("attributes".to_string())
                    .or_insert_with(|| JsonValue::Array(Vec::new()));
            }

            if let Some("scope") = key_hint {
                updated
                    .entry("dropped_attributes_count".to_string())
                    .or_insert_with(|| JsonValue::Number(serde_json::Number::from(0u32)));
                updated
                    .entry("name".to_string())
                    .or_insert_with(|| JsonValue::String(String::new()));
                updated
                    .entry("version".to_string())
                    .or_insert_with(|| JsonValue::String(String::new()));
                updated
                    .entry("attributes".to_string())
                    .or_insert_with(|| JsonValue::Array(Vec::new()));
            }

            Ok(JsonValue::Object(updated))
        }
        JsonValue::Array(values) => {
            let mut converted = Vec::with_capacity(values.len());
            for item in values {
                converted.push(normalise_json_value(item, key_hint)?);
            }
            Ok(JsonValue::Array(converted))
        }
        JsonValue::String(s) => {
            if let Some(key) = key_hint {
                if let Some(converted) = convert_string_field(key, &s)? {
                    return Ok(converted);
                }
            }
            Ok(JsonValue::String(s))
        }
        other => Ok(other),
    }
}

fn convert_string_field(key: &str, value: &str) -> Result<Option<JsonValue>> {
    if value.is_empty() {
        return Ok(None);
    }

    if U64_FIELDS.contains(&key) {
        let parsed = value
            .parse::<u64>()
            .with_context(|| format!("Failed to parse '{}' as u64 for field '{}'", value, key))?;
        return Ok(Some(JsonValue::Number(parsed.into())));
    }

    if U32_FIELDS.contains(&key) {
        let parsed = value
            .parse::<u32>()
            .with_context(|| format!("Failed to parse '{}' as u32 for field '{}'", value, key))?;
        return Ok(Some(JsonValue::Number(parsed.into())));
    }

    if I64_FIELDS.contains(&key) {
        let parsed = value
            .parse::<i64>()
            .with_context(|| format!("Failed to parse '{}' as i64 for field '{}'", value, key))?;
        return Ok(Some(JsonValue::Number(parsed.into())));
    }

    if F64_FIELDS.contains(&key) {
        let parsed = value
            .parse::<f64>()
            .with_context(|| format!("Failed to parse '{}' as f64 for field '{}'", value, key))?;
        let number = serde_json::Number::from_f64(parsed).ok_or_else(|| {
            anyhow!(
                "Invalid floating point value '{}' for field '{}'",
                value,
                key
            )
        })?;
        return Ok(Some(JsonValue::Number(number)));
    }

    if matches!(key, "trace_id" | "span_id")
        && value.len() % 2 == 0
        && value.chars().all(|c| c.is_ascii_hexdigit())
    {
        let bytes = Vec::from_hex(value).with_context(|| {
            format!(
                "Failed to decode hex string '{}' for field '{}'",
                value, key
            )
        })?;
        let json_bytes = bytes
            .into_iter()
            .map(|b| JsonValue::Number(serde_json::Number::from(b as u64)))
            .collect();
        return Ok(Some(JsonValue::Array(json_bytes)));
    }

    if key == "array_value" && value.is_empty() {
        // handled via recursive structure, no special casing required
        return Ok(None);
    }

    Ok(None)
}

fn camel_to_snake_case(input: &str) -> String {
    let mut result = String::with_capacity(input.len() + 4);
    let mut prev_underscore = false;
    for ch in input.chars() {
        if ch.is_ascii_uppercase() {
            if !result.is_empty() && !prev_underscore {
                result.push('_');
            }
            result.push(ch.to_ascii_lowercase());
            prev_underscore = false;
        } else {
            prev_underscore = ch == '_';
            result.push(ch);
        }
    }
    result
}

fn snake_to_pascal_case(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    let mut capitalize_next = true;
    for ch in input.chars() {
        if ch == '_' {
            capitalize_next = true;
            continue;
        }
        if capitalize_next {
            result.push(ch.to_ascii_uppercase());
            capitalize_next = false;
        } else {
            result.push(ch);
        }
    }
    result
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
