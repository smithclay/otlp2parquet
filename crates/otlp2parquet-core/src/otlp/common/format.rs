use std::str;

use anyhow::{anyhow, Context, Result};
use prost::Message;
use serde::de::DeserializeOwned;
use serde_json::Value as JsonValue;

/// Supported input formats for OTLP payloads.
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
    /// Detect format from Content-Type header.
    ///
    /// Defaults to Protobuf if header is missing or unrecognized for backward compatibility.
    pub fn from_content_type(content_type: Option<&str>) -> Self {
        match content_type {
            Some(ct) => {
                let ct_lower = ct.to_lowercase();
                if ct_lower.contains("application/x-ndjson")
                    || ct_lower.contains("application/jsonl")
                {
                    Self::Jsonl
                } else if ct_lower.contains("application/json") {
                    Self::Json
                } else {
                    Self::Protobuf
                }
            }
            None => Self::Protobuf,
        }
    }

    /// Get the canonical Content-Type string for this format.
    pub fn content_type(&self) -> &'static str {
        match self {
            Self::Protobuf => "application/x-protobuf",
            Self::Json => "application/json",
            Self::Jsonl => "application/x-ndjson",
        }
    }
}

/// Function pointer used to normalize canonical OTLP JSON into prost-friendly shape.
pub type JsonNormalizer = fn(&mut JsonValue, Option<&str>) -> Result<()>;

/// Trait implemented by OTLP signal request types that can be parsed from multiple formats.
pub trait OtlpSignalRequest: Message + Default + DeserializeOwned {
    /// Error message returned when JSONL input does not contain any usable records.
    const JSONL_EMPTY_ERROR: &'static str;

    /// Merge another request into `self`.
    fn merge(&mut self, other: Self);

    /// Returns `true` if the request does not contain any signal data.
    fn is_empty(&self) -> bool;
}

/// Parse OTLP requests from bytes in the specified format.
pub fn parse_request<R>(
    bytes: &[u8],
    format: InputFormat,
    normalizer: Option<JsonNormalizer>,
) -> Result<R>
where
    R: OtlpSignalRequest,
{
    match format {
        InputFormat::Protobuf => parse_protobuf(bytes),
        InputFormat::Json => parse_json(bytes, normalizer),
        InputFormat::Jsonl => parse_jsonl(bytes, normalizer),
    }
}

fn parse_protobuf<R>(bytes: &[u8]) -> Result<R>
where
    R: OtlpSignalRequest,
{
    R::decode(bytes).context("Failed to decode OTLP protobuf message")
}

fn parse_json<R>(bytes: &[u8], normalizer: Option<JsonNormalizer>) -> Result<R>
where
    R: OtlpSignalRequest,
{
    let value: JsonValue = serde_json::from_slice(bytes)
        .context("Failed to parse OTLP JSON message into serde_json::Value")?;
    canonical_json_to_request(value, normalizer)
}

fn parse_jsonl<R>(bytes: &[u8], normalizer: Option<JsonNormalizer>) -> Result<R>
where
    R: OtlpSignalRequest,
{
    let text = str::from_utf8(bytes).context("JSONL input is not valid UTF-8")?;

    let mut merged = R::default();
    let mut saw_line = false;

    for (line_num, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let value: JsonValue = serde_json::from_str(trimmed).with_context(|| {
            format!("Failed to parse JSONL line {} as JSON value", line_num + 1)
        })?;

        let request = canonical_json_to_request(value, normalizer).with_context(|| {
            format!(
                "Failed to convert JSONL line {} into protobuf struct",
                line_num + 1
            )
        })?;

        OtlpSignalRequest::merge(&mut merged, request);
        saw_line = true;
    }

    if !saw_line || merged.is_empty() {
        return Err(anyhow!(R::JSONL_EMPTY_ERROR));
    }

    Ok(merged)
}

fn canonical_json_to_request<R>(
    mut value: JsonValue,
    normalizer: Option<JsonNormalizer>,
) -> Result<R>
where
    R: OtlpSignalRequest,
{
    if let Some(normalise) = normalizer {
        normalise(&mut value, None)
            .context("Failed to normalize canonical OTLP JSON into prost-compatible shape")?;
    }

    serde_json::from_value(value)
        .context("Failed to convert canonical OTLP JSON to protobuf struct")
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
        assert_eq!(
            InputFormat::from_content_type(Some("text/plain")),
            InputFormat::Protobuf
        );
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
}
