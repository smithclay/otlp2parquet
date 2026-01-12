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
                let ct_lower = ct.to_ascii_lowercase();
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

#[cfg(test)]
mod tests {
    use super::InputFormat;

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
