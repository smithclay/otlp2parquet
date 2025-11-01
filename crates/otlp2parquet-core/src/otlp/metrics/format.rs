use anyhow::Result;
use otlp2parquet_proto::opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceRequest;

use crate::otlp::common::{
    json_normalizer::normalise_json_value, parse_request, InputFormat, OtlpSignalRequest,
};

/// Parse OTLP metrics from bytes in the specified format.
pub fn parse_otlp_request(
    bytes: &[u8],
    format: InputFormat,
) -> Result<ExportMetricsServiceRequest> {
    parse_request(bytes, format, Some(normalise_json_value))
}

impl OtlpSignalRequest for ExportMetricsServiceRequest {
    const JSONL_EMPTY_ERROR: &'static str = "JSONL input contained no valid metric records";

    fn merge(&mut self, mut other: Self) {
        self.resource_metrics.append(&mut other.resource_metrics);
    }

    fn is_empty(&self) -> bool {
        self.resource_metrics.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_empty_jsonl() {
        let result = parse_otlp_request(b"", InputFormat::Jsonl);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("no valid metric records"));

        let result = parse_otlp_request(b"\n\n  \n", InputFormat::Jsonl);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_invalid_utf8_jsonl() {
        let invalid_utf8 = vec![0xFF, 0xFE, 0xFD];
        let result = parse_otlp_request(&invalid_utf8, InputFormat::Jsonl);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not valid UTF-8"));
    }
}
