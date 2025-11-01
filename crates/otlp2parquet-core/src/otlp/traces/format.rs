use anyhow::Result;
use otlp2parquet_proto::opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest;

use crate::otlp::common::{
    json_normalizer::normalise_json_value, parse_request, InputFormat, OtlpSignalRequest,
};

/// Type alias representing an OTLP trace export request.
pub type TraceRequest = ExportTraceServiceRequest;

/// Parse OTLP traces from bytes in the specified format.
pub fn parse_otlp_trace_request(bytes: &[u8], format: InputFormat) -> Result<TraceRequest> {
    parse_request(bytes, format, Some(normalise_json_value))
}

impl OtlpSignalRequest for ExportTraceServiceRequest {
    const JSONL_EMPTY_ERROR: &'static str = "JSONL input contained no valid trace records";

    fn merge(&mut self, mut other: Self) {
        self.resource_spans.append(&mut other.resource_spans);
    }

    fn is_empty(&self) -> bool {
        self.resource_spans.is_empty()
    }
}
