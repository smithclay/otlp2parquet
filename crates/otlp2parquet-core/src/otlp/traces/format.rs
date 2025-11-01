use anyhow::{anyhow, Result};

use crate::otlp::InputFormat;

/// Placeholder type representing an OTLP trace export request.
#[derive(Debug, Clone)]
pub struct TraceRequest;

/// Parse OTLP traces from bytes in the specified format.
pub fn parse_otlp_trace_request(_bytes: &[u8], _format: InputFormat) -> Result<TraceRequest> {
    Err(anyhow!("trace ingestion not implemented"))
}
