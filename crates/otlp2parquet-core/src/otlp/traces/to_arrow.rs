use std::sync::Arc;

use anyhow::{anyhow, Result};
use arrow::record_batch::RecordBatch;

use super::format::TraceRequest;

/// Metadata extracted from OTLP trace export requests during conversion.
#[derive(Debug, Clone)]
pub struct TraceMetadata {
    pub service_name: Arc<str>,
    pub first_timestamp_nanos: i64,
    pub span_count: usize,
}

/// Converts OTLP trace data to Arrow record batches.
pub struct TraceArrowConverter;

impl TraceArrowConverter {
    /// Convert the supplied trace request into an Arrow record batch and metadata.
    pub fn convert(_request: &TraceRequest) -> Result<(RecordBatch, TraceMetadata)> {
        Err(anyhow!("trace ingestion not implemented"))
    }
}
