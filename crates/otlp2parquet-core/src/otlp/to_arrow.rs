// Convert OTLP log records to Arrow RecordBatch
//
// This is the core processing logic that extracts data from OTLP protobuf
// messages and builds Arrow columns according to the ClickHouse schema.

use anyhow::Result;
use arrow::array::RecordBatch;

/// Converts OTLP log records to Arrow RecordBatch
pub struct ArrowConverter {
    // TODO: Add builders for each column
    // timestamp_builder: TimestampNanosecondBuilder,
    // severity_text_builder: StringBuilder,
    // etc.
}

impl ArrowConverter {
    pub fn new() -> Self {
        // TODO: Initialize builders with schema
        Self {}
    }

    pub fn add_log_record(&mut self, _record: &[u8]) -> Result<()> {
        // TODO: Parse OTLP record and append to builders
        // - Extract timestamp, severity, body, trace context
        // - Extract and categorize attributes
        // - Append to appropriate builders
        Ok(())
    }

    pub fn finish(self) -> Result<RecordBatch> {
        // TODO: Build RecordBatch from all builders
        Err(anyhow::anyhow!("Not implemented yet"))
    }
}

impl Default for ArrowConverter {
    fn default() -> Self {
        Self::new()
    }
}
