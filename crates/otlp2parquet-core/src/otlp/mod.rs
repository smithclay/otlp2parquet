// OTLP to Arrow conversion
//
// This module handles converting OpenTelemetry Protocol (OTLP) signal data
// into Arrow RecordBatches using the ClickHouse-compatible schema.

pub mod common;
pub mod logs;
pub mod traces;

pub use logs::field_names;
pub use logs::field_numbers;
pub use logs::{parse_otlp_request, ArrowConverter, InputFormat, LogMetadata};
