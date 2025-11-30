// OTLP to Arrow conversion
//
// This module handles converting OpenTelemetry Protocol (OTLP) signal data
// into Arrow RecordBatches using the ClickHouse-compatible schema.

pub mod common;
pub mod logs;
pub mod metrics;
pub mod traces;

pub use common::{field_names, field_numbers, InputFormat};
pub use logs::{parse_otlp_request, ArrowConverter, LogMetadata};
pub use metrics::{
    parse_otlp_request as parse_metrics_request, ArrowConverter as MetricsArrowConverter,
    MetricsMetadata,
};
pub use traces::{
    parse_otlp_trace_request as parse_traces_request, TraceArrowConverter as TracesArrowConverter,
    TraceMetadata,
};
