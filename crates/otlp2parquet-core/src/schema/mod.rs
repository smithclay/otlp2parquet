pub mod logs;
pub mod metrics;
pub mod traces;

pub use logs::{otel_logs_schema, otel_logs_schema_arc, EXTRACTED_RESOURCE_ATTRS};
pub use metrics::{
    otel_metrics_exponential_histogram_schema, otel_metrics_exponential_histogram_schema_arc,
    otel_metrics_gauge_schema, otel_metrics_gauge_schema_arc, otel_metrics_histogram_schema,
    otel_metrics_histogram_schema_arc, otel_metrics_sum_schema, otel_metrics_sum_schema_arc,
    otel_metrics_summary_schema, otel_metrics_summary_schema_arc,
};
pub use traces::{otel_traces_schema, otel_traces_schema_arc};
