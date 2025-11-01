pub mod logs;
pub mod traces;

pub(crate) use logs::any_value_fields;
pub use logs::{otel_logs_schema, otel_logs_schema_arc, EXTRACTED_RESOURCE_ATTRS};
pub use traces::{otel_traces_schema, otel_traces_schema_arc};
