use std::sync::{Arc, OnceLock};

use arrow::datatypes::Schema;

/// Returns the Arrow schema for OTLP traces.
pub fn otel_traces_schema() -> Schema {
    otel_traces_schema_arc().as_ref().clone()
}

/// Returns a cached `Arc<Schema>` for the OTLP traces schema.
pub fn otel_traces_schema_arc() -> Arc<Schema> {
    static SCHEMA: OnceLock<Arc<Schema>> = OnceLock::new();
    Arc::clone(SCHEMA.get_or_init(|| Arc::new(Schema::empty())))
}
