// Arrow builder construction helpers
//
// Provides utilities for creating Arrow builders with the correct schema

use arrow::array::{
    builder::MapFieldNames, ArrayBuilder, BinaryBuilder, BooleanBuilder, Float64Builder,
    Int64Builder, LargeStringBuilder, StringBuilder, StructBuilder,
};

/// Size of OpenTelemetry TraceId in bytes (128 bits)
pub(crate) const TRACE_ID_SIZE: i32 = 16;

/// Size of OpenTelemetry SpanId in bytes (64 bits)
pub(crate) const SPAN_ID_SIZE: i32 = 8;

/// Field names for Map types in Arrow schema
pub(crate) fn map_field_names() -> MapFieldNames {
    MapFieldNames {
        entry: "entries".to_string(),
        key: "key".to_string(),
        value: "value".to_string(),
    }
}

/// Create a new StructBuilder for AnyValue with correct field order and types
pub(crate) fn new_any_value_struct_builder() -> StructBuilder {
    // Use fields without field_id metadata to avoid schema mismatch errors
    let fields = crate::schema::any_value_fields_for_builder();
    StructBuilder::new(
        fields.clone(),
        vec![
            Box::new(StringBuilder::new()) as Box<dyn ArrayBuilder>,
            Box::new(StringBuilder::new()),
            Box::new(BooleanBuilder::new()),
            Box::new(Int64Builder::new()),
            Box::new(Float64Builder::new()),
            Box::new(BinaryBuilder::new()),
            Box::new(LargeStringBuilder::new()),
        ],
    )
}
