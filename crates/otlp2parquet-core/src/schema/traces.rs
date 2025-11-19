use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

use crate::otlp::field_names::arrow as field;

/// Helper to create a Field with PARQUET:field_id metadata for Iceberg compatibility
fn field_with_id(name: &str, data_type: DataType, nullable: bool, id: i32) -> Field {
    let metadata = HashMap::from([("PARQUET:field_id".to_string(), id.to_string())]);
    Field::new(name, data_type, nullable).with_metadata(metadata)
}

/// Helper to create a List element Field with PARQUET:field_id metadata
///
/// Iceberg requires field IDs on all nested fields, including List elements.
/// Without this metadata, DuckDB fails when reading Iceberg tables with:
/// "GetValueInternal on a value that is NULL" because the manifest references
/// field IDs that don't exist in the Parquet file metadata.
fn list_element_field(data_type: DataType, nullable: bool, element_id: i32) -> Field {
    let metadata = HashMap::from([("PARQUET:field_id".to_string(), element_id.to_string())]);
    Field::new("item", data_type, nullable).with_metadata(metadata)
}

/// Returns the Arrow schema for OTLP traces.
pub fn otel_traces_schema() -> Schema {
    otel_traces_schema_arc().as_ref().clone()
}

/// Returns a cached `Arc<Schema>` for the OTLP traces schema.
pub fn otel_traces_schema_arc() -> Arc<Schema> {
    static SCHEMA: OnceLock<Arc<Schema>> = OnceLock::new();
    Arc::clone(SCHEMA.get_or_init(|| Arc::new(build_schema())))
}

fn build_schema() -> Schema {
    // S3 Tables doesn't support complex types (Map, Struct) - use JSON-encoded strings instead
    // Iceberg v1/v2 only supports microsecond precision timestamps
    let timestamp_us = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let string_type = DataType::Utf8;

    // List types with element field IDs matching Iceberg schema
    let events_timestamp_list = DataType::List(Arc::new(list_element_field(
        timestamp_us.clone(),
        false,
        5001,
    )));
    let events_name_list = DataType::List(Arc::new(list_element_field(
        string_type.clone(),
        false,
        5002,
    )));
    // Events/Links attributes: List<String> for S3 Tables compatibility (JSON-encoded strings)
    let events_attributes_list = DataType::List(Arc::new(list_element_field(
        string_type.clone(),
        false,
        5003,
    )));
    let links_trace_id_list = DataType::List(Arc::new(list_element_field(
        string_type.clone(),
        false,
        5004,
    )));
    let links_span_id_list = DataType::List(Arc::new(list_element_field(
        string_type.clone(),
        false,
        5005,
    )));
    let links_trace_state_list = DataType::List(Arc::new(list_element_field(
        string_type.clone(),
        true,
        5006,
    )));
    let links_attributes_list = DataType::List(Arc::new(list_element_field(
        string_type.clone(),
        false,
        5007,
    )));

    let fields = vec![
        // ============ Common Fields (IDs 1-20) ============
        // Shared across all signal types for cross-signal queries and schema evolution
        field_with_id(field::TIMESTAMP, timestamp_us.clone(), false, 1),
        field_with_id(field::TRACE_ID, string_type.clone(), false, 2),
        field_with_id(field::SPAN_ID, string_type.clone(), false, 3),
        field_with_id(field::SERVICE_NAME, string_type.clone(), true, 4),
        // ResourceAttributes: JSON-encoded string for S3 Tables compatibility
        field_with_id(field::RESOURCE_ATTRIBUTES, string_type.clone(), false, 7),
        field_with_id(field::SCOPE_NAME, string_type.clone(), true, 9),
        field_with_id(field::SCOPE_VERSION, string_type.clone(), true, 10),
        // ============ Traces-Specific Fields (IDs 51+) ============
        field_with_id(field::PARENT_SPAN_ID, string_type.clone(), true, 51),
        field_with_id(field::TRACE_STATE, string_type.clone(), true, 52),
        field_with_id(field::SPAN_NAME, string_type.clone(), false, 53),
        field_with_id(field::SPAN_KIND, string_type.clone(), false, 54),
        // SpanAttributes: JSON-encoded string for S3 Tables compatibility
        field_with_id(field::SPAN_ATTRIBUTES, string_type, false, 55),
        field_with_id(field::DURATION, DataType::Int64, false, 56),
        field_with_id(field::STATUS_CODE, DataType::Utf8, true, 57),
        field_with_id(field::STATUS_MESSAGE, DataType::Utf8, true, 58),
        field_with_id(field::EVENTS_TIMESTAMP, events_timestamp_list, false, 59),
        field_with_id(field::EVENTS_NAME, events_name_list, false, 60),
        field_with_id(field::EVENTS_ATTRIBUTES, events_attributes_list, false, 61),
        field_with_id(field::LINKS_TRACE_ID, links_trace_id_list, false, 62),
        field_with_id(field::LINKS_SPAN_ID, links_span_id_list, false, 63),
        field_with_id(field::LINKS_TRACE_STATE, links_trace_state_list, false, 64),
        field_with_id(field::LINKS_ATTRIBUTES, links_attributes_list, false, 65),
    ];

    let mut metadata = HashMap::new();
    metadata.insert(
        "otlp2parquet.traces_schema_version".to_string(),
        "1.0.0".to_string(),
    );

    Schema::new_with_metadata(fields, metadata)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_layout() {
        let schema = otel_traces_schema();
        assert_eq!(schema.fields().len(), 22);

        // Verify common fields
        assert_eq!(schema.field(0).name(), field::TIMESTAMP);
        assert_eq!(schema.field(1).name(), field::TRACE_ID);
        assert_eq!(schema.field(2).name(), field::SPAN_ID);
        assert_eq!(schema.field(3).name(), field::SERVICE_NAME);
        assert_eq!(schema.field(4).name(), field::RESOURCE_ATTRIBUTES);
        assert_eq!(schema.field(5).name(), field::SCOPE_NAME);

        // Verify traces-specific fields
        assert_eq!(schema.field(7).name(), field::PARENT_SPAN_ID);
        assert_eq!(schema.field(11).name(), field::SPAN_ATTRIBUTES);
        assert_eq!(schema.field(21).name(), field::LINKS_ATTRIBUTES);
    }
}
