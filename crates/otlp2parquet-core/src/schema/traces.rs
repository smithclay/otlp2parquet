use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};

use crate::otlp::field_names::arrow as field;

/// Helper to create a Field with PARQUET:field_id metadata for Iceberg compatibility
fn field_with_id(name: &str, data_type: DataType, nullable: bool, id: i32) -> Field {
    let metadata = HashMap::from([("PARQUET:field_id".to_string(), id.to_string())]);
    Field::new(name, data_type, nullable).with_metadata(metadata)
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
    let timestamp_ns = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
    let map_type = map_type();

    let events_timestamp_list =
        DataType::List(Arc::new(Field::new("item", timestamp_ns.clone(), false)));
    let events_name_list = DataType::List(Arc::new(Field::new("item", DataType::Utf8, false)));
    let events_attributes_list =
        DataType::List(Arc::new(Field::new("item", map_type.clone(), false)));
    let links_trace_id_list = DataType::List(Arc::new(Field::new("item", DataType::Utf8, false)));
    let links_span_id_list = DataType::List(Arc::new(Field::new("item", DataType::Utf8, false)));
    let links_trace_state_list = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
    let links_attributes_list =
        DataType::List(Arc::new(Field::new("item", map_type.clone(), false)));

    let fields = vec![
        field_with_id(field::TIMESTAMP, timestamp_ns.clone(), false, 1),
        field_with_id(field::TRACE_ID, DataType::Utf8, false, 2),
        field_with_id(field::SPAN_ID, DataType::Utf8, false, 3),
        field_with_id(field::PARENT_SPAN_ID, DataType::Utf8, true, 4),
        field_with_id(field::TRACE_STATE, DataType::Utf8, true, 5),
        field_with_id(field::SPAN_NAME, DataType::Utf8, false, 6),
        field_with_id(field::SPAN_KIND, DataType::Utf8, false, 7),
        field_with_id(field::SERVICE_NAME, DataType::Utf8, true, 8),
        field_with_id(field::RESOURCE_ATTRIBUTES, map_type.clone(), false, 9),
        field_with_id(field::SCOPE_NAME, DataType::Utf8, true, 10),
        field_with_id(field::SCOPE_VERSION, DataType::Utf8, true, 11),
        field_with_id(field::SPAN_ATTRIBUTES, map_type.clone(), false, 12),
        field_with_id(field::DURATION, DataType::Int64, false, 13),
        field_with_id(field::STATUS_CODE, DataType::Utf8, true, 14),
        field_with_id(field::STATUS_MESSAGE, DataType::Utf8, true, 15),
        field_with_id(field::EVENTS_TIMESTAMP, events_timestamp_list, false, 16),
        field_with_id(field::EVENTS_NAME, events_name_list, false, 17),
        field_with_id(field::EVENTS_ATTRIBUTES, events_attributes_list, false, 18),
        field_with_id(field::LINKS_TRACE_ID, links_trace_id_list, false, 19),
        field_with_id(field::LINKS_SPAN_ID, links_span_id_list, false, 20),
        field_with_id(field::LINKS_TRACE_STATE, links_trace_state_list, false, 21),
        field_with_id(field::LINKS_ATTRIBUTES, links_attributes_list, false, 22),
    ];

    let mut metadata = HashMap::new();
    metadata.insert(
        "otlp2parquet.traces_schema_version".to_string(),
        "1.0.0".to_string(),
    );

    Schema::new_with_metadata(fields, metadata)
}

fn map_type() -> DataType {
    let entry_fields: Fields = vec![
        Field::new(field::KEY, DataType::Utf8, false),
        Field::new(field::VALUE, DataType::Utf8, true),
    ]
    .into();

    DataType::Map(
        Arc::new(Field::new(
            field::ENTRIES,
            DataType::Struct(entry_fields),
            false,
        )),
        false,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_layout() {
        let schema = otel_traces_schema();
        assert_eq!(schema.fields().len(), 22);
        assert_eq!(schema.field(0).name(), field::TIMESTAMP);
        assert_eq!(schema.field(7).name(), field::SERVICE_NAME);
        assert_eq!(schema.field(11).name(), field::SPAN_ATTRIBUTES);
        assert_eq!(schema.field(21).name(), field::LINKS_ATTRIBUTES);
    }
}
