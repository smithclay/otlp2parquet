// ClickHouse-compatible Arrow schema for OpenTelemetry logs
//
// This schema matches the ClickHouse OTel exporter format with PascalCase naming.
// Common resource attributes are extracted to dedicated columns.

use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

/// Returns the Arrow schema for OpenTelemetry logs compatible with ClickHouse
pub fn otel_logs_schema() -> Schema {
    otel_logs_schema_arc().as_ref().clone()
}

/// Returns a cached `Arc<Schema>` for the OTLP logs schema.
pub fn otel_logs_schema_arc() -> Arc<Schema> {
    static SCHEMA: OnceLock<Arc<Schema>> = OnceLock::new();
    Arc::clone(SCHEMA.get_or_init(|| Arc::new(build_schema())))
}

fn build_schema() -> Schema {
    let fields = vec![
        // Timestamps - nanosecond precision, UTC
        Field::new(
            "Timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new(
            "ObservedTimestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        // Trace context
        Field::new("TraceId", DataType::FixedSizeBinary(16), false),
        Field::new("SpanId", DataType::FixedSizeBinary(8), false),
        Field::new("TraceFlags", DataType::UInt32, false),
        // Severity
        Field::new("SeverityText", DataType::Utf8, false),
        Field::new("SeverityNumber", DataType::Int32, false),
        // Body as structured AnyValue
        any_value_field("Body", true),
        // Resource attributes - extracted common fields
        Field::new("ServiceName", DataType::Utf8, false),
        Field::new("ServiceNamespace", DataType::Utf8, true),
        Field::new("ServiceInstanceId", DataType::Utf8, true),
        // Scope
        Field::new("ScopeName", DataType::Utf8, false),
        Field::new("ScopeVersion", DataType::Utf8, true),
        // Remaining attributes as Map<String, AnyValue>
        Field::new(
            "ResourceAttributes",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            any_value_field("value", true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            false,
        ),
        Field::new(
            "LogAttributes",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            any_value_field("value", true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            false,
        ),
    ];

    let mut metadata = HashMap::new();
    metadata.insert(
        "otlp2parquet.schema_version".to_string(),
        "1.1.0".to_string(),
    );

    Schema::new_with_metadata(fields, metadata)
}

/// Common resource attribute keys that are extracted to dedicated columns
pub const EXTRACTED_RESOURCE_ATTRS: &[&str] =
    &["service.name", "service.namespace", "service.instance.id"];

pub(crate) fn any_value_fields() -> Fields {
    vec![
        Field::new("Type", DataType::Utf8, false),
        Field::new("StringValue", DataType::Utf8, true),
        Field::new("BoolValue", DataType::Boolean, true),
        Field::new("IntValue", DataType::Int64, true),
        Field::new("DoubleValue", DataType::Float64, true),
        Field::new("BytesValue", DataType::Binary, true),
        Field::new("JsonValue", DataType::LargeUtf8, true),
    ]
    .into()
}

fn any_value_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::Struct(any_value_fields()), nullable)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_creation() {
        let schema = otel_logs_schema();
        assert_eq!(schema.fields().len(), 15);

        // Verify timestamp fields
        assert_eq!(schema.field(0).name(), "Timestamp");
        assert_eq!(schema.field(1).name(), "ObservedTimestamp");

        // Verify trace fields
        assert_eq!(schema.field(2).name(), "TraceId");
        assert_eq!(schema.field(3).name(), "SpanId");

        // Verify service fields
        assert_eq!(schema.field(8).name(), "ServiceName");
    }
}
