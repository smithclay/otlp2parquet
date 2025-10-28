// ClickHouse-compatible Arrow schema for OpenTelemetry logs
//
// This schema matches the ClickHouse OTel exporter format with PascalCase naming.
// Common resource attributes are extracted to dedicated columns.

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use std::sync::Arc;

/// Returns the Arrow schema for OpenTelemetry logs compatible with ClickHouse
pub fn otel_logs_schema() -> Schema {
    Schema::new(vec![
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
        // Body
        Field::new("Body", DataType::Utf8, false),
        // Resource attributes - extracted common fields
        Field::new("ServiceName", DataType::Utf8, false),
        Field::new("ServiceNamespace", DataType::Utf8, true),
        Field::new("ServiceInstanceId", DataType::Utf8, true),
        // Scope
        Field::new("ScopeName", DataType::Utf8, false),
        Field::new("ScopeVersion", DataType::Utf8, true),
        // Remaining attributes as Map<String, String>
        Field::new(
            "ResourceAttributes",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Utf8, true),
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
                            Field::new("value", DataType::Utf8, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            false,
        ),
    ])
}

/// Common resource attribute keys that are extracted to dedicated columns
pub const EXTRACTED_RESOURCE_ATTRS: &[&str] =
    &["service.name", "service.namespace", "service.instance.id"];

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
