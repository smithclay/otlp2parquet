// ClickHouse-compatible Arrow schema for OpenTelemetry logs
//
// This schema matches the ClickHouse OTel exporter format with PascalCase naming.
// Common resource attributes are extracted to dedicated columns.
//
// Note: This schema uses PascalCase field names for ClickHouse compatibility,
// diverging from the OTLP standard which uses snake_case. The conversion happens
// during the OTLP → Arrow transformation. See CLAUDE.md for rationale.

use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use super::helpers::field_with_id;
use crate::otlp::field_names::{arrow as field, semconv};

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
    let mut id = 1;

    let fields = vec![
        // Timestamps - nanosecond precision, UTC
        field_with_id(
            field::TIMESTAMP,
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
            &mut id,
        ),
        field_with_id(
            field::TIMESTAMP_TIME,
            DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
            false,
            &mut id,
        ),
        field_with_id(
            field::OBSERVED_TIMESTAMP,
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
            &mut id,
        ),
        // Trace context
        field_with_id(
            field::TRACE_ID,
            DataType::FixedSizeBinary(16),
            false,
            &mut id,
        ),
        field_with_id(field::SPAN_ID, DataType::FixedSizeBinary(8), false, &mut id),
        field_with_id(field::TRACE_FLAGS, DataType::UInt32, false, &mut id),
        // Severity
        field_with_id(field::SEVERITY_TEXT, DataType::Utf8, false, &mut id),
        field_with_id(field::SEVERITY_NUMBER, DataType::Int32, false, &mut id),
        // Body as structured AnyValue
        any_value_field(field::BODY, true, &mut id),
        // Resource attributes - extracted common fields
        field_with_id(field::SERVICE_NAME, DataType::Utf8, false, &mut id),
        field_with_id(field::SERVICE_NAMESPACE, DataType::Utf8, true, &mut id),
        field_with_id(field::SERVICE_INSTANCE_ID, DataType::Utf8, true, &mut id),
        field_with_id(field::RESOURCE_SCHEMA_URL, DataType::Utf8, true, &mut id),
        // Scope
        field_with_id(field::SCOPE_NAME, DataType::Utf8, false, &mut id),
        field_with_id(field::SCOPE_VERSION, DataType::Utf8, true, &mut id),
        field_with_id(
            field::SCOPE_ATTRIBUTES,
            DataType::Map(
                Arc::new(Field::new(
                    field::ENTRIES,
                    DataType::Struct(
                        vec![
                            Field::new(field::KEY, DataType::Utf8, false),
                            any_value_field_no_id(field::VALUE, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            false,
            &mut id,
        ),
        field_with_id(field::SCOPE_SCHEMA_URL, DataType::Utf8, true, &mut id),
        // Remaining attributes as Map<String, AnyValue>
        field_with_id(
            field::RESOURCE_ATTRIBUTES,
            DataType::Map(
                Arc::new(Field::new(
                    field::ENTRIES,
                    DataType::Struct(
                        vec![
                            Field::new(field::KEY, DataType::Utf8, false),
                            any_value_field_no_id(field::VALUE, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            false,
            &mut id,
        ),
        field_with_id(
            field::LOG_ATTRIBUTES,
            DataType::Map(
                Arc::new(Field::new(
                    field::ENTRIES,
                    DataType::Struct(
                        vec![
                            Field::new(field::KEY, DataType::Utf8, false),
                            any_value_field_no_id(field::VALUE, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            false,
            &mut id,
        ),
    ];

    let mut metadata = HashMap::new();
    metadata.insert(
        "otlp2parquet.schema_version".to_string(),
        "1.1.0".to_string(),
    );

    Schema::new_with_metadata(fields, metadata)
}

/// Common resource attribute keys that are extracted to dedicated columns.
///
/// These semantic convention attributes are extracted from the resource attributes
/// map and promoted to dedicated columns for better query performance and
/// ClickHouse compatibility.
///
/// Reference: <https://opentelemetry.io/docs/specs/semconv/resource/>
pub const EXTRACTED_RESOURCE_ATTRS: &[&str] = &[
    semconv::SERVICE_NAME,
    semconv::SERVICE_NAMESPACE,
    semconv::SERVICE_INSTANCE_ID,
];

pub(crate) fn any_value_fields() -> Fields {
    vec![
        Field::new(field::TYPE, DataType::Utf8, false),
        Field::new(field::STRING_VALUE, DataType::Utf8, true),
        Field::new(field::BOOL_VALUE, DataType::Boolean, true),
        Field::new(field::INT_VALUE, DataType::Int64, true),
        Field::new(field::DOUBLE_VALUE, DataType::Float64, true),
        Field::new(field::BYTES_VALUE, DataType::Binary, true),
        Field::new(field::JSON_VALUE, DataType::LargeUtf8, true),
    ]
    .into()
}

fn any_value_field(name: &str, nullable: bool, id: &mut i32) -> Field {
    field_with_id(name, DataType::Struct(any_value_fields()), nullable, id)
}

fn any_value_field_no_id(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::Struct(any_value_fields()), nullable)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_creation() {
        let schema = otel_logs_schema();
        assert_eq!(schema.fields().len(), 19);

        // Verify timestamp fields
        assert_eq!(schema.field(0).name(), field::TIMESTAMP);
        assert_eq!(schema.field(1).name(), field::TIMESTAMP_TIME);
        assert_eq!(schema.field(2).name(), field::OBSERVED_TIMESTAMP);

        // Verify trace fields
        assert_eq!(schema.field(3).name(), field::TRACE_ID);
        assert_eq!(schema.field(4).name(), field::SPAN_ID);

        // Verify service fields
        assert_eq!(schema.field(9).name(), field::SERVICE_NAME);

        // Verify new schema URL fields
        assert_eq!(schema.field(12).name(), field::RESOURCE_SCHEMA_URL);
        assert_eq!(schema.field(16).name(), field::SCOPE_SCHEMA_URL);
        assert_eq!(schema.field(15).name(), field::SCOPE_ATTRIBUTES);
    }

    #[test]
    fn test_field_ids_present() {
        let schema = otel_logs_schema();

        // Verify all top-level fields have Parquet field IDs
        for field in schema.fields() {
            assert!(
                field.metadata().contains_key("PARQUET:field_id"),
                "Field '{}' is missing PARQUET:field_id metadata",
                field.name()
            );
        }
    }

    #[test]
    fn test_field_ids_sequential() {
        let schema = otel_logs_schema();

        // Verify field IDs are sequential starting from 1
        for (idx, field) in schema.fields().iter().enumerate() {
            let field_id = field
                .metadata()
                .get("PARQUET:field_id")
                .expect("Field should have PARQUET:field_id");
            let expected_id = (idx + 1).to_string();
            assert_eq!(
                field_id,
                &expected_id,
                "Field '{}' has ID {} but expected {}",
                field.name(),
                field_id,
                expected_id
            );
        }
    }
}
