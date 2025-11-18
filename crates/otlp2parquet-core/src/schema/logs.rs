// ClickHouse-compatible Arrow schema for OpenTelemetry logs
//
// This schema matches the ClickHouse OTel exporter format with PascalCase naming.
// Common resource attributes are extracted to dedicated columns.
//
// Note: This schema uses PascalCase field names for ClickHouse compatibility,
// diverging from the OTLP standard which uses snake_case. The conversion happens
// during the OTLP → Arrow transformation. See CLAUDE.md for rationale.

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use crate::otlp::field_names::{arrow as field, semconv};

/// Helper to create a Field with PARQUET:field_id metadata for Iceberg compatibility
fn field_with_id(name: &str, data_type: DataType, nullable: bool, id: i32) -> Field {
    let metadata = HashMap::from([("PARQUET:field_id".to_string(), id.to_string())]);
    Field::new(name, data_type, nullable).with_metadata(metadata)
}

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
    // S3 Tables doesn't support complex types (Map, Struct) - use JSON-encoded strings instead
    // This matches the Iceberg schema definition in iceberg_schemas::logs_schema()
    let string_type = DataType::Utf8;

    let fields = vec![
        // ============ Common Fields (IDs 1-20) ============
        // Shared across all signal types for cross-signal queries and schema evolution
        field_with_id(
            field::TIMESTAMP,
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
            1,
        ),
        field_with_id(field::TRACE_ID, DataType::FixedSizeBinary(16), false, 2),
        field_with_id(field::SPAN_ID, DataType::FixedSizeBinary(8), false, 3),
        field_with_id(field::SERVICE_NAME, DataType::Utf8, false, 4),
        field_with_id(field::SERVICE_NAMESPACE, DataType::Utf8, true, 5),
        field_with_id(field::SERVICE_INSTANCE_ID, DataType::Utf8, true, 6),
        // ResourceAttributes: JSON-encoded string for S3 Tables compatibility
        field_with_id(field::RESOURCE_ATTRIBUTES, string_type.clone(), false, 7),
        field_with_id(field::RESOURCE_SCHEMA_URL, DataType::Utf8, true, 8),
        field_with_id(field::SCOPE_NAME, DataType::Utf8, false, 9),
        field_with_id(field::SCOPE_VERSION, DataType::Utf8, true, 10),
        // ScopeAttributes: JSON-encoded string for S3 Tables compatibility
        field_with_id(field::SCOPE_ATTRIBUTES, string_type.clone(), false, 11),
        field_with_id(field::SCOPE_SCHEMA_URL, DataType::Utf8, true, 12),
        // ============ Logs-Specific Fields (IDs 21+) ============
        field_with_id(
            field::TIMESTAMP_TIME,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
            21,
        ),
        field_with_id(
            field::OBSERVED_TIMESTAMP,
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
            22,
        ),
        field_with_id(field::TRACE_FLAGS, DataType::UInt32, false, 23),
        field_with_id(field::SEVERITY_TEXT, DataType::Utf8, false, 24),
        field_with_id(field::SEVERITY_NUMBER, DataType::Int32, false, 25),
        // Body: JSON-encoded string for S3 Tables compatibility
        field_with_id(field::BODY, string_type.clone(), true, 26),
        // LogAttributes: JSON-encoded string for S3 Tables compatibility
        field_with_id(field::LOG_ATTRIBUTES, string_type, false, 27),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_creation() {
        let schema = otel_logs_schema();
        assert_eq!(schema.fields().len(), 19);

        // Verify common fields (IDs 1-12)
        assert_eq!(schema.field(0).name(), field::TIMESTAMP);
        assert_eq!(schema.field(1).name(), field::TRACE_ID);
        assert_eq!(schema.field(2).name(), field::SPAN_ID);
        assert_eq!(schema.field(3).name(), field::SERVICE_NAME);
        assert_eq!(schema.field(6).name(), field::RESOURCE_ATTRIBUTES);
        assert_eq!(schema.field(7).name(), field::RESOURCE_SCHEMA_URL);
        assert_eq!(schema.field(8).name(), field::SCOPE_NAME);
        assert_eq!(schema.field(10).name(), field::SCOPE_ATTRIBUTES);
        assert_eq!(schema.field(11).name(), field::SCOPE_SCHEMA_URL);

        // Verify logs-specific fields (IDs 21+)
        assert_eq!(schema.field(12).name(), field::TIMESTAMP_TIME);
        assert_eq!(schema.field(13).name(), field::OBSERVED_TIMESTAMP);
        assert_eq!(schema.field(18).name(), field::LOG_ATTRIBUTES);
    }

    #[test]
    fn test_arrow_iceberg_schema_compatibility() {
        use crate::iceberg_schemas;
        use icepick::spec::PrimitiveType;

        // Get both schemas
        let arrow_schema = otel_logs_schema();
        let iceberg_schema = iceberg_schemas::logs_schema();

        // Critical fields that MUST match between Arrow and Iceberg for S3 Tables compatibility
        // These fields use complex types in traditional deployments but MUST be String for S3 Tables
        let critical_string_fields = vec![
            field::RESOURCE_ATTRIBUTES,
            field::SCOPE_ATTRIBUTES,
            field::BODY,
            field::LOG_ATTRIBUTES,
        ];

        for field_name in critical_string_fields {
            // Find field in Arrow schema
            let arrow_field = arrow_schema
                .field_with_name(field_name)
                .unwrap_or_else(|_| panic!("Arrow schema missing field: {}", field_name));

            // Find field in Iceberg schema
            let iceberg_field = iceberg_schema
                .fields()
                .iter()
                .find(|f| f.name() == field_name)
                .unwrap_or_else(|| panic!("Iceberg schema missing field: {}", field_name));

            // Arrow field MUST be String (Utf8)
            assert_eq!(
                arrow_field.data_type(),
                &DataType::Utf8,
                "Arrow schema field '{}' must be DataType::Utf8 (String) for S3 Tables compatibility. \
                Found: {:?}. This indicates incomplete S3 Tables migration - attributes must be JSON-encoded strings.",
                field_name,
                arrow_field.data_type()
            );

            // Iceberg field MUST be PrimitiveType::String
            assert!(
                matches!(iceberg_field.field_type(), icepick::spec::Type::Primitive(PrimitiveType::String)),
                "Iceberg schema field '{}' must be PrimitiveType::String for S3 Tables compatibility. \
                Found: {:?}. This indicates schema drift between Arrow and Iceberg definitions.",
                field_name,
                iceberg_field.field_type()
            );
        }

        // Verify field count matches (basic sanity check)
        assert_eq!(
            arrow_schema.fields().len(),
            iceberg_schema.fields().len(),
            "Arrow and Iceberg schemas have different field counts - indicates schema drift"
        );

        // Verify all field names match in order
        for (arrow_field, iceberg_field) in
            arrow_schema.fields().iter().zip(iceberg_schema.fields())
        {
            assert_eq!(
                arrow_field.name(),
                iceberg_field.name(),
                "Field name mismatch at position - Arrow: '{}', Iceberg: '{}'. \
                This indicates field ordering drift between schemas.",
                arrow_field.name(),
                iceberg_field.name()
            );
        }
    }
}
