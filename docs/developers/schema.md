# Schema

`otlp2parquet` converts OpenTelemetry logs into Parquet files using a ClickHouse-compatible schema. This ensures that the generated data is easily queryable and integrates well with analytical databases.

## ClickHouse-Compatible Schema Definition

The schema is designed with 15 fields, using PascalCase naming conventions to align with ClickHouse best practices. Common resource attributes are extracted into dedicated columns for easier querying.

```rust
// crates/core/src/schema.rs

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use std::sync::Arc;

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
                    DataType::Struct(vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Utf8, true),
                    ].into()),
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
                    DataType::Struct(vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Utf8, true),
                    ].into()),
                    false,
                )),
                false,
            ),
            false,
        ),
    ])
}

// Common resource attribute keys to extract
pub const EXTRACTED_RESOURCE_ATTRS: &[&str] = &[
    "service.name",
    "service.namespace",
    "service.instance.id",
];
```

## Extracted Attributes

To optimize for common query patterns and reduce data redundancy, the following OpenTelemetry resource attributes are extracted into dedicated top-level columns:

*   `service.name` → `ServiceName`
*   `service.namespace` → `ServiceNamespace`
*   `service.instance.id` → `ServiceInstanceId`

All other resource and log attributes are stored as `Map<String, String>` in the `ResourceAttributes` and `LogAttributes` columns, respectively.
