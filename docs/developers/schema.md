# Schema

`otlp2parquet` converts OpenTelemetry data into Parquet files using a ClickHouse-compatible schema. This makes the data easy to query and integrate with analytical databases.

## Schema Definition

The schema uses 15 fields with PascalCase naming conventions to align with ClickHouse best practices. It also extracts common resource attributes into dedicated columns to simplify querying.

```rust
// crates/otlp2parquet-core/src/schema.rs

pub fn otel_logs_schema() -> Schema {
    Schema::new(vec![
        // Timestamps (nanosecond precision, UTC)
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

        // Extracted resource attributes
        Field::new("ServiceName", DataType::Utf8, false),
        Field::new("ServiceNamespace", DataType::Utf8, true),
        Field::new("ServiceInstanceId", DataType::Utf8, true),

        // Scope
        Field::new("ScopeName", DataType::Utf8, false),
        Field::new("ScopeVersion", DataType::Utf8, true),

        // Remaining attributes
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
```

## Extracted Attributes

To optimize for common queries, the following OpenTelemetry resource attributes are extracted into their own top-level columns:

*   `service.name` → `ServiceName`
*   `service.namespace` → `ServiceNamespace`
*   `service.instance.id` → `ServiceInstanceId`

All other resource and log attributes are stored as key-value maps in the `ResourceAttributes` and `LogAttributes` columns.
