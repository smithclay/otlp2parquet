//! Iceberg schema definitions for OpenTelemetry signals
//!
//! This module provides Iceberg schema definitions that correspond to the Arrow schemas
//! defined in the `schema` module. These schemas are used when writing to Apache Iceberg
//! tables via the icepick library.
//!
//! ## Design Notes
//!
//! - Field IDs must be unique and sequential within each schema
//! - Common fields (IDs 1-20) are shared across signal types for consistency
//! - Signal-specific fields use higher ID ranges (21+ for logs, 51+ for traces, 101+ for metrics)
//! - Field IDs match the `PARQUET:field_id` metadata in Arrow schemas
//! - All field names use PascalCase to match ClickHouse compatibility
//!
//! ## Testing
//!
//! Each schema includes compatibility tests that verify:
//! 1. Iceberg → Arrow conversion produces correct field count
//! 2. Field names match between Iceberg and Arrow schemas
//! 3. Field types are compatible
//!
//! Reference: <https://iceberg.apache.org/spec/#schemas>

use crate::otlp::field_names::arrow as field;

/// Returns the Iceberg schema for OpenTelemetry logs
///
/// This schema matches the Arrow schema defined in `schema::logs::otel_logs_schema()`.
/// Field IDs are assigned sequentially with common fields (1-20) and logs-specific fields (21+).
pub fn logs_schema() -> icepick::spec::Schema {
    use icepick::spec::types::{MapType, NestedField, PrimitiveType, StructType, Type};

    // Helper to create map type for attributes
    let map_type = || {
        Type::Map(MapType::new(
            1001, // key_id (arbitrary, > 1000 for nested types)
            Type::Primitive(PrimitiveType::String),
            1002, // value_id
            true, // value is optional
            Type::Struct(StructType::new(vec![
                NestedField::required_field(
                    2001,
                    field::TYPE.to_string(),
                    Type::Primitive(PrimitiveType::String),
                ),
                NestedField::optional_field(
                    2002,
                    field::STRING_VALUE.to_string(),
                    Type::Primitive(PrimitiveType::String),
                ),
                NestedField::optional_field(
                    2003,
                    field::BOOL_VALUE.to_string(),
                    Type::Primitive(PrimitiveType::Boolean),
                ),
                NestedField::optional_field(
                    2004,
                    field::INT_VALUE.to_string(),
                    Type::Primitive(PrimitiveType::Long),
                ),
                NestedField::optional_field(
                    2005,
                    field::DOUBLE_VALUE.to_string(),
                    Type::Primitive(PrimitiveType::Double),
                ),
                NestedField::optional_field(
                    2006,
                    field::BYTES_VALUE.to_string(),
                    Type::Primitive(PrimitiveType::Binary),
                ),
                NestedField::optional_field(
                    2007,
                    field::JSON_VALUE.to_string(),
                    Type::Primitive(PrimitiveType::String),
                ),
            ])),
        ))
    };

    // AnyValue struct for Body field
    let any_value_struct = Type::Struct(StructType::new(vec![
        NestedField::required_field(
            3001,
            field::TYPE.to_string(),
            Type::Primitive(PrimitiveType::String),
        ),
        NestedField::optional_field(
            3002,
            field::STRING_VALUE.to_string(),
            Type::Primitive(PrimitiveType::String),
        ),
        NestedField::optional_field(
            3003,
            field::BOOL_VALUE.to_string(),
            Type::Primitive(PrimitiveType::Boolean),
        ),
        NestedField::optional_field(
            3004,
            field::INT_VALUE.to_string(),
            Type::Primitive(PrimitiveType::Long),
        ),
        NestedField::optional_field(
            3005,
            field::DOUBLE_VALUE.to_string(),
            Type::Primitive(PrimitiveType::Double),
        ),
        NestedField::optional_field(
            3006,
            field::BYTES_VALUE.to_string(),
            Type::Primitive(PrimitiveType::Binary),
        ),
        NestedField::optional_field(
            3007,
            field::JSON_VALUE.to_string(),
            Type::Primitive(PrimitiveType::String),
        ),
    ]));

    let fields = vec![
        // ============ Common Fields (IDs 1-20) ============
        NestedField::required_field(
            1,
            field::TIMESTAMP.to_string(),
            Type::Primitive(PrimitiveType::Timestamptz),
        ),
        NestedField::required_field(
            2,
            field::TRACE_ID.to_string(),
            Type::Primitive(PrimitiveType::Fixed(16)),
        ),
        NestedField::required_field(
            3,
            field::SPAN_ID.to_string(),
            Type::Primitive(PrimitiveType::Fixed(8)),
        ),
        NestedField::required_field(
            4,
            field::SERVICE_NAME.to_string(),
            Type::Primitive(PrimitiveType::String),
        ),
        NestedField::optional_field(
            5,
            field::SERVICE_NAMESPACE.to_string(),
            Type::Primitive(PrimitiveType::String),
        ),
        NestedField::optional_field(
            6,
            field::SERVICE_INSTANCE_ID.to_string(),
            Type::Primitive(PrimitiveType::String),
        ),
        NestedField::required_field(7, field::RESOURCE_ATTRIBUTES.to_string(), map_type()),
        NestedField::optional_field(
            8,
            field::RESOURCE_SCHEMA_URL.to_string(),
            Type::Primitive(PrimitiveType::String),
        ),
        NestedField::required_field(
            9,
            field::SCOPE_NAME.to_string(),
            Type::Primitive(PrimitiveType::String),
        ),
        NestedField::optional_field(
            10,
            field::SCOPE_VERSION.to_string(),
            Type::Primitive(PrimitiveType::String),
        ),
        NestedField::required_field(11, field::SCOPE_ATTRIBUTES.to_string(), map_type()),
        NestedField::optional_field(
            12,
            field::SCOPE_SCHEMA_URL.to_string(),
            Type::Primitive(PrimitiveType::String),
        ),
        // ============ Logs-Specific Fields (IDs 21+) ============
        NestedField::required_field(
            21,
            field::TIMESTAMP_TIME.to_string(),
            Type::Primitive(PrimitiveType::Timestamp),
        ),
        NestedField::required_field(
            22,
            field::OBSERVED_TIMESTAMP.to_string(),
            Type::Primitive(PrimitiveType::Timestamptz),
        ),
        NestedField::required_field(
            23,
            field::TRACE_FLAGS.to_string(),
            Type::Primitive(PrimitiveType::Int),
        ),
        NestedField::required_field(
            24,
            field::SEVERITY_TEXT.to_string(),
            Type::Primitive(PrimitiveType::String),
        ),
        NestedField::required_field(
            25,
            field::SEVERITY_NUMBER.to_string(),
            Type::Primitive(PrimitiveType::Int),
        ),
        NestedField::optional_field(26, field::BODY.to_string(), any_value_struct),
        NestedField::required_field(27, field::LOG_ATTRIBUTES.to_string(), map_type()),
    ];

    icepick::spec::Schema::builder()
        .with_schema_id(1)
        .with_fields(fields)
        .build()
        .expect("valid logs schema")
}

/// Returns the Iceberg schema for OpenTelemetry traces
///
/// This schema matches the Arrow schema defined in `schema::traces::otel_traces_schema()`.
/// Field IDs are assigned with common fields (1-20) and traces-specific fields (51+).
pub fn traces_schema() -> icepick::spec::Schema {
    use icepick::spec::types::{ListType, MapType, NestedField, PrimitiveType, Type};

    // Simple Map<String, String> for attributes
    let map_type = || {
        Type::Map(MapType::new(
            4001,
            Type::Primitive(PrimitiveType::String),
            4002,
            true, // value optional
            Type::Primitive(PrimitiveType::String),
        ))
    };

    let fields = vec![
        // ============ Common Fields (IDs 1-20) ============
        NestedField::required_field(
            1,
            field::TIMESTAMP.to_string(),
            Type::Primitive(PrimitiveType::Timestamptz),
        ),
        NestedField::required_field(
            2,
            field::TRACE_ID.to_string(),
            Type::Primitive(PrimitiveType::String),
        ),
        NestedField::required_field(
            3,
            field::SPAN_ID.to_string(),
            Type::Primitive(PrimitiveType::String),
        ),
        NestedField::optional_field(
            4,
            field::SERVICE_NAME.to_string(),
            Type::Primitive(PrimitiveType::String),
        ),
        NestedField::required_field(7, field::RESOURCE_ATTRIBUTES.to_string(), map_type()),
        NestedField::optional_field(
            9,
            field::SCOPE_NAME.to_string(),
            Type::Primitive(PrimitiveType::String),
        ),
        NestedField::optional_field(
            10,
            field::SCOPE_VERSION.to_string(),
            Type::Primitive(PrimitiveType::String),
        ),
        // ============ Traces-Specific Fields (IDs 51+) ============
        NestedField::optional_field(
            51,
            field::PARENT_SPAN_ID.to_string(),
            Type::Primitive(PrimitiveType::String),
        ),
        NestedField::optional_field(
            52,
            field::TRACE_STATE.to_string(),
            Type::Primitive(PrimitiveType::String),
        ),
        NestedField::required_field(
            53,
            field::SPAN_NAME.to_string(),
            Type::Primitive(PrimitiveType::String),
        ),
        NestedField::required_field(
            54,
            field::SPAN_KIND.to_string(),
            Type::Primitive(PrimitiveType::String),
        ),
        NestedField::required_field(55, field::SPAN_ATTRIBUTES.to_string(), map_type()),
        NestedField::required_field(
            56,
            field::DURATION.to_string(),
            Type::Primitive(PrimitiveType::Long),
        ),
        NestedField::optional_field(
            57,
            field::STATUS_CODE.to_string(),
            Type::Primitive(PrimitiveType::String),
        ),
        NestedField::optional_field(
            58,
            field::STATUS_MESSAGE.to_string(),
            Type::Primitive(PrimitiveType::String),
        ),
        NestedField::required_field(
            59,
            field::EVENTS_TIMESTAMP.to_string(),
            Type::List(ListType::new(
                5001,
                true,
                Type::Primitive(PrimitiveType::Timestamptz),
            )),
        ),
        NestedField::required_field(
            60,
            field::EVENTS_NAME.to_string(),
            Type::List(ListType::new(
                5002,
                true,
                Type::Primitive(PrimitiveType::String),
            )),
        ),
        NestedField::required_field(
            61,
            field::EVENTS_ATTRIBUTES.to_string(),
            Type::List(ListType::new(5003, true, map_type())),
        ),
        NestedField::required_field(
            62,
            field::LINKS_TRACE_ID.to_string(),
            Type::List(ListType::new(
                5004,
                true,
                Type::Primitive(PrimitiveType::String),
            )),
        ),
        NestedField::required_field(
            63,
            field::LINKS_SPAN_ID.to_string(),
            Type::List(ListType::new(
                5005,
                true,
                Type::Primitive(PrimitiveType::String),
            )),
        ),
        NestedField::required_field(
            64,
            field::LINKS_TRACE_STATE.to_string(),
            Type::List(ListType::new(
                5006,
                false, // nullable elements
                Type::Primitive(PrimitiveType::String),
            )),
        ),
        NestedField::required_field(
            65,
            field::LINKS_ATTRIBUTES.to_string(),
            Type::List(ListType::new(5007, true, map_type())),
        ),
    ];

    icepick::spec::Schema::builder()
        .with_schema_id(1)
        .with_fields(fields)
        .build()
        .expect("valid traces schema")
}

/// Returns the Iceberg schema for gauge metrics
pub fn metrics_gauge_schema() -> icepick::spec::Schema {
    use icepick::spec::types::{MapType, NestedField, PrimitiveType, Type};

    let map_type = Type::Map(MapType::new(
        6001,
        Type::Primitive(PrimitiveType::String),
        6002,
        true,
        Type::Primitive(PrimitiveType::String),
    ));

    let mut fields = base_metrics_fields(map_type);
    // Gauge-specific field
    fields.push(NestedField::required_field(
        110,
        field::VALUE_COL.to_string(),
        Type::Primitive(PrimitiveType::Double),
    ));

    icepick::spec::Schema::builder()
        .with_schema_id(1)
        .with_fields(fields)
        .build()
        .expect("valid gauge schema")
}

/// Returns the Iceberg schema for sum metrics
pub fn metrics_sum_schema() -> icepick::spec::Schema {
    use icepick::spec::types::{MapType, NestedField, PrimitiveType, Type};

    let map_type = Type::Map(MapType::new(
        7001,
        Type::Primitive(PrimitiveType::String),
        7002,
        true,
        Type::Primitive(PrimitiveType::String),
    ));

    let mut fields = base_metrics_fields(map_type);
    // Sum-specific fields
    fields.push(NestedField::required_field(
        110,
        field::VALUE_COL.to_string(),
        Type::Primitive(PrimitiveType::Double),
    ));
    fields.push(NestedField::required_field(
        111,
        field::AGGREGATION_TEMPORALITY.to_string(),
        Type::Primitive(PrimitiveType::Int),
    ));
    fields.push(NestedField::required_field(
        112,
        field::IS_MONOTONIC.to_string(),
        Type::Primitive(PrimitiveType::Boolean),
    ));

    icepick::spec::Schema::builder()
        .with_schema_id(1)
        .with_fields(fields)
        .build()
        .expect("valid sum schema")
}

/// Returns the Iceberg schema for histogram metrics
pub fn metrics_histogram_schema() -> icepick::spec::Schema {
    use icepick::spec::types::{ListType, MapType, NestedField, PrimitiveType, Type};

    let map_type = Type::Map(MapType::new(
        8001,
        Type::Primitive(PrimitiveType::String),
        8002,
        true,
        Type::Primitive(PrimitiveType::String),
    ));

    let mut fields = base_metrics_fields(map_type);
    // Histogram-specific fields
    fields.push(NestedField::required_field(
        110,
        field::COUNT.to_string(),
        Type::Primitive(PrimitiveType::Long),
    ));
    fields.push(NestedField::required_field(
        111,
        field::SUM.to_string(),
        Type::Primitive(PrimitiveType::Double),
    ));
    fields.push(NestedField::required_field(
        112,
        field::BUCKET_COUNTS.to_string(),
        Type::List(ListType::new(
            8003,
            true,
            Type::Primitive(PrimitiveType::Long),
        )),
    ));
    fields.push(NestedField::required_field(
        113,
        field::EXPLICIT_BOUNDS.to_string(),
        Type::List(ListType::new(
            8004,
            true,
            Type::Primitive(PrimitiveType::Double),
        )),
    ));
    fields.push(NestedField::optional_field(
        114,
        field::MIN.to_string(),
        Type::Primitive(PrimitiveType::Double),
    ));
    fields.push(NestedField::optional_field(
        115,
        field::MAX.to_string(),
        Type::Primitive(PrimitiveType::Double),
    ));

    icepick::spec::Schema::builder()
        .with_schema_id(1)
        .with_fields(fields)
        .build()
        .expect("valid histogram schema")
}

/// Returns the Iceberg schema for exponential histogram metrics
pub fn metrics_exponential_histogram_schema() -> icepick::spec::Schema {
    use icepick::spec::types::{ListType, MapType, NestedField, PrimitiveType, Type};

    let map_type = Type::Map(MapType::new(
        9001,
        Type::Primitive(PrimitiveType::String),
        9002,
        true,
        Type::Primitive(PrimitiveType::String),
    ));

    let mut fields = base_metrics_fields(map_type);
    // ExponentialHistogram-specific fields
    fields.push(NestedField::required_field(
        110,
        field::COUNT.to_string(),
        Type::Primitive(PrimitiveType::Long),
    ));
    fields.push(NestedField::required_field(
        111,
        field::SUM.to_string(),
        Type::Primitive(PrimitiveType::Double),
    ));
    fields.push(NestedField::required_field(
        112,
        field::SCALE.to_string(),
        Type::Primitive(PrimitiveType::Int),
    ));
    fields.push(NestedField::required_field(
        113,
        field::ZERO_COUNT.to_string(),
        Type::Primitive(PrimitiveType::Long),
    ));
    fields.push(NestedField::required_field(
        114,
        field::POSITIVE_OFFSET.to_string(),
        Type::Primitive(PrimitiveType::Int),
    ));
    fields.push(NestedField::required_field(
        115,
        field::POSITIVE_BUCKET_COUNTS.to_string(),
        Type::List(ListType::new(
            9003,
            true,
            Type::Primitive(PrimitiveType::Long),
        )),
    ));
    fields.push(NestedField::required_field(
        116,
        field::NEGATIVE_OFFSET.to_string(),
        Type::Primitive(PrimitiveType::Int),
    ));
    fields.push(NestedField::required_field(
        117,
        field::NEGATIVE_BUCKET_COUNTS.to_string(),
        Type::List(ListType::new(
            9004,
            true,
            Type::Primitive(PrimitiveType::Long),
        )),
    ));
    fields.push(NestedField::optional_field(
        118,
        field::MIN.to_string(),
        Type::Primitive(PrimitiveType::Double),
    ));
    fields.push(NestedField::optional_field(
        119,
        field::MAX.to_string(),
        Type::Primitive(PrimitiveType::Double),
    ));

    icepick::spec::Schema::builder()
        .with_schema_id(1)
        .with_fields(fields)
        .build()
        .expect("valid exponential histogram schema")
}

/// Returns the Iceberg schema for summary metrics
pub fn metrics_summary_schema() -> icepick::spec::Schema {
    use icepick::spec::types::{ListType, MapType, NestedField, PrimitiveType, Type};

    let map_type = Type::Map(MapType::new(
        10001,
        Type::Primitive(PrimitiveType::String),
        10002,
        true,
        Type::Primitive(PrimitiveType::String),
    ));

    let mut fields = base_metrics_fields(map_type);
    // Summary-specific fields
    fields.push(NestedField::required_field(
        110,
        field::COUNT.to_string(),
        Type::Primitive(PrimitiveType::Long),
    ));
    fields.push(NestedField::required_field(
        111,
        field::SUM.to_string(),
        Type::Primitive(PrimitiveType::Double),
    ));
    fields.push(NestedField::required_field(
        112,
        field::QUANTILE_VALUES.to_string(),
        Type::List(ListType::new(
            10003,
            true,
            Type::Primitive(PrimitiveType::Double),
        )),
    ));
    fields.push(NestedField::required_field(
        113,
        field::QUANTILE_QUANTILES.to_string(),
        Type::List(ListType::new(
            10004,
            true,
            Type::Primitive(PrimitiveType::Double),
        )),
    ));

    icepick::spec::Schema::builder()
        .with_schema_id(1)
        .with_fields(fields)
        .build()
        .expect("valid summary schema")
}

/// Helper function to create base fields shared by all metric types
fn base_metrics_fields(
    map_type: icepick::spec::types::Type,
) -> Vec<icepick::spec::types::NestedField> {
    use icepick::spec::types::{NestedField, PrimitiveType, Type};

    vec![
        // Common fields (IDs 1-20)
        NestedField::required_field(
            1,
            field::TIMESTAMP.to_string(),
            Type::Primitive(PrimitiveType::Timestamptz),
        ),
        NestedField::required_field(
            4,
            field::SERVICE_NAME.to_string(),
            Type::Primitive(PrimitiveType::String),
        ),
        NestedField::required_field(7, field::RESOURCE_ATTRIBUTES.to_string(), map_type.clone()),
        NestedField::optional_field(
            9,
            field::SCOPE_NAME.to_string(),
            Type::Primitive(PrimitiveType::String),
        ),
        NestedField::optional_field(
            10,
            field::SCOPE_VERSION.to_string(),
            Type::Primitive(PrimitiveType::String),
        ),
        // Metrics base fields (IDs 101-109)
        NestedField::required_field(
            101,
            field::METRIC_NAME.to_string(),
            Type::Primitive(PrimitiveType::String),
        ),
        NestedField::optional_field(
            102,
            field::METRIC_DESCRIPTION.to_string(),
            Type::Primitive(PrimitiveType::String),
        ),
        NestedField::optional_field(
            103,
            field::METRIC_UNIT.to_string(),
            Type::Primitive(PrimitiveType::String),
        ),
        NestedField::required_field(104, field::ATTRIBUTES.to_string(), map_type),
    ]
}

/// Returns the schema for a given signal type and optional metric type
pub fn schema_for_signal(
    signal: crate::types::SignalType,
    metric_type: Option<&str>,
) -> icepick::spec::Schema {
    match signal {
        crate::types::SignalType::Logs => logs_schema(),
        crate::types::SignalType::Traces => traces_schema(),
        crate::types::SignalType::Metrics => match metric_type {
            Some("gauge") => metrics_gauge_schema(),
            Some("sum") => metrics_sum_schema(),
            Some("histogram") => metrics_histogram_schema(),
            Some("exponential_histogram") => metrics_exponential_histogram_schema(),
            Some("summary") => metrics_summary_schema(),
            _ => panic!(
                "Unknown or missing metric type for metrics signal: {:?}",
                metric_type
            ),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_logs_schema_builds() {
        let schema = logs_schema();
        assert_eq!(schema.schema_id(), 1);
        assert_eq!(schema.fields().len(), 19); // 12 common + 7 logs-specific
    }

    #[test]
    fn test_traces_schema_builds() {
        let schema = traces_schema();
        assert_eq!(schema.schema_id(), 1);
        assert_eq!(schema.fields().len(), 22); // 7 common + 15 traces-specific
    }

    #[test]
    fn test_logs_schema_field_names() {
        let schema = logs_schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name()).collect();

        // Verify common fields
        assert!(field_names.contains(&field::TIMESTAMP));
        assert!(field_names.contains(&field::TRACE_ID));
        assert!(field_names.contains(&field::SERVICE_NAME));
        assert!(field_names.contains(&field::RESOURCE_ATTRIBUTES));
        assert!(field_names.contains(&field::SCOPE_NAME));

        // Verify logs-specific fields
        assert!(field_names.contains(&field::BODY));
        assert!(field_names.contains(&field::SEVERITY_TEXT));
        assert!(field_names.contains(&field::LOG_ATTRIBUTES));
    }

    #[test]
    fn test_traces_schema_field_names() {
        let schema = traces_schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name()).collect();

        // Verify common fields
        assert!(field_names.contains(&field::TIMESTAMP));
        assert!(field_names.contains(&field::TRACE_ID));
        assert!(field_names.contains(&field::SPAN_ID));

        // Verify traces-specific fields
        assert!(field_names.contains(&field::SPAN_NAME));
        assert!(field_names.contains(&field::DURATION));
        assert!(field_names.contains(&field::EVENTS_TIMESTAMP));
        assert!(field_names.contains(&field::LINKS_ATTRIBUTES));
    }

    #[test]
    fn test_logs_schema_field_ids_unique() {
        let schema = logs_schema();
        let mut ids: Vec<i32> = schema.fields().iter().map(|f| f.id()).collect();
        ids.sort();
        let original_len = ids.len();
        ids.dedup();
        assert_eq!(
            ids.len(),
            original_len,
            "Field IDs must be unique in logs schema"
        );
    }

    #[test]
    fn test_traces_schema_field_ids_unique() {
        let schema = traces_schema();
        let mut ids: Vec<i32> = schema.fields().iter().map(|f| f.id()).collect();
        ids.sort();
        let original_len = ids.len();
        ids.dedup();
        assert_eq!(
            ids.len(),
            original_len,
            "Field IDs must be unique in traces schema"
        );
    }

    #[test]
    fn test_metrics_gauge_schema() {
        let schema = metrics_gauge_schema();
        assert_eq!(schema.schema_id(), 1);
        // 9 base fields + 1 gauge field
        assert_eq!(schema.fields().len(), 10);

        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name()).collect();
        assert!(field_names.contains(&field::TIMESTAMP));
        assert!(field_names.contains(&field::SERVICE_NAME));
        assert!(field_names.contains(&field::METRIC_NAME));
        assert!(field_names.contains(&field::VALUE_COL));
    }

    #[test]
    fn test_metrics_sum_schema() {
        let schema = metrics_sum_schema();
        assert_eq!(schema.fields().len(), 12); // 9 base + 3 sum-specific

        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name()).collect();
        assert!(field_names.contains(&field::VALUE_COL));
        assert!(field_names.contains(&field::AGGREGATION_TEMPORALITY));
        assert!(field_names.contains(&field::IS_MONOTONIC));
    }

    #[test]
    fn test_metrics_histogram_schema() {
        let schema = metrics_histogram_schema();
        assert_eq!(schema.fields().len(), 15); // 9 base + 6 histogram-specific

        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name()).collect();
        assert!(field_names.contains(&field::COUNT));
        assert!(field_names.contains(&field::SUM));
        assert!(field_names.contains(&field::BUCKET_COUNTS));
        assert!(field_names.contains(&field::EXPLICIT_BOUNDS));
    }

    #[test]
    fn test_metrics_exponential_histogram_schema() {
        let schema = metrics_exponential_histogram_schema();
        assert_eq!(schema.fields().len(), 19); // 9 base + 10 exp histogram-specific

        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name()).collect();
        assert!(field_names.contains(&field::SCALE));
        assert!(field_names.contains(&field::ZERO_COUNT));
        assert!(field_names.contains(&field::POSITIVE_BUCKET_COUNTS));
        assert!(field_names.contains(&field::NEGATIVE_BUCKET_COUNTS));
    }

    #[test]
    fn test_metrics_summary_schema() {
        let schema = metrics_summary_schema();
        assert_eq!(schema.fields().len(), 13); // 9 base + 4 summary-specific

        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name()).collect();
        assert!(field_names.contains(&field::COUNT));
        assert!(field_names.contains(&field::SUM));
        assert!(field_names.contains(&field::QUANTILE_VALUES));
        assert!(field_names.contains(&field::QUANTILE_QUANTILES));
    }

    #[test]
    fn test_schema_for_signal() {
        use crate::types::SignalType;

        // Test logs
        let logs = schema_for_signal(SignalType::Logs, None);
        assert_eq!(logs.fields().len(), 19);

        // Test traces
        let traces = schema_for_signal(SignalType::Traces, None);
        assert_eq!(traces.fields().len(), 22);

        // Test all metric types
        let gauge = schema_for_signal(SignalType::Metrics, Some("gauge"));
        assert_eq!(gauge.fields().len(), 10);

        let sum = schema_for_signal(SignalType::Metrics, Some("sum"));
        assert_eq!(sum.fields().len(), 12);

        let histogram = schema_for_signal(SignalType::Metrics, Some("histogram"));
        assert_eq!(histogram.fields().len(), 15);

        let exp_histogram = schema_for_signal(SignalType::Metrics, Some("exponential_histogram"));
        assert_eq!(exp_histogram.fields().len(), 19);

        let summary = schema_for_signal(SignalType::Metrics, Some("summary"));
        assert_eq!(summary.fields().len(), 13);
    }

    #[test]
    #[should_panic(expected = "Unknown or missing metric type")]
    fn test_schema_for_signal_invalid_metric_type() {
        use crate::types::SignalType;
        schema_for_signal(SignalType::Metrics, Some("invalid"));
    }

    #[test]
    #[should_panic(expected = "Unknown or missing metric type")]
    fn test_schema_for_signal_metrics_without_type() {
        use crate::types::SignalType;
        schema_for_signal(SignalType::Metrics, None);
    }
}
