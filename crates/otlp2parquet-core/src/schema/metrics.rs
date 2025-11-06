// ClickHouse-compatible Arrow schemas for OpenTelemetry metrics
//
// This module provides separate schemas for each metric type following the
// reference implementation from duckdb-otlp. Each metric type (gauge, sum,
// histogram, exponential_histogram, summary) has its own schema that shares
// a common set of base columns.
//
// Reference: https://github.com/smithclay/duckdb-otlp/blob/main/src/schema/otlp_metrics_schemas.hpp

use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use crate::otlp::field_names::arrow as field;

/// Helper to create a Field with PARQUET:field_id metadata for Iceberg compatibility
fn field_with_id(name: &str, data_type: DataType, nullable: bool, id: i32) -> Field {
    let metadata = HashMap::from([("PARQUET:field_id".to_string(), id.to_string())]);
    Field::new(name, data_type, nullable).with_metadata(metadata)
}

/// Returns the base fields shared by all metric types
fn base_fields() -> Vec<Field> {
    let map_type = map_type();

    vec![
        // Timestamp - nanosecond precision, UTC
        field_with_id(
            field::TIMESTAMP,
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
            1,
        ),
        // Service identification
        field_with_id(field::SERVICE_NAME, DataType::Utf8, false, 2),
        // Metric metadata
        field_with_id(field::METRIC_NAME, DataType::Utf8, false, 3),
        field_with_id(field::METRIC_DESCRIPTION, DataType::Utf8, true, 4),
        field_with_id(field::METRIC_UNIT, DataType::Utf8, true, 5),
        // Resource and scope information
        field_with_id(field::RESOURCE_ATTRIBUTES, map_type.clone(), false, 6),
        field_with_id(field::SCOPE_NAME, DataType::Utf8, true, 7),
        field_with_id(field::SCOPE_VERSION, DataType::Utf8, true, 8),
        // Data point attributes
        field_with_id(field::ATTRIBUTES, map_type, false, 9),
    ]
}

/// Helper function to create a Map<String, String> type
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

/// Returns the Arrow schema for gauge metrics
pub fn otel_metrics_gauge_schema() -> Schema {
    otel_metrics_gauge_schema_arc().as_ref().clone()
}

/// Returns a cached `Arc<Schema>` for gauge metrics
pub fn otel_metrics_gauge_schema_arc() -> Arc<Schema> {
    static SCHEMA: OnceLock<Arc<Schema>> = OnceLock::new();
    Arc::clone(SCHEMA.get_or_init(|| Arc::new(build_gauge_schema())))
}

fn build_gauge_schema() -> Schema {
    let mut fields = base_fields();
    fields.push(field_with_id(
        field::VALUE_COL,
        DataType::Float64,
        false,
        10,
    ));

    let mut metadata = HashMap::new();
    metadata.insert(
        "otlp2parquet.metrics_schema_version".to_string(),
        "1.0.0".to_string(),
    );
    metadata.insert("otlp2parquet.metric_type".to_string(), "gauge".to_string());

    Schema::new_with_metadata(fields, metadata)
}

/// Returns the Arrow schema for sum metrics
pub fn otel_metrics_sum_schema() -> Schema {
    otel_metrics_sum_schema_arc().as_ref().clone()
}

/// Returns a cached `Arc<Schema>` for sum metrics
pub fn otel_metrics_sum_schema_arc() -> Arc<Schema> {
    static SCHEMA: OnceLock<Arc<Schema>> = OnceLock::new();
    Arc::clone(SCHEMA.get_or_init(|| Arc::new(build_sum_schema())))
}

fn build_sum_schema() -> Schema {
    let mut fields = base_fields();
    fields.push(field_with_id(
        field::VALUE_COL,
        DataType::Float64,
        false,
        10,
    ));
    fields.push(field_with_id(
        field::AGGREGATION_TEMPORALITY,
        DataType::Int32,
        false,
        11,
    ));
    fields.push(field_with_id(
        field::IS_MONOTONIC,
        DataType::Boolean,
        false,
        12,
    ));

    let mut metadata = HashMap::new();
    metadata.insert(
        "otlp2parquet.metrics_schema_version".to_string(),
        "1.0.0".to_string(),
    );
    metadata.insert("otlp2parquet.metric_type".to_string(), "sum".to_string());

    Schema::new_with_metadata(fields, metadata)
}

/// Returns the Arrow schema for histogram metrics
pub fn otel_metrics_histogram_schema() -> Schema {
    otel_metrics_histogram_schema_arc().as_ref().clone()
}

/// Returns a cached `Arc<Schema>` for histogram metrics
pub fn otel_metrics_histogram_schema_arc() -> Arc<Schema> {
    static SCHEMA: OnceLock<Arc<Schema>> = OnceLock::new();
    Arc::clone(SCHEMA.get_or_init(|| Arc::new(build_histogram_schema())))
}

fn build_histogram_schema() -> Schema {
    let mut fields = base_fields();
    fields.push(field_with_id(field::COUNT, DataType::Int64, false, 10));
    fields.push(field_with_id(field::SUM, DataType::Float64, false, 11));
    fields.push(field_with_id(
        field::BUCKET_COUNTS,
        DataType::List(Arc::new(Field::new("item", DataType::Int64, false))),
        false,
        12,
    ));
    fields.push(field_with_id(
        field::EXPLICIT_BOUNDS,
        DataType::List(Arc::new(Field::new("item", DataType::Float64, false))),
        false,
        13,
    ));
    fields.push(field_with_id(field::MIN, DataType::Float64, true, 14));
    fields.push(field_with_id(field::MAX, DataType::Float64, true, 15));

    let mut metadata = HashMap::new();
    metadata.insert(
        "otlp2parquet.metrics_schema_version".to_string(),
        "1.0.0".to_string(),
    );
    metadata.insert(
        "otlp2parquet.metric_type".to_string(),
        "histogram".to_string(),
    );

    Schema::new_with_metadata(fields, metadata)
}

/// Returns the Arrow schema for exponential histogram metrics
pub fn otel_metrics_exponential_histogram_schema() -> Schema {
    otel_metrics_exponential_histogram_schema_arc()
        .as_ref()
        .clone()
}

/// Returns a cached `Arc<Schema>` for exponential histogram metrics
pub fn otel_metrics_exponential_histogram_schema_arc() -> Arc<Schema> {
    static SCHEMA: OnceLock<Arc<Schema>> = OnceLock::new();
    Arc::clone(SCHEMA.get_or_init(|| Arc::new(build_exponential_histogram_schema())))
}

fn build_exponential_histogram_schema() -> Schema {
    let mut fields = base_fields();
    fields.push(field_with_id(field::COUNT, DataType::Int64, false, 10));
    fields.push(field_with_id(field::SUM, DataType::Float64, false, 11));
    fields.push(field_with_id(field::SCALE, DataType::Int32, false, 12));
    fields.push(field_with_id(field::ZERO_COUNT, DataType::Int64, false, 13));
    fields.push(field_with_id(
        field::POSITIVE_OFFSET,
        DataType::Int32,
        false,
        14,
    ));
    fields.push(field_with_id(
        field::POSITIVE_BUCKET_COUNTS,
        DataType::List(Arc::new(Field::new("item", DataType::Int64, false))),
        false,
        15,
    ));
    fields.push(field_with_id(
        field::NEGATIVE_OFFSET,
        DataType::Int32,
        false,
        16,
    ));
    fields.push(field_with_id(
        field::NEGATIVE_BUCKET_COUNTS,
        DataType::List(Arc::new(Field::new("item", DataType::Int64, false))),
        false,
        17,
    ));
    fields.push(field_with_id(field::MIN, DataType::Float64, true, 18));
    fields.push(field_with_id(field::MAX, DataType::Float64, true, 19));

    let mut metadata = HashMap::new();
    metadata.insert(
        "otlp2parquet.metrics_schema_version".to_string(),
        "1.0.0".to_string(),
    );
    metadata.insert(
        "otlp2parquet.metric_type".to_string(),
        "exponential_histogram".to_string(),
    );

    Schema::new_with_metadata(fields, metadata)
}

/// Returns the Arrow schema for summary metrics
pub fn otel_metrics_summary_schema() -> Schema {
    otel_metrics_summary_schema_arc().as_ref().clone()
}

/// Returns a cached `Arc<Schema>` for summary metrics
pub fn otel_metrics_summary_schema_arc() -> Arc<Schema> {
    static SCHEMA: OnceLock<Arc<Schema>> = OnceLock::new();
    Arc::clone(SCHEMA.get_or_init(|| Arc::new(build_summary_schema())))
}

fn build_summary_schema() -> Schema {
    let mut fields = base_fields();
    fields.push(field_with_id(field::COUNT, DataType::Int64, false, 10));
    fields.push(field_with_id(field::SUM, DataType::Float64, false, 11));
    fields.push(field_with_id(
        field::QUANTILE_VALUES,
        DataType::List(Arc::new(Field::new("item", DataType::Float64, false))),
        false,
        12,
    ));
    fields.push(field_with_id(
        field::QUANTILE_QUANTILES,
        DataType::List(Arc::new(Field::new("item", DataType::Float64, false))),
        false,
        13,
    ));

    let mut metadata = HashMap::new();
    metadata.insert(
        "otlp2parquet.metrics_schema_version".to_string(),
        "1.0.0".to_string(),
    );
    metadata.insert(
        "otlp2parquet.metric_type".to_string(),
        "summary".to_string(),
    );

    Schema::new_with_metadata(fields, metadata)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gauge_schema() {
        let schema = otel_metrics_gauge_schema();
        // 9 base fields + 1 value field
        assert_eq!(schema.fields().len(), 10);
        assert_eq!(schema.field(0).name(), field::TIMESTAMP);
        assert_eq!(schema.field(1).name(), field::SERVICE_NAME);
        assert_eq!(schema.field(2).name(), field::METRIC_NAME);
        assert_eq!(schema.field(9).name(), field::VALUE_COL);
    }

    #[test]
    fn test_sum_schema() {
        let schema = otel_metrics_sum_schema();
        // 9 base fields + 3 sum-specific fields
        assert_eq!(schema.fields().len(), 12);
        assert_eq!(schema.field(9).name(), field::VALUE_COL);
        assert_eq!(schema.field(10).name(), field::AGGREGATION_TEMPORALITY);
        assert_eq!(schema.field(11).name(), field::IS_MONOTONIC);
    }

    #[test]
    fn test_histogram_schema() {
        let schema = otel_metrics_histogram_schema();
        // 9 base fields + 6 histogram-specific fields
        assert_eq!(schema.fields().len(), 15);
        assert_eq!(schema.field(9).name(), field::COUNT);
        assert_eq!(schema.field(10).name(), field::SUM);
        assert_eq!(schema.field(11).name(), field::BUCKET_COUNTS);
        assert_eq!(schema.field(12).name(), field::EXPLICIT_BOUNDS);
    }

    #[test]
    fn test_exponential_histogram_schema() {
        let schema = otel_metrics_exponential_histogram_schema();
        // 9 base fields + 10 exp histogram-specific fields
        assert_eq!(schema.fields().len(), 19);
        assert_eq!(schema.field(9).name(), field::COUNT);
        assert_eq!(schema.field(11).name(), field::SCALE);
        assert_eq!(schema.field(13).name(), field::POSITIVE_OFFSET);
    }

    #[test]
    fn test_summary_schema() {
        let schema = otel_metrics_summary_schema();
        // 9 base fields + 4 summary-specific fields
        assert_eq!(schema.fields().len(), 13);
        assert_eq!(schema.field(9).name(), field::COUNT);
        assert_eq!(schema.field(10).name(), field::SUM);
        assert_eq!(schema.field(11).name(), field::QUANTILE_VALUES);
        assert_eq!(schema.field(12).name(), field::QUANTILE_QUANTILES);
    }

    #[test]
    fn test_schema_metadata() {
        let gauge_schema = otel_metrics_gauge_schema();
        assert_eq!(
            gauge_schema
                .metadata()
                .get("otlp2parquet.metric_type")
                .unwrap(),
            "gauge"
        );

        let sum_schema = otel_metrics_sum_schema();
        assert_eq!(
            sum_schema
                .metadata()
                .get("otlp2parquet.metric_type")
                .unwrap(),
            "sum"
        );
    }
}
