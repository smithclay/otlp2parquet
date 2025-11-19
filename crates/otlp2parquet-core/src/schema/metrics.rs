// ClickHouse-compatible Arrow schemas for OpenTelemetry metrics
//
// This module provides separate schemas for each metric type following the
// reference implementation from duckdb-otlp. Each metric type (gauge, sum,
// histogram, exponential_histogram, summary) has its own schema that shares
// a common set of base columns.
//
// Reference: https://github.com/smithclay/duckdb-otlp/blob/main/src/schema/otlp_metrics_schemas.hpp

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use crate::otlp::field_names::arrow as field;

/// Helper to create a Field with PARQUET:field_id metadata for Iceberg compatibility
fn field_with_id(name: &str, data_type: DataType, nullable: bool, id: i32) -> Field {
    let metadata = HashMap::from([("PARQUET:field_id".to_string(), id.to_string())]);
    Field::new(name, data_type, nullable).with_metadata(metadata)
}

/// Helper to create a List element Field with PARQUET:field_id metadata
///
/// Iceberg requires field IDs on all nested fields, including List elements.
fn list_element_field(data_type: DataType, nullable: bool, element_id: i32) -> Field {
    let metadata = HashMap::from([("PARQUET:field_id".to_string(), element_id.to_string())]);
    Field::new("item", data_type, nullable).with_metadata(metadata)
}

/// Returns the base fields shared by all metric types
fn base_fields() -> Vec<Field> {
    // S3 Tables doesn't support complex types (Map, Struct) - use JSON-encoded strings instead
    // This matches the Iceberg schema definition in iceberg_schemas
    let string_type = DataType::Utf8;

    vec![
        // ============ Common Fields (IDs 1-5) ============
        // Shared across all signal types for cross-signal queries and schema evolution
        // Iceberg v1/v2 only supports microsecond precision timestamps
        field_with_id(
            field::TIMESTAMP,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
            1,
        ),
        field_with_id(field::SERVICE_NAME, DataType::Utf8, false, 2),
        // ResourceAttributes: JSON-encoded string for S3 Tables compatibility
        field_with_id(field::RESOURCE_ATTRIBUTES, string_type.clone(), false, 3),
        field_with_id(field::SCOPE_NAME, DataType::Utf8, true, 4),
        field_with_id(field::SCOPE_VERSION, DataType::Utf8, true, 5),
        // ============ Metrics-Specific Base Fields (IDs 6-9) ============
        field_with_id(field::METRIC_NAME, DataType::Utf8, false, 6),
        field_with_id(field::METRIC_DESCRIPTION, DataType::Utf8, true, 7),
        field_with_id(field::METRIC_UNIT, DataType::Utf8, true, 8),
        // Attributes: JSON-encoded string for S3 Tables compatibility
        field_with_id(field::ATTRIBUTES, string_type, false, 9),
    ]
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
    // ============ Gauge-Specific Fields (IDs 10+) ============
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
    // ============ Sum-Specific Fields (IDs 10-12) ============
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
    // ============ Histogram-Specific Fields (IDs 10-15) ============
    fields.push(field_with_id(field::COUNT, DataType::Int64, false, 10));
    fields.push(field_with_id(field::SUM, DataType::Float64, false, 11));
    fields.push(field_with_id(
        field::BUCKET_COUNTS,
        DataType::List(Arc::new(list_element_field(DataType::Int64, false, 16))),
        false,
        12,
    ));
    fields.push(field_with_id(
        field::EXPLICIT_BOUNDS,
        DataType::List(Arc::new(list_element_field(DataType::Float64, false, 17))),
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
    // ============ ExponentialHistogram-Specific Fields (IDs 10-19) ============
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
        DataType::List(Arc::new(list_element_field(DataType::Int64, false, 20))),
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
        DataType::List(Arc::new(list_element_field(DataType::Int64, false, 21))),
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
    // ============ Summary-Specific Fields (IDs 10-13) ============
    fields.push(field_with_id(field::COUNT, DataType::Int64, false, 10));
    fields.push(field_with_id(field::SUM, DataType::Float64, false, 11));
    fields.push(field_with_id(
        field::QUANTILE_VALUES,
        DataType::List(Arc::new(list_element_field(DataType::Float64, false, 14))),
        false,
        12,
    ));
    fields.push(field_with_id(
        field::QUANTILE_QUANTILES,
        DataType::List(Arc::new(list_element_field(DataType::Float64, false, 15))),
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

        // Verify common fields
        assert_eq!(schema.field(0).name(), field::TIMESTAMP);
        assert_eq!(schema.field(1).name(), field::SERVICE_NAME);
        assert_eq!(schema.field(2).name(), field::RESOURCE_ATTRIBUTES);

        // Verify metrics base fields
        assert_eq!(schema.field(5).name(), field::METRIC_NAME);

        // Verify gauge-specific fields
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
