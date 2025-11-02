//! Field name constants for OTLP and Arrow schemas.
//!
//! This module provides centralized constants for field names used throughout the
//! OTLP to Parquet conversion pipeline. There are two naming conventions in use:
//!
//! - **OTLP protobuf fields** (snake_case): Used in JSON normalization and protobuf parsing
//! - **Arrow schema fields** (PascalCase): Used in the ClickHouse-compatible Parquet schema
//!
//! The naming conventions differ intentionally:
//! - OTLP spec uses snake_case (e.g., "time_unix_nano")
//! - ClickHouse OTel exporter uses PascalCase (e.g., "Timestamp")
//!
//! Reference: <https://github.com/ClickHouse/ClickHouse/tree/master/src/Storages/ObjectStorage/DataLakes>

/// OTLP protobuf field names (snake_case as per OpenTelemetry specification)
///
/// These constants are used for:
/// - JSON normalization (converting canonical OTLP JSON to prost format)
/// - Protobuf field parsing and validation
/// - Default value insertion for missing fields
pub mod otlp {
    // Log record timestamp fields
    /// Log record timestamp in nanoseconds since Unix epoch
    pub const TIME_UNIX_NANO: &str = "time_unix_nano";
    /// Observer timestamp in nanoseconds since Unix epoch
    pub const OBSERVED_TIME_UNIX_NANO: &str = "observed_time_unix_nano";

    // Trace context fields
    /// W3C trace ID (16 bytes)
    pub const TRACE_ID: &str = "trace_id";
    /// W3C span ID (8 bytes)
    pub const SPAN_ID: &str = "span_id";
    /// W3C trace flags (8 bits)
    pub const TRACE_FLAGS: &str = "trace_flags";
    /// W3C trace state
    pub const TRACE_STATE: &str = "trace_state";
    /// Parent span identifier (8 bytes)
    pub const PARENT_SPAN_ID: &str = "parent_span_id";
    /// Log record flags
    pub const FLAGS: &str = "flags";

    // Severity fields
    /// Numeric severity level (0-24, per OpenTelemetry spec)
    pub const SEVERITY_NUMBER: &str = "severity_number";
    /// Human-readable severity text (e.g., "INFO", "ERROR")
    pub const SEVERITY_TEXT: &str = "severity_text";
    /// Span start time in nanoseconds
    pub const START_TIME_UNIX_NANO: &str = "start_time_unix_nano";
    /// Span end time in nanoseconds
    pub const END_TIME_UNIX_NANO: &str = "end_time_unix_nano";
    /// Span kind enumeration value
    pub const KIND: &str = "kind";
    /// Span events container
    pub const EVENTS: &str = "events";
    /// Span links container
    pub const LINKS: &str = "links";
    /// Span status container
    pub const STATUS: &str = "status";
    /// Status code field name
    pub const CODE: &str = "code";
    /// Status message field name
    pub const MESSAGE: &str = "message";

    // Content fields
    /// Log record body (AnyValue)
    pub const BODY: &str = "body";
    /// Log record attributes (key-value pairs)
    pub const ATTRIBUTES: &str = "attributes";

    // Metadata fields
    /// Number of dropped attributes due to limits
    pub const DROPPED_ATTRIBUTES_COUNT: &str = "dropped_attributes_count";
    /// Number of dropped span events
    pub const DROPPED_EVENTS_COUNT: &str = "dropped_events_count";
    /// Number of dropped span links
    pub const DROPPED_LINKS_COUNT: &str = "dropped_links_count";
    /// Schema URL for versioning
    pub const SCHEMA_URL: &str = "schema_url";

    // AnyValue variant field names (snake_case in JSON, PascalCase after normalization)
    /// String value variant
    pub const STRING_VALUE: &str = "string_value";
    /// Boolean value variant
    pub const BOOL_VALUE: &str = "bool_value";
    /// Integer value variant (int64)
    pub const INT_VALUE: &str = "int_value";
    /// Double value variant (float64)
    pub const DOUBLE_VALUE: &str = "double_value";
    /// Array value variant (repeated AnyValue)
    pub const ARRAY_VALUE: &str = "array_value";
    /// Key-value list variant (map/struct)
    pub const KVLIST_VALUE: &str = "kvlist_value";
    /// Bytes value variant
    pub const BYTES_VALUE: &str = "bytes_value";

    // Scope/instrumentation fields
    /// Instrumentation scope name
    pub const NAME: &str = "name";
    /// Instrumentation scope version
    pub const VERSION: &str = "version";

    // Container fields for nested structures
    /// Array of log records
    pub const LOG_RECORDS: &str = "log_records";
    /// Array of scope logs
    pub const SCOPE_LOGS: &str = "scope_logs";
    /// Array of scope spans
    pub const SCOPE_SPANS: &str = "scope_spans";
    /// Array of resource logs
    pub const RESOURCE_LOGS: &str = "resource_logs";
    /// Array of resource spans
    pub const RESOURCE_SPANS: &str = "resource_spans";
    /// Array of resource metrics
    pub const RESOURCE_METRICS: &str = "resource_metrics";
    /// Array of scope metrics
    pub const SCOPE_METRICS: &str = "scope_metrics";
    /// Array of metrics within a scope
    pub const METRICS: &str = "metrics";
    /// Array of spans within a scope
    pub const SPANS: &str = "spans";
    /// Resource container
    pub const RESOURCE: &str = "resource";
    /// Scope container
    pub const SCOPE: &str = "scope";

    // Metrics fields
    /// Metric name
    pub const METRIC_NAME: &str = "name";
    /// Metric description
    pub const DESCRIPTION: &str = "description";
    /// Metric unit
    pub const UNIT: &str = "unit";
    /// Gauge data
    pub const GAUGE: &str = "gauge";
    /// Sum data
    pub const SUM: &str = "sum";
    /// Histogram data
    pub const HISTOGRAM: &str = "histogram";
    /// Exponential histogram data
    pub const EXPONENTIAL_HISTOGRAM: &str = "exponential_histogram";
    /// Summary data
    pub const SUMMARY: &str = "summary";
    /// Data points
    pub const DATA_POINTS: &str = "data_points";
    /// Aggregation temporality
    pub const AGGREGATION_TEMPORALITY: &str = "aggregation_temporality";
    /// Is monotonic flag
    pub const IS_MONOTONIC: &str = "is_monotonic";
    /// Count field
    pub const COUNT: &str = "count";
    /// Bucket counts
    pub const BUCKET_COUNTS: &str = "bucket_counts";
    /// Explicit bounds
    pub const EXPLICIT_BOUNDS: &str = "explicit_bounds";
    /// Min value
    pub const MIN: &str = "min";
    /// Max value
    pub const MAX: &str = "max";
    /// Scale for exponential histogram
    pub const SCALE: &str = "scale";
    /// Zero count
    pub const ZERO_COUNT: &str = "zero_count";
    /// Positive offset
    pub const POSITIVE_OFFSET: &str = "offset";
    /// Positive bucket counts
    pub const POSITIVE_BUCKET_COUNTS: &str = "bucket_counts";
    /// Negative offset
    pub const NEGATIVE_OFFSET: &str = "offset";
    /// Negative bucket counts
    pub const NEGATIVE_BUCKET_COUNTS: &str = "bucket_counts";
    /// Positive buckets container
    pub const POSITIVE: &str = "positive";
    /// Negative buckets container
    pub const NEGATIVE: &str = "negative";
    /// Quantile values
    pub const QUANTILE_VALUES: &str = "quantile_values";
    /// Quantile value
    pub const QUANTILE: &str = "quantile";
    /// As double value
    pub const AS_DOUBLE: &str = "as_double";
    /// As int value
    pub const AS_INT: &str = "as_int";

    // Key-value pair fields
    /// Attribute key
    pub const KEY: &str = "key";
    /// Attribute value
    pub const VALUE: &str = "value";
}

/// Arrow schema field names (PascalCase for ClickHouse compatibility)
///
/// These constants define the column names in the Parquet output schema.
/// They follow ClickHouse's OTel exporter naming convention which uses PascalCase.
pub mod arrow {
    // Timestamp columns
    /// Log timestamp (corresponds to time_unix_nano)
    pub const TIMESTAMP: &str = "Timestamp";
    /// Timestamp rounded to second (for partitioning and efficient queries)
    pub const TIMESTAMP_TIME: &str = "TimestampTime";
    /// Observer timestamp (corresponds to observed_time_unix_nano)
    pub const OBSERVED_TIMESTAMP: &str = "ObservedTimestamp";

    // Trace context columns
    /// W3C trace ID
    pub const TRACE_ID: &str = "TraceId";
    /// W3C span ID
    pub const SPAN_ID: &str = "SpanId";
    /// W3C trace flags
    pub const TRACE_FLAGS: &str = "TraceFlags";
    /// W3C trace state
    pub const TRACE_STATE: &str = "TraceState";
    /// Parent span identifier
    pub const PARENT_SPAN_ID: &str = "ParentSpanId";

    // Severity columns
    /// Severity level as text
    pub const SEVERITY_TEXT: &str = "SeverityText";
    /// Severity level as number
    pub const SEVERITY_NUMBER: &str = "SeverityNumber";

    // Content columns
    /// Log body as structured AnyValue
    pub const BODY: &str = "Body";
    /// Log attributes map
    pub const LOG_ATTRIBUTES: &str = "LogAttributes";
    /// Resource attributes map (after extraction)
    pub const RESOURCE_ATTRIBUTES: &str = "ResourceAttributes";
    /// Span name column
    pub const SPAN_NAME: &str = "SpanName";
    /// Span kind column
    pub const SPAN_KIND: &str = "SpanKind";
    /// Span attributes column
    pub const SPAN_ATTRIBUTES: &str = "SpanAttributes";
    /// Span duration column
    pub const DURATION: &str = "Duration";
    /// Span status code column
    pub const STATUS_CODE: &str = "StatusCode";
    /// Span status message column
    pub const STATUS_MESSAGE: &str = "StatusMessage";
    /// Span events timestamp list column
    pub const EVENTS_TIMESTAMP: &str = "EventsTimestamp";
    /// Span events name list column
    pub const EVENTS_NAME: &str = "EventsName";
    /// Span events attributes list column
    pub const EVENTS_ATTRIBUTES: &str = "EventsAttributes";
    /// Span links trace_id list column
    pub const LINKS_TRACE_ID: &str = "LinksTraceId";
    /// Span links span_id list column
    pub const LINKS_SPAN_ID: &str = "LinksSpanId";
    /// Span links trace_state list column
    pub const LINKS_TRACE_STATE: &str = "LinksTraceState";
    /// Span links attributes list column
    pub const LINKS_ATTRIBUTES: &str = "LinksAttributes";

    // Extracted resource attribute columns
    /// Service name (extracted from resource attributes)
    pub const SERVICE_NAME: &str = "ServiceName";
    /// Service namespace (extracted from resource attributes)
    pub const SERVICE_NAMESPACE: &str = "ServiceNamespace";
    /// Service instance ID (extracted from resource attributes)
    pub const SERVICE_INSTANCE_ID: &str = "ServiceInstanceId";

    // Resource metadata columns
    /// Resource schema URL for versioning
    pub const RESOURCE_SCHEMA_URL: &str = "ResourceSchemaUrl";

    // Scope columns
    /// Instrumentation scope name
    pub const SCOPE_NAME: &str = "ScopeName";
    /// Instrumentation scope version
    pub const SCOPE_VERSION: &str = "ScopeVersion";
    /// Instrumentation scope attributes map
    pub const SCOPE_ATTRIBUTES: &str = "ScopeAttributes";
    /// Scope schema URL for versioning
    pub const SCOPE_SCHEMA_URL: &str = "ScopeSchemaUrl";

    // AnyValue struct field names (PascalCase)
    /// Type discriminator field in AnyValue struct
    pub const TYPE: &str = "Type";
    /// String value field
    pub const STRING_VALUE: &str = "StringValue";
    /// Boolean value field
    pub const BOOL_VALUE: &str = "BoolValue";
    /// Integer value field
    pub const INT_VALUE: &str = "IntValue";
    /// Double value field
    pub const DOUBLE_VALUE: &str = "DoubleValue";
    /// Bytes value field
    pub const BYTES_VALUE: &str = "BytesValue";
    /// JSON-serialized value field (for complex types)
    pub const JSON_VALUE: &str = "JsonValue";

    // Map entry field names
    /// Map key field
    pub const KEY: &str = "key";
    /// Map value field
    pub const VALUE: &str = "value";
    /// Map entries container
    pub const ENTRIES: &str = "entries";

    // Metrics columns
    /// Metric name column
    pub const METRIC_NAME: &str = "MetricName";
    /// Metric description column
    pub const METRIC_DESCRIPTION: &str = "MetricDescription";
    /// Metric unit column
    pub const METRIC_UNIT: &str = "MetricUnit";
    /// Metric attributes column
    pub const ATTRIBUTES: &str = "Attributes";
    /// Metric value column (for gauge and sum)
    pub const VALUE_COL: &str = "Value";
    /// Aggregation temporality column
    pub const AGGREGATION_TEMPORALITY: &str = "AggregationTemporality";
    /// Is monotonic flag column
    pub const IS_MONOTONIC: &str = "IsMonotonic";
    /// Count column
    pub const COUNT: &str = "Count";
    /// Sum column
    pub const SUM: &str = "Sum";
    /// Bucket counts column
    pub const BUCKET_COUNTS: &str = "BucketCounts";
    /// Explicit bounds column
    pub const EXPLICIT_BOUNDS: &str = "ExplicitBounds";
    /// Min value column
    pub const MIN: &str = "Min";
    /// Max value column
    pub const MAX: &str = "Max";
    /// Scale column
    pub const SCALE: &str = "Scale";
    /// Zero count column
    pub const ZERO_COUNT: &str = "ZeroCount";
    /// Positive offset column
    pub const POSITIVE_OFFSET: &str = "PositiveOffset";
    /// Positive bucket counts column
    pub const POSITIVE_BUCKET_COUNTS: &str = "PositiveBucketCounts";
    /// Negative offset column
    pub const NEGATIVE_OFFSET: &str = "NegativeOffset";
    /// Negative bucket counts column
    pub const NEGATIVE_BUCKET_COUNTS: &str = "NegativeBucketCounts";
    /// Quantile values column
    pub const QUANTILE_VALUES: &str = "QuantileValues";
    /// Quantile quantiles column
    pub const QUANTILE_QUANTILES: &str = "QuantileQuantiles";
}

/// OpenTelemetry semantic conventions for resource attributes.
///
/// These are well-known attribute keys defined by the OpenTelemetry specification
/// for common resource properties. We extract some of these to dedicated columns
/// for better query performance and ClickHouse compatibility.
///
/// Reference: <https://opentelemetry.io/docs/specs/semconv/resource/>
pub mod semconv {
    // Service identification
    /// Logical name of the service (e.g., "checkout-service")
    pub const SERVICE_NAME: &str = "service.name";
    /// Namespace for grouping related services (e.g., "production", "staging")
    pub const SERVICE_NAMESPACE: &str = "service.namespace";
    /// Unique identifier for this service instance (e.g., pod ID, hostname)
    pub const SERVICE_INSTANCE_ID: &str = "service.instance.id";
    /// Version of the service code (e.g., "1.2.3", git commit SHA)
    pub const SERVICE_VERSION: &str = "service.version";

    // Deployment environment
    /// Environment name (e.g., "production", "development", "qa")
    pub const DEPLOYMENT_ENVIRONMENT: &str = "deployment.environment";
    /// Environment type (e.g., "staging", "production")
    pub const DEPLOYMENT_ENVIRONMENT_NAME: &str = "deployment.environment.name";

    // Host/container identification
    /// Hostname of the physical or virtual machine
    pub const HOST_NAME: &str = "host.name";
    /// Host identifier (e.g., instance ID, serial number)
    pub const HOST_ID: &str = "host.id";
    /// Container ID (e.g., Docker container ID)
    pub const CONTAINER_ID: &str = "container.id";
    /// Container name
    pub const CONTAINER_NAME: &str = "container.name";
    /// Kubernetes pod name
    pub const K8S_POD_NAME: &str = "k8s.pod.name";
    /// Kubernetes namespace
    pub const K8S_NAMESPACE_NAME: &str = "k8s.namespace.name";
    /// Kubernetes cluster name
    pub const K8S_CLUSTER_NAME: &str = "k8s.cluster.name";

    // Cloud provider attributes
    /// Cloud provider (e.g., "aws", "gcp", "azure")
    pub const CLOUD_PROVIDER: &str = "cloud.provider";
    /// Cloud region (e.g., "us-east-1", "europe-west1")
    pub const CLOUD_REGION: &str = "cloud.region";
    /// Cloud availability zone
    pub const CLOUD_AVAILABILITY_ZONE: &str = "cloud.availability_zone";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_otlp_field_names() {
        // Verify snake_case convention
        assert!(!otlp::TIME_UNIX_NANO.chars().any(|c| c.is_uppercase()));
        assert!(!otlp::OBSERVED_TIME_UNIX_NANO
            .chars()
            .any(|c| c.is_uppercase()));
        assert_eq!(otlp::TIME_UNIX_NANO, "time_unix_nano");
    }

    #[test]
    fn test_arrow_field_names() {
        // Verify PascalCase convention
        assert!(arrow::TIMESTAMP.chars().next().unwrap().is_uppercase());
        assert!(arrow::SERVICE_NAME.chars().next().unwrap().is_uppercase());
        assert_eq!(arrow::TIMESTAMP, "Timestamp");
        assert_eq!(arrow::SERVICE_NAME, "ServiceName");
    }

    #[test]
    fn test_semantic_conventions() {
        // Verify dot notation
        assert!(semconv::SERVICE_NAME.contains('.'));
        assert!(semconv::SERVICE_NAMESPACE.contains('.'));
        assert_eq!(semconv::SERVICE_NAME, "service.name");
    }

    #[test]
    fn test_anyvalue_fields_consistency() {
        // OTLP uses snake_case
        assert_eq!(otlp::STRING_VALUE, "string_value");
        assert_eq!(otlp::INT_VALUE, "int_value");

        // Arrow uses PascalCase
        assert_eq!(arrow::STRING_VALUE, "StringValue");
        assert_eq!(arrow::INT_VALUE, "IntValue");
    }
}
