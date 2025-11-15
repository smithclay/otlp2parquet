# Schema Reference

This guide defines the Parquet schema for logs, traces, and metrics.

`otlp2parquet` converts OpenTelemetry data into a ClickHouse-compatible Parquet schema. This makes the data easy to query and integrate with analytical databases.

## Logs

The logs schema contains 15 fields. It uses `PascalCase` naming to align with ClickHouse conventions and extracts common resource attributes into dedicated columns to simplify querying.

| Field | Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `Timestamp` | `Timestamp (ns)` | No | Time the event occurred, with nanosecond precision. |
| `ObservedTimestamp` | `Timestamp (ns)` | No | Time the event was observed by the collector. |
| `TraceId` | `Binary(16)` | No | Unique identifier for the trace this log is a part of. |
| `SpanId` | `Binary(8)` | No | Unique identifier for the span this log is a part of. |
| `TraceFlags` | `UInt32` | No | W3C trace flags, indicating sampling status. |
| `SeverityText` | `String` | No | Severity level of the log (e.g., `INFO`, `WARN`). |
| `SeverityNumber` | `Int32` | No | Numerical representation of the severity. |
| `Body` | `String` | No | The primary log message content. |
| `ServiceName` | `String` | No | **Extracted** from the `service.name` resource attribute. |
| `ServiceNamespace` | `String` | Yes | **Extracted** from the `service.namespace` resource attribute. |
| `ServiceInstanceId` | `String` | Yes | **Extracted** from the `service.instance.id` resource attribute. |
| `ScopeName` | `String` | No | Name of the entity that emitted the log (e.g., an instrumentation library). |
| `ScopeVersion` | `String` | Yes | Version of the entity that emitted the log. |
| `ResourceAttributes` | `Map<String, String>` | No | All remaining resource attributes. |
| `LogAttributes` | `Map<String, String>` | No | All attributes attached to the log record itself. |

## Traces

The traces schema is designed for querying span data and their relationships.

| Field | Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `Timestamp` | `Timestamp (ns)` | No | The start time of the span. |
| `TraceId` | `String` | No | Unique identifier for the trace. |
| `SpanId` | `String` | No | Unique identifier for the span. |
| `ParentSpanId` | `String` | Yes | `SpanId` of the parent span, if any. |
| `TraceState` | `String` | Yes | W3C trace state information. |
| `SpanName` | `String` | No | The name of the span. |
| `SpanKind` | `String` | No | The kind of span (e.g., `SERVER`, `CLIENT`). |
| `ServiceName` | `String` | Yes | **Extracted** from the `service.name` resource attribute. |
| `Duration` | `Int64` | No | Duration of the span in nanoseconds. |
| `StatusCode` | `String` | Yes | Status code of the span (`Ok`, `Error`, `Unset`). |
| `StatusMessage` | `String` | Yes | A descriptive message for the status. |
| `ResourceAttributes` | `Map<String, String>` | No | All resource attributes. |
| `SpanAttributes` | `Map<String, String>` | No | All attributes attached to the span. |
| `ScopeName` | `String` | Yes | The name of the instrumentation scope. |
| `ScopeVersion` | `String` | Yes | The version of the instrumentation scope. |
| `Events_Timestamp` | `List<Timestamp (ns)>` | No | Timestamps of events within the span. |
| `Events_Name` | `List<String>` | No | Names of events within the span. |
| `Events_Attributes` | `List<Map<String, String>>` | No | Attributes for each event. |
| `Links_TraceId` | `List<String>` | No | `TraceId`s of linked spans. |
| `Links_SpanId` | `List<String>` | No | `SpanId`s of linked spans. |
| `Links_TraceState` | `List<String>` | Yes | `TraceState` of linked spans. |
| `Links_Attributes` | `List<Map<String, String>>` | No | Attributes for each link. |

## Metrics

Metrics are stored in separate tables based on their type (`gauge`, `sum`, etc.). All metric tables share a set of common base fields.

### Base Metric Fields

| Field | Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `Timestamp` | `Timestamp (ns)` | No | Time the data point was recorded. |
| `ServiceName` | `String` | No | **Extracted** from the `service.name` resource attribute. |
| `MetricName` | `String` | No | The name of the metric. |
| `MetricDescription` | `String` | Yes | A description of the metric. |
| `MetricUnit` | `String` | Yes | The unit of the metric. |
| `ResourceAttributes` | `Map<String, String>` | No | All resource attributes. |
| `Attributes` | `Map<String, String>` | No | All attributes attached to the data point. |
| `ScopeName` | `String` | Yes | The name of the instrumentation scope. |
| `ScopeVersion` | `String` | Yes | The version of the instrumentation scope. |

### Gauge Metrics (`otel_metrics_gauge`)

Contains the base fields plus:

| Field | Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `Value` | `Float64` | No | The observed value of the gauge. |

### Sum Metrics (`otel_metrics_sum`)

Contains the base fields plus:

| Field | Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `Value` | `Float64` | No | The sum of the observed values. |
| `AggregationTemporality` | `Int32` | No | Aggregation temporality (`Delta` or `Cumulative`). |
| `IsMonotonic` | `Boolean` | No | Whether the sum is monotonic. |

### Histogram Metrics (`otel_metrics_histogram`)

Contains the base fields plus:

| Field | Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `Count` | `Int64` | No | The number of values in the histogram. |
| `Sum` | `Float64` | No | The sum of all values in the histogram. |
| `BucketCounts` | `List<Int64>` | No | The number of values in each bucket. |
| `ExplicitBounds` | `List<Float64>` | No | The upper bounds of the histogram buckets. |
| `Min` | `Float64` | Yes | The minimum value recorded. |
| `Max` | `Float64` | Yes | The maximum value recorded. |

### Exponential Histogram Metrics (`otel_metrics_exponential_histogram`)

Contains the base fields plus:

| Field | Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `Count` | `Int64` | No | The number of values in the histogram. |
| `Sum` | `Float64` | No | The sum of all values in the histogram. |
| `Scale` | `Int32` | No | The scale of the exponential histogram. |
| `ZeroCount` | `Int64` | No | The number of values equal to zero. |
| `PositiveOffset` | `Int32` | No | The offset for the positive buckets. |
| `PositiveBucketCounts` | `List<Int64>` | No | The counts for the positive buckets. |
| `NegativeOffset` | `Int32` | No | The offset for the negative buckets. |
| `NegativeBucketCounts` | `List<Int64>` | No | The counts for the negative buckets. |
| `Min` | `Float64` | Yes | The minimum value recorded. |
| `Max` | `Float64` | Yes | The maximum value recorded. |

### Summary Metrics (`otel_metrics_summary`)

Contains the base fields plus:

| Field | Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `Count` | `Int64` | No | The number of values in the summary. |
| `Sum` | `Float64` | No | The sum of all values in the summary. |
| `QuantileValues` | `List<Float64>` | No | The values at different quantiles. |
| `QuantileQuantiles` | `List<Float64>` | No | The quantiles for which values are provided. |

## Extracted Attributes

To optimize for common queries, the following OpenTelemetry resource attributes are extracted into their own top-level columns:

*   `service.name` → `ServiceName`
*   `service.namespace` → `ServiceNamespace`
*   `service.instance.id` → `ServiceInstanceId`

All other resource and log attributes are stored as key-value maps in the `ResourceAttributes` and `LogAttributes` columns.
