# Reference

Configuration, environment variables, and schema definitions.

## Environment Variables

All variables use the `OTLP2PARQUET_` prefix and override config file values.

### Storage

| Variable | Default | Description |
|----------|---------|-------------|
| `OTLP2PARQUET_STORAGE_BACKEND` | Auto | Storage type: `s3` or `fs` |
| `OTLP2PARQUET_S3_BUCKET` | - | S3 bucket name |
| `OTLP2PARQUET_S3_REGION` | - | Storage region |
| `OTLP2PARQUET_S3_ENDPOINT` | Auto | Custom S3 endpoint for MinIO or other S3-compatible storage |
| `OTLP2PARQUET_STORAGE_PATH` | `./data` | Filesystem storage path |

### Server

| Variable | Default | Description |
|----------|---------|-------------|
| `OTLP2PARQUET_LISTEN_ADDR` | `0.0.0.0:4318` | HTTP listen address |
| `OTLP2PARQUET_LOG_LEVEL` | `info` | Log level: `trace`, `debug`, `info`, `warn`, `error` |
| `OTLP2PARQUET_LOG_FORMAT` | `text` | Log format: `text` or `json` |
| `OTLP2PARQUET_MAX_PAYLOAD_BYTES` | `8388608` | Max request size (8MB) |

### Batching

| Variable | Default | Description |
|----------|---------|-------------|
| `OTLP2PARQUET_BATCHING_ENABLED` | `true` | Enable in-memory batching |
| `OTLP2PARQUET_BATCH_MAX_ROWS` | `200000` | Max rows per batch |
| `OTLP2PARQUET_BATCH_MAX_BYTES` | `134217728` | Max bytes per batch (128MB) |
| `OTLP2PARQUET_BATCH_MAX_AGE_SECS` | `10` | Max batch age in seconds |

---

## Schema

All schemas use PascalCase column names for ClickHouse compatibility.

### Logs

| Field | Type | Description |
|-------|------|-------------|
| `Timestamp` | `Timestamp(μs)` | Event time |
| `TimestampTime` | `Timestamp(μs)` | Timestamp rounded to second |
| `ObservedTimestamp` | `Timestamp(μs)` | Observer time |
| `TraceId` | `Binary` | W3C trace ID (16 bytes) |
| `SpanId` | `Binary` | W3C span ID (8 bytes) |
| `TraceFlags` | `UInt32` | W3C trace flags |
| `SeverityText` | `String` | Severity level (`INFO`, `WARN`, `ERROR`) |
| `SeverityNumber` | `Int32` | Numeric severity (0-24) |
| `Body` | `String` | Log message (JSON-encoded) |
| `ServiceName` | `String` | Extracted from `service.name` |
| `ServiceNamespace` | `String` | Extracted from `service.namespace` |
| `ServiceInstanceId` | `String` | Extracted from `service.instance.id` |
| `ResourceAttributes` | `String` | Resource attributes (JSON-encoded) |
| `ResourceSchemaUrl` | `String` | Resource schema URL |
| `ScopeName` | `String` | Instrumentation scope name |
| `ScopeVersion` | `String` | Instrumentation scope version |
| `ScopeAttributes` | `String` | Scope attributes (JSON-encoded) |
| `ScopeSchemaUrl` | `String` | Scope schema URL |
| `LogAttributes` | `String` | Log attributes (JSON-encoded) |

### Traces

| Field | Type | Description |
|-------|------|-------------|
| `Timestamp` | `Timestamp(μs)` | Span start time |
| `TraceId` | `String` | W3C trace ID |
| `SpanId` | `String` | W3C span ID |
| `ParentSpanId` | `String` | Parent span ID |
| `TraceState` | `String` | W3C trace state |
| `SpanName` | `String` | Span name |
| `SpanKind` | `String` | Span kind (`SERVER`, `CLIENT`) |
| `ServiceName` | `String` | Extracted from `service.name` |
| `ResourceAttributes` | `String` | Resource attributes (JSON-encoded) |
| `ScopeName` | `String` | Instrumentation scope name |
| `ScopeVersion` | `String` | Instrumentation scope version |
| `SpanAttributes` | `String` | Span attributes (JSON-encoded) |
| `Duration` | `Int64` | Duration in nanoseconds |
| `StatusCode` | `String` | Status code (`Ok`, `Error`, `Unset`) |
| `StatusMessage` | `String` | Status message |
| `EventsTimestamp` | `List<Timestamp>` | Event timestamps |
| `EventsName` | `List<String>` | Event names |
| `EventsAttributes` | `List<String>` | Event attributes (JSON-encoded) |
| `LinksTraceId` | `List<String>` | Linked trace IDs |
| `LinksSpanId` | `List<String>` | Linked span IDs |
| `LinksTraceState` | `List<String>` | Linked trace states |
| `LinksAttributes` | `List<String>` | Link attributes (JSON-encoded) |

### Metrics

Metrics are stored in separate tables by type: `gauge`, `sum`, `histogram`, `exponential_histogram`, `summary`.

**Base fields (all metric types):**

| Field | Type | Description |
|-------|------|-------------|
| `Timestamp` | `Timestamp(μs)` | Data point time |
| `ServiceName` | `String` | Extracted from `service.name` |
| `ResourceAttributes` | `String` | Resource attributes (JSON-encoded) |
| `ScopeName` | `String` | Instrumentation scope name |
| `ScopeVersion` | `String` | Instrumentation scope version |
| `MetricName` | `String` | Metric name |
| `MetricDescription` | `String` | Metric description |
| `MetricUnit` | `String` | Metric unit |
| `Attributes` | `String` | Data point attributes (JSON-encoded) |

**Type-specific fields:**

**Gauge:**

| Field | Type | Description |
|-------|------|-------------|
| `Value` | `Float64` | Observed value |

**Sum:**

| Field | Type | Description |
|-------|------|-------------|
| `Value` | `Float64` | Sum value |
| `AggregationTemporality` | `Int32` | Delta or Cumulative |
| `IsMonotonic` | `Boolean` | Monotonic flag |

**Histogram:**

| Field | Type | Description |
|-------|------|-------------|
| `Count` | `Int64` | Number of values |
| `Sum` | `Float64` | Sum of values |
| `BucketCounts` | `List<Int64>` | Values per bucket |
| `ExplicitBounds` | `List<Float64>` | Bucket upper bounds |
| `Min` | `Float64` | Minimum value |
| `Max` | `Float64` | Maximum value |

**Exponential Histogram:**

| Field | Type | Description |
|-------|------|-------------|
| `Count` | `Int64` | Number of values |
| `Sum` | `Float64` | Sum of values |
| `Scale` | `Int32` | Histogram scale |
| `ZeroCount` | `Int64` | Zero values |
| `PositiveOffset` | `Int32` | Positive bucket offset |
| `PositiveBucketCounts` | `List<Int64>` | Positive bucket counts |
| `NegativeOffset` | `Int32` | Negative bucket offset |
| `NegativeBucketCounts` | `List<Int64>` | Negative bucket counts |
| `Min` | `Float64` | Minimum value |
| `Max` | `Float64` | Maximum value |

**Summary:**

| Field | Type | Description |
|-------|------|-------------|
| `Count` | `Int64` | Number of values |
| `Sum` | `Float64` | Sum of values |
| `QuantileValues` | `List<Float64>` | Values at quantiles |
| `QuantileQuantiles` | `List<Float64>` | Quantile points |

---

## File Layout

Parquet files use Hive-style partitioning:

```
logs/{service}/year={year}/month={month}/day={day}/hour={hour}/{timestamp}-{uuid}.parquet
traces/{service}/year={year}/month={month}/day={day}/hour={hour}/{timestamp}-{uuid}.parquet
metrics/{type}/{service}/year={year}/month={month}/day={day}/hour={hour}/{timestamp}-{uuid}.parquet
```

Where `{type}` is one of: `gauge`, `sum`, `histogram`, `exponential_histogram`, `summary`.
