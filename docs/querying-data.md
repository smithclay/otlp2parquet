# Querying Data with DuckDB

After `otlp2parquet` stores your data as Parquet files, you can use a query engine to analyze it. [DuckDB](https://duckdb.org/) is an excellent choice because it can query Parquet files directly from object storage without requiring a separate database or ingest process.

This guide shows how to configure DuckDB and run queries against your logs, metrics, and traces.

!!! tip "Try it in your browser"
    Explore a deterministic sample dataset with DuckDB-Wasm in the interactive [querying demo](query-demo/index.html).

## Setup

First, install the necessary DuckDB extensions and configure access to your S3-compatible object storage (e.g., MinIO, AWS S3, or R2).

```sql
-- Install and load the httpfs extension to read from S3
INSTALL httpfs; LOAD httpfs;

-- Configure S3 settings for your object storage
-- This example is for the local MinIO container
SET s3_endpoint='localhost:9000';
SET s3_url_style='path';
SET s3_use_ssl=false;
SET s3_access_key_id='minioadmin';
SET s3_secret_access_key='minioadmin';
```

## Querying Logs

Log files are stored in a Hive-style partitioned format: `logs/{service}/year={yyyy}/month={mm}/day={dd}/hour={hh}/{timestamp}-{hash}.parquet`.

```sql
SELECT Timestamp, ServiceName, SeverityText, Body
FROM read_parquet('s3://otlp-logs/logs/**/*.parquet')
ORDER BY Timestamp DESC LIMIT 10;
```

## Querying Metrics

Metrics are partitioned by type: `metrics/{type}/{service}/year={yyyy}/month={mm}/day={dd}/hour={hh}/{timestamp}-{hash}.parquet`. Each metric type has its own schema for optimal query performance.

```sql
-- Query gauge metrics
SELECT Timestamp, ServiceName, MetricName, Value, Attributes
FROM read_parquet('s3://otlp-logs/metrics/gauge/**/*.parquet')
WHERE MetricName = 'cpu.usage'
ORDER BY Timestamp DESC LIMIT 10;

-- Query sum metrics
SELECT Timestamp, ServiceName, MetricName, Value,
       AggregationTemporality, IsMonotonic
FROM read_parquet('s3://otlp-logs/metrics/sum/**/*.parquet')
WHERE MetricName = 'http.requests.total'
ORDER BY Timestamp DESC LIMIT 10;

-- Query histogram metrics
SELECT Timestamp, ServiceName, MetricName,
       Count, Sum, BucketCounts, ExplicitBounds
FROM read_parquet('s3://otlp-logs/metrics/histogram/**/*.parquet')
WHERE MetricName = 'http.request.duration'
ORDER BY Timestamp DESC LIMIT 10;
```

## Querying Traces

Traces are partitioned by service: `traces/{service}/year={yyyy}/month={mm}/day={dd}/hour={hh}/{timestamp}-{hash}.parquet`.

```sql
-- Query spans with basic fields
SELECT Timestamp, ServiceName, TraceId, SpanId, ParentSpanId,
       Name, Kind, StatusCode
FROM read_parquet('s3://otlp-logs/traces/**/*.parquet')
ORDER BY Timestamp DESC LIMIT 10;

-- Find slow traces (duration > 1 second)
SELECT TraceId, ServiceName, Name,
       (EndTimeUnixNano - StartTimeUnixNano) / 1e9 as duration_seconds
FROM read_parquet('s3://otlp-logs/traces/**/*.parquet')
WHERE (EndTimeUnixNano - StartTimeUnixNano) > 1000000000
ORDER BY duration_seconds DESC;
```

These examples provide a starting point for exploring your observability data. Refer to the [DuckDB documentation](https://duckdb.org/docs/) for more advanced querying capabilities.
