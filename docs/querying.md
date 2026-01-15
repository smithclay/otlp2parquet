# Query Data

Query your Parquet files with DuckDB or any Parquet reader.

## DuckDB (Recommended)

[DuckDB](https://duckdb.org/) reads Parquet files directly from disk or S3.

### Local files

```sql
SELECT Timestamp, ServiceName, Body
FROM read_parquet('data/logs/**/*.parquet')
ORDER BY Timestamp DESC
LIMIT 10;
```

### S3 / MinIO

```sql
-- Configure S3 access
INSTALL httpfs; LOAD httpfs;
SET s3_region = 'us-west-2';
SET s3_access_key_id = 'your-key';
SET s3_secret_access_key = 'your-secret';

-- For MinIO (local Docker setup)
SET s3_endpoint = 'localhost:9000';
SET s3_url_style = 'path';
SET s3_use_ssl = false;

-- Query logs
SELECT Timestamp, ServiceName, Body
FROM read_parquet('s3://otlp-logs/logs/**/*.parquet')
WHERE Timestamp > current_timestamp - interval '1 hour'
LIMIT 100;
```

## Example Queries

### Logs by service

```sql
SELECT ServiceName, COUNT(*) as count
FROM read_parquet('s3://bucket/logs/**/*.parquet')
GROUP BY ServiceName
ORDER BY count DESC;
```

### Recent errors

```sql
SELECT Timestamp, ServiceName, SeverityText, Body
FROM read_parquet('s3://bucket/logs/**/*.parquet')
WHERE SeverityText IN ('ERROR', 'FATAL')
  AND Timestamp > current_timestamp - interval '1 hour'
ORDER BY Timestamp DESC
LIMIT 50;
```

### Error traces

```sql
SELECT TraceId, SpanName, Duration, StatusMessage
FROM read_parquet('s3://bucket/traces/**/*.parquet')
WHERE StatusCode = 'STATUS_CODE_ERROR'
ORDER BY Duration DESC
LIMIT 20;
```

### Slow traces

```sql
SELECT
  TraceId,
  SpanName,
  Duration / 1e9 as duration_seconds,
  ServiceName
FROM read_parquet('s3://bucket/traces/**/*.parquet')
WHERE Duration > 5e9  -- 5 seconds in nanoseconds
ORDER BY Duration DESC
LIMIT 10;
```

### Metrics over time

```sql
SELECT
  date_trunc('hour', Timestamp) as hour,
  MetricName,
  AVG(Value) as avg_value
FROM read_parquet('s3://bucket/metrics/gauge/**/*.parquet')
GROUP BY hour, MetricName
ORDER BY hour;
```

### Histogram percentiles

```sql
-- Approximate p95 from histogram buckets
SELECT
  MetricName,
  ExplicitBounds,
  BucketCounts
FROM read_parquet('s3://bucket/metrics/histogram/**/*.parquet')
WHERE MetricName = 'http.server.duration'
LIMIT 10;
```

### Join logs and traces

```sql
SELECT
  l.Timestamp,
  l.ServiceName,
  l.Body,
  t.SpanName,
  t.Duration
FROM read_parquet('s3://bucket/logs/**/*.parquet') l
JOIN read_parquet('s3://bucket/traces/**/*.parquet') t
  ON l.TraceId = t.TraceId
WHERE l.SeverityText = 'ERROR'
LIMIT 100;
```

## Other Query Engines

??? note "Apache Spark"
    ```python
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("otlp").getOrCreate()

    # Read logs
    logs = spark.read.parquet("s3://my-bucket/logs/")
    logs.filter(logs.ServiceName == "my-service").show()

    # Read traces
    traces = spark.read.parquet("s3://my-bucket/traces/")
    traces.filter(traces.StatusCode == "STATUS_CODE_ERROR") \
          .orderBy("Duration", ascending=False) \
          .show(20)

    # Read gauge metrics
    metrics = spark.read.parquet("s3://my-bucket/metrics/gauge/")
    metrics.groupBy("MetricName").avg("Value").show()

    # Join logs and traces
    error_logs = logs.filter(logs.SeverityText == "ERROR")
    error_traces = error_logs.join(traces, "TraceId")
    error_traces.select("Timestamp", "ServiceName", "Body", "SpanName", "Duration").show()
    ```

## Tips

**Partition pruning**: Use time-based filters to skip scanning irrelevant files:

```sql
-- Good: scans only recent partitions
WHERE Timestamp > current_timestamp - interval '1 hour'

-- Bad: scans all files
WHERE ServiceName = 'api'
```

**Column projection**: Select only needed columns to reduce I/O:

```sql
-- Good: reads 3 columns
SELECT ServiceName, Timestamp, Body FROM ...

-- Bad: reads all columns
SELECT * FROM ...
```

**Predicate pushdown**: Filter in the query, not after:

```sql
-- Good: filtering happens during scan
WHERE SeverityText = 'ERROR'

-- Bad: filtering happens after full scan
... | SELECT * WHERE severity = 'ERROR'
```

**Glob patterns**: Use specific paths when possible:

```sql
-- Good: scans one service
FROM read_parquet('s3://bucket/logs/my-service/**/*.parquet')

-- Less efficient: scans all services
FROM read_parquet('s3://bucket/logs/**/*.parquet')
WHERE ServiceName = 'my-service'
```
