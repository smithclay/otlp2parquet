# Usage

Once `otlp2parquet` is deployed, you can start sending OpenTelemetry logs, metrics, and traces, then query the resulting Parquet files.

## Send Logs

Send logs to your deployed `otlp2parquet` instance. The exact endpoint will depend on your deployment method (e.g., `http://localhost:4318/v1/logs` for Docker, your Worker URL for Cloudflare, or your Function URL for AWS Lambda).

```bash
# Protobuf format
curl -X POST http://localhost:4318/v1/logs \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @logs.pb

# JSON format
curl -X POST http://localhost:4318/v1/logs \
  -H "Content-Type: application/json" \
  -d @logs.json
```

## Send Metrics

Send metrics to the `/v1/metrics` endpoint. Multiple formats are supported:

```bash
# Protobuf format
curl -X POST http://localhost:4318/v1/metrics \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @metrics.pb

# JSON format
curl -X POST http://localhost:4318/v1/metrics \
  -H "Content-Type: application/json" \
  -d @metrics.json

# JSONL format
curl -X POST http://localhost:4318/v1/metrics \
  -H "Content-Type: application/x-ndjson" \
  --data-binary @metrics.jsonl
```

The response includes counts per metric type:

```json
{
  "status": "ok",
  "data_points_processed": 150,
  "gauge_count": 50,
  "sum_count": 75,
  "histogram_count": 25,
  "exponential_histogram_count": 0,
  "summary_count": 0,
  "partitions": [
    "metrics/gauge/my-service/year=2024/.../file.parquet",
    "metrics/sum/my-service/year=2024/.../file.parquet",
    "metrics/histogram/my-service/year=2024/.../file.parquet"
  ]
}
```

## Send Traces

Send traces to the `/v1/traces` endpoint:

```bash
# Protobuf format
curl -X POST http://localhost:4318/v1/traces \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @traces.pb

# JSON format
curl -X POST http://localhost:4318/v1/traces \
  -H "Content-Type: application/json" \
  -d @traces.json

# JSONL format
curl -X POST http://localhost:4318/v1/traces \
  -H "Content-Type: application/x-ndjson" \
  --data-binary @traces.jsonl
```

## Query with DuckDB

### Logs

Files are stored in a Hive-style partitioned format: `logs/{service}/year={yyyy}/month={mm}/day={dd}/hour={hh}/{timestamp}-{hash}.parquet`.

```sql
INSTALL httpfs; LOAD httpfs;
SET s3_endpoint='localhost:9000'; SET s3_url_style='path'; SET s3_use_ssl=false;
SET s3_access_key_id='minioadmin'; SET s3_secret_access_key='minioadmin';

SELECT Timestamp, ServiceName, SeverityText, Body
FROM read_parquet('s3://otlp-logs/logs/**/*.parquet')
ORDER BY Timestamp DESC LIMIT 10;
```

### Metrics

Metrics are partitioned by type: `metrics/{type}/{service}/year={yyyy}/month={mm}/day={dd}/hour={hh}/{timestamp}-{hash}.parquet`

Each metric type has its own schema for optimal query performance:

```sql
-- Query gauge metrics
SELECT Timestamp, ServiceName, MetricName, Value, Attributes
FROM read_parquet('s3://otlp-logs/metrics/gauge/**/*.parquet')
WHERE MetricName = 'cpu.usage'
ORDER BY Timestamp DESC LIMIT 10;

-- Query sum metrics with temporality
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

-- Query all metric types together
SELECT 'gauge' as type, COUNT(*) as count FROM read_parquet('s3://otlp-logs/metrics/gauge/**/*.parquet')
UNION ALL
SELECT 'sum', COUNT(*) FROM read_parquet('s3://otlp-logs/metrics/sum/**/*.parquet')
UNION ALL
SELECT 'histogram', COUNT(*) FROM read_parquet('s3://otlp-logs/metrics/histogram/**/*.parquet');
```

### Traces

Traces are partitioned by service: `traces/{service}/year={yyyy}/month={mm}/day={dd}/hour={hh}/{timestamp}-{hash}.parquet`

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

-- Query spans for a specific service
SELECT Timestamp, Name, Kind, StatusCode, Attributes
FROM read_parquet('s3://otlp-logs/traces/**/*.parquet')
WHERE ServiceName = 'my-service'
ORDER BY Timestamp DESC LIMIT 10;
```

## Troubleshooting

For detailed configuration options, see the [Configuration Guide](configuration.md).

### OTLP Protobuf Parse Errors

Ensure you're sending valid OTLP v1.3.2 format:
```bash
# Verify with otel-cli
otel-cli logs --protocol http/protobuf --dry-run
```

### Storage Write Failures

**Cloudflare Workers:**
- Verify R2 bucket binding in `wrangler.toml`
- Check environment variables: `OTLP2PARQUET_R2_BUCKET`, `OTLP2PARQUET_R2_ACCOUNT_ID`, `OTLP2PARQUET_R2_ACCESS_KEY_ID`
- Ensure secret is set: `wrangler secret put OTLP2PARQUET_R2_SECRET_ACCESS_KEY`
- Check bucket permissions

**AWS Lambda:**
- Verify IAM role has `s3:PutObject` permission
- Check environment variables: `OTLP2PARQUET_S3_BUCKET`, `OTLP2PARQUET_S3_REGION`
- Lambda automatically uses S3 backend (validated at startup)

**Server Mode (Docker/Local):**
- **Filesystem:** Verify `OTLP2PARQUET_STORAGE_PATH` directory exists and is writable
- **S3:** Verify AWS credentials and S3 bucket permissions
  - Set: `OTLP2PARQUET_STORAGE_BACKEND=s3`
  - Set: `OTLP2PARQUET_S3_BUCKET` and `OTLP2PARQUET_S3_REGION`
- **R2:** Verify R2 credentials and bucket permissions
  - Set: `OTLP2PARQUET_STORAGE_BACKEND=r2`
  - Set: `OTLP2PARQUET_R2_*` variables
- Check `/health` and `/ready` endpoints for diagnostics

### Configuration Issues

**Check active configuration:**
```bash
# Server mode - check startup logs
docker-compose logs otlp2parquet | grep -E "(storage|batch|payload)"

# Lambda - check CloudWatch logs
sam logs --tail

# Cloudflare Workers - check logs
wrangler tail
```

**Common issues:**
- Environment variable names must use `OTLP2PARQUET_` prefix
- Storage backend must match platform (S3 for Lambda, R2 for Cloudflare)
- Required fields must be set (bucket name, region, credentials)
