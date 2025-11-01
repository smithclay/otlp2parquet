# Usage

Once `otlp2parquet` is deployed, you can start sending OpenTelemetry logs and metrics, then query the resulting Parquet files.

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

## Troubleshooting

### OTLP Protobuf Parse Errors

Ensure you're sending valid OTLP v1.3.2 format:
```bash
# Verify with otel-cli
otel-cli logs --protocol http/protobuf --dry-run
```

### Storage Write Failures

**Cloudflare Workers:**
- Verify R2 bucket binding in `wrangler.toml`
- Check bucket permissions

**AWS Lambda:**
- Verify IAM role has `s3:PutObject` permission
- Check `AWS_REGION` and `BUCKET_NAME` environment variables

**Server Mode (Docker/Local):**
- **Filesystem:** Verify `STORAGE_PATH` directory exists and is writable
- **S3:** Verify AWS credentials and S3 bucket permissions
- **R2:** Verify R2 credentials and bucket permissions
- Check `/health` and `/ready` endpoints for diagnostics
