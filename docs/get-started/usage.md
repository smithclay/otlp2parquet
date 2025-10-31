# Usage

Once `otlp2parquet` is deployed, you can start sending OpenTelemetry logs and querying the resulting Parquet files.

## Send Logs

Send logs to your deployed `otlp2parquet` instance. The exact endpoint will depend on your deployment method (e.g., `http://localhost:8080/v1/logs` for Docker, your Worker URL for Cloudflare, or your Function URL for AWS Lambda).

```bash
curl -X POST http://localhost:8080/v1/logs \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @logs.pb
```

## Query with DuckDB

Files are stored in a Hive-style partitioned format: `logs/{service}/year={yyyy}/month={mm}/day={dd}/hour={hh}/{uuid}.parquet`.

You can query these Parquet files directly from object storage using tools like DuckDB. Below is an example for querying logs stored in MinIO (used in Docker local development):

```sql
INSTALL httpfs; LOAD httpfs;
SET s3_endpoint='localhost:9000'; SET s3_url_style='path'; SET s3_use_ssl=false;
SET s3_access_key_id='minioadmin'; SET s3_secret_access_key='minioadmin';

SELECT Timestamp, ServiceName, SeverityText, Body
FROM read_parquet('s3://otlp-logs/logs/**/*.parquet')
ORDER BY Timestamp DESC LIMIT 10;
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
