# otlp2parquet

> Put your observability data in cheap object storage, servers optional.

`otlp2parquet` ingests OpenTelemetry (OTLP) data and stores it as compressed Parquet files in object storage. It runs in various environments, including Docker, AWS Lambda, and Cloudflare Workers, providing a fast and cost-effective way to manage your observability data.

---

## Quick Start

This guide gets you from zero to querying your data in three steps.

### Choose a Deployment Method

Pick your preferred platform and run the command.

*   **Docker (Recommended for Local)**: Starts a local server and a MinIO S3-compatible storage container.
    ```bash
    docker-compose up
    ```
*   **Cloudflare Workers**: Deploys to the Cloudflare global network. A paid plan is recommended.
    ```bash
    cd crates/otlp2parquet-cloudflare && wrangler deploy
    ```
*   **AWS Lambda**: Deploys to your AWS account using the SAM CLI.
    ```bash
    cd crates/otlp2parquet-lambda && sam deploy --guided
    ```

### Send Telemetry Data

Once deployed, send an OTLP request to your endpoint. This example sends a log record to the local Docker endpoint.

```bash
curl -X POST http://localhost:4318/v1/logs \
  -H "Content-Type: application/json" \
  -d @testdata/log.json
```

### Query Your Data

Use DuckDB to query the Parquet file directly from object storage. This example queries the local MinIO container.

```sql
-- Configure DuckDB for local MinIO
INSTALL httpfs; LOAD httpfs;
SET s3_endpoint='localhost:9000';
SET s3_url_style='path';
SET s3_use_ssl=false;
SET s3_access_key_id='minioadmin';
SET s3_secret_access_key='minioadmin';

-- Query the log record you just sent
SELECT Timestamp, ServiceName, Body
FROM read_parquet('s3://otlp-logs/logs/**/*.parquet');
```

## Next Steps

*   **[Sending Data](sending-data.md)**: Learn about sending different signals and using the OpenTelemetry Collector.
*   **[Querying Data](querying-data.md)**: See more advanced query examples.
*   **[Deployment Guides](deployment/docker.md)**: Get detailed setup instructions for each platform.
*   **[Configuration](configuration.md)**: View all configuration options.
