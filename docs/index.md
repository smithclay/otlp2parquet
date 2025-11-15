# OTLP to Parquet

> Store your observability data in cheap object storage. Servers optional.

`otlp2parquet` ingests OpenTelemetry (OTLP) data, converts it to Parquet, and writes it to object storage with optional Apache Iceberg support. It runs anywhere—from a Docker container to serverless platforms like AWS Lambda and Cloudflare Workers—providing a fast, cost-effective way to store and query your telemetry.

---

## How It Works

Getting started is a three-step process.

### 1. Deploy the lightweight binary

Choose a platform and follow the setup guide. Each guide provides a production-ready starting point.

*   [**Docker Guide**](./setup/docker.md)
*   [**AWS Lambda Guide**](./setup/aws-lambda.md)
*   [**Cloudflare Workers Guide**](./setup/cloudflare.md)

### 2. Send Telemetry Data

Point your OpenTelemetry SDK or Collector to your new endpoint. This example sends a test log record via `curl`.

```bash
curl -X POST http://localhost:4318/v1/logs \
  -H "Content-Type: application/json" \
  -d @testdata/log.json
```

### 3. Query Your Data

Use any Parquet-aware query engine to analyze your data directly in object storage. This example uses DuckDB to query the local MinIO container from the Docker setup.

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

*   **[Concepts](./concepts/architecture.md)**: Learn about the architecture, schema, and storage layers.
*   **[Setup Guides](./setup/overview.md)**: Get detailed setup instructions for each platform.
*   **[Sending Data](./guides/sending-data.md)**: Learn how to send logs, traces, and metrics.
*   **[Querying Data](./guides/querying-data.md)**: See more advanced query examples.
*   **[Configuration](./concepts/configuration.md)**: View all configuration options.
