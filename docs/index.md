# otlp2parquet

> Put your observability data in cheap object storage, servers optional.

`otlp2parquet` ingests OpenTelemetry logs, metrics, and traces and stores them in object storage as Parquet files. It runs natively in serverless runtimes like AWS Lambda and Cloudflare Workers, or as a standalone server.

## Features

- **Universal OTLP Ingestion**: Accepts OTLP HTTP in protobuf, JSON, or JSONL format
- **All Signals**: Logs, metrics (all 5 types), and traces with full fidelity
- **ClickHouse-Compatible Schema**: Query with DuckDB, ClickHouse, or any Parquet-compatible tool
- **Multi-Platform**: Docker, Cloudflare Workers (WASM), or AWS Lambda
- **Small & Fast**: ~5 MB binary, written in Rust with aggressive size optimization

## Platform Support

| Feature / Platform | Docker (Server) | Cloudflare Workers | AWS Lambda |
| :----------------- | :----------------- | :----------------- | :--------- |
| **OTLP Protocol** | | | |
| HTTP-JSON | ✅ | ✅ | ✅ |
| HTTP-Protobuf | ✅ | ✅ | ✅ |
| gRPC | ❌ | ❌ | ❌ |
| **Signals** | | | |
| Logs | ✅ | ✅ | ✅ |
| Metrics (all types) | ✅ | ✅ | ✅ |
| Traces | ✅ | ✅ | ✅ |
| **Features** | | | |
| In-Memory Batching | ✅ | ❌* | ❌* |

*For serverless deployments, use an upstream agent like [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) or [Vector](https://vector.dev/) for batching ([see example](get-started/usage.md#sending-data-via-opentelemetry-collector-recommended)).

---

## Quick Start

Choose your deployment platform and get started in minutes:

### Docker (Recommended for Local Development)

Run locally with Docker Compose and MinIO:

```bash
docker-compose up
```

Access MinIO console at [http://localhost:9001](http://localhost:9001) (credentials: `minioadmin`/`minioadmin`).

The OTLP endpoint will be available at `http://localhost:4318`.

**[→ Full Docker Guide](get-started/deployment/docker.md)**

### Cloudflare Workers

Deploy to Cloudflare Workers with one click:

[![Deploy to Cloudflare Workers](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/smithclay/otlp2parquet)

Or deploy from the command line:

```bash
cd crates/otlp2parquet-cloudflare
wrangler deploy
```

> **Note**: CPU limits on the free tier may cause timeouts. A paid plan is recommended.

**[→ Full Cloudflare Guide](get-started/deployment/cloudflare.md)**

### AWS Lambda

Deploy to AWS using SAM CLI:

```bash
cd crates/otlp2parquet-lambda
sam deploy --guided
```

**[→ Full Lambda Guide](get-started/deployment/aws-lambda.md)**

---

## Sending Data

### Direct HTTP Requests

Send OTLP data directly to the appropriate endpoint. Replace `localhost:4318` with your deployment URL:

```bash
# Logs (JSON format)
curl -X POST http://localhost:4318/v1/logs \
  -H "Content-Type: application/json" \
  -d @testdata/log.json

# Metrics (Protobuf format)
curl -X POST http://localhost:4318/v1/metrics \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @testdata/metrics_gauge.pb

# Traces (JSON format)
curl -X POST http://localhost:4318/v1/traces \
  -H "Content-Type: application/json" \
  -d @testdata/trace.json
```

### Via OpenTelemetry Collector (Recommended for Production)

For serverless deployments, batch data using the OpenTelemetry Collector to optimize performance and reduce costs:

```yaml
# otel-collector-config.yaml
receivers:
  otlp:
    protocols:
      grpc:
      http:

processors:
  batch:
    send_batch_max_size: 104857600  # 100 MiB
    timeout: 10s

exporters:
  otlphttp:
    endpoint: http://localhost:4318/  # Your otlp2parquet URL

service:
  pipelines:
    logs:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlphttp]
    metrics:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlphttp]
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlphttp]
```

**[→ Full Usage Guide](get-started/usage.md)**

---

## Querying Your Data

The Parquet files use a ClickHouse-compatible schema and can be queried with DuckDB, ClickHouse, or any Parquet-compatible tool.

### Example: Query Logs with DuckDB

```sql
-- Query logs from the last hour
SELECT
  Timestamp,
  ServiceName,
  SeverityText,
  Body
FROM 'logs/**/*.parquet'
WHERE Timestamp > NOW() - INTERVAL 1 HOUR
ORDER BY Timestamp DESC
LIMIT 100;
```

### Example: Analyze Metrics

```sql
-- Average request duration by service
SELECT
  ServiceName,
  AVG(Value) as avg_duration_ms
FROM 'metrics/gauge/**/*.parquet'
WHERE MetricName = 'http.server.duration'
GROUP BY ServiceName;
```

**[→ Full Querying Guide](get-started/querying-data.md)**

---

## Configuration

Configure `otlp2parquet` using environment variables with the `OTLP2PARQUET_` prefix:

```bash
# Storage backend (auto-detected by platform)
OTLP2PARQUET_STORAGE_BACKEND=s3  # s3, r2, gcs, azure, or fs

# S3/R2 configuration
OTLP2PARQUET_S3_BUCKET=my-telemetry-bucket
OTLP2PARQUET_S3_REGION=us-east-1

# Batch settings (server mode only)
OTLP2PARQUET_BATCH_MAX_SIZE_BYTES=104857600  # 100 MiB
OTLP2PARQUET_BATCH_MAX_AGE_SECONDS=300       # 5 minutes
```

**[→ Full Configuration Reference](get-started/configuration.md)**

---

## Next Steps

- **[Deployment Guides](get-started/deployment/docker.md)**: Detailed setup for each platform
- **[Usage Patterns](get-started/usage.md)**: Advanced usage and troubleshooting
- **[Developer Docs](developers/index.md)**: Architecture, codebase overview, and contributing
- **[Schema Reference](developers/schema.md)**: Parquet schema details

## Project Status

While functional, this project is **experimental** as the API and schema will evolve. We plan to converge with the official [OpenTelemetry Arrow Protocol](https://github.com/open-telemetry/otel-arrow) once it becomes generally available.

## Contributing

We welcome contributions! See the [Developer Documentation](developers/index.md) for guidelines.

## License

Apache-2.0
