# otlp2parquet

> Store your observability data in cheap object storage. Servers optional.

**otlp2parquet** ingests OpenTelemetry logs, traces, and metrics, converts them to Parquet, and writes to object storage. Deploy to Cloudflare Workers, AWS Lambda, or run locally with Docker.

## Why otlp2parquet?

- **Cheap storage** - Write to S3, R2, or local filesystem. No vendor lock-in.
- **Serverless** - Deploy to Cloudflare Workers (<3MB WASM) or AWS Lambda.
- **Query anywhere** - DuckDB, Athena, Spark - any Parquet reader works.
- **Optional Iceberg** - Add ACID transactions with S3 Tables or R2 Data Catalog.

## Get Started

Deploy in under 5 minutes:

```bash
# Install the CLI
cargo install otlp2parquet

# Deploy to your platform
otlp2parquet deploy cloudflare
otlp2parquet deploy aws
```

Or run locally with Docker:

```bash
docker-compose up
curl -X POST http://localhost:4318/v1/logs \
  -H "Content-Type: application/json" \
  -d '{"resourceLogs":[{"resource":{},"scopeLogs":[{"scope":{},"logRecords":[{"body":{"stringValue":"Hello"}}]}]}]}'
```

[**Deploy Now →**](deploying.md)
