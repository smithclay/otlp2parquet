# otlp2parquet

> Store your observability data in cheap object storage. Servers optional.

**otlp2parquet** ingests OpenTelemetry logs, traces, and metrics, converts them to Parquet, and writes to object storage. Run locally with Docker or on your own servers.

## Why otlp2parquet?

- **Cheap storage** - Write to S3-compatible object storage or local filesystem. No vendor lock-in.
- **Portable** - Run as a local binary or container.
- **Query anywhere** - DuckDB, Spark, Trino - any Parquet reader works.

## Get Started

Deploy in under 5 minutes:

```bash
# Install the CLI
cargo install otlp2parquet

# Run locally
otlp2parquet
```

Or run locally with Docker:

```bash
docker-compose up
curl -X POST http://localhost:4318/v1/logs \
  -H "Content-Type: application/json" \
  -d '{"resourceLogs":[{"resource":{},"scopeLogs":[{"scope":{},"logRecords":[{"body":{"stringValue":"Hello"}}]}]}]}'
```

[**Deploy Now →**](deploying.md)
