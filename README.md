# otlp2parquet

[![CI](https://github.com/smithclay/otlp2parquet/actions/workflows/ci.yml/badge.svg)](https://github.com/smithclay/otlp2parquet/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/otlp2parquet)](https://crates.io/crates/otlp2parquet)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

What if your observability data was just Parquet files?

This tool receives OpenTelemetry logs, metrics, and traces over HTTP and writes them as Parquet to local disk or cloud storage. Query with DuckDB, Spark, or anything that reads Parquet.

## Quick Start

```bash
brew install smithclay/tap/otlp2parquet  # coming soon
cargo install otlp2parquet               # from crates.io (after publish)

otlp2parquet
```

Server starts on `http://localhost:4318`. Send some data:

```bash
curl -X POST http://localhost:4318/v1/logs \
  -H "Content-Type: application/json" \
  -d '{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":{"stringValue":"hello world"}}]}]}]}'
```

Query it:

```bash
duckdb -c "SELECT * FROM 'data/logs/**/*.parquet'"
```

## What You Can Do With It

- **Keep logs forever** — Parquet on S3 costs ~$0.02/GB/month vs $2+/GB in most vendors
- **Query with real tools** — DuckDB, Spark, Athena, Trino, pandas
- **Add Iceberg** — Optional catalog support for table semantics and time travel
- **Deploy anywhere** — Local binary, Cloudflare Workers, AWS Lambda

## Deploy to the Cloud

Once you've kicked the tires locally, deploy to serverless:

**Cloudflare Workers + R2:**
```bash
otlp2parquet deploy cloudflare
```

**AWS Lambda + S3:**
```bash
otlp2parquet deploy aws
```

Both commands walk you through setup and generate the config files you need.

## Supported Signals

Logs ✅ Metrics ✅ Traces ✅ — all via OTLP/HTTP (protobuf or JSON)

## Learn More

- [Sending data from your app](docs/sending-data.md)
- [Querying Parquet files](docs/querying.md)
- [Configuration reference](docs/reference.md)
- [Full deployment guide](docs/deploying.md)

---

<details>
<summary>Caveats</summary>

- **Batching**: Serverless deployments write one file per request. Use an OTel Collector upstream to batch, or enable S3 Tables / R2 Data Catalog for automatic compaction.
- **Schema**: Uses ClickHouse-compatible column names. Will converge with OTel Arrow (OTAP) when it stabilizes.
- **Status**: Functional but evolving. API may change.

</details>
