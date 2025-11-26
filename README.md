# otlp2parquet

[![CI](https://github.com/smithclay/otlp2parquet/actions/workflows/ci.yml/badge.svg)](https://github.com/smithclay/otlp2parquet/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/otlp2parquet)](https://crates.io/crates/otlp2parquet)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

What if your observability data was just Parquet files?

Receive OpenTelemetry logs, metrics, and traces over HTTP and write them as Parquet to local disk, cloud storage or [Apache Iceberg](https://iceberg.apache.org/). Query with DuckDB, Spark, or anything that reads Parquet.

## Quick Start

```bash
# requires Rust toolchain: `curl https://sh.rustup.rs -sSf | sh`
cargo install otlp2parquet

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

## Why does this exist?

- **Keep logs forever** — Parquet on S3 costs ~$0.02/GB/month vs $2+/GB in most vendors
- **Query with real tools** — DuckDB, Spark, Athena, Trino, pandas
- **Add Iceberg** — Optional catalog support for table semantics and time travel
- **Deploy anywhere** — Local binary, Cloudflare Workers, AWS Lambda

## Deploy to the Cloud

Once you've kicked the tires locally, deploy to serverless:

**Cloudflare Workers + R2:**
```bash
# Generates Cloudformation template
otlp2parquet deploy cloudflare

# Deploy with Cloudformation
aws cloudformation deploy --template-file template.yaml --stack-name otlp2parquet
```

**AWS Lambda + S3:**
```bash
# Generates a wranger.toml file
otlp2parquet deploy aws

# Deploy to Cloudflare with wrangler
wrangler deploy
```

Both commands walk you through setup and generate the config files you need.

## Supported Signals

Logs ✅ Metrics ✅ Traces ✅ — all via OTLP/HTTP (protobuf or JSON). No gRPC support for now.

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
