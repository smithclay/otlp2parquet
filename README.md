# otlp2parquet

[![CI](https://github.com/smithclay/otlp2parquet/actions/workflows/ci.yml/badge.svg)](https://github.com/smithclay/otlp2parquet/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/otlp2parquet)](https://crates.io/crates/otlp2parquet)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

> What if your observability data was just a bunch of Parquet files?

Receive OpenTelemetry logs, metrics, and traces and write them as Parquet files to local disk, cloud storage or [Apache Iceberg](https://iceberg.apache.org/). Query with DuckDB, Spark, or anything that reads Parquet.

![otlp2parquet architecture](docs/otlp2parquet-arch-dec2025.png)

## Quick Start

See [Deploy to Cloud](#deploy-to-the-cloud) for running in an AWS Lambda or Cloudflare Worker.

```bash
# requires rust toolchain: `curl https://sh.rustup.rs -sSf | sh`
cargo install otlp2parquet

otlp2parquet
```

Server starts on `http://localhost:4318`. Send a simple OTLP HTTP log:

```bash
curl -X POST http://localhost:4318/v1/logs \
  -H "Content-Type: application/json" \
  -d '{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":{"stringValue":"hello world"}}]}]}]}'
```

Query it:

```bash
# see https://duckdb.org/install
duckdb -c "SELECT * FROM './data/logs/**/*.parquet'"
```

## Why?

- **Keep monitoring data around a long time** Parquet on S3 can be 90% cheaper than large monitoring vendors for long-term analytics.
- **Query with good tools** — duckDB, Spark, Athena, Trino, Pandas
- **Easy Iceberg** — Optional catalog support, including [S3 Tables](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables.html) and [R2 Data Catalog](https://developers.cloudflare.com/r2/data-catalog/)
- **Deploy anywhere** — Local binary, Cloudflare Workers (WASM), AWS Lambda. In basic testing, converting to Parquet with a Lambda or Worker costs around $0.01 to $0.02 per uncompressed GB of log data in compute.

## Deploy to the Cloud

Once you've kicked the tires locally, deploy to serverless:

**Cloudflare Workers + R2 or R2 Data Catalog with [wrangler](https://developers.cloudflare.com/workers/wrangler/install-and-update/) CLI:**
```bash
# Generates config for workers
otlp2parquet deploy cloudflare

# Deploy to Cloudflare
wrangler deploy
```

**AWS Lambda + S3 or S3 Tables with [AWS CLI](https://aws.amazon.com/cli/):**
```bash
# Generates a Cloudformation template for Lambda + S3
otlp2parquet deploy aws

# Deploy with Cloudformation
aws cloudformation deploy --template-file template.yaml --stack-name otlp2parquet --capabilities CAPABILITY_IAM

# Send a log (requires IAM sigv4 auth by default)
uvx awscurl \
  --service lambda \
  --region $AWS_REGION \
  -X POST $FUNCTION_URL \
  -H "Content-Type: application/json" \
  -d '{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":{"stringValue":"hello world"}}]}]}]}'
```

Both commands walk you through setup and generate the config files you need.

## Supported Signals

Logs, Metrics, Traces via OTLP/HTTP (protobuf or JSON, gzip compression supported). No gRPC support for now.


## APIs, schemas, and partition layout
- OTLP/HTTP endpoints: `/v1/logs`, `/v1/metrics`, `/v1/traces` (protobuf or JSON; gzip supported)
- Partition layout: `logs/{service}/year=.../hour=.../{ts}-{uuid}.parquet`, `metrics/{type}/{service}/...`, `traces/{service}/...`
- Storage: filesystem, S3, or R2 with optional Iceberg catalog
- Schemas: ClickHouse-compatible, PascalCase columns; five metric schemas (Gauge, Sum, Histogram, ExponentialHistogram, Summary)
- Error model: HTTP 400 on invalid input/too large; 5xx on conversion/storage

**Best-effort catalog commits**: Parquet files are always written to storage first. If you enable an Iceberg catalog (S3 Tables, R2 Data Catalog), catalog registration happens after the write.

If catalog registration fails (network error, conflict), the data is still safely stored and a warning is logged—your data is never lost due to catalog issues. **This is not a production-ready solution for catalog commits**. PRs welcome.

## Future work (contributions welcome)
- OpenTelemetry Arrow alignment
- Additional platforms: Azure Functions; Kubernetes manifests
- Iceberg ergonomics: queued commits (SQS/Queues), richer partition configs


## Learn More

- [Sending data from your app](https://smithclay.github.io/otlp2parquet/sending-data/)
- [Querying Parquet files](https://smithclay.github.io/otlp2parquet/querying/)
- [Configuration reference](https://smithclay.github.io/otlp2parquet/reference/)
- [Full deployment guide](https://smithclay.github.io/otlp2parquet/deploying/)

---

<details>
<summary>Caveats</summary>

- **Batching**: Serverless deployments write one file per request. Don't write a lot of small files or your performance and cloud bill will explode. Use an OTel Collector upstream to batch, or enable S3 Tables / R2 Data Catalog for automatic compaction.
- **Schema**: Uses ClickHouse-compatible column names. Will converge with OTel Arrow (OTAP) when it stabilizes.
- **Status**: Functional but evolving. API may change.

</details>
