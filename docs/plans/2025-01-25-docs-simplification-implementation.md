# Docs Simplification Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Reduce docs from 25+ files to 5 task-oriented pages centered on the CLI `deploy` command.

**Architecture:** Create new consolidated pages, then delete old directories. Update mkdocs.yml to reflect new structure.

**Tech Stack:** Markdown, MkDocs Material (tabs, admonitions, details)

---

## Task 1: Create new index.md (landing page)

**Files:**
- Replace: `docs/index.md`

**Step 1: Write the new index.md**

```markdown
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
```

**Step 2: Commit**

```bash
git add docs/index.md
git commit -m "docs: simplify landing page"
```

---

## Task 2: Create deploying.md (unified setup)

**Files:**
- Create: `docs/deploying.md`

**Step 1: Write deploying.md with platform tabs**

```markdown
# Deploy

Deploy otlp2parquet to Cloudflare Workers, AWS Lambda, or run locally with Docker.

## Prerequisites

Install the CLI:

```bash
cargo install otlp2parquet
```

Or download from [GitHub Releases](https://github.com/smithclay/otlp2parquet/releases).

---

## Cloudflare Workers

Deploy to Cloudflare's global edge network with R2 storage.

### Quick Start

```bash
otlp2parquet deploy cloudflare
```

The wizard prompts for:
- **Worker name** (default: auto-generated like `swift-beacon-4821`)
- **R2 bucket name**
- **Cloudflare Account ID**
- **Catalog mode** - Plain Parquet or Iceberg (R2 Data Catalog)

Then follow the output:

```bash
# 1. Create bucket
wrangler r2 bucket create my-bucket

# 2. Set secrets
wrangler secret put AWS_ACCESS_KEY_ID
wrangler secret put AWS_SECRET_ACCESS_KEY

# 3. Deploy
wrangler deploy
```

### Send test data

```bash
curl -X POST https://your-worker.workers.dev/v1/logs \
  -H "Content-Type: application/json" \
  -d '{"resourceLogs":[{"resource":{},"scopeLogs":[{"scope":{},"logRecords":[{"body":{"stringValue":"Hello"}}]}]}]}'
```

??? note "Non-interactive mode (CI/CD)"
    ```bash
    otlp2parquet deploy cloudflare \
      --worker-name my-worker \
      --bucket my-logs \
      --account-id abc123def456... \
      --catalog none \
      --force
    ```

??? note "Manual setup"
    If you prefer to create `wrangler.toml` manually, see the [generated template](https://github.com/smithclay/otlp2parquet/blob/main/crates/otlp2parquet-server/templates/wrangler.toml) for reference.

??? warning "Production considerations"
    - **Authentication**: Enable `OTLP2PARQUET_BASIC_AUTH_ENABLED` or add Cloudflare Access
    - **Batching**: Use an OTel Collector upstream to batch requests
    - **Binary size**: Current WASM is ~1.3MB (limit 3MB)

---

## AWS Lambda

Deploy to AWS Lambda with S3 or S3 Tables (Iceberg) storage.

### Quick Start

1. Download the Lambda binary from [GitHub Releases](https://github.com/smithclay/otlp2parquet/releases)
2. Upload to your S3 bucket:
   ```bash
   aws s3 cp otlp2parquet-lambda-arm64.zip s3://my-bucket/
   ```
3. Generate the CloudFormation template:
   ```bash
   otlp2parquet deploy aws
   ```

The wizard prompts for:
- **Lambda S3 URI** (e.g., `s3://my-bucket/otlp2parquet-lambda-arm64.zip`)
- **Stack name** (default: auto-generated)
- **Data bucket name**
- **Catalog mode** - Plain Parquet (S3) or Iceberg (S3 Tables)

Then deploy:

```bash
aws cloudformation deploy \
  --template-file template.yaml \
  --stack-name my-stack \
  --capabilities CAPABILITY_IAM
```

### Get your endpoint

```bash
aws cloudformation describe-stacks \
  --stack-name my-stack \
  --query 'Stacks[0].Outputs[?OutputKey==`FunctionUrl`].OutputValue' \
  --output text
```

??? note "Non-interactive mode (CI/CD)"
    ```bash
    otlp2parquet deploy aws \
      --lambda-s3-uri s3://my-bucket/lambda.zip \
      --stack-name my-stack \
      --bucket my-data \
      --catalog none \
      --force
    ```

??? warning "Production considerations"
    - **Authentication**: Lambda URL uses IAM auth by default (SigV4)
    - **Cold starts**: First invocation takes 5-10s; configure retries in OTel exporter
    - **Batching**: Use an OTel Collector to batch requests and reduce invocations

---

## Docker (Local / Self-Hosted)

Run locally for development or self-host in your own infrastructure.

### Quick Start

```bash
docker-compose up
```

This starts:
- **otlp2parquet** on port 4318
- **MinIO** (S3-compatible storage) on ports 9000/9001

### Send test data

```bash
curl -X POST http://localhost:4318/v1/logs \
  -H "Content-Type: application/json" \
  -d '{"resourceLogs":[{"resource":{},"scopeLogs":[{"scope":{},"logRecords":[{"body":{"stringValue":"Hello"}}]}]}]}'
```

### Verify output

Open MinIO Console at `http://localhost:9001` (login: `minioadmin`/`minioadmin`).

??? note "Production with cloud storage"
    Point Docker at real S3/R2 instead of MinIO:
    ```bash
    OTLP2PARQUET_STORAGE_BACKEND=s3 \
    OTLP2PARQUET_S3_BUCKET=my-prod-bucket \
    OTLP2PARQUET_S3_REGION=us-west-2 \
    docker-compose up
    ```

??? note "Common commands"
    - View logs: `docker-compose logs -f otlp2parquet`
    - Reset data: `docker-compose down -v`
    - Rebuild: `docker-compose up --build`
```

**Step 2: Commit**

```bash
git add docs/deploying.md
git commit -m "docs: add unified deploying page"
```

---

## Task 3: Create sending-data.md

**Files:**
- Create: `docs/sending-data.md`

**Step 1: Write sending-data.md**

```markdown
# Send Data

Send OpenTelemetry data to your otlp2parquet endpoint.

## Endpoints

| Signal | Endpoint | Content-Type |
|--------|----------|--------------|
| Logs | `/v1/logs` | `application/json` or `application/x-protobuf` |
| Traces | `/v1/traces` | `application/json` or `application/x-protobuf` |
| Metrics | `/v1/metrics` | `application/json` or `application/x-protobuf` |

## Quick Test (curl)

=== "Logs"
    ```bash
    curl -X POST http://localhost:4318/v1/logs \
      -H "Content-Type: application/json" \
      -d @testdata/log.json
    ```

=== "Traces"
    ```bash
    curl -X POST http://localhost:4318/v1/traces \
      -H "Content-Type: application/json" \
      -d @testdata/trace.json
    ```

=== "Metrics"
    ```bash
    curl -X POST http://localhost:4318/v1/metrics \
      -H "Content-Type: application/json" \
      -d @testdata/metrics_gauge.json
    ```

## OpenTelemetry Collector (Recommended)

For production, use the [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) to batch data before sending. This reduces costs and creates more efficient Parquet files.

```yaml
# otel-collector-config.yaml
receivers:
  otlp:
    protocols:
      grpc:
      http:

processors:
  batch:
    send_batch_max_size: 10000
    timeout: 10s

exporters:
  otlphttp:
    endpoint: http://your-endpoint:4318/

service:
  pipelines:
    logs:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlphttp]
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlphttp]
    metrics:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlphttp]
```

## SDK Configuration

Point your OTel SDK exporter at your endpoint:

=== "Python"
    ```python
    from opentelemetry.exporter.otlp.proto.http.log_exporter import OTLPLogExporter

    exporter = OTLPLogExporter(endpoint="http://localhost:4318/v1/logs")
    ```

=== "Node.js"
    ```javascript
    const { OTLPLogExporter } = require('@opentelemetry/exporter-logs-otlp-http');

    const exporter = new OTLPLogExporter({
      url: 'http://localhost:4318/v1/logs',
    });
    ```

=== "Go"
    ```go
    import "go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"

    exporter, _ := otlploghttp.New(ctx,
      otlploghttp.WithEndpoint("localhost:4318"),
      otlploghttp.WithInsecure(),
    )
    ```

## Troubleshooting

| Problem | Solution |
|---------|----------|
| Parse errors | Ensure valid OTLP JSON/protobuf payload |
| 413 Payload Too Large | Batch smaller or increase `OTLP2PARQUET_MAX_PAYLOAD_BYTES` |
| Connection refused | Check endpoint URL and firewall rules |
| Storage write failures | Check bucket permissions and credentials |
```

**Step 2: Commit**

```bash
git add docs/sending-data.md
git commit -m "docs: add sending-data page"
```

---

## Task 4: Create querying.md

**Files:**
- Create: `docs/querying.md`

**Step 1: Write querying.md**

```markdown
# Query Data

Query your Parquet files with DuckDB, Athena, or any Parquet-compatible tool.

## DuckDB (Recommended)

[DuckDB](https://duckdb.org/) is the fastest way to query Parquet files locally or in S3.

### Local files

```sql
SELECT Timestamp, ServiceName, Body
FROM read_parquet('data/logs/**/*.parquet')
ORDER BY Timestamp DESC
LIMIT 10;
```

### S3 / MinIO

```sql
-- Configure S3 access
INSTALL httpfs; LOAD httpfs;
SET s3_region = 'us-west-2';
SET s3_access_key_id = 'your-key';
SET s3_secret_access_key = 'your-secret';

-- For MinIO (local Docker setup)
SET s3_endpoint = 'localhost:9000';
SET s3_url_style = 'path';
SET s3_use_ssl = false;

-- Query logs
SELECT Timestamp, ServiceName, Body
FROM read_parquet('s3://otlp-logs/logs/**/*.parquet')
WHERE Timestamp > current_timestamp - interval '1 hour'
LIMIT 100;
```

### Iceberg tables (S3 Tables)

```sql
INSTALL iceberg; LOAD iceberg;
CREATE SECRET (TYPE S3, REGION 'us-west-2');

SELECT * FROM iceberg_scan('s3://my-bucket/logs');
```

## Example Queries

### Logs by service

```sql
SELECT ServiceName, COUNT(*) as count
FROM read_parquet('s3://bucket/logs/**/*.parquet')
GROUP BY ServiceName
ORDER BY count DESC;
```

### Error traces

```sql
SELECT TraceId, SpanName, Duration, StatusMessage
FROM read_parquet('s3://bucket/traces/**/*.parquet')
WHERE StatusCode = 'Error'
ORDER BY Duration DESC
LIMIT 20;
```

### Metrics over time

```sql
SELECT
  date_trunc('hour', Timestamp) as hour,
  MetricName,
  AVG(Value) as avg_value
FROM read_parquet('s3://bucket/metrics/gauge/**/*.parquet')
GROUP BY hour, MetricName
ORDER BY hour;
```

## Other Query Engines

??? note "AWS Athena"
    ```sql
    -- Create external table
    CREATE EXTERNAL TABLE otlp_logs (
      Timestamp TIMESTAMP,
      ServiceName STRING,
      Body STRING
    )
    STORED AS PARQUET
    LOCATION 's3://my-bucket/logs/';

    -- Query
    SELECT * FROM otlp_logs LIMIT 10;
    ```

??? note "Apache Spark"
    ```python
    df = spark.read.parquet("s3://my-bucket/logs/")
    df.filter(df.ServiceName == "my-service").show()
    ```

## Iceberg vs Plain Parquet

| Feature | Plain Parquet | Iceberg |
|---------|---------------|---------|
| Query syntax | `read_parquet('s3://...')` | `iceberg_scan('s3://...')` |
| ACID transactions | No | Yes |
| Schema evolution | No | Yes |
| Time travel | No | Yes |
| Setup complexity | Lower | Higher |
```

**Step 2: Commit**

```bash
git add docs/querying.md
git commit -m "docs: add querying page"
```

---

## Task 5: Create reference.md

**Files:**
- Create: `docs/reference.md`

**Step 1: Write reference.md (consolidated reference)**

```markdown
# Reference

Configuration, environment variables, and schema reference.

## Environment Variables

All variables use the `OTLP2PARQUET_` prefix and override config file values.

### Storage

| Variable | Default | Description |
|----------|---------|-------------|
| `OTLP2PARQUET_STORAGE_BACKEND` | Auto | `s3`, `r2`, or `fs` |
| `OTLP2PARQUET_S3_BUCKET` | - | S3 bucket name |
| `OTLP2PARQUET_S3_REGION` | - | AWS region |
| `OTLP2PARQUET_S3_ENDPOINT` | AWS | Custom endpoint (MinIO, LocalStack) |
| `OTLP2PARQUET_R2_BUCKET` | - | R2 bucket name |
| `CLOUDFLARE_ACCOUNT_ID` | - | Cloudflare account ID |

### Catalog (Iceberg)

| Variable | Default | Description |
|----------|---------|-------------|
| `OTLP2PARQUET_CATALOG_MODE` | `none` | `none` or `iceberg` |
| `OTLP2PARQUET_ICEBERG_BUCKET_ARN` | - | S3 Tables bucket ARN (Lambda) |
| `OTLP2PARQUET_ICEBERG_NAMESPACE` | `otel` | Iceberg namespace |

### Server

| Variable | Default | Description |
|----------|---------|-------------|
| `OTLP2PARQUET_LISTEN_ADDR` | `0.0.0.0:4318` | HTTP listen address |
| `OTLP2PARQUET_LOG_LEVEL` | `info` | `trace`, `debug`, `info`, `warn`, `error` |
| `OTLP2PARQUET_MAX_PAYLOAD_BYTES` | `10485760` | Max request size (10MB) |

### Batching (Server only)

| Variable | Default | Description |
|----------|---------|-------------|
| `OTLP2PARQUET_BATCHING_ENABLED` | `true` | Enable in-memory batching |
| `OTLP2PARQUET_BATCH_MAX_ROWS` | `1000` | Max rows per batch |
| `OTLP2PARQUET_BATCH_MAX_BYTES` | `10485760` | Max bytes per batch |
| `OTLP2PARQUET_BATCH_MAX_AGE_SECS` | `300` | Max batch age (5 min) |

---

## Schema

otlp2parquet uses ClickHouse-compatible PascalCase column names.

### Logs

| Field | Type | Description |
|-------|------|-------------|
| `Timestamp` | `Timestamp(ns)` | Event time |
| `TraceId` | `Binary(16)` | Trace ID |
| `SpanId` | `Binary(8)` | Span ID |
| `SeverityText` | `String` | `INFO`, `WARN`, `ERROR`, etc. |
| `SeverityNumber` | `Int32` | Numeric severity |
| `Body` | `String` | Log message |
| `ServiceName` | `String` | Extracted from resource |
| `ResourceAttributes` | `Map<String,String>` | Resource attributes |
| `LogAttributes` | `Map<String,String>` | Log attributes |

### Traces

| Field | Type | Description |
|-------|------|-------------|
| `Timestamp` | `Timestamp(ns)` | Span start time |
| `TraceId` | `String` | Trace ID |
| `SpanId` | `String` | Span ID |
| `ParentSpanId` | `String` | Parent span ID |
| `SpanName` | `String` | Span name |
| `SpanKind` | `String` | `SERVER`, `CLIENT`, etc. |
| `Duration` | `Int64` | Duration in nanoseconds |
| `StatusCode` | `String` | `Ok`, `Error`, `Unset` |
| `ServiceName` | `String` | Extracted from resource |
| `SpanAttributes` | `Map<String,String>` | Span attributes |

### Metrics

Metrics are stored in separate tables by type: `gauge`, `sum`, `histogram`, `exponential_histogram`, `summary`.

**Common fields (all metric types):**

| Field | Type | Description |
|-------|------|-------------|
| `Timestamp` | `Timestamp(ns)` | Data point time |
| `MetricName` | `String` | Metric name |
| `ServiceName` | `String` | Extracted from resource |
| `Attributes` | `Map<String,String>` | Data point attributes |

**Type-specific fields:**

| Type | Additional Fields |
|------|-------------------|
| Gauge | `Value` (Float64) |
| Sum | `Value`, `AggregationTemporality`, `IsMonotonic` |
| Histogram | `Count`, `Sum`, `BucketCounts`, `ExplicitBounds`, `Min`, `Max` |
| Summary | `Count`, `Sum`, `QuantileValues`, `QuantileQuantiles` |

---

## Catalog Modes

| Mode | Platforms | Description |
|------|-----------|-------------|
| `none` | All | Plain Parquet files, no catalog |
| `iceberg` | All | Apache Iceberg tables |

**Iceberg catalog backends:**

| Backend | Platform | Configuration |
|---------|----------|---------------|
| S3 Tables | Lambda | `OTLP2PARQUET_ICEBERG_BUCKET_ARN` |
| R2 Data Catalog | Cloudflare | `OTLP2PARQUET_CATALOG_MODE=iceberg` in wrangler.toml |
| Nessie | Server | `OTLP2PARQUET_ICEBERG_REST_URI` |
| AWS Glue | Server | `OTLP2PARQUET_ICEBERG_REST_URI` (Glue endpoint) |

---

## File Layout

Parquet files are written with Hive-style partitioning:

```
logs/{service}/year={year}/month={month}/day={day}/hour={hour}/{uuid}.parquet
traces/{service}/year={year}/month={month}/day={day}/hour={hour}/{uuid}.parquet
metrics/{type}/{service}/year={year}/month={month}/day={day}/hour={hour}/{uuid}.parquet
```
```

**Step 2: Commit**

```bash
git add docs/reference.md
git commit -m "docs: add consolidated reference page"
```

---

## Task 6: Update mkdocs.yml

**Files:**
- Modify: `mkdocs.yml`

**Step 1: Replace nav section**

Replace the entire `nav:` section in `mkdocs.yml` with:

```yaml
nav:
  - 'Home': 'index.md'
  - 'Deploy': 'deploying.md'
  - 'Send Data': 'sending-data.md'
  - 'Query Data': 'querying.md'
  - 'Reference': 'reference.md'
  - 'Query Demo ↗': 'https://smithclay.github.io/otlp2parquet/query-demo/'
```

**Step 2: Commit**

```bash
git add mkdocs.yml
git commit -m "docs: update mkdocs nav for simplified structure"
```

---

## Task 7: Delete old docs directories

**Files:**
- Delete: `docs/setup/` (entire directory)
- Delete: `docs/concepts/` (entire directory)
- Delete: `docs/guides/` (entire directory)
- Delete: `docs/reference/` (entire directory)
- Delete: `docs/contributing/` (entire directory)
- Delete: `docs/testing/` (entire directory)

**Step 1: Remove old directories**

```bash
rm -rf docs/setup docs/concepts docs/guides docs/reference docs/contributing docs/testing
```

**Step 2: Commit**

```bash
git add -A
git commit -m "docs: remove old docs directories"
```

---

## Task 8: Move contributing content to CONTRIBUTING.md

**Files:**
- Create: `CONTRIBUTING.md` (repo root)

**Step 1: Create CONTRIBUTING.md**

```markdown
# Contributing to otlp2parquet

## Development Setup

```bash
# Clone and build
git clone https://github.com/smithclay/otlp2parquet
cd otlp2parquet
make dev

# Run tests
make test

# Run clippy
make clippy
```

## Code Style

- Use `tracing::*` macros for logging (no `println!`)
- Avoid `unwrap()`/`expect()` in production code
- Keep dependencies minimal with `default-features = false`
- Zero clippy warnings

## Testing

```bash
# Unit tests
cargo test

# E2E tests (requires Docker)
make test-e2e

# WASM size check
make wasm-size
```

## Commit Messages

Use [Conventional Commits](https://www.conventionalcommits.org/):

- `feat: add new feature`
- `fix: fix bug`
- `docs: update documentation`
- `refactor: code cleanup`
```

**Step 2: Commit**

```bash
git add CONTRIBUTING.md
git commit -m "docs: add CONTRIBUTING.md"
```

---

## Task 9: Update design doc status and final verification

**Files:**
- Modify: `docs/plans/2025-01-25-docs-simplification-design.md`

**Step 1: Mark design as implemented**

Change the status line at the top of the file from:
```markdown
> **Status:** Planned
```
to:
```markdown
> **Status:** Implemented
```

**Step 2: Verify docs build**

```bash
# If mkdocs is installed
mkdocs build

# Or just verify structure
ls docs/
# Should show: index.md deploying.md sending-data.md querying.md reference.md plans/
```

**Step 3: Final commit**

```bash
git add docs/plans/2025-01-25-docs-simplification-design.md
git commit -m "docs: mark simplification design as implemented"
```

---

## Summary

After completing all tasks:

- **5 pages** instead of 25+: `index.md`, `deploying.md`, `sending-data.md`, `querying.md`, `reference.md`
- **Flat structure** - no subdirectories (except `plans/` for internal docs)
- **CLI-first** - `deploy` command is the primary path
- **Task-oriented** - Deploy → Send → Query flow
- **Progressive disclosure** - advanced content in collapsibles
