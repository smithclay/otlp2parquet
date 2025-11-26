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

Deploy to Cloudflare's edge network with R2 storage.

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
    If you prefer to create `wrangler.toml` manually, see the [generated template](https://github.com/smithclay/otlp2parquet/blob/main/crates/otlp2parquet-cli/templates/wrangler.toml) for reference.

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

Run locally for development or self-host in your infrastructure.

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
