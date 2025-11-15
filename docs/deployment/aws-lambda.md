# AWS Lambda Deployment Guide

This guide shows how to develop, test, and deploy the `otlp2parquet` function to AWS Lambda using the AWS SAM CLI.

> **Note**: All `sam` commands in this guide should be run from the `crates/otlp2parquet-lambda` directory.

## Prerequisites

1.  **AWS Account** with appropriate permissions.
2.  **AWS CLI** configured (`aws configure`).
3.  **AWS SAM CLI**: The official tool for Lambda deployment.
4.  **Docker Desktop**: Required for local testing.
5.  **Rust Toolchain**: Including `cargo-lambda`.

```bash
# Install necessary tools (macOS example)
brew install aws-sam-cli
cargo install cargo-lambda
```

## Local Development & Testing

This workflow uses `sam sync` for hot-reloading, providing the fastest and most realistic local development experience.

### Setup: Three-Terminal Workflow

**Terminal 1 - Start Local S3 (MinIO):**

```bash
# From the workspace root
docker-compose up minio minio-init
```

**Terminal 2 - Start Local Lambda Endpoint:**

```bash
# From the crates/otlp2parquet-lambda directory
sam local start-api \
  --warm-containers EAGER \
  --docker-network otlp2parquet_default \
  --env-vars local-env.json
```

Your function is now available at `http://localhost:3000`.

**Terminal 3 - Watch for Changes:**

```bash
# From the crates/otlp2parquet-lambda directory
sam sync --watch --stack-name otlp2parquet-dev
```

### Development Loop

1.  **Edit Code**: Make changes to the Rust source code.
2.  **Auto-Rebuild**: `sam sync --watch` detects changes and automatically rebuilds the function.
3.  **Test**: Send a test request to the local endpoint.

    ```bash
    curl -X POST http://localhost:3000/v1/logs \
      --data-binary @testdata/logs.pb \
      -H "Content-Type: application/x-protobuf"
    ```

4.  **Verify**: Check the MinIO bucket for the output Parquet file.

    ```bash
    aws s3 ls s3://otlp-logs/logs/ --recursive \
      --endpoint-url http://localhost:9000
    ```

### Alternate Testing: `sam local invoke`

For reliably testing binary payloads like Protobuf, `sam local invoke` is a better tool than `curl` because it avoids potential encoding issues.

```bash
# First, build the function if you haven't already
sam build --beta-features

# Then, invoke the function with a test event
sam local invoke OtlpToParquetFunction \
  -e test-events/logs-protobuf.json \
  --docker-network otlp2parquet_default \
  --env-vars local-env.json
```

## Deploy to AWS

After local testing, deploy the function to your AWS account.

### Initial Deployment

Use the guided deployment process to configure your stack the first time.

```bash
sam deploy --guided
```

You will be prompted for a **Stack Name**, **AWS Region**, and the S3 **BucketName** for storing Parquet files. After deployment, SAM will output the public **FunctionUrl** for your endpoint.

### Subsequent Deployments

After the first deployment, updates are simpler.

```bash
# Deploy using the saved configuration
sam deploy

# Deploy to a specific environment (e.g., staging)
sam deploy --config-env staging
```

## Configuration

### Deployment Parameters

Deployment settings are stored in `crates/otlp2parquet-lambda/samconfig.toml`. You can define profiles for different environments (e.g., `staging`, `production`).

### Local Environment Variables

For local testing, create a `local-env.json` file inside `crates/otlp2parquet-lambda`.

```json
{
  "OtlpToParquetFunction": {
    "RUST_LOG": "debug,otlp2parquet=trace",
    "OTLP2PARQUET_S3_BUCKET": "otlp-logs",
    "OTLP2PARQUET_S3_REGION": "us-east-1",
    "OTLP2PARQUET_S3_ENDPOINT": "http://minio:9000",
    "OTLP2PARQUET_BATCHING_ENABLED": "false",
    "AWS_ACCESS_KEY_ID": "minioadmin",
    "AWS_SECRET_ACCESS_KEY": "minioadmin"
  }
}
```

> **Important**: `OTLP2PARQUET_BATCHING_ENABLED` must be set to `"false"` for Lambda. Because Lambda invocations are stateless, any data held in a batch risks being lost when the invocation ends. For production workloads, batching should be handled by an upstream agent like the OpenTelemetry Collector.

*Note: Add `local-env.json` to your `.gitignore` file.*

### Shared Configuration (`config.toml`)

Lambda now reads the same `config.toml` file as the server binary. You can keep your storage, batching, and Iceberg settings in one place (plus override anything with `OTLP2PARQUET_*` environment variables).

Add a `[lambda]` block to toggle integrated S3 Tables / Glue commits:

```toml
[lambda]
integrated_iceberg = true

[iceberg]
rest_uri = "https://glue.us-west-2.amazonaws.com/iceberg"
warehouse = "123456789012"           # Glue catalog ID
namespace = "otel.prod"
data_location = "s3://otel-prod-data" # Required for Glue
```

When `integrated_iceberg` is `true`, the Lambda runtime requires a populated `[iceberg]` section (and corresponding AWS permissions) so it can perform write+commit in a single invocation. Set the same option via env var with `OTLP2PARQUET_LAMBDA_INTEGRATED_ICEBERG=true` if you prefer to manage configuration entirely through environment variables.

## Monitoring & Troubleshooting

### View CloudWatch Logs

```bash
sam logs --tail --stack-name otlp2parquet
```

### Common Local Issues

*   **"Cannot connect to Docker daemon"**: Ensure Docker Desktop is running.
*   **"Connection refused to MinIO"**: Verify the `docker-compose` containers are running and you are using the correct `--docker-network` flag.
*   **Slow Local Invocations**: Use the `--warm-containers EAGER` flag with `sam local start-api` to keep the local function warm between requests.
