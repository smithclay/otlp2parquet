# AWS Lambda Deployment Guide

This guide provides a focused workflow for developing, testing, and deploying the `otlp2parquet` function to AWS Lambda using the AWS SAM CLI.

## Prerequisites

1.  **AWS Account** with appropriate permissions.
2.  **AWS CLI** configured (`aws configure`).
3.  **AWS SAM CLI**: The official tool for Lambda deployment.
4.  **Docker Desktop**: Required for local testing.
5.  **Rust Toolchain**: Including `cargo-lambda`.

```bash
# Install cargo-lambda
cargo install cargo-lambda

# Install AWS SAM CLI (macOS example)
brew install aws-sam-cli
```

## 1. Build the Lambda Function

Before testing or deploying, you need to build the Lambda function binary from the `otlp2parquet-lambda` package.

```bash
# Build for Lambda using cargo-lambda directly
cargo lambda build --release --arm64 -p otlp2parquet-lambda

# Copy the binary to SAM's build directory
mkdir -p .aws-sam/build/OtlpToParquetFunction
cp target/lambda/otlp2parquet/bootstrap .aws-sam/build/OtlpToParquetFunction/

# The bootstrap binary is now ready at:
# .aws-sam/build/OtlpToParquetFunction/bootstrap
```

**Why not `sam build`?** SAM's Rust support via `cargo-lambda` is currently a beta feature. Building with `cargo lambda` directly with the `-p` flag ensures the correct package is built.

## 2. Local Development & Testing

This workflow uses `sam sync` for hot-reloading, providing the fastest and most realistic local development experience.

### Setup: Three-Terminal Workflow

**Terminal 1 - Start Local S3 (MinIO):**

```bash
# Start a local S3-compatible storage container.
docker-compose up minio minio-init
```

**Terminal 2 - Start Local Lambda Endpoint:**

```bash
# Start the SAM local API, connecting it to the Docker network.
sam local start-api \
  --warm-containers EAGER \
  --docker-network otlp2parquet_default \
  --env-vars local-env.json

# Your function is now available at http://localhost:3000
```

*Note: Create `local-env.json` as described in the Configuration section below.*

**Terminal 3 - Watch for Changes:**

```bash
# This command watches for code changes and automatically rebuilds the function.
sam sync --watch --stack-name otlp2parquet-dev
```

### Development Loop

1.  **Edit Code**: Make changes to the Rust source code in your editor.
2.  **Auto-Rebuild**: `sam sync --watch` detects the changes and automatically rebuilds the Lambda function.
3.  **Test**: Send a test request to the local endpoint.

    ```bash
    curl -X POST http://localhost:3000/v1/logs \
      --data-binary @testdata/logs.pb \
      -H "Content-Type: application/x-protobuf"
    ```

4.  **Verify**: Check the local MinIO bucket for the output Parquet file.

    ```bash
    aws s3 ls s3://otlp-logs/logs/ --recursive \
      --endpoint-url http://localhost:9000
    ```

This setup provides instant feedback in a realistic, production-like environment.

### Testing with Event Files

For testing with binary protobuf data, use `sam local invoke` with pre-built event files:

```bash
# Build first (if not already built)
cargo lambda build --release --arm64 -p otlp2parquet-lambda
mkdir -p .aws-sam/build/OtlpToParquetFunction
cp target/lambda/otlp2parquet/bootstrap .aws-sam/build/OtlpToParquetFunction/

# IMPORTANT: SAM local invoke requires environment variables in template.yaml
# The --env-vars flag doesn't work reliably with sam local invoke.
# You must update .aws-sam/build/template.yaml with actual values:
#   OTLP2PARQUET_S3_BUCKET: otlp-logs
#   OTLP2PARQUET_S3_REGION: us-east-1
#   OTLP2PARQUET_S3_ENDPOINT: http://minio:9000
#   AWS_ACCESS_KEY_ID: minioadmin
#   AWS_SECRET_ACCESS_KEY: minioadmin

# Test with logs (with MinIO)
sam local invoke OtlpToParquetFunction \
  -e docs/get-started/deployment/events/logs-protobuf.json \
  --docker-network otlp2parquet_default

# Test with traces
sam local invoke OtlpToParquetFunction \
  -e docs/get-started/deployment/events/traces-protobuf.json \
  --docker-network otlp2parquet_default

# Test with metrics
sam local invoke OtlpToParquetFunction \
  -e docs/get-started/deployment/events/metrics-gauge-protobuf.json \
  --docker-network otlp2parquet_default
```

**Why event files?** `sam local start-api` has known issues with binary data (protobuf). Using `sam local invoke` with base64-encoded event files is more reliable for testing binary payloads.

**Important Notes:**
- The `--docker-network` flag is required for the Lambda container to reach MinIO
- SAM local invoke doesn't support CloudFormation references (like `!Ref LogsBucket`) in the template
- You must manually edit `.aws-sam/build/template.yaml` to replace CloudFormation references with actual values for local testing

## 3. Deploy to AWS

Once you have tested your changes locally, you can deploy the function to your AWS account.

### Initial Deployment

Use the guided deployment process for the first time to configure your stack.

```bash
# This command will prompt you for configuration parameters.
sam deploy --guided
```

You will be prompted for:

*   **Stack Name**: A unique name for your CloudFormation stack (e.g., `otlp2parquet-prod`).
*   **AWS Region**: The AWS region to deploy to.
*   **BucketName**: The name of the S3 bucket where Parquet files will be stored.

After deployment, SAM will output the public **FunctionUrl** for your new Lambda endpoint.

### Subsequent Deployments

After the initial setup, deployments are much simpler.

```bash
# Deploy using the saved configuration from the first run.
sam deploy

# Deploy to a specific environment (e.g., staging, production).
sam deploy --config-env staging
```

## 4. Configuration

### Deployment Parameters

Deployment settings are stored in `samconfig.toml`. You can create profiles for different environments (e.g., `staging`, `production`).

```toml
# samconfig.toml
[default.deploy.parameters]
stack_name = "otlp2parquet"
region = "us-east-1"

[production.deploy.parameters]
stack_name = "otlp2parquet-prod"
parameter_overrides = [
    "BucketName=my-prod-logs-bucket"
]
```

### Local Environment Variables

For local testing, create a `local-env.json` file to configure the function.

```json
// local-env.json
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

**Important Configuration Notes:**

- **Environment Variable Prefix**: All otlp2parquet config variables use the `OTLP2PARQUET_` prefix
- **Batching on Lambda**: Set `OTLP2PARQUET_BATCHING_ENABLED: "false"` for Lambda
  - **Why?** Lambda invocations are stateless - if batching is enabled, unflushed batches are lost when the invocation ends
  - Batching thresholds: 200K rows, 128MB, or 10 seconds - with single test records, data will be lost
  - For production, use batching ONLY if you have high-volume, sustained traffic that will trigger flushes
- **MinIO Testing**: When using `sam local invoke`, you must manually edit `.aws-sam/build/template.yaml` to add environment variables (see Testing with Event Files section)

*Note: Add `local-env.json` to your `.gitignore` file.*

### Available Environment Variables

All configuration uses the `OTLP2PARQUET_` prefix:

| Variable | Description | Default | Example |
|----------|-------------|---------|---------|
| `OTLP2PARQUET_S3_BUCKET` | S3 bucket name | `otlp-logs` | `my-logs-bucket` |
| `OTLP2PARQUET_S3_REGION` | AWS region | `us-east-1` | `eu-west-1` |
| `OTLP2PARQUET_S3_ENDPOINT` | Custom S3 endpoint (optional) | None | `http://minio:9000` |
| `OTLP2PARQUET_BATCHING_ENABLED` | Enable batching (⚠️ risk of data loss) | `true` | `false` |
| `OTLP2PARQUET_BATCH_MAX_ROWS` | Batch flush threshold - rows | `200000` | `100000` |
| `OTLP2PARQUET_BATCH_MAX_BYTES` | Batch flush threshold - bytes | `134217728` (128MB) | `67108864` (64MB) |
| `OTLP2PARQUET_BATCH_MAX_AGE_SECS` | Batch flush threshold - seconds | `10` | `5` |
| `OTLP2PARQUET_MAX_PAYLOAD_BYTES` | Max request payload size | `6291456` (6MB) | `10485760` (10MB) |

**AWS Credentials** (no prefix):
- `AWS_ACCESS_KEY_ID` - AWS access key (Lambda IAM role preferred)
- `AWS_SECRET_ACCESS_KEY` - AWS secret key
- `RUST_LOG` - Logging level (e.g., `info`, `debug`, `trace`)

## 5. Monitoring & Troubleshooting

### View CloudWatch Logs

```bash
# Tail logs for the deployed stack
sam logs --tail --stack-name otlp2parquet
```

### Common Local Issues

*   **"Cannot connect to Docker daemon"**: Ensure Docker Desktop is running.
*   **"Connection refused to MinIO"**: Verify the `docker-compose` containers are running and that you are using the correct `--docker-network` for `sam local start-api`.
*   **Cold Starts**: Use the `--warm-containers EAGER` flag with `sam local start-api` to keep the local function warm for faster iteration.
