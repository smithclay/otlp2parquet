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

SAM's beta Rust support handles building the function automatically using cargo-lambda under the hood.

```bash
# Build from the lambda crate directory
cd crates/otlp2parquet-lambda
sam build --beta-features

# Or from the workspace root
sam build --beta-features --template crates/otlp2parquet-lambda/template.yaml
```

The built bootstrap binary will be at `.aws-sam/build/OtlpToParquetFunction/bootstrap`.

**Why `--beta-features`?** SAM's Rust support is currently in beta. The `template.yaml` is already configured with the correct `BuildMethod: rust-cargolambda` metadata to use cargo-lambda automatically.

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
cd crates/otlp2parquet-lambda
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
cd crates/otlp2parquet-lambda
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
cd crates/otlp2parquet-lambda
sam build --beta-features

# Test with logs (with MinIO)
sam local invoke OtlpToParquetFunction \
  -e test-events/logs-protobuf.json \
  --docker-network otlp2parquet_default \
  --env-vars local-env.json

# Test with traces
sam local invoke OtlpToParquetFunction \
  -e test-events/traces-protobuf.json \
  --docker-network otlp2parquet_default \
  --env-vars local-env.json

# Test with metrics
sam local invoke OtlpToParquetFunction \
  -e test-events/metrics-gauge-protobuf.json \
  --docker-network otlp2parquet_default \
  --env-vars local-env.json
```

**Why event files?** `sam local start-api` has known issues with binary data (protobuf). Using `sam local invoke` with base64-encoded event files is more reliable for testing binary payloads.

**Important Notes:**
- The `--docker-network` flag is required for the Lambda container to reach MinIO
- Create `local-env.json` as described in the Configuration section below for local MinIO testing

## 3. Deploy to AWS

Once you have tested your changes locally, you can deploy the function to your AWS account.

### Initial Deployment

Use the guided deployment process for the first time to configure your stack.

```bash
# This command will prompt you for configuration parameters.
cd crates/otlp2parquet-lambda
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
cd crates/otlp2parquet-lambda
sam deploy

# Deploy to a specific environment (e.g., staging, production).
sam deploy --config-env staging
```

## 4. Configuration

### Deployment Parameters

Deployment settings are stored in `crates/otlp2parquet-lambda/samconfig.toml`. You can create profiles for different environments (e.g., `staging`, `production`).

```toml
# crates/otlp2parquet-lambda/samconfig.toml
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

For local testing, create a `local-env.json` file in the `crates/otlp2parquet-lambda` directory:

```bash
# Copy the example file
cd crates/otlp2parquet-lambda
cp local-env.json.example local-env.json
```

The file configures environment variables for `sam local` commands:

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

**Important Configuration Notes:**

- **Environment Variable Prefix**: All otlp2parquet config variables use the `OTLP2PARQUET_` prefix
- **Batching on Lambda**: Set `OTLP2PARQUET_BATCHING_ENABLED: "false"` for Lambda
  - **Why?** Lambda invocations are stateless - if batching is enabled, unflushed batches are lost when the invocation ends
  - Batching thresholds: 200K rows, 128MB, or 10 seconds - with single test records, data will be lost
  - For production, use batching ONLY if you have high-volume, sustained traffic that will trigger flushes
- **MinIO Testing**: Use `--env-vars local-env.json` with `sam local invoke` to provide environment variables for local testing

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
cd crates/otlp2parquet-lambda
sam logs --tail --stack-name otlp2parquet
```

### Common Local Issues

*   **"Cannot connect to Docker daemon"**: Ensure Docker Desktop is running.
*   **"Connection refused to MinIO"**: Verify the `docker-compose` containers are running and that you are using the correct `--docker-network` for `sam local start-api`.
*   **Cold Starts**: Use the `--warm-containers EAGER` flag with `sam local start-api` to keep the local function warm for faster iteration.
