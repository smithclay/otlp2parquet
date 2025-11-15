# Guide: Lambda Local Development

This guide shows how to develop and test `otlp2parquet` Lambda functions locally using the AWS SAM CLI.

!!! note "Target Audience"
    This is an advanced guide for developers who need to iterate on the Lambda function locally. For production, use the [AWS Lambda Setup](../setup/aws-lambda.md) guide instead.

## Prerequisites

*   AWS SAM CLI: `brew install aws-sam-cli`
*   Docker Desktop
*   Rust toolchain with `cargo-lambda`: `cargo install cargo-lambda`
*   AWS CLI configured: `aws configure`

## Local Development Workflow

The recommended workflow uses three terminals to manage the local environment.

**Terminal 1: Local S3 (MinIO)**
```bash
# From the workspace root
docker-compose up minio minio-init
```

**Terminal 2: Local Lambda Endpoint**
```bash
# From the crates/otlp2parquet-lambda directory
sam local start-api \
  --warm-containers EAGER \
  --docker-network otlp2parquet_default \
  --env-vars local-env.json
```

**Terminal 3: Watch for Changes**
```bash
# From the crates/otlp2parquet-lambda directory
sam sync --watch --stack-name otlp2parquet-dev
```

### Development Loop
1.  Edit code in your editor.
2.  `sam sync --watch` automatically rebuilds the function.
3.  Send test requests to `http://localhost:3000/v1/logs`.
4.  Verify Parquet output in the MinIO console (`http://localhost:9001`).

### Testing Binary Payloads
For protobuf testing, `sam local invoke` avoids potential encoding issues with `curl`.
```bash
sam build --beta-features
sam local invoke OtlpToParquetFunction \
  -e test-events/logs-protobuf.json \
  --docker-network otlp2parquet_default \
  --env-vars local-env.json
```

## Configuration

Create a `local-env.json` file in `crates/otlp2parquet-lambda/` to configure the local environment.

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
!!! warning "Gitignore"
    Add `local-env.json` to your `.gitignore` file to avoid committing credentials.

## Deploying with SAM

While this guide focuses on local development, you can also deploy to AWS using SAM.

*   **Guided Deployment**: `sam deploy --guided`
*   **Subsequent Deployments**: `sam deploy`

## Troubleshooting

*   **View CloudWatch Logs**: `sam logs --tail --stack-name otlp2parquet`
*   **"Cannot connect to Docker daemon"**: Ensure Docker Desktop is running.
*   **"Connection refused to MinIO"**: Verify the `docker-compose` containers are running and you are using the `--docker-network otlp2parquet_default` flag.
*   **Slow local invocations**: Use the `--warm-containers EAGER` flag.

## See Also

*   [AWS Lambda Setup](../setup/aws-lambda.md)
*   [Configuration Concepts](../concepts/configuration.md)
*   [Architecture Concepts](../concepts/architecture.md)
