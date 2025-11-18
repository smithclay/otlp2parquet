# Guide: Lambda Local Development

This guide shows how to develop and test `otlp2parquet` Lambda functions locally using the AWS SAM CLI with MinIO and Nessie.

!!! note "Target Audience"
    This is an advanced guide for developers who need to iterate on the Lambda function locally. For production, use the [AWS Lambda Setup](../setup/aws-lambda.md) guide instead.

## Prerequisites

*   **uvx** (recommended) or AWS SAM CLI: `brew install aws-sam-cli`
    *   If using `uvx`, Python 3.13+ compatible environment
*   **Docker Desktop** - for running MinIO and Nessie containers
*   **Rust toolchain** - for building Lambda binaries

## Local Development Workflow

The Lambda function now supports **Iceberg REST catalogs** (like Nessie) for local testing with MinIO. This provides a complete local development environment.

### Step 1: Start Local Services

Start MinIO (S3-compatible storage) and Nessie (Iceberg REST catalog):

```bash
# From the workspace root
docker compose up minio nessie
```

This starts:
- **MinIO**: S3-compatible object storage at `http://localhost:9000`
  - Console UI: `http://localhost:9001` (credentials: `minioadmin` / `minioadmin`)
- **Nessie**: Iceberg REST catalog at `http://localhost:19120`

### Step 2: Build the Lambda Function

Use `uvx` to run SAM CLI with Python 3.13 (avoids SAM CLI Python 3.14 compatibility issues):

```bash
# From the crates/otlp2parquet-lambda directory
uvx --python 3.13 --from aws-sam-cli sam build --beta-features
```

!!! tip "Why uvx?"
    SAM CLI has a dependency on Pydantic v1 which is incompatible with Python 3.14+. Using `uvx --python 3.13` ensures SAM runs with a compatible Python version.

### Step 3: Test with Sample Events

Invoke the Lambda function locally with test events:

```bash
# Test with logs (JSON format)
uvx --python 3.13 --from aws-sam-cli sam local invoke \
  --beta-features \
  --skip-pull-image \
  --event test-events/logs-json.json

# Test with traces (protobuf format)
uvx --python 3.13 --from aws-sam-cli sam local invoke \
  --beta-features \
  --skip-pull-image \
  --event test-events/traces-protobuf.json
```

!!! note "Environment Variables"
    The Lambda function reads configuration from the built template's environment variables. After running `sam build`, you'll need to update `.aws-sam/build/template.yaml` with the local configuration (see below).

### Development Loop

1.  Edit code in your editor
2.  Run `uvx --python 3.13 --from aws-sam-cli sam build --beta-features`
3.  Update `.aws-sam/build/template.yaml` environment variables (if needed)
4.  Run `sam local invoke` with a test event
5.  Verify Parquet files in MinIO console at `http://localhost:9001`

## Configuration

The Lambda function requires environment variables to be configured in the SAM template. After building with `sam build`, edit `.aws-sam/build/template.yaml` to add the environment variables:

```yaml
Environment:
  Variables:
    RUST_LOG: "debug,otlp2parquet_lambda=trace,otlp2parquet_writer=trace,icepick=debug"
    RUST_BACKTRACE: "full"
    # S3 storage configuration (MinIO)
    OTLP2PARQUET_S3_BUCKET: "otlp-logs"
    OTLP2PARQUET_S3_REGION: "us-east-1"
    OTLP2PARQUET_S3_ENDPOINT: "http://host.docker.internal:9000"
    # Iceberg REST catalog configuration (Nessie)
    OTLP2PARQUET_ICEBERG_REST_URI: "http://host.docker.internal:19120/iceberg"
    OTLP2PARQUET_ICEBERG_NAMESPACE: "otlp"
    # MinIO credentials
    AWS_ACCESS_KEY_ID: "minioadmin"
    AWS_SECRET_ACCESS_KEY: "minioadmin"
    # Disable batching for Lambda
    OTLP2PARQUET_BATCHING_ENABLED: "false"
```

!!! important "host.docker.internal"
    Use `host.docker.internal` instead of `localhost` to allow the Lambda container to reach services running on your host machine (MinIO and Nessie).

### Configuration Options

The Lambda function supports two catalog modes:

**Local Development (REST Catalog with Nessie):**
```bash
OTLP2PARQUET_ICEBERG_REST_URI=http://host.docker.internal:19120/iceberg
OTLP2PARQUET_ICEBERG_NAMESPACE=otlp
OTLP2PARQUET_S3_ENDPOINT=http://host.docker.internal:9000
```

**Production (AWS S3 Tables):**
```bash
OTLP2PARQUET_ICEBERG_BUCKET_ARN=arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket
# No S3_ENDPOINT needed - uses native AWS S3
```

## Deploying with SAM

While this guide focuses on local development, you can also deploy to AWS using SAM.

*   **Guided Deployment**: `sam deploy --guided`
*   **Subsequent Deployments**: `sam deploy`

## Troubleshooting

### SAM CLI Issues

**"ModuleNotFoundError: No module named 'pydantic'"**

SAM CLI is incompatible with Python 3.14+. Use `uvx` with Python 3.13:
```bash
uvx --python 3.13 --from aws-sam-cli sam build --beta-features
```

**"Building wrong binary (otlp2parquet-server instead of Lambda)"**

This happens if the SAM template has incorrect `CodeUri`. The template should have:
```yaml
Properties:
  CodeUri: .  # Current directory (lambda crate)
```

NOT `CodeUri: ../..` (workspace root).

### Connection Issues

**"Cannot connect to Docker daemon"**

Ensure Docker Desktop is running.

**"Connection refused to MinIO/Nessie"**

1. Verify containers are running: `docker compose ps`
2. Check that you're using `host.docker.internal` in environment variables (not `localhost`)
3. Ensure MinIO is accessible: `curl http://localhost:9000`
4. Ensure Nessie is accessible: `curl http://localhost:19120/api/v2/config`

### Lambda Runtime Issues

**"Lambda requires iceberg configuration"**

The environment variables in `.aws-sam/build/template.yaml` are missing. Add either:
- `OTLP2PARQUET_ICEBERG_REST_URI` for local development, or
- `OTLP2PARQUET_ICEBERG_BUCKET_ARN` for AWS S3 Tables

**"Event deserialization failed"**

The test event format doesn't match the Lambda handler. Use test events from `test-events/` directory:
- `logs-json.json` - OTLP logs in JSON format
- `logs-protobuf.json` - OTLP logs in protobuf (base64)
- `traces-protobuf.json` - OTLP traces in protobuf

### Debugging Tips

View detailed Lambda logs with trace-level logging:
```bash
# In .aws-sam/build/template.yaml, set:
RUST_LOG: "trace"
RUST_BACKTRACE: "full"
```

Check what binary was built:
```bash
strings .aws-sam/build/OtlpToParquetFunction/bootstrap | grep otlp2parquet
```

Verify MinIO data:
```bash
docker exec otlp2parquet-minio-1 mc ls -r minio/otlp-logs/
```

## See Also

*   [AWS Lambda Setup](../setup/aws-lambda.md)
*   [Configuration Concepts](../concepts/configuration.md)
*   [Architecture Concepts](../concepts/architecture.md)
