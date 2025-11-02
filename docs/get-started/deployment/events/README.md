# Lambda Event Files

These event files contain base64-encoded OTLP payloads for testing the Lambda function locally using `sam local invoke`.

## Available Events

- `logs-protobuf.json` - OTLP logs in protobuf format
- `traces-protobuf.json` - OTLP traces in protobuf format
- `metrics-gauge-protobuf.json` - OTLP gauge metrics in protobuf format

## Usage

### Build the Lambda function first

```bash
# From the repository root
cargo lambda build --release --arm64 --features lambda
mkdir -p .aws-sam/build/OtlpToParquetFunction
cp target/lambda/otlp2parquet/bootstrap .aws-sam/build/OtlpToParquetFunction/
```

**Note**: We use `cargo lambda build` directly instead of `sam build` because SAM doesn't correctly pass the `Features` property to cargo-lambda.

### Create local-env.json

Create a `local-env.json` file in the repository root with the correct environment variable names:

```json
{
  "OtlpToParquetFunction": {
    "RUST_LOG": "debug,otlp2parquet=trace",
    "OTLP2PARQUET_S3_BUCKET": "otlp-logs",
    "OTLP2PARQUET_S3_REGION": "us-east-1",
    "OTLP2PARQUET_BATCHING_ENABLED": "false",
    "AWS_ACCESS_KEY_ID": "minioadmin",
    "AWS_SECRET_ACCESS_KEY": "minioadmin"
  }
}
```

**Important**: Set `OTLP2PARQUET_BATCHING_ENABLED: "false"` to avoid data loss. See the main documentation for details.

### Test with sam local invoke

```bash
# From the repository root - Test logs
sam local invoke OtlpToParquetFunction \
  -e docs/get-started/deployment/events/logs-protobuf.json \
  --env-vars local-env.json

# Test traces
sam local invoke OtlpToParquetFunction \
  -e docs/get-started/deployment/events/traces-protobuf.json \
  --env-vars local-env.json

# Test metrics
sam local invoke OtlpToParquetFunction \
  -e docs/get-started/deployment/events/metrics-gauge-protobuf.json \
  --env-vars local-env.json
```

## Regenerating Events

To regenerate events from testdata (if testdata files change):

```bash
# From the repository root
# Create logs event
cat > docs/get-started/deployment/events/logs-protobuf.json <<EOF
{
  "version": "2.0",
  "routeKey": "ANY /{proxy+}",
  "rawPath": "/v1/logs",
  "headers": {"content-type": "application/x-protobuf"},
  "requestContext": {"http": {"method": "POST", "path": "/v1/logs"}},
  "body": "$(base64 -i testdata/logs.pb | tr -d '\n')",
  "isBase64Encoded": true
}
EOF
```

**Important**:
- Use `tr -d '\n'` to remove newlines from base64 output for proper JSON formatting
- Run all commands from the repository root directory
