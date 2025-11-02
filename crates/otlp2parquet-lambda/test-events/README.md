# Lambda Test Events

These event files contain base64-encoded OTLP payloads for testing the Lambda function locally using `sam local invoke`.

## Available Events

- `logs-protobuf.json` - OTLP logs in protobuf format
- `traces-protobuf.json` - OTLP traces in protobuf format
- `metrics-gauge-protobuf.json` - OTLP gauge metrics in protobuf format

## Usage

### Build the Lambda function first

```bash
# From the lambda crate directory
cd crates/otlp2parquet-lambda
sam build --beta-features
```

### Create local-env.json

Copy the example file and customize as needed:

```bash
# From crates/otlp2parquet-lambda
cp local-env.json.example local-env.json
```

**Important**: Set `OTLP2PARQUET_BATCHING_ENABLED: "false"` to avoid data loss. See the main documentation for details.

### Test with sam local invoke

```bash
# From crates/otlp2parquet-lambda - Test logs
sam local invoke OtlpToParquetFunction \
  -e test-events/logs-protobuf.json \
  --docker-network otlp2parquet_default \
  --env-vars local-env.json

# Test traces
sam local invoke OtlpToParquetFunction \
  -e test-events/traces-protobuf.json \
  --docker-network otlp2parquet_default \
  --env-vars local-env.json

# Test metrics
sam local invoke OtlpToParquetFunction \
  -e test-events/metrics-gauge-protobuf.json \
  --docker-network otlp2parquet_default \
  --env-vars local-env.json
```

## Regenerating Events

To regenerate events from testdata (if testdata files change):

```bash
# From the repository root
# Create logs event
cat > crates/otlp2parquet-lambda/test-events/logs-protobuf.json <<EOF
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
- Update paths for `traces-protobuf.json` and `metrics-gauge-protobuf.json` similarly
