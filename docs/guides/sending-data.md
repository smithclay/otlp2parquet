# Sending Telemetry Data

This guide shows how to send OpenTelemetry data to `otlp2parquet`.

There are two primary methods for sending data:

1.  **Directly via HTTP**: Best for quick tests and low-volume scenarios.
2.  **Via the OpenTelemetry Collector**: The recommended method for production. It provides batching, retries, and advanced routing.

## Direct to Endpoint

You can send OTLP data directly to the signal-specific endpoint using `curl` or any HTTP client. The examples below use the default Docker endpoint (`http://localhost:4318`). Replace this with your Lambda or Cloudflare Worker URL.

### Logs (`/v1/logs`)
```bash
# Protobuf
curl -X POST http://localhost:4318/v1/logs \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @testdata/logs.pb

# JSON
curl -X POST http://localhost:4318/v1/logs \
  -H "Content-Type: application/json" \
  -d @testdata/log.json
```

### Metrics (`/v1/metrics`)
```bash
# Protobuf
curl -X POST http://localhost:4318/v1/metrics \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @testdata/metrics_gauge.pb

# JSON
curl -X POST http://localhost:4318/v1/metrics \
  -H "Content-Type: application/json" \
  -d @testdata/metrics_gauge.json
```

### Traces (`/v1/traces`)
```bash
# Protobuf
curl -X POST http://localhost:4318/v1/traces \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @testdata/traces.pb

# JSON
curl -X POST http://localhost:4318/v1/traces \
  -H "Content-Type: application/json" \
  -d @testdata/trace.json
```

## Via OpenTelemetry Collector (Recommended)

For production and serverless environments, using an agent like the [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) to batch data is critical for performance and cost. The collector receives data from your applications, groups it into larger batches, and then forwards those batches to `otlp2parquet`.

!!! tip "Generate Sample Traffic with the OpenTelemetry Demo"
    A great way to generate realistic sample traffic is by running the [OpenTelemetry Demo](https://github.com/open-telemetry/opentelemetry-demo) and pointing its collector at your `otlp2parquet` instance.

### Example Collector Configuration

This example configures the `batch` processor to create batches up to 100 MiB, which is ideal for writing efficient Parquet files.

```yaml
# otel-collector-config.yaml
receivers:
  otlp:
    protocols:
      grpc:
      http:

processors:
  batch:
    # Sets a soft limit on batch size. Batches may exceed this.
    send_batch_max_size: 104857600 # 100 MiB
    # Sends a batch every 10 seconds, even if it's not full.
    timeout: 10s

exporters:
  otlphttp:
    # Replace with the endpoint of your otlp2parquet deployment.
    endpoint: http://localhost:4318/

service:
  pipelines:
    logs:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlphttp]
    metrics:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlphttp]
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlphttp]
```

Using a collector provides better control over file sizes, reduces function invocations in serverless environments, and lowers overall cost.

## Troubleshooting

For detailed configuration options, see the [Configuration Guide](../concepts/configuration.md).

### OTLP Parse Errors
If you receive parsing errors, ensure you are sending valid OTLP data. You can verify your payload with a tool like `otel-cli`.

### Storage Write Failures
*   **Cloudflare**: Verify the R2 bucket binding in `wrangler.toml` and check your secrets.
*   **AWS Lambda**: Ensure the function's IAM role has `s3:PutObject` permission for the target bucket.
*   **Docker**: If using the filesystem, ensure the storage path is writable. If using S3/R2, check credentials and bucket permissions.

### Configuration Issues
Check the startup logs for your deployment to see the active configuration.
*   **Docker**: `docker-compose logs otlp2parquet`
*   **AWS Lambda**: Check the function's logs in CloudWatch.
*   **Cloudflare Workers**: `wrangler tail`
