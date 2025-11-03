# Sending Telemetry Data

This guide covers the two primary methods for sending OpenTelemetry data to `otlp2parquet`.

1.  **Directly via HTTP**: Best for quick tests and low-volume scenarios.
2.  **Via the OpenTelemetry Collector**: The recommended method for production, as it allows for batching, retries, and more advanced routing.

## Sending Data Directly

You can send OTLP data directly to the appropriate endpoint for each signal type using a tool like `curl`. The examples below use the default Docker endpoint (`http://localhost:4318`). Replace this with your specific Lambda or Cloudflare Worker URL when deployed.

### Logs

Send log data to the `/v1/logs` endpoint.

```bash
# Protobuf format
curl -X POST http://localhost:4318/v1/logs \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @testdata/logs.pb

# JSON format
curl -X POST http://localhost:4318/v1/logs \
  -H "Content-Type: application/json" \
  -d @testdata/log.json
```

### Metrics

Send metric data to the `/v1/metrics` endpoint.

```bash
# Protobuf format
curl -X POST http://localhost:4318/v1/metrics \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @testdata/metrics_gauge.pb

# JSON format
curl -X POST http://localhost:4318/v1/metrics \
  -H "Content-Type: application/json" \
  -d @testdata/metrics_gauge.json
```

### Traces

Send trace data to the `/v1/traces` endpoint.

```bash
# Protobuf format
curl -X POST http://localhost:4318/v1/traces \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @testdata/traces.pb

# JSON format
curl -X POST http://localhost:4318/v1/traces \
  -H "Content-Type: application/json" \
  -d @testdata/trace.json
```

## Sending Data via OpenTelemetry Collector (Recommended)

For serverless environments like AWS Lambda and Cloudflare Workers, sending individual requests is inefficient. Using an agent like the [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) to batch data is critical for optimizing performance and reducing cost.

The collector receives data from your applications, groups it into larger batches, and then forwards those batches to `otlp2parquet`.

!!! tip "Bootstrap Sample Traffic with the OpenTelemetry Demo"

    You can generate realistic sample traffic by running the [OpenTelemetry Demo](https://github.com/open-telemetry/opentelemetry-demo) and pointing its collector at your `otlp2parquet` instance.

    Create a `compose.extras.otlp2parquet.yaml` file and instruct the demo's collector to forward its traffic over OTLP/HTTP.

    ```yaml
    # compose.extras.otlp2parquet.yaml
    services:
      otlp2parquet:
        image: ghcr.io/smithclay/otlp2parquet:latest
        environment:
          OTLP2PARQUET_STORAGE_BACKEND: fs
          OTLP2PARQUET_STORAGE_FS_PATH: /data
        volumes:
          - ./otlp2parquet-data:/data
        ports:
          - "4318:4318"

      otelcol:
        environment:
          OTEL_EXPORTER_OTLP_ENDPOINT: http://otlp2parquet:4318
          OTEL_EXPORTER_OTLP_PROTOCOL: http/protobuf
    ```

    Run the demo with `docker compose -f docker-compose.yaml -f compose.extras.otlp2parquet.yaml up`. Parquet files will appear in the `./otlp2parquet-data` folder.

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

Using a collector provides better control over file sizes, reduces the number of function invocations in serverless environments, and lowers overall cost.

## Troubleshooting

For detailed configuration options, see the [Configuration Guide](configuration.md).

### OTLP Parse Errors

If you receive parsing errors, ensure you are sending valid OTLP data. You can verify your payload with `otel-cli`.

```bash
# Example verification for logs
otel-cli logs --protocol http/protobuf --dry-run
```

### Storage Write Failures

*   **Cloudflare Workers**: Verify the R2 bucket binding in `wrangler.toml` and ensure the `OTLP2PARQUET_R2_*` environment variables and secrets are correctly set.
*   **AWS Lambda**: Ensure the function's IAM role has `s3:PutObject` permission for the target bucket.
*   **Server Mode (Docker)**: If using filesystem storage, ensure the `OTLP2PARQUET_STORAGE_PATH` directory is writable. If using S3 or R2, check your credentials and bucket permissions.

### Configuration Issues

To see the active configuration, check the startup logs for your deployment:

*   **Server Mode**: `docker-compose logs otlp2parquet | grep -E "(storage|batch|payload)"`
*   **AWS Lambda**: Check the function's logs in CloudWatch.
*   **Cloudflare Workers**: Use the `wrangler tail` command.

**Common Mistakes:**
*   Forgetting the `OTLP2PARQUET_` prefix for environment variables.
*   A mismatch between the configured `STORAGE_BACKEND` and the platform (e.g., using `fs` on Lambda).
*   Missing required fields like bucket names or credentials.
