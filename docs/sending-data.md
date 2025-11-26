# Send Data

Send OpenTelemetry data to your otlp2parquet endpoint.

## Endpoints

| Signal | Endpoint | Content-Type |
|--------|----------|--------------|
| Logs | `/v1/logs` | `application/json` or `application/x-protobuf` |
| Traces | `/v1/traces` | `application/json` or `application/x-protobuf` |
| Metrics | `/v1/metrics` | `application/json` or `application/x-protobuf` |

## Quick Test (curl)

=== "Logs"
    ```bash
    curl -X POST http://localhost:4318/v1/logs \
      -H "Content-Type: application/json" \
      -d @testdata/log.json
    ```

=== "Traces"
    ```bash
    curl -X POST http://localhost:4318/v1/traces \
      -H "Content-Type: application/json" \
      -d @testdata/trace.json
    ```

=== "Metrics"
    ```bash
    curl -X POST http://localhost:4318/v1/metrics \
      -H "Content-Type: application/json" \
      -d @testdata/metrics_gauge.json
    ```

## OpenTelemetry Collector (Recommended)

For production, use the [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) to batch data before sending. This reduces costs and creates more efficient Parquet files.

```yaml
# otel-collector-config.yaml
receivers:
  otlp:
    protocols:
      grpc:
      http:

processors:
  batch:
    send_batch_max_size: 10000
    timeout: 10s

exporters:
  otlphttp:
    endpoint: http://your-endpoint:4318/

service:
  pipelines:
    logs:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlphttp]
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlphttp]
    metrics:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlphttp]
```

## SDK Configuration

Point your OTel SDK exporter at your endpoint:

=== "Python"
    ```python
    from opentelemetry.exporter.otlp.proto.http.log_exporter import OTLPLogExporter

    exporter = OTLPLogExporter(endpoint="http://localhost:4318/v1/logs")
    ```

=== "Node.js"
    ```javascript
    const { OTLPLogExporter } = require('@opentelemetry/exporter-logs-otlp-http');

    const exporter = new OTLPLogExporter({
      url: 'http://localhost:4318/v1/logs',
    });
    ```

=== "Go"
    ```go
    import "go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"

    exporter, _ := otlploghttp.New(ctx,
      otlploghttp.WithEndpoint("localhost:4318"),
      otlploghttp.WithInsecure(),
    )
    ```

## Troubleshooting

| Problem | Solution |
|---------|----------|
| Parse errors | Ensure valid OTLP JSON/protobuf payload |
| 413 Payload Too Large | Batch smaller or increase `OTLP2PARQUET_MAX_PAYLOAD_BYTES` |
| Connection refused | Check endpoint URL and firewall rules |
| Storage write failures | Check bucket permissions and credentials |
