# otlp2parquet

`otlp2parquet` is a multi-platform tool for ingesting OpenTelemetry logs, metrics, and traces, converting them to Apache Parquet format, and storing them efficiently in object storage. It's designed for high performance and cost-effectiveness, with a ClickHouse-compatible schema.

**Key Features:**
*   Ingests OTLP HTTP (protobuf, JSON, or JSONL) for logs, metrics, and traces.
*   Converts to Apache Arrow RecordBatch and writes Parquet files.
*   Supports Docker, Cloudflare Workers (WASM), and AWS Lambda deployments.
*   Separate Parquet files per metric type for optimal query performance.
*   Optimized for binary size and performance.
*   Unified storage layer via Apache OpenDAL (S3, R2, Filesystem, etc.).

## Platform & Feature Matrix

| Feature / Platform | Docker (Server) | Cloudflare Workers | AWS Lambda |
| :----------------- | :----------------- | :----------------- | :--------- |
| OTLP Logs Ingestion | ✅ | ✅ | ✅ |
| OTLP Metrics Ingestion | ✅ | ✅ | ✅ |
| OTLP Traces Ingestion | ✅ | ✅ | ✅ |
| Parquet Conversion | ✅ | ✅ | ✅ |
| ClickHouse Schema | ✅ | ✅ | ✅ |
| OpenDAL Storage | ✅ (Multi-backend) | ✅ (R2) | ✅ (S3) |
| Output Formats | Filesystem, S3, R2, GCS, Azure | R2 | S3 |
| Binary Size Optimized | N/A (Native) | ✅ (<3MB WASM) | ✅ (Native) |

## Metrics Support

Metrics are written to separate Parquet files per metric type for efficient querying:
- **Gauge** - Instant measurements
- **Sum** - Cumulative or delta aggregations
- **Histogram** - Distribution with explicit buckets
- **ExponentialHistogram** - Distribution with exponential buckets
- **Summary** - Quantile-based distributions

Partition structure: `metrics/{type}/{service}/year={year}/month={month}/day={day}/hour={hour}/file.parquet`

## Quick Start

To get started quickly, check out the [Quickstart Guide](docs/get-started/quickstart.md).

## Documentation

For comprehensive information on deployment, usage, architecture, and development, please visit our full documentation site:

➡️ [**View Documentation**](https://smithclay.github.io/otlp2parquet/)

## Contributing

We welcome contributions! Please see our [Developer Documentation](docs/developers/index.md) for guidelines on how to contribute.

## License

Apache-2.0
