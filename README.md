# otlp2parquet

[![CI](https://github.com/smithclay/otlp2parquet/actions/workflows/ci.yml/badge.svg)](https://github.com/smithclay/otlp2parquet/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

> Put your observability data in cost-effective cloud object storage, servers optional.

`otlp2parquet` ingests OpenTelemetry logs, metrics, and traces and stores them in object storage as Parquet files. It runs natively in serverless runtimes like AWS Lambda and Cloudflare Workers. With [some optimization](#notes-on-batch-sizes) and using a duckdb client, this is likely one of the cheapest ways to store and query structured observability data in the cloud with long-term retention.

While functional, the project is experimental as the [API and schema are evolving](#parquet-schema-for-logs-metrics-and-traces). It is _not_ a telemetry pipeline: in production scenarios, it would be paired with [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/), [Vector](https://vector.dev/), Cribl, or similar to perform transformations, routing, and batching.

**Key Features:**
*   Ingests OTLP HTTP (protobuf, JSON, or JSONL) for logs, metrics, and traces.
*   Writes Parquet files for [easy and efficient querying in duckdb](https://duckdb.org/docs/stable/data/parquet/overview).**
*   Supports Docker, Cloudflare Workers (WASM, paid-tier), and AWS Lambda deployments.
*   Small and fast: written in Rust, uncompressed binary size is ~5 MB.

_Note: If you want to query or convert existing OTLP files, you can use the [otlp-duckdb](https://github.com/smithclay/duckdb-otlp) extension. This project is focused on converting streaming OTLP data._
## Platform & Feature Matrix

| Feature / Platform | Docker (Server) | Cloudflare Workers | AWS Lambda |
| :----------------- | :----------------- | :----------------- | :--------- |
| **OTLP Protocol Support** | | | |
| OTLP/HTTP-JSON | ✅ | ✅ | ✅ |
| OTLP/HTTP-Protobuf | ✅ | ✅ | ✅ |
| OTLP/gRPC | ❌ | ❌ | ❌ |
| **Signal Support** | | | |
| OTLP Logs | ✅ | ✅ | ✅ |
| OTLP Metrics | ✅ | ✅ | ✅ |
| OTLP Traces | ✅ | ✅ | ✅ |
| OTLP Profiles | ❌ | ❌ | ❌ |
| **Security*** | | | |
| Basic Auth Header | ❌ | ✅ | ❌ |
| **Advanced Features** | | | |
| In-Memory Batching | ✅ | ❌** | ❌** |

## Documentation

For deployment instructions, usage examples, and detailed guides:

➡️ [**View Documentation**](https://smithclay.github.io/otlp2parquet/)

## Notes on batch sizes

Lots of small writes to cloud object storage will explode your bill: proceed with caution.

In serverless environments, batching must be handled by an upstream agent like the [OpenTelemetry Collector](https://github.com/open-telemetry/opentelemetry-collector) or [Vector](https://github.com/vectordotdev/vector) to optimize performance and cost ([see example configuration](docs/get-started/usage.md#batching-with-opentelemetry-collector)).

## Parquet Schema for logs, metrics, and traces

The current schema is based on the ClickHouse OpenTelemetry exporter schema, similar to the [duckdb-otlp extension](https://github.com/smithclay/duckdb-otlp). This project serves as a bridge to the official [OpenTelemetry Arrow Protocol](https://github.com/open-telemetry/otel-arrow) (OTAP) protocol under active development.

We plan to converge with the official protocol once it becomes generally available, which will make much of the OTLP->Arrow translation this tool does unnecessary.

## Related projects

Below are other projects that also have support for converting OTLP to Parquet format.

- https://github.com/open-telemetry/opentelemetry-rust
- https://github.com/Mooncake-Labs/moonlink
- https://github.com/streamfold/rotel

## Contributing

We welcome contributions! Please see our [Developer Documentation](docs/developers/index.md) for guidelines on how to contribute.

## License

Apache-2.0
