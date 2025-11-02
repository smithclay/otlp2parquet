# otlp2parquet

> Experimental ingest and store observability data in cheap object storage, servers optional

`otlp2parquet` is a multi-platform tool for ingesting OpenTelemetry logs, metrics, and traces, converting them to Apache Parquet format, and storing them efficiently in object storage. It's designed for running in serverless runtimes like AWS Lambda and Cloudflare workers.

The current schema, like the [duckdb-otlp extension](https://github.com/smithclay/duckdb-otlp), is based on the ClickHouse OpenTelemetry exporter schema. __Long-term, one goal of this project is to beome obsolete__: we hope to replace or converge this proof-of-concept with the [OpenTelemetry Arrow Protocol](https://github.com/open-telemetry/otel-arrow) that will make much of the OTLP->Arrow translation this tool does unnessecary.

**Key Features:**
*   Ingests OTLP HTTP (protobuf, JSON, or JSONL) for logs, metrics, and traces.
*   Writes Parquet files for easy querying in duckdb.
*   Supports Docker, Cloudflare Workers (WASM), and AWS Lambda deployments.

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
| **Advanced Features** | | | |
| In-Memory Batching | ✅ | ❌** | ❌** |

__**Important__: Something like the [OpenTelemetry Collector](https://github.com/open-telemetry/opentelemetry-collector) or [vector](https://github.com/vectordotdev/vector) is need to batch (group) data before being received by a Lambda function or Cloudflare worker to reduce cloud spend and optimize performance ([see example configuration](docs/get-started/usage.md#batching-with-opentelemetry-collector)).

## Quick Start

To get started quickly, check out the [Quickstart Guide](docs/get-started/quickstart.md).

## Documentation

For comprehensive information on deployment, usage, architecture, and development, please visit our full documentation site:

➡️ [**View Documentation**](https://smithclay.github.io/otlp2parquet/)

## Contributing

We welcome contributions! Please see our [Developer Documentation](docs/developers/index.md) for guidelines on how to contribute.

## License

Apache-2.0
