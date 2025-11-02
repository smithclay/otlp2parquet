# otlp2parquet

[![CI](https://github.com/smithclay/otlp2parquet/actions/workflows/ci.yml/badge.svg)](https://github.com/smithclay/otlp2parquet/actions/workflows/ci.yml)
[![Build Artifacts](https://github.com/smithclay/otlp2parquet/actions/workflows/build-artifacts.yml/badge.svg)](https://github.com/smithclay/otlp2parquet/actions/workflows/build-artifacts.yml)
[![Docs](https://github.com/smithclay/otlp2parquet/actions/workflows/docs.yml/badge.svg)](https://github.com/smithclay/otlp2parquet/actions/workflows/docs.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

> Put your observability data in cheap object storage, servers optional.

`otlp2parquet` ingests OpenTelemetry logs, metrics, and traces and stores them in object storage in Parquet format. It can run natively in serverless runtimes like AWS Lambda and Cloudflare Workers.

While functional, __the project is currently considered experimental__ as the API and schema will evolve (see "Notes on the schema" below). This project is not a telemetry pipeline replacement: in production scenarios, it would be paired with [pOpenTelemetry Collector](https://opentelemetry.io/docs/collector/) or [Vector](https://vector.dev/) agents that do advanced transformations, routing, and batching.

**Key Features:**
*   Ingests OTLP HTTP (protobuf, JSON, or JSONL) for logs, metrics, and traces.
*   Writes Parquet files for [easy and efficient querying in duckdb](https://duckdb.org/docs/stable/data/parquet/overview).
*   Supports Docker, Cloudflare Workers (WASM), and AWS Lambda deployments.
*   Small and fast: written in Rust, uncompressed binary size is ~5 MB.

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

** In serverless environments, batching must be handled by an upstream agent like the [OpenTelemetry Collector](https://github.com/open-telemetry/opentelemetry-collector) or [Vector](https://github.com/vectordotdev/vector) to optimize performance and cost ([see example configuration](docs/get-started/usage.md#batching-with-opentelemetry-collector)).

## Quick Start

To get started quickly, check out the [Quickstart Guide](docs/get-started/quickstart.md).

## Documentation

For comprehensive information on deployment, usage, architecture, and development, please visit our full documentation site:

➡️ [**View Documentation**](https://smithclay.github.io/otlp2parquet/)

## Notes on the schema

The current schema is based on the ClickHouse OpenTelemetry exporter schema, similar to the [duckdb-otlp extension](https://github.com/smithclay/duckdb-otlp). This project serves as a bridge to the official [OpenTelemetry Arrow Protocol](https://github.com/open-telemetry/otel-arrow).

We plan to converge with the official protocol once it becomes generally available, which will make much of the OTLP->Arrow translation this tool does unnecessary.

## Contributing

We welcome contributions! Please see our [Developer Documentation](docs/developers/index.md) for guidelines on how to contribute.

## License

Apache-2.0
