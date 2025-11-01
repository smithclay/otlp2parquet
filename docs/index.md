# Welcome to otlp2parquet

otlp2parquet is a multi-platform tool designed to ingest OpenTelemetry logs via OTLP HTTP (protobuf), convert them into Apache Arrow RecordBatches, and then write them as Parquet files to object storage. It features a ClickHouse-compatible schema and is optimized for various deployment environments, including Docker, Cloudflare Workers (WASM), and AWS Lambda.

Our goal is to provide a fast, efficient, and cost-effective solution for collecting and storing your OpenTelemetry logs, making them easily queryable for analysis.

## Get Started

Ready to deploy and start collecting logs? Head over to the [Quickstart guide](get-started/quickstart.md) to get up and running.

## For Developers

Interested in contributing, understanding the architecture, or extending the project? Visit the [Developers section](developers/index.md).
