# Developer Overview

This section provides an overview of the project's mission, architecture, and development philosophy for contributors and developers.

## Mission

`otlp2parquet` is a Rust binary that ingests OTLP/HTTP data, converts it to Parquet, and writes it to object storage. A key design goal is multi-platform compatibility, supporting a full-featured server mode while also compiling to a small WASM binary for Cloudflare Workers and a native binary for AWS Lambda.

## Design Philosophy

The project operates under several critical constraints:

*   **Minimal Binary Size**: The compressed WASM binary must be under 3MB for Cloudflare Workers.
*   **Schema Compatibility**: The output Parquet schema is ClickHouse-compatible.
*   **Automatic Platform Detection**: The binary adapts its behavior by auto-detecting its runtime environment (Server, Lambda, or Cloudflare).
*   **Unified Storage**: [Apache OpenDAL](https://opendal.apache.org/) provides a consistent storage API across all platforms (S3, R2, Filesystem, etc.).

The core philosophy separates the **essence** (pure OTLP→Parquet conversion) from the **accident** (platform-specific I/O and networking). This results in a pure, deterministic core logic adapted by platform-specific entry points.

## Architecture Highlights

*   **Server as Default**: The full-featured server mode uses an Axum HTTP server with structured logging and graceful shutdown.
*   **Unified Storage**: A consistent API for all object storage backends, powered by Apache OpenDAL.
*   **Pure Core**: The OTLP processing logic is deterministic and has no I/O dependencies.
*   **Platform-Native**: Each runtime uses its native async model (e.g., `worker` for Cloudflare, `tokio` for Lambda/Server).

## Status & Roadmap

### Current Status

The project has completed its foundational phases, including the workspace structure, schema definition, OTLP-to-Arrow conversion, and the unified storage layer using Apache OpenDAL. Platform-specific entry points and deployment guides are also complete.

### Planned Features

Future development includes:

*   Batch size tuning suggestions.
*   Cost analysis per provider.
*   Load testing and performance benchmarks.

For more detail, see the [Architecture](architecture.md) and [Performance](performance.md) documentation.
