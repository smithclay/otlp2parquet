# Developer Overview

Welcome to the `otlp2parquet` developer documentation! This section provides insights into the project's mission, architecture, development philosophy, and current status. Whether you're looking to contribute, understand the codebase, or extend its functionality, you'll find the necessary information here.

## Mission

`otlp2parquet` aims to build a Rust binary that ingests OpenTelemetry logs via OTLP HTTP (protobuf), converts them to Apache Arrow RecordBatch format, and writes them as Parquet files to object storage. A key design goal is multi-platform compatibility, compiling to a small compressed WASM binary for Cloudflare Workers (free plan) and a native binary for AWS Lambda, while also supporting a full-featured server mode.

## Critical Constraints & Design Philosophy

The project operates under several critical constraints that drive its architecture and implementation:

*   **Binary Size:** Especially for Cloudflare Workers, the compressed WASM binary must be under 3MB.
*   **Schema Compatibility:** The output Parquet schema is designed to be ClickHouse-compatible, using specific column names and PascalCase conventions.
*   **Platform Detection:** The application auto-detects its runtime environment (Cloudflare Workers, AWS Lambda, or Server) to adapt its behavior.
*   **Unified Storage:** Leverages Apache OpenDAL as a mature, battle-tested storage abstraction to provide a consistent API across all platforms (S3, R2, Filesystem, GCS, Azure).

Our core philosophy emphasizes **conceptual integrity**, separating the **essence** (pure OTLP→Parquet conversion) from **accident** (platform I/O, networking, runtime). This leads to a design where a pure core processing logic is adapted by platform-specific entry points and a unified storage layer.

## Architecture Highlights

*   **Server is Default:** The full-featured server mode uses an Axum HTTP server with structured logging and graceful shutdown.
*   **Unified Storage:** Apache OpenDAL provides a consistent API for interacting with various object storage backends.
*   **Pure Core:** The OTLP processing logic is deterministic and free of I/O dependencies.
*   **Platform-Native:** Each runtime utilizes its native async model (e.g., `worker` for Cloudflare, `tokio` for Lambda/Server).
*   **Binary Size Optimization:** Aggressive optimization ensures small binary sizes, particularly for WASM deployments.

## Status & Roadmap

### Current Status

The project has successfully completed its foundational phases, including:

*   Workspace structure and size optimizations.
*   Arrow schema definition and OTLP protobuf integration.
*   OTLP to Arrow conversion and Parquet writing.
*   **Apache OpenDAL unified storage layer** implementation.
*   HTTP protocol handlers and platform-specific entry points for Cloudflare Workers, AWS Lambda, and Server mode.
*   Comprehensive deployment guides and CI/CD workflows for Docker, Cloudflare Workers, and AWS Lambda.

### Recent Changes

The most significant recent change was the migration to Apache OpenDAL for unified storage, which led to the removal of platform-specific AWS SDK dependencies, reduced code complexity, and maintained excellent binary size.

### Planned Features

Future development includes:

*   JSON and JSONL input format support for OTLP compliance and bonus features.
*   Kubernetes manifests and Helm charts.
*   Load testing and performance benchmarks.
*   Grafana dashboards for monitoring.
*   Integration tests with real OTLP clients.

For a more detailed look at the architecture and implementation details, refer to the [Architecture](architecture.md) and [Codebase](codebase.md) sections.
