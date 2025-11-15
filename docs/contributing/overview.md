# Contributing Overview

This document provides an overview of the project's mission, architecture, and development philosophy for new contributors.

## Getting Started

To get started, you will need to set up a local development environment. Follow the [**Building from Source**](../guides/building-from-source.md) guide to install the necessary tools and learn how to build and test the project.

Once your environment is set up, you can explore the project's design and architecture below.

## Mission

`otlp2parquet` ingests OTLP/HTTP data, converts it to Parquet, and writes it to object storage. A key design goal is multi-platform compatibility, supporting a full-featured server mode while also compiling to a small WASM binary for Cloudflare Workers and a native binary for AWS Lambda.

## Design Philosophy

The project operates under several critical constraints:

*   **Minimal Binary Size**: The compressed WASM binary must be under 3MB for Cloudflare Workers.
*   **Schema Compatibility**: The output Parquet schema is compatible with the ClickHouse ecosystem.
*   **Unified Storage**: [Apache OpenDAL](https://opendal.apache.org/) provides a consistent storage API across all platforms.

The core philosophy separates the **essence** (pure OTLP→Parquet conversion) from the **accident** (platform-specific I/O and networking). This results in a pure, deterministic core logic adapted by platform-specific entry points.

## Architecture Highlights

*   **Pure Core**: The OTLP processing logic is deterministic and has no I/O dependencies.
*   **Unified Storage**: A consistent API for all object storage backends, powered by Apache OpenDAL.
*   **Platform-Native**: Each runtime uses its native async model (e.g., `worker` for Cloudflare, `tokio` for Lambda/Server).

For more detail, see the main [Architecture](../concepts/architecture.md) concept guide.

## Status & Roadmap

### Current Status

The project has completed its foundational phases, including workspace structure, schema definition, OTLP-to-Arrow conversion, and the unified storage layer. Platform-specific entry points and setup guides are also complete.

### Planned Features

Future development includes:
*   Batch size tuning suggestions.
*   Cost analysis per provider.
*   Load testing and performance benchmarks.

To learn more about performance, see the [Performance](./performance.md) guide.
