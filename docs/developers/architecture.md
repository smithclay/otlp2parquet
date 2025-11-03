# Architecture

This document outlines the architectural design of `otlp2parquet`, its core principles, and its different runtimes.

## Core Principles

The architecture separates the **essence** (the pure OTLP-to-Parquet conversion logic) from the **accident** (platform-specific I/O and networking). This results in a pure, deterministic core that is adapted for different environments.

### Execution Runtimes

`otlp2parquet` supports multiple execution runtimes:

*   **Server Mode (Default):** A full-featured implementation that uses an Axum HTTP server with multi-backend storage capabilities.
*   **Lambda & Cloudflare (Constrained Runtimes):** Specialized runtimes that use the same core logic but operate under platform-specific limitations (e.g., S3-only for Lambda, R2-only for Cloudflare Workers).

## High-Level Diagram

This diagram shows the data flow and separation of concerns within `otlp2parquet`:

```
┌─────────────────────────────────────────┐
│  Platform-Specific Entry Points         │
│  ├─ Server (default): Axum HTTP server │
│  │   Full-featured, multi-backend       │
│  ├─ Lambda: lambda_runtime::run()       │
│  │   Event-driven, S3 only             │
│  └─ Cloudflare: #[event(fetch)]        │
│      WASM-constrained, R2 only          │
└─────────────────────────────────────────┘
                  ↓
┌─────────────────────────────────────────┐
│  Protocol Layer (HTTP handlers)         │
│  └─ Parse HTTP request → OTLP bytes    │
└─────────────────────────────────────────┘
                  ↓
┌─────────────────────────────────────────┐
│  Core Processing (PURE - no I/O)       │
│  process_otlp_logs(bytes) -> bytes      │
│  ├─ Parse OTLP protobuf ✅              │
│  ├─ Convert to Arrow RecordBatch ✅     │
│  ├─ Write Parquet (Snappy) ✅           │
│  └─ Generate partition path ✅          │
└─────────────────────────────────────────┘
                  ↓
┌─────────────────────────────────────────┐
│  Unified Storage Layer (Apache OpenDAL) │
│  ├─ S3 (Lambda, Server)                │
│  ├─ R2 (Cloudflare, Server)            │
│  ├─ Filesystem (Server)                │
│  └─ GCS, Azure, etc. (Server-ready)    │
└─────────────────────────────────────────┘
```

## Architecture Highlights

*   **Server as Default:** The server mode provides a full-featured Axum HTTP server, structured logging, and graceful shutdown.
*   **Unified Storage:** Apache OpenDAL provides a consistent API across all platforms, which abstracts away the complexities of different object storage backends.
*   **Pure Core:** The OTLP processing logic is deterministic and has no I/O dependencies, which improves reliability and testability.
*   **Platform-Native:** Each runtime uses its native async model (e.g., `worker` for Cloudflare, `tokio` for Lambda/Server) for optimal performance.
*   **Binary Size:** Aggressive optimizations ensure a minimal binary size, which is crucial for WASM deployments.
