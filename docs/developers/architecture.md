# Architecture

This document details the architectural design of `otlp2parquet`, emphasizing its core principles, platform-specific considerations, and the role of Apache OpenDAL as a unified storage abstraction.

### Default + Constrained Runtimes

*   **Server Mode (Default):** This is the full-featured implementation, providing a robust Axum HTTP server with multi-backend storage capabilities.
*   **Lambda & Cloudflare (Constrained Runtimes):** These are specialized cases that leverage the same core processing logic but operate under platform-specific limitations (e.g., S3-only for Lambda, R2-only for Cloudflare Workers due to WASM constraints).

## High-Level Architecture Diagram

The following diagram illustrates the flow of data and the separation of concerns within `otlp2parquet`:

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

*   **Server is Default:** Full-featured mode with Axum HTTP server, structured logging, graceful shutdown.
*   **Unified Storage:** Apache OpenDAL provides a consistent API across all platforms, abstracting away the complexities of different object storage backends.
*   **Pure Core:** The OTLP processing logic is designed to be deterministic and free of I/O dependencies, ensuring high reliability and testability.
*   **Platform-Native:** Each runtime leverages its native async model (e.g., `worker` for Cloudflare, `tokio` for Lambda/Server) for optimal performance.
*   **Binary Size:** Aggressive optimization techniques are applied to ensure minimal binary sizes, crucial for WASM deployments like Cloudflare Workers.
