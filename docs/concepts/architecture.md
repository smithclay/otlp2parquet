# Architecture

This document explains the design of `otlp2parquet`.

## Core Principle: Essence vs. Accident

The architecture separates the **essence** (the pure OTLP-to-Parquet conversion logic) from the **accident** (platform-specific I/O and networking). This creates a pure, deterministic core that is adapted for different environments.

`otlp2parquet` supports multiple execution runtimes:

*   **Server**: A full-featured Axum HTTP server with multi-backend storage.
*   **Lambda & Cloudflare**: Specialized runtimes that use the same core logic but operate under platform constraints (e.g., S3-only for Lambda, R2-only for Cloudflare Workers).

## Data Flow

The following diagram shows the data flow and separation of concerns.

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
│  Core Processing (Pure Logic, No I/O)   │
│  process_otlp(bytes) -> Parquet bytes   │
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

## Key Design Points

*   **Pure Core**: The OTLP processing logic is deterministic and has no I/O dependencies, which improves reliability and testability.
*   **Unified Storage**: Apache OpenDAL provides a consistent API that abstracts away the complexities of different object storage backends.
*   **Platform-Native**: Each runtime uses its native async model (e.g., `worker` for Cloudflare, `tokio` for Lambda/Server) for optimal performance.
*   **Minimal Binary Size**: Aggressive optimizations ensure a small binary, which is critical for serverless and WASM deployments.
