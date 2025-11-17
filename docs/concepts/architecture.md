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
┌──────────────────────────────────────────────┐
│  Platform-Specific Entry Points              │
│  ├─ Server (default): Axum HTTP server      │
│  │   Full-featured, multi-backend            │
│  ├─ Lambda: lambda_runtime::run()            │
│  │   Event-driven, S3 Tables catalog         │
│  └─ Cloudflare: #[event(fetch)]             │
│      WASM-constrained, R2 Data Catalog       │
└──────────────────────────────────────────────┘
                     ↓
┌──────────────────────────────────────────────┐
│  Protocol Layer (HTTP handlers)              │
│  └─ Parse HTTP request → OTLP bytes         │
└──────────────────────────────────────────────┘
                     ↓
┌──────────────────────────────────────────────┐
│  Core Processing (Pure Logic, No I/O)        │
│  process_otlp(bytes) -> Arrow RecordBatch    │
│  ├─ Parse OTLP protobuf ✅                   │
│  ├─ Convert to Arrow RecordBatch ✅          │
│  └─ Generate partition path ✅               │
└──────────────────────────────────────────────┘
                     ↓
┌──────────────────────────────────────────────┐
│  Writer Abstraction (OtlpWriter trait)       │
│  └─ IcepickWriter implementation             │
└──────────────────────────────────────────────┘
                     ↓
┌──────────────────────────────────────────────┐
│  icepick Library (External Dependency)       │
│  ├─ Arrow → Parquet conversion ✅            │
│  ├─ Parquet file writing (Snappy) ✅         │
│  └─ Optional catalog commit ✅               │
└──────────────────────────────────────────────┘
                     ↓
┌──────────────────────────────────────────────┐
│  Storage Layer (via icepick + OpenDAL)       │
│  ├─ S3 (Lambda, Server)                     │
│  ├─ R2 (Cloudflare, Server)                 │
│  ├─ Filesystem (Server)                     │
│  └─ GCS, Azure, etc. (Server-ready)         │
└──────────────────────────────────────────────┘
                     ↓
┌──────────────────────────────────────────────┐
│  Optional: Iceberg Catalog Commit            │
│  ├─ S3 Tables (Lambda - ARN-based)          │
│  ├─ R2 Data Catalog (Cloudflare - WASM)     │
│  ├─ Nessie (Server - REST API)              │
│  └─ AWS Glue (Server - REST API)            │
└──────────────────────────────────────────────┘
```

## Key Design Points

*   **Pure Core**: The OTLP processing logic is deterministic and has no I/O dependencies, which improves reliability and testability.
*   **Writer Abstraction**: The `OtlpWriter` trait provides a unified interface for writing Parquet files with optional catalog integration. `IcepickWriter` is the primary implementation.
*   **icepick Integration**: External library ([icepick](https://crates.io/crates/icepick)) handles Parquet writing and catalog operations, reducing internal complexity and binary size.
*   **Catalog Flexibility**: Support for multiple Iceberg catalogs (S3 Tables, R2 Data Catalog, Nessie, Glue) or plain Parquet without catalog overhead.
*   **Warn-and-Succeed Pattern**: Catalog failures are logged as warnings but don't block Parquet file creation, prioritizing data durability over catalog consistency.
*   **Unified Storage**: Apache OpenDAL (via icepick) provides a consistent API that abstracts away the complexities of different object storage backends.
*   **Platform-Native**: Each runtime uses its native async model (e.g., `worker` for Cloudflare, `tokio` for Lambda/Server) for optimal performance.
*   **Minimal Binary Size**: Aggressive optimizations and lean dependencies ensure a small binary. WASM binary is 1.3MB compressed (43.8% of Cloudflare's 3MB limit).

## Platform Differences

### Lambda (AWS)

**Storage:** S3 only (event-driven constraint)

**Catalog:** S3 Tables via ARN-based configuration
- Simplified setup: just provide bucket ARN
- Native AWS SDK integration (not WASM-compatible)
- Auto-creates tables on first write
- Managed service with automatic compaction

**Configuration:**
```yaml
Environment:
  Variables:
    OTLP2PARQUET_ICEBERG_BUCKET_ARN: arn:aws:s3tables:region:account:bucket/name
```

### Cloudflare Workers (Edge)

**Storage:** R2 only (WASM constraint)

**Catalog:** R2 Data Catalog (optional)
- WASM-compatible Iceberg catalog
- Edge-native with zero egress fees
- Enabled via `OTLP2PARQUET_CATALOG_TYPE="r2"`
- Also supports plain Parquet mode (no catalog)

**Configuration:**
```toml
[[r2_buckets]]
binding = "OTLP_BUCKET"
bucket_name = "otlp-data"

[vars]
OTLP2PARQUET_CATALOG_TYPE = "r2"  # Optional, for Iceberg
```

**Binary Size:** 1.3MB compressed, optimized for WASM deployment

### Server (Self-Hosted)

**Storage:** Multi-backend via OpenDAL
- S3, R2, Filesystem, GCS, Azure, etc.
- Configurable via `config.toml` or environment variables

**Catalog:** Multiple options
- Nessie (REST catalog with Git-like versioning)
- AWS Glue (REST catalog for AWS integration)
- Plain Parquet (no catalog)

**Additional Features:**
- In-memory batching for optimal file sizes
- Flexible logging (text or JSON format)
- Full-featured HTTP server with configurable listen address

**Configuration:**
```toml
[storage]
backend = "s3"

[storage.s3]
bucket = "my-data"
region = "us-west-2"

[iceberg]
rest_uri = "http://nessie:19120/api/v1"
warehouse = "warehouse"
namespace = "otel"
```

## Architecture Evolution

**Previous Architecture:**
- Separate `otlp2parquet-storage` and `otlp2parquet-iceberg` crates
- Internal Parquet writing implementation
- Custom Iceberg catalog client

**Current Architecture:**
- Single `otlp2parquet-writer` crate
- External `icepick` library for Parquet and catalog operations
- Unified `OtlpWriter` trait abstraction
- Simpler codebase with fewer dependencies
