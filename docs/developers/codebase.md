# Codebase Details

This section delves into the core implementation details of `otlp2parquet`, covering the key phases of development and the current status of input format support.

## Core Implementation Priorities

The development of `otlp2parquet` has been structured into several phases, focusing on building a robust and efficient OTLP ingestion pipeline.

### Phase 1: Foundation (Completed)

This phase established the basic project structure and essential components:

1.  **Project Setup:** Initialized the Rust project with a workspace structure and configured `Cargo.toml` for size optimizations.
2.  **Generate OTLP Protobuf Code:** Set up `build.rs` to generate Rust code from OpenTelemetry protobuf definitions.
3.  **Define Arrow Schema:** Implemented the `otel_logs_schema()` function, defining the 15-field ClickHouse-compatible Arrow schema.
4.  **Platform Detection:** Implemented runtime detection for Cloudflare Workers, AWS Lambda, and Server environments.

### Phase 2: Core Processing (Completed)

This phase focused on the central logic for converting OTLP logs to Parquet:

5.  **OTLP → Arrow Conversion:** Developed `ArrowConverter` to efficiently transform `ExportLogsServiceRequest` into Arrow `RecordBatch`es, handling attribute extraction and field mapping.
6.  **Minimal Parquet Writer:** Implemented `write_parquet` to serialize Arrow `RecordBatch`es into Snappy-compressed Parquet files, optimized for minimal features.
7.  **Partition Path Generation:** Created logic to generate Hive-style partition paths (e.g., `logs/{service}/year={yyyy}/month={mm}/day={dd}/hour={hh}/{uuid}.parquet`).

### Phase 3: Storage Layer (Completed - OpenDAL)

This phase introduced a unified abstraction for object storage:

*   **Architecture Decision:** Adopted Apache OpenDAL for unified storage abstraction.
*   **Philosophy:** Leverage mature, battle-tested external abstractions rather than building custom implementations.
*   **Unified Storage:** Apache OpenDAL (v0.54+) is used as a unified storage layer across all platforms (Cloudflare Workers, AWS Lambda, Server) for R2, S3, Filesystem, GCS, Azure, etc.
*   **Benefits:** Reduced code duplication, battle-tested reliability, automatic S3/R2 compatibility, minimal binary size impact, zero-cost abstractions, and future-proof extensibility.
*   **Removed Dependencies:** Eliminated `aws-sdk-s3` and `aws-config` in favor of OpenDAL, resulting in smaller binaries and faster compile times.

### Phase 4: Protocol Handlers (In Progress)

This phase focuses on implementing the HTTP handlers for ingesting OTLP data:

*   **Core Function Available:** `otlp2parquet_core::process_otlp_logs(bytes) -> Result<Vec<u8>>` is the central function for processing OTLP data.
*   **HTTP Handler:** A generic `handle_otlp_http` function is being developed to parse protobuf requests, convert to Arrow, write Parquet, generate paths, and store data using the unified storage layer.

### Phase 5: Platform Adapters (In Progress)

This phase involves creating the platform-specific entry points:

*   **Cloudflare Workers Entry:** An `#[event(fetch)]` handler for Cloudflare Workers, utilizing R2 storage via OpenDAL.
*   **Lambda Entry:** An `aws_lambda_runtime` handler for AWS Lambda, utilizing S3 storage via OpenDAL.
*   **Universal Main:** A `main.rs` that detects the runtime platform and dispatches to the appropriate entry point.

## Input Format Support

### Currently Implemented

*   ✅ **Protobuf (Binary)** - `application/x-protobuf`
    *   OTLP's native format, most efficient (smallest, fastest).
    *   Fully implemented via `prost::Message::decode()`.

### Planned (OTLP Compliance & Bonus Features)

*   **JSON** - `application/json`
    *   Required by OTLP specification.
    *   Will use `prost-serde` for automatic protobuf ↔ JSON conversion.
    *   HTTP handlers will detect `Content-Type` to route to the appropriate decoder.
*   **JSONL (Newline-Delimited JSON)**
    *   Not part of OTLP spec, but common for bulk log ingestion.
    *   Will parse each line as a separate `LogRecord`.

### Implementation Plan for JSON/JSONL

1.  **Add `prost-serde` dependency** to `Cargo.toml`.
2.  **Update `crates/core/src/otlp/mod.rs`** with an `InputFormat` enum and a `parse_otlp_logs` function to handle different formats.
3.  **Modify HTTP Handlers** to detect the input format from the `Content-Type` header and call the appropriate parser.

### Size Impact Analysis (for JSON/JSONL)

*   `prost-serde`: Approximately +50KB to the binary.
*   `serde_json`: Already included (used for responses), so minimal additional impact.
*   Net impact: ~50KB for OTLP compliance, which is acceptable within the WASM size constraints.
