# otlp2parquet - Agentic Coding Notes
**Universal OTel Logs, Metrics & Traces Ingestion Pipeline (Rust)**

## Mission
Ingest OTLP logs/metrics/traces over HTTP (protobuf/JSON/JSONL), convert to Arrow RecordBatch, and write Parquet to object storage with optional Iceberg catalog. Must ship as <3MB compressed WASM for Cloudflare Workers and native binary for AWS Lambda.

## Non-Negotiables
- **Binary size**: target 2.5MB compressed (limit 3MB). Use `default-features = false` everywhere, profile with `make wasm-size`/`twiggy`, keep dependencies minimal.
- **Schema**: ClickHouse-compatible PascalCase columns. Metrics use five distinct schemas (Gauge, Sum, Histogram, ExponentialHistogram, Summary).
- **Platform detection**: `AWS_LAMBDA_FUNCTION_NAME` => Lambda; `CF_WORKER` => Cloudflare; otherwise Server.
- **Storage**: OpenDAL via icepick. Platform limits: Lambda=S3, Cloudflare=R2, Server=multi-backend (S3/R2/FS).
- **Config**: `config.toml` plus `OTLP2PARQUET_*` env overrides; platform defaults chosen automatically.

## Workspace Map
- `otlp2parquet-proto`: Generated OTLP protobuf definitions (prost).
- `otlp2parquet-core`: OTLP parsing, schema, Arrow conversion, batching, config, and platform detection. Pure layer (no async I/O).
- `otlp2parquet-writer`: Parquet writing + optional Iceberg catalog via icepick. Best-effort catalog commits.
- `otlp2parquet-handlers`: Shared HTTP/signal processing (parse → convert → write) reused by all platforms.
- `otlp2parquet`: Main CLI/Server (Axum HTTP, multi-backend storage).
- `otlp2parquet-lambda`: AWS Lambda runtime adapter (S3/S3 Tables).
- `otlp2parquet-cloudflare`: Cloudflare Workers WASM adapter (R2/R2 Data Catalog).

## Signals & Partitioning
- **Logs**: single schema; `logs/{service}/year=.../month=.../day=.../hour=.../{timestamp}-{uuid}.parquet`
- **Metrics**: five schemas; `metrics/{type}/{service}/year=.../month=.../day=.../hour=.../{timestamp}-{uuid}.parquet`
- **Traces**: single schema; `traces/{service}/year=.../month=.../day=.../hour=.../{timestamp}-{uuid}.parquet`

## Catalog & Storage
- Catalog modes: `iceberg` or `none` via `OTLP2PARQUET_CATALOG_MODE`.
- Platform catalog options: Lambda (S3 Tables ARN or REST), Server (Nessie/Glue REST or none), Cloudflare (R2 Data Catalog or none).
- **Best-effort catalog commits**: Parquet files are always written to storage first. Catalog registration happens after—if it fails, data is still safely stored and a warning is logged.

## Coding Standards
- Use `tracing::*` macros only; no `println!/eprintln!` in production paths.
- Avoid `unwrap/expect` in prod; propagate errors with context.
- Favor `&str`, lean allocations, and `Arc` for shared data. Keep default features off.
- Zero clippy warnings; panic-free production code; HTTP client is unified via `reqwest`.

## Build & Test Flow
- Make targets: `make dev`, `make pre-commit`, `make clippy`, `make check`, `make test`, `make test-e2e`, `make wasm-full`, `make wasm-size`, `make wasm-profile`.
- Conventional Commits required (e.g., `feat: ...`, `fix: ...`); hooks enforce format.
