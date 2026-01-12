# otlp2parquet - Agentic Coding Notes
**Universal OTel Logs, Metrics & Traces Ingestion Pipeline (Rust)**

## Mission
Ingest OTLP logs/metrics/traces over HTTP (protobuf/JSON/JSONL), convert to Arrow RecordBatch, and write Parquet to object storage. Must ship as <3MB compressed WASM for Cloudflare Workers and native binary for AWS Lambda.

## Non-Negotiables
- **Binary size**: target 2.5MB compressed (limit 3MB). Use `default-features = false` everywhere, profile with `make wasm-size`/`twiggy`, keep dependencies minimal.
- **Schema**: ClickHouse-compatible PascalCase columns. Metrics use five distinct schemas (Gauge, Sum, Histogram, ExponentialHistogram, Summary).
- **Platform detection**: `AWS_LAMBDA_FUNCTION_NAME` => Lambda; `CF_WORKER` => Cloudflare; otherwise Server.
- **Storage**: OpenDAL. Platform limits: Lambda=S3, Cloudflare=R2, Server=multi-backend (S3/R2/FS).
- **Config**: `config.toml` plus `OTLP2PARQUET_*` env overrides; platform defaults chosen automatically.

## Workspace Map
- `otlp2parquet-proto`: Generated OTLP protobuf definitions (prost).
- `otlp2parquet-common`: Shared types, configuration, and re-exports from otlp2records.
- `otlp2parquet-writer`: Parquet writing and storage initialization.
- `otlp2parquet-handlers`: Shared HTTP/signal processing (parse → convert → write) reused by all platforms.
- `otlp2parquet`: Main CLI/Server (Axum HTTP, multi-backend storage, in-memory batching).
- `otlp2parquet-lambda`: AWS Lambda runtime adapter (S3, writes per-request).
- `otlp2parquet-cloudflare`: Cloudflare Workers WASM adapter (R2, writes per-request).

## Signals & Partitioning
- **Logs**: single schema; `logs/{service}/year=.../month=.../day=.../hour=.../{timestamp}-{uuid}.parquet`
- **Metrics**: five schemas; `metrics/{type}/{service}/year=.../month=.../day=.../hour=.../{timestamp}-{uuid}.parquet`
- **Traces**: single schema; `traces/{service}/year=.../month=.../day=.../hour=.../{timestamp}-{uuid}.parquet`

## Coding Standards
- Use `tracing::*` macros only; no `println!/eprintln!` in production paths.
- Avoid `unwrap/expect` in prod; propagate errors with context.
- Favor `&str`, lean allocations, and `Arc` for shared data. Keep default features off.
- Zero clippy warnings; panic-free production code; HTTP client is unified via `reqwest`.

## Build & Test Flow
- Make targets: `make dev`, `make pre-commit`, `make clippy`, `make check`, `make test`, `make test-e2e`, `make wasm-full`, `make wasm-size`, `make wasm-profile`.
- Conventional Commits required (e.g., `feat: ...`, `fix: ...`); hooks enforce format.
