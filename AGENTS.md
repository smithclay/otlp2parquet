# otlp2parquet - Agentic Coding Notes
**Universal OTel Logs, Metrics & Traces Ingestion Pipeline (Rust)**

## Mission
Ingest OTLP logs/metrics/traces over HTTP (protobuf/JSON/JSONL), convert to Arrow RecordBatch, and write Parquet to object storage with optional Iceberg catalog. Must ship as <3MB compressed WASM for Cloudflare Workers and native binary for AWS Lambda.

## Non-Negotiables
- **Binary size**: target 2.5MB compressed (limit 3MB). Use `default-features = false` everywhere, profile with `make wasm-size`/`twiggy`, keep dependencies minimal.
- **Schema**: ClickHouse-compatible PascalCase columns. Metrics use five distinct schemas (Gauge, Sum, Histogram, ExponentialHistogram, Summary).
- **Platform detection**: `AWS_LAMBDA_FUNCTION_NAME` => Lambda; `CF_WORKER` => Cloudflare; otherwise Server.
- **Storage**: OpenDAL via icepick. Platform limits: Lambda=S3, Cloudflare=R2, Server=multi-backend (S3/R2/FS/GCS/Azure).
- **Config**: `config.toml` plus `OTLP2PARQUET_*` env overrides; platform defaults chosen automatically.

## Workspace Map
- `otlp2parquet-core`: OTLP parsing, schema, Arrow conversion, partition paths.
- `otlp2parquet-batch`: In-memory batching (passthrough) for all signals.
- `otlp2parquet-config`: Config parsing/defaults and platform detection.
- `otlp2parquet-writer`: `OtlpWriter` + `IcepickWriter`; plain Parquet + optional Iceberg via icepick; warn-and-succeed on catalog failures.
- `otlp2parquet-handlers`: Shared HTTP/signal processing (parse -> convert -> write) reused by Cloudflare/Lambda/Server.
- Platform crates: `otlp2parquet-cloudflare` (WASM, R2-only, optional R2 catalog), `otlp2parquet-lambda` (S3/S3 Tables), `otlp2parquet` (Axum, multi-backend CLI). `otlp2parquet-proto` holds generated OTLP protos.

## Signals & Partitioning
- **Logs**: single schema; `logs/{service}/year={year}/month={month}/day={day}/hour={hour}/{timestamp}-{uuid}.parquet`.
- **Metrics**: five schemas; per-type files at `metrics/{type}/{service}/year=.../hour=.../{timestamp}-{uuid}.parquet`.
- **Traces**: single schema with events/links/status; `traces/{service}/year=.../hour=.../{timestamp}-{uuid}.parquet`.

## Catalog & Storage
- Catalog modes: `iceberg` or `none` via `OTLP2PARQUET_CATALOG_MODE`.
- Platform catalog options: Lambda (S3 Tables ARN or REST), Server (Nessie/Glue REST or none), Cloudflare (R2 Data Catalog or none).
- Parquet write always happens; catalog commit is best-effort and logs warnings instead of failing.

## Coding Standards
- Use `tracing::*` macros only; no `println!/eprintln!` in production paths.
- Avoid `unwrap/expect` in prod; propagate errors with context.
- Favor `&str`, lean allocations, and `Arc` for shared data. Keep default features off.
- Zero clippy warnings; panic-free production code; HTTP client is unified via `reqwest`.

## Build & Test Flow
- Make targets: `make dev`, `make pre-commit`, `make clippy`, `make check`, `make test`, `make test-e2e`, `make wasm-full`, `make wasm-size`, `make wasm-profile`.
- Conventional Commits required (e.g., `feat: ...`, `fix: ...`); hooks enforce format.
