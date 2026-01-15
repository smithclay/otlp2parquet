# otlp2parquet - Agentic Coding Notes
**Universal OTel Logs, Metrics & Traces Ingestion Pipeline (Rust)**

## Mission
Ingest OTLP logs/metrics/traces over HTTP (protobuf/JSON/JSONL), convert to Arrow RecordBatch, and write Parquet to object storage. Must ship as <3MB compressed WASM and native binary.

## Non-Negotiables
- **Binary size**: target 2.5MB compressed (limit 3MB). Use `default-features = false` everywhere, profile with `twiggy`, keep dependencies minimal.
- **Schema**: ClickHouse-compatible PascalCase columns. Metrics use five distinct schemas (Gauge, Sum, Histogram, ExponentialHistogram, Summary).
- **Storage**: OpenDAL. Server supports multi-backend (S3/R2/FS).
- **Config**: `config.toml` plus `OTLP2PARQUET_*` env overrides; platform defaults chosen automatically.

## Workspace Map
- `otlp2parquet-proto`: Generated OTLP protobuf definitions (prost).
- `otlp2parquet`: Main CLI/Server (Axum HTTP, multi-backend storage, in-memory batching, writer + codecs; owns config/types).

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
- Make targets: `make dev`, `make pre-commit`, `make clippy`, `make check`, `make test`, `make test-e2e`.
- Conventional Commits required (e.g., `feat: ...`, `fix: ...`); hooks enforce format.
