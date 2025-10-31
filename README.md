# otlp2parquet

## Overview
otlp2parquet is an OpenTelemetry ingestion pipeline that converts OTLP log exports into ClickHouse-compatible Parquet files. The project compiles to three deployment modes from the same codebase:

- **Cloudflare Workers (`cf` feature):** WebAssembly worker optimized for <3MB bundles. Choose this when you need a globally distributed ingestion edge with R2 storage.
- **Standalone server (`server` feature):** Tokio-based HTTP binary. Use for containers, bare-metal, or Kubernetes with direct access to object storage.
- **AWS Lambda (`lambda` feature):** AWS Lambda function packaged as a zipped binary. Ideal for on-demand ingestion with S3 persistence.

See the mode-specific guides in [`docs/`](./docs/) for deployment details.

## Quickstart
Spin up the standalone server locally, ingest a test log, and inspect the resulting Parquet file.

1. **Clone and enter the repository.**
   ```bash
   git clone https://github.com/<ACCOUNT_ID>/otlp2parquet.git && cd otlp2parquet
   ```
   Expected output: Repository files listed when running `ls`.
2. **Install Rust toolchain (stable) and required targets.**
   ```bash
   rustup toolchain install stable && rustup default stable
   ```
   Expected output: `stable-x86_64-unknown-linux-gnu installed - default set to stable`.
3. **Build the server binary with size optimizations.**
   ```bash
   cargo build --release --features server
   ```
   Expected output: `Finished release [optimized] target(s) in ...`.
4. **Prepare local configuration.**
   ```bash
   cp config.example.env .env && sed -i 's/STORAGE_BACKEND=.*/STORAGE_BACKEND=fs/' .env
   ```
   Expected output: `.env` present with filesystem storage enabled.
5. **Run the server.**
   ```bash
   STORAGE_BACKEND=fs STORAGE_PATH=./data ./target/release/otlp2parquet
   ```
   Expected output: `Listening on 0.0.0.0:8080 (server mode)`.
6. **Send a sample OTLP log via curl.**
   ```bash
   curl -sS -X POST http://127.0.0.1:8080/v1/logs \
     -H "Content-Type: application/json" \
     -d '{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"demo-service"}}]},"scopeLogs":[{"logRecords":[{"timeUnixNano":"1710000000000000000","severityText":"INFO","body":{"stringValue":"Quickstart log"}}]}]}]}'
   ```
   Expected output: `{}` (empty JSON response).
7. **Inspect generated Parquet files.**
   ```bash
   ls data/logs/demo-service
   ```
   Expected output: Partitioned Parquet file path such as `year=2025/month=01/.../logs.parquet`.
8. **Query the Parquet file with DuckDB.**
   ```bash
   duckdb -c "SELECT Body FROM read_parquet('data/logs/demo-service/**/*.parquet');"
   ```
   Expected output: `Quickstart log` returned in query results.
9. **Stop the server with Ctrl+C when finished.**
   ```bash
   # Press Ctrl+C in the server terminal
   ```
   Expected output: `Shutting down gracefully`.

Now you should have otlp2parquet running locally at http://127.0.0.1:8080/v1/logs.

## Build Matrix
| Target | Cargo feature | Build command | Required env vars | Output artifact | Cold-start notes |
|--------|---------------|---------------|-------------------|-----------------|------------------|
| Cloudflare Workers | `cf` | `cargo build --release --target wasm32-unknown-unknown --features cf` | `CF_ACCOUNT_ID`, `CF_API_TOKEN`, `R2_BUCKET` | `./target/wasm32-unknown-unknown/release/otlp2parquet.wasm` | Warmed edge caches keep p99 <15ms; first hit loads WASM (~50ms). |
| Standalone server | `server` | `cargo build --release --features server` | `STORAGE_BACKEND`, `STORAGE_PATH` or S3/R2 vars | `./target/release/otlp2parquet` | Cold start equals process boot (<500ms) on typical x86 hosts. |
| AWS Lambda | `lambda` | `cargo lambda build --release --features lambda` | `AWS_REGION`, `S3_BUCKET`, `S3_PREFIX` | `./target/lambda/otlp2parquet/bootstrap.zip` | Provisioned concurrency avoids 1-2s first-invoke penalty. |

For detailed deployment procedures, follow the guides in [`docs/cloudflare.md`](./docs/cloudflare.md), [`docs/server.md`](./docs/server.md), and [`docs/lambda.md`](./docs/lambda.md).
