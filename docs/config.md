# Configuration & Shared Guidance

## Environment Variables
| Name | Default | Example | Description | Modes |
|------|---------|---------|-------------|-------|
| `STORAGE_BACKEND` | `fs` | `s3` | Storage driver: `fs`, `s3`, or `r2`. | server, lambda, cf |
| `STORAGE_PATH` | `./data` | `/var/lib/otlp2parquet` | Filesystem directory for Parquet output. | server |
| `S3_BUCKET` | _required (non-fs)_ | `logs-<ENV>` | Destination bucket for Parquet files. | server, lambda |
| `S3_REGION` | _required (non-fs)_ | `<REGION>` | AWS region for S3 access. | server, lambda |
| `S3_PREFIX` | `logs/` | `tenants/<ENV>/` | Prefix prepended to Parquet keys. | server, lambda |
| `R2_BUCKET` | _required (cf)_ | `logs` | Cloudflare R2 bucket binding. | cf |
| `R2_ACCOUNT_ID` | _required (server r2)_ | `<ACCOUNT_ID>` | Cloudflare account for R2 API. | server |
| `AWS_ACCESS_KEY_ID` | inherited | `<KEY>` | Access key for AWS-compatible storage. | server, lambda |
| `AWS_SECRET_ACCESS_KEY` | inherited | `<SECRET>` | Secret key for AWS-compatible storage. | server, lambda |
| `LISTEN_ADDR` | `0.0.0.0:8080` | `127.0.0.1:9000` | Bind address for server mode. | server |
| `LOG_FORMAT` | `text` | `json` | Structured logging format. | all |
| `BATCH_FLUSH_INTERVAL_MS` | `1000` | `5000` | Flush cadence for Parquet writes. | all |
| `STORAGE_BUFFER_SIZE` | `4194304` | `8388608` | In-memory buffer for Arrow batches (bytes). | all |
| `PROMETHEUS_EXPORTER` | `false` | `true` | Enables `/metrics` endpoint in server mode. | server |

## Examples
1. **Local filesystem server with JSON logs.**
   ```bash
   STORAGE_BACKEND=fs STORAGE_PATH=./data LOG_FORMAT=json ./target/release/otlp2parquet
   ```
   Expected output: `Listening on 0.0.0.0:8080 (server mode)`.
2. **S3 deployment with prefixing.**
   ```bash
   STORAGE_BACKEND=s3 S3_BUCKET=logs-<ENV> S3_REGION=<REGION> S3_PREFIX=services/<ENV>/ ./target/release/otlp2parquet
   ```
   Expected output: `Configured OpenDAL S3 operator (prefix=services/<ENV>/)`.
3. **Sample request/response using curl.**
   ```bash
   curl -sS -X POST http://127.0.0.1:8080/v1/logs \
     -H "Content-Type: application/json" \
     -d '{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"example"}}]},"scopeLogs":[{"logRecords":[{"timeUnixNano":"1710000000000000000","severityText":"INFO","body":{"stringValue":"Config example"}}]}]}]}'
   ```
   Expected output: `{}` (empty JSON response).

## Versioning & Compatibility
- **Rust toolchain:** Stable channel ≥ 1.75.0 for `wasm32-unknown-unknown` optimizations.
- **Cargo features:** `cf`, `server`, and `lambda` are mutually exclusive; build with exactly one.
- **Runtime targets:**
  - Cloudflare Workers: WASM Module, compatibility date ≥ `2024-01-01`.
  - Standalone server: Linux x86_64 or aarch64 (musl/glibc) with Tokio runtime.
  - AWS Lambda: `provided.al2` runtime with bootstrap binary built via `cargo lambda`.
- **Dependency policy:** Workspace pinned via `Cargo.lock`; run `cargo update -p <crate>` for selective upgrades.

## Security
- **Secrets handling:** Load credentials from environment variables or secrets managers; avoid checking `.env` into version control.
- **Least privilege IAM:** Grant `s3:PutObject`, `s3:AbortMultipartUpload`, and logging permissions only to ingestion buckets.
- **Dependency scanning:** Run `cargo audit` weekly; add to CI to alert on vulnerable crates.
- **Transport security:** Terminate TLS at Cloudflare, API Gateway, or an ingress proxy; standalone server expects TLS termination upstream.
- **Data retention:** Configure lifecycle rules on R2/S3 to delete or transition old Parquet files according to compliance needs.
