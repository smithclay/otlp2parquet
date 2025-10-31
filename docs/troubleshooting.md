# Troubleshooting

## Error: `Missing STORAGE_BACKEND` on startup
- **Symptom:** Server exits immediately.
- **Log sample:**
  ```text
  ERROR storage::config > STORAGE_BACKEND not set; expected one of [fs, s3, r2]
  ```
- **Fix:** Set the variable explicitly.
  ```bash
  STORAGE_BACKEND=fs ./target/release/otlp2parquet
  ```
  Expected output: `Listening on 0.0.0.0:8080 (server mode)`.

## Error: `binding LOG_BUCKET not found` in Cloudflare Worker
- **Symptom:** Deployment succeeds but runtime returns 500.
- **Log sample:**
  ```text
  Uncaught ReferenceError: LOG_BUCKET is not defined
  ```
- **Fix:** Ensure `wrangler.toml` includes the R2 binding.
  ```bash
  wrangler r2 bucket create logs
  ```
  Expected output: `Created bucket logs`.

## Error: `AccessDenied` when writing to S3
- **Symptom:** Lambda succeeds but no objects stored.
- **Log sample:**
  ```text
  ERROR opendal::layers::retry > request failed: AccessDenied (403)
  ```
- **Fix:** Attach write policy to the Lambda role.
  ```bash
  aws iam put-role-policy --role-name otlp2parquet-lambda --policy-name otlp2parquet-write --policy-document file://policy.json
  ```
  Expected output: `PutRolePolicy` success message.

## Error: `target not found: wasm32-unknown-unknown`
- **Symptom:** `cargo build --target wasm32-unknown-unknown` fails.
- **Log sample:**
  ```text
  error: target not found: wasm32-unknown-unknown
  ```
- **Fix:** Add the target before building.
  ```bash
  rustup target add wasm32-unknown-unknown
  ```
  Expected output: `info: downloading component 'rust-std' for 'wasm32-unknown-unknown'`.

## Error: `command not found: cargo-lambda`
- **Symptom:** Lambda build step fails.
- **Log sample:**
  ```text
  error: no such subcommand: `lambda`
  ```
- **Fix:** Install the CLI globally.
  ```bash
  cargo install cargo-lambda
  ```
  Expected output: `Installed package 'cargo-lambda'`.

## Error: `address already in use` when starting server
- **Symptom:** Standalone server fails to bind.
- **Log sample:**
  ```text
  ERROR server::main > Failed to bind 0.0.0.0:8080: Address already in use (os error 98)
  ```
- **Fix:** Override `LISTEN_ADDR` or stop the conflicting process.
  ```bash
  LISTEN_ADDR=127.0.0.1:9000 ./target/release/otlp2parquet
  ```
  Expected output: `Listening on 127.0.0.1:9000 (server mode)`.

## Error: HTTP 400 `invalid payload`
- **Symptom:** API rejects incoming request.
- **Log sample:**
  ```text
  WARN ingest::handler > Failed to decode OTLP payload: missing resourceLogs
  ```
- **Fix:** Validate payload before sending.
  ```bash
  otel-cli logs --endpoint http://127.0.0.1:8080/v1/logs --protocol http/json --dry-run
  ```
  Expected output: `payload valid`.

## Error: `413 Payload Too Large` on Cloudflare
- **Symptom:** Large OTLP batch rejected.
- **Log sample:**
  ```text
  413 Payload Too Large (1.5 MB > 1 MB limit)
  ```
- **Fix:** Reduce batch size by splitting uploads.
  ```bash
  otel-cli logs --endpoint https://otlp2parquet-<ENV>.<ACCOUNT_ID>.workers.dev/v1/logs --batch-size 500
  ```
  Expected output: `sent 500 log records`.

## Error: `OOM` during Parquet flush
- **Symptom:** Process killed under heavy load.
- **Log sample:**
  ```text
  WARN memory::monitor > memory high watermark reached (512MB) -- flushing now
  ```
- **Fix:** Lower buffer size environment variable.
  ```bash
  STORAGE_BUFFER_SIZE=2097152 ./target/release/otlp2parquet
  ```
  Expected output: `Configured buffer size: 2097152 bytes`.

## Error: `Schema evolution detected` when reading
- **Symptom:** Downstream query fails due to schema change.
- **Log sample:**
  ```text
  ERROR parquet::schema > Column count mismatch: expected 15, found 14
  ```
- **Fix:** Inspect the Parquet schema and align consumers.
  ```bash
  parquet-tools schema data/logs/<SERVICE>/year=*/month=*/day=*/hour=*/*.parquet
  ```
  Expected output: Printed schema showing column definitions.
