# Cloudflare Workers Deployment

## Prerequisites
- Cloudflare account with Workers and R2 enabled.
- `<ACCOUNT_ID>` and API token with `Workers Scripts` and `R2 Storage` permissions.
- Rust stable toolchain with `wasm32-unknown-unknown` target.
- `wrangler` CLI (`npm install -g wrangler`).
- Configured R2 bucket for Parquet storage.

## Build
1. **Install the WebAssembly target.**
   ```bash
   rustup target add wasm32-unknown-unknown
   ```
   Expected output: `info: downloading component 'rust-std' for 'wasm32-unknown-unknown'`.
2. **Build the worker artifact.**
   ```bash
   cargo build --release --target wasm32-unknown-unknown --features cf
   ```
   Expected output: `Finished release [optimized] target(s) in ...`.

## Configure
1. **Create `wrangler.toml`.**
   ```bash
   cat <<'CFG' > wrangler.toml
   name = "otlp2parquet"
   account_id = "<ACCOUNT_ID>"
   main = "./target/wasm32-unknown-unknown/release/otlp2parquet.wasm"
   compatibility_date = "2024-01-01"

   [vars]
   R2_BUCKET = "logs"
   STORAGE_BACKEND = "r2"

   [[r2_buckets]]
   binding = "LOG_BUCKET"
   bucket_name = "logs"
   CFG
   ```
   Expected output: `wrangler.toml` created with Cloudflare bindings.
2. **Login to Cloudflare.**
   ```bash
   wrangler login
   ```
   Expected output: `Successfully logged in.`

## Deploy
1. **Publish the worker to production.**
   ```bash
   wrangler deploy --env <ENV>
   ```
   Expected output: `✨  Success! Uploaded otlp2parquet (<ENV>)`.
2. **Bind R2 bucket (if not auto-bound).**
   ```bash
   wrangler r2 bucket create logs
   ```
   Expected output: `Created bucket logs`.

## Verify
1. **Send a test request.**
   ```bash
   curl -sS -X POST https://otlp2parquet-<ENV>.<ACCOUNT_ID>.workers.dev/v1/logs \
     -H "Content-Type: application/json" \
     -d '{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"cf-demo"}}]},"scopeLogs":[{"logRecords":[{"timeUnixNano":"1710000000000000000","severityText":"INFO","body":{"stringValue":"Cloudflare log"}}]}]}]}'
   ```
   Expected output: `{}` (empty JSON response).
2. **List the R2 bucket contents.**
   ```bash
   wrangler r2 object list logs --limit 1
   ```
   Expected output: First Parquet object key under `logs/cf-demo/`.

Now you should have otlp2parquet running at https://otlp2parquet-<ENV>.<ACCOUNT_ID>.workers.dev/v1/logs.

## Rollback
1. **View recent deployments.**
   ```bash
   wrangler deployments list --env <ENV>
   ```
   Expected output: Table of deployment IDs and timestamps.
2. **Roll back to a specific deployment.**
   ```bash
   wrangler deployments rollback --env <ENV> <DEPLOYMENT_ID>
   ```
   Expected output: `Rolled back otlp2parquet (<ENV>) to <DEPLOYMENT_ID>`.

## Performance & Cost Tips
- Use `cargo build -Z build-std=std,panic_abort` via `RUSTFLAGS="-C link-arg=-s"` to shave ~10% off WASM size.
- Cache-control headers are automatic; keep request payloads small to fit under 1MB Worker limits.
- Bind the R2 bucket in the Worker script to avoid extra API calls and reduce latency.
- Schedule `wrangler deploy --env staging` nightly to warm caches before high-traffic periods.
