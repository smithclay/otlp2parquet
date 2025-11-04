# Cloudflare Workers Deployment Guide

This guide shows how to develop, test, and deploy `otlp2parquet` to Cloudflare Workers with R2 storage.

## Prerequisites

1.  **Cloudflare Account**: A free tier account is sufficient to get started.
2.  **Node.js and Wrangler**: The command-line tool for Cloudflare Workers.
3.  **Rust Toolchain**: Including the `wasm32-unknown-unknown` target and `wasm-opt` for optimization.

```bash
# Install Wrangler CLI
npm install -g wrangler

# Add WASM target for Rust
rustup target add wasm32-unknown-unknown

# Install wasm-opt (macOS example)
brew install binaryen
```

## Local Development & Testing

Use `wrangler dev` to run the Worker locally, with support for hot-reloading and a preview R2 bucket.

> **Note**: All `wrangler` commands in this guide should be run from the `crates/otlp2parquet-cloudflare` directory.

### Development Workflow

1.  **Login to Cloudflare**:

    ```bash
    wrangler login
    ```

2.  **Configure `wrangler.toml`**:

    Update `crates/otlp2parquet-cloudflare/wrangler.toml` to link point to a local instance of minio (start with docker-compose with the command `docker-compose up minio minio-init`). You can also point the worker to R2 using the R3 [S3-compatible API](https://developers.cloudflare.com/r2/api/s3/api/).

3.  **Create a Preview R2 Bucket** (optional, only if pointing the local worker to R2):

    ```bash
    # This bucket is used by `wrangler dev` for local testing.
    wrangler r2 bucket create otlp-logs-preview
    ```

4.  **Start the Local Server**:

    ```bash
    # This command starts a local server that simulates the Cloudflare environment.
    wrangler dev
    ```

    Your Worker is now available at `http://localhost:8787`.

5.  **Test Locally**:

    Send a test request to your local Worker.

    ```bash
    curl -X POST http://localhost:8787/v1/logs \
      -H "Content-Type: application/x-protobuf" \
      --data-binary @testdata/logs.pb
    ```

6.  **Verify Output**:

    Check the contents of your preview R2 bucket.

    ```bash
    wrangler r2 object list otlp-logs-preview
    ```

## Deployment to Cloudflare

After local testing, you can deploy the Worker to the Cloudflare global network. By default, the Cloudflare worker has no authentication enabled: implement your own authentication or set `OTLP2PARQUET_BASIC_AUTH_ENABLED` in `wrangler.toml`.

### Option A: Quick Start (Deploy Button)

The easiest way to deploy is with the deploy button, which forks the repository and handles the initial setup automatically.

[![Deploy to Cloudflare Workers](https://deploy.workers.cloudflare.com/button)](https://github.com/smithclay/otlp2parquet/tree/main/crates/otlp2parquet-cloudflare)

### Option B: Manual Deployment

1.  **Create a Production R2 Bucket**:

    ```bash
    # use a different name, this must be globally unique
    wrangler r2 bucket create otlp-logs
    ```

2.  **Configure `wrangler.toml`**:

    Update `crates/otlp2parquet-cloudflare/wrangler.toml` to link to your R2 bucket and account.

    ```toml
    # crates/otlp2parquet-cloudflare/wrangler.toml
    OTLP2PARQUET_S3_ENDPOINT = "https://YOUR_ACCOUNT_HERE.r2.cloudflarestorage.com"

    [[r2_buckets]]
    binding = "LOGS_BUCKET"
    bucket_name = "otlp-logs"
    preview_bucket_name = "otlp-logs-preview"
    ```

3.  **Build the WASM Binary**:

    From the workspace root, run the build command.

    ```bash
    # This command builds, optimizes, and compresses the WASM binary.
    make build-cloudflare
    ```

4.  **Deploy**:

    From the `crates/otlp2parquet-cloudflare` directory, run the deploy command.

    ```bash
    # Deploy to your production environment.
    wrangler deploy --env production
    ```

    After deployment, Wrangler will output the public URL for your Worker.

## Configuration

Manage your Worker's configuration in `crates/otlp2parquet-cloudflare/wrangler.toml`.

### Environment Variables

Set non-sensitive configuration, such as batching parameters, in the `[vars]` section.

```toml
# crates/otlp2parquet-cloudflare/wrangler.toml
[vars]
# Maximum request payload size in bytes (default: 10MB for Cloudflare Workers)
# Adjust based on your data volume and Worker memory constraints (128MB total)
OTLP2PARQUET_MAX_PAYLOAD_BYTES = "10485760"  # 10 MB

# S3/R2 configuration
OTLP2PARQUET_S3_BUCKET = "otlp-logs"
OTLP2PARQUET_S3_REGION = "auto"
```

> **Note**: The default max payload is 10MB for Cloudflare Workers. You can increase this up to ~50MB depending on your data complexity and memory usage, but monitor Worker memory carefully to avoid exceeding the 128MB limit.

### Secrets

Use `wrangler secret` to store sensitive credentials like R2 access keys. **Do not** store secrets in `wrangler.toml`.

```bash
# Run from the crates/otlp2parquet-cloudflare directory
# This will prompt you to enter the secret value securely.
wrangler secret put OTLP2PARQUET_R2_SECRET_ACCESS_KEY
```

## Monitoring & Troubleshooting

> **Note**: Run these commands from the `crates/otlp2parquet-cloudflare` directory.

### View Logs

Stream logs from your deployed Worker in real-time.

```bash
# Tail logs from your worker
wrangler tail
```

### Common Issues

*   **Build fails - WASM too large**: The WASM binary must be under 3MB (compressed). Use `make wasm-profile` from the workspace root to analyze binary size.
*   **R2 Bucket Access Denied**: Ensure the `binding` name in `wrangler.toml` matches the one used in the code (`env.LOGS_BUCKET`). Also, verify your API token has the necessary R2 permissions.
*   **413 Payload Too Large**: This error comes from your application's `OTLP2PARQUET_MAX_PAYLOAD_BYTES` setting (default 10MB), not from Cloudflare. Increase the limit via environment variable if needed, but monitor Worker memory usage (128MB total limit). The error message will show the current limit for debugging.
