# Cloudflare Workers Deployment Guide

This guide provides a focused workflow for developing, testing, and deploying `otlp2parquet` to Cloudflare Workers with R2 storage.

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

## 1. Local Development & Testing

`wrangler dev` allows you to run your Worker locally, with hot-reloading and access to a preview R2 bucket.

### Development Workflow

1.  **Login to Cloudflare**:

    ```bash
    wrangler login
    ```

2.  **Create a Preview R2 Bucket**:

    ```bash
    # This bucket is used by `wrangler dev` for local testing.
    wrangler r2 bucket create otlp-logs-preview
    ```

3.  **Start the Local Server**:

    ```bash
    # This command starts a local server that simulates the Cloudflare environment.
    wrangler dev
    ```

    Your Worker is now available at `http://localhost:8787`.

4.  **Test Locally**:

    Send a test request to your local Worker.

    ```bash
    curl -X POST http://localhost:8787/v1/logs \
      -H "Content-Type: application/x-protobuf" \
      --data-binary @testdata/logs.pb
    ```

5.  **Verify Output**:

    Check the contents of your preview R2 bucket.

    ```bash
    wrangler r2 object list otlp-logs-preview
    ```

## 2. Deployment to Cloudflare

Once you've tested locally, you can deploy your Worker to the Cloudflare global network.

### Option A: Quick Start (Deploy Button)

The easiest way to deploy is to use the deploy button, which forks the repository and handles the initial setup automatically.

[![Deploy to Cloudflare Workers](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/smithclay/otlp2parquet)

### Option B: Manual Deployment

1.  **Create a Production R2 Bucket**:

    ```bash
    wrangler r2 bucket create otlp-logs
    ```

2.  **Configure `wrangler.toml`**:

    Update your `wrangler.toml` file to link to your R2 bucket.

    ```toml
    # wrangler.toml
    [[r2_buckets]]
    binding = "LOGS_BUCKET"
    bucket_name = "otlp-logs"
    preview_bucket_name = "otlp-logs-preview"
    ```

3.  **Build the WASM Binary**:

    ```bash
    # This command builds, optimizes, and compresses the WASM binary.
    make build-cloudflare
    ```

4.  **Deploy**:

    ```bash
    # Deploy to your production environment.
    wrangler deploy
    ```

    After deployment, Wrangler will output the public URL for your Worker.

## 3. Configuration

Configuration for your Worker is managed in `wrangler.toml`.

### Environment Variables

Set non-sensitive configuration, such as batching parameters, in the `[vars]` section.

```toml
# wrangler.toml
[vars]
OTLP2PARQUET_BATCH_MAX_ROWS = "100000"
OTLP2PARQUET_BATCHING_ENABLED = "true"
```

### Secrets

Use `wrangler secret` to store sensitive credentials like R2 access keys. **Do not** store secrets in `wrangler.toml`.

```bash
# This will prompt you to enter the secret value securely.
wrangler secret put OTLP2PARQUET_R2_SECRET_ACCESS_KEY
```

### Environments

You can define different environments (e.g., `staging`, `production`) in `wrangler.toml` to deploy different versions of your Worker with separate configurations.

```bash
# Deploy to a specific environment
wrangler deploy --env staging
```

## 4. Monitoring & Troubleshooting

### View Logs

Stream logs from your deployed Worker in real-time.

```bash
# Tail logs from your worker
wrangler tail
```

### Common Issues

*   **Build fails - WASM too large**: The WASM binary must be under 3MB (compressed). Use `make wasm-profile` to analyze binary size and identify large dependencies.
*   **R2 Bucket Access Denied**: Ensure the `binding` name in `wrangler.toml` matches the one used in the code (`env.LOGS_BUCKET`). Also, verify that your API token has the necessary R2 permissions.
