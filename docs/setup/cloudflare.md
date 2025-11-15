# Cloudflare Workers Setup

This guide shows how to develop, test, and deploy `otlp2parquet` to Cloudflare Workers with R2 storage.

## Use Cases

This setup is a good fit for:
*   Integrating with Cloudflare R2 for its zero-egress fee data storage.
*   Globally distributed, low-latency data ingestion at the edge.
*   Pay-per-use, serverless deployments.

!!! note
    All `wrangler` commands in this guide must be run from the `crates/otlp2parquet-cloudflare` directory.

## Prerequisites

*   A Cloudflare account.
*   Node.js and the Wrangler CLI.
*   The Rust toolchain, including the `wasm32-unknown-unknown` target.

```bash
# Install Wrangler CLI
npm install -g wrangler

# Add WASM target for Rust
rustup target add wasm32-unknown-unknown
```

## Quick Start

This workflow takes you from local development to a production deployment.

1.  **Local Development**: Use `wrangler dev` to run the Worker locally with hot-reloading and a simulated R2 environment.
    1.  Run `wrangler login`.
    2.  Update `wrangler.toml` to bind a preview bucket:
        ```toml
        [[r2_buckets]]
        binding = "LOGS_BUCKET"
        bucket_name = "otlp-logs"
        preview_bucket_name = "otlp-logs-preview"
        ```
    3.  Run `wrangler r2 bucket create otlp-logs-preview`.
    4.  Run `wrangler dev` to start the local server.
    5.  Send a test request to `http://localhost:8787`.

2.  **Deployment**: Deploy the Worker to the Cloudflare global network.
    *   **Easy Mode**: Use the **Deploy Button**, which handles forking and setup automatically.
        [![Deploy to Cloudflare Workers](https://deploy.workers.cloudflare.com/button)](https://github.com/smithclay/otlp2parquet/tree/main/crates/otlp2parquet-cloudflare)
    *   **Manual Mode**: Create a production R2 bucket, update `wrangler.toml`, then run `make build-cloudflare` and `wrangler deploy`.

## Configuration

Configure your Worker in `wrangler.toml`. For a full overview, see the [Configuration Guide](../concepts/configuration.md).

*   **Variables (`[vars]`)**: Set non-sensitive configuration like `OTLP2PARQUET_MAX_PAYLOAD_BYTES`.
*   **Secrets**: Use `wrangler secret put <NAME>` to store sensitive credentials like `OTLP2PARQUET_R2_SECRET_ACCESS_KEY`.

## Common Tasks

*   **View Logs**: Stream logs from your deployed Worker in real-time with `wrangler tail`.
*   **Verify Output**: Check the contents of an R2 bucket with `wrangler r2 object list <BUCKET_NAME>`.

## Production Considerations

*   **Authentication**: By default, the deployed Worker has no authentication. Secure your endpoint by setting `OTLP2PARQUET_BASIC_AUTH_ENABLED` in `wrangler.toml` or implementing a custom authentication flow.
*   **Binary Size**: The compressed WASM binary must be under 3MB. Run `make wasm-profile` from the workspace root to analyze and reduce binary size if your build fails.

## Troubleshooting

*   **R2 Access Denied**: Ensure the `binding` name in `wrangler.toml` matches the one used in the code (`env.LOGS_BUCKET`). Verify your API token has R2 permissions.
*   **413 Payload Too Large**: This error comes from the `OTLP2PARQUET_MAX_PAYLOAD_BYTES` setting. You can increase this, but monitor Worker memory usage to avoid exceeding the 128MB limit.

## Next Steps
*   [**Sending Data**](../guides/sending-data.md)
