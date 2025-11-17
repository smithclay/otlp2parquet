# Cloudflare Workers Setup

This guide shows how to develop, test, and deploy `otlp2parquet` to Cloudflare Workers with R2 storage and optional R2 Data Catalog support.

## Use Cases

This setup is a good fit for:
*   Integrating with Cloudflare R2 for its zero-egress fee data storage
*   R2 Data Catalog support for Apache Iceberg tables at the edge
*   Globally distributed, low-latency data ingestion at the edge
*   Pay-per-use, serverless deployments with minimal infrastructure
*   Edge-native observability with WASM (1.3MB compressed, 43.8% of 3MB limit)

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

### Plain Parquet Mode (Default)

Basic R2 storage without Iceberg catalog:

```toml
[[r2_buckets]]
binding = "LOGS_BUCKET"
bucket_name = "otlp-logs"
preview_bucket_name = "otlp-logs-preview"
```

This writes Parquet files directly to R2 without catalog metadata.

### R2 Data Catalog Mode (Iceberg)

Enable Apache Iceberg support with R2 Data Catalog:

```toml
[[r2_buckets]]
binding = "OTLP_BUCKET"
bucket_name = "otlp-data"

[vars]
OTLP2PARQUET_CATALOG_TYPE = "r2"
```

**Benefits:**
- ACID transactions for data consistency
- Schema evolution support
- Time travel queries
- Metadata management at the edge

**Requirements:**
- R2 bucket configured with Data Catalog enabled
- `OTLP2PARQUET_CATALOG_TYPE` set to `"r2"`
- Binding name must match code expectations

### Configuration Options

*   **Variables (`[vars]`)**: Set non-sensitive configuration:
    - `OTLP2PARQUET_MAX_PAYLOAD_BYTES` - Maximum request payload size
    - `OTLP2PARQUET_CATALOG_TYPE` - Catalog type: `"plain"` (default) or `"r2"` for Iceberg
    - `OTLP2PARQUET_BASIC_AUTH_ENABLED` - Enable HTTP Basic Auth
*   **Secrets**: Use `wrangler secret put <NAME>` for sensitive data:
    - `OTLP2PARQUET_S3_ACCESS_KEY_ID` - R2 access key
    - `OTLP2PARQUET_S3_SECRET_ACCESS_KEY` - R2 secret key
    - `OTLP2PARQUET_BASIC_AUTH_PASSWORD` - Auth password if enabled

## Common Tasks

*   **View Logs**: Stream logs from your deployed Worker in real-time with `wrangler tail`.
*   **Verify Output**: Check the contents of an R2 bucket with `wrangler r2 object list <BUCKET_NAME>`.

## Production Considerations

*   **Authentication**: By default, the deployed Worker has no authentication. Secure your endpoint by setting `OTLP2PARQUET_BASIC_AUTH_ENABLED` in `wrangler.toml` or implementing a custom authentication flow.
*   **Binary Size**: The compressed WASM binary must be under 3MB. Current size is 1.3MB (43.8% of limit). Run `make wasm-size` from the workspace root to verify size after changes.
*   **Catalog Choice**: Decide between plain Parquet (simpler, no catalog overhead) and R2 Data Catalog (Iceberg features like ACID and schema evolution). Most use cases start with plain Parquet.
*   **Batching**: For high-volume scenarios, use an upstream OTel Collector or Vector to batch requests before sending to the Worker. This optimizes Parquet file sizes and reduces R2 API costs.

## Troubleshooting

### Common Issues

**R2 Access Denied:**
- **Cause**: Binding name mismatch or missing R2 permissions
- **Solution**:
  - Verify binding name in `wrangler.toml` matches code expectations
  - Check API token has R2 read/write permissions
  - For R2 Data Catalog, ensure bucket has catalog enabled

**413 Payload Too Large:**
- **Cause**: Request exceeds `OTLP2PARQUET_MAX_PAYLOAD_BYTES` limit
- **Solution**:
  - Increase the limit in `wrangler.toml` vars section
  - Monitor Worker memory usage (128MB limit)
  - Implement upstream batching in OTel Collector

**Worker Deployment Exceeds 3MB:**
- **Cause**: WASM binary size exceeds Cloudflare's limit
- **Solution**:
  - Run `make wasm-size` to check current size (should be ~1.3MB)
  - Avoid adding large dependencies
  - Use `make wasm-full` to rebuild with optimizations

**Catalog Operations Failing:**
- **Cause**: R2 Data Catalog not enabled or misconfigured
- **Solution**:
  - Check logs with `wrangler tail` for specific errors
  - Verify `OTLP2PARQUET_CATALOG_TYPE="r2"` is set
  - Confirm R2 bucket has Data Catalog feature enabled
  - Remember: Parquet files are written even if catalog fails (warn-and-succeed pattern)

## Next Steps
*   [**Sending Data**](../guides/sending-data.md)
