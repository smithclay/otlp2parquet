# Configuration

This guide explains how to configure `otlp2parquet`. For a complete list of all settings, see the upcoming *Configuration Reference*.

## Configuration Layers

`otlp2parquet` uses a unified system that applies settings in a specific order. The highest priority source wins.

1.  **Environment Variables** (Highest priority)
2.  **TOML Configuration File**
3.  **Platform Defaults** (Lowest priority)

The tool automatically applies sensible defaults for each platform (Server, Lambda, Cloudflare). For example, the Cloudflare environment has lower memory limits, so it uses smaller batch sizes by default. You can override these defaults as needed.

## Configuration Methods

You can configure the application using environment variables or a TOML file.

### Environment Variables

This method is best for simple overrides or for containerized environments. All variables are prefixed with `OTLP2PARQUET_`.

```bash
# Example: Override the storage backend and log level
export OTLP2PARQUET_STORAGE_BACKEND=s3
export OTLP2PARQUET_LOG_LEVEL=debug
```

### TOML File (Recommended)

For managing multiple environments or more complex setups, a TOML file is recommended. The application looks for `config.toml` or `.otlp2parquet.toml` in the current directory. You can also specify a path via the `OTLP2PARQUET_CONFIG` environment variable.

**Example `config.toml`:**
```toml
# config.toml

# Request handling settings
[request]
max_payload_bytes = 10485760  # 10 MiB

# In-memory batch processor settings
[batch]
max_rows = 200_000
max_age_secs = 10

# Storage backend settings
[storage]
backend = "s3"

[storage.s3]
bucket = "my-otlp-bucket"
region = "us-east-1"

# Apache Iceberg settings
[iceberg]
rest_uri = "https://s3tables.us-east-1.amazonaws.com/iceberg"
warehouse = "s3://my-iceberg-warehouse"
namespace = "otel.production"
```
Environment variables will always override settings defined in a TOML file. For more on Iceberg, see the [Storage Layers](./storage.md) guide.

## Platform Defaults

Each platform is optimized for its constraints.

### Server (Docker/Kubernetes)

*   **Batching**: 200k rows, 128 MiB, 10 seconds
*   **Max Payload**: 8 MiB
*   **Storage**: Filesystem (`fs`)

### AWS Lambda

*   **Batching**: 200k rows, 128 MiB, 10 seconds
*   **Max Payload**: 6 MiB
*   **Storage**: S3 (required)

### Cloudflare Workers

*   **Batching**: 100k rows, 64 MiB, 5 seconds
*   **Max Payload**: 10 MiB
*   **Storage**: R2 (required)
