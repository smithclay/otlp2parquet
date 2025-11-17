# Configuration

This guide explains how to configure `otlp2parquet`. For a complete list of all settings, see the [Environment Variables Reference](../reference/environment-variables.md).

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
# Example: Configure S3 storage
export OTLP2PARQUET_STORAGE_BACKEND=s3
export OTLP2PARQUET_S3_BUCKET=my-data-bucket
export OTLP2PARQUET_S3_REGION=us-west-2

# Example: Configure S3 Tables catalog (Lambda)
export OTLP2PARQUET_ICEBERG_BUCKET_ARN=arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket

# Example: Configure R2 Data Catalog (Cloudflare Workers)
export OTLP2PARQUET_CATALOG_TYPE=r2

# Example: Configure Nessie catalog (Server)
export OTLP2PARQUET_ICEBERG_REST_URI=http://nessie:19120/api/v1
export OTLP2PARQUET_ICEBERG_WAREHOUSE=warehouse
export OTLP2PARQUET_ICEBERG_NAMESPACE=otel
```

For a complete list of all environment variables, see the [Environment Variables Reference](../reference/environment-variables.md).

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

# Apache Iceberg settings (Server with Nessie)
[iceberg]
rest_uri = "http://nessie:19120/api/v1"
warehouse = "warehouse"
namespace = "otel"
data_location = "s3://my-data-bucket"
```

**Alternative: S3 Tables (Lambda only)**
```toml
# For Lambda, use bucket_arn instead of rest_uri
[iceberg]
bucket_arn = "arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket"
```

Environment variables will always override settings defined in a TOML file. For more on Iceberg catalogs, see the [Catalog Types Reference](../reference/catalog-types.md).

## Platform Detection

otlp2parquet automatically detects the runtime platform using environment variables:

| Platform | Detection Variable | Set By |
|----------|-------------------|---------|
| **Lambda** | `AWS_LAMBDA_FUNCTION_NAME` | AWS (automatic) |
| **Cloudflare Workers** | `CF_WORKER` | Cloudflare (automatic) |
| **Server** | Neither variable present | Default |

**Detection Logic:**
1. If `AWS_LAMBDA_FUNCTION_NAME` exists → Lambda platform
2. Else if `CF_WORKER` exists → Cloudflare Workers platform
3. Else → Server platform (default)

This detection determines platform-specific defaults and constraints (storage backends, catalog support, etc.).

## Platform Defaults

Each platform is optimized for its constraints.

### Server (Docker/Kubernetes)

*   **Batching**: Enabled (in-memory batching for optimal file sizes)
*   **Max Payload**: 10 MiB
*   **Storage**: S3 (default), also supports R2, Filesystem, GCS, Azure
*   **Catalog**: Nessie (REST), AWS Glue (REST), or plain Parquet

### AWS Lambda

*   **Batching**: Disabled (per-request processing)
*   **Max Payload**: 6 MiB (Lambda constraint)
*   **Storage**: S3 only (event-driven constraint)
*   **Catalog**: S3 Tables (ARN-based), simplified configuration

### Cloudflare Workers

*   **Batching**: Disabled (per-request processing)
*   **Max Payload**: 10 MiB
*   **Storage**: R2 only (WASM constraint)
*   **Catalog**: R2 Data Catalog (WASM-compatible) or plain Parquet
*   **Binary Size**: 1.3MB compressed (43.8% of 3MB limit)
