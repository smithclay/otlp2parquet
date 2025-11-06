# Configuration Guide

This guide explains how to configure `otlp2parquet`.

## How Configuration Works

`otlp2parquet` uses a unified configuration system that works across all platforms (Server, Lambda, and Cloudflare Workers). The tool automatically applies sensible defaults for each platform, which you can then override as needed.

For example, the Cloudflare Workers environment has lower memory and payload size limits than the Server (Docker) environment. The system detects the platform at startup and applies the correct defaults automatically.

## Configuration Priority

Settings are applied in a specific order, with later sources overriding earlier ones. The highest priority source wins.

1.  **Environment Variables** (Highest priority)
2.  **TOML Configuration File**
3.  **Platform Defaults** (Lowest priority)

## Configuration Methods

You can configure the application using environment variables or a TOML file.

### Using Environment Variables

This method is best for simple overrides or for containerized environments where you pass variables to the runtime.

All variables are prefixed with `OTLP2PARQUET_`. For example, to change the storage backend and log level:

```bash
export OTLP2PARQUET_STORAGE_BACKEND=s3
export OTLP2PARQUET_LOG_LEVEL=debug
```

#### Common Environment Variables

*   `OTLP2PARQUET_MAX_PAYLOAD_BYTES` - Maximum request body size in bytes (default varies by platform)
*   `OTLP2PARQUET_STORAGE_BACKEND` - Storage backend: `fs`, `s3`, or `r2`
*   `OTLP2PARQUET_S3_BUCKET` - S3/R2 bucket name
*   `OTLP2PARQUET_S3_REGION` - S3 region (default: `auto` for R2)
*   `OTLP2PARQUET_S3_ENDPOINT` - Custom S3 endpoint (for MinIO, R2, etc.)

Specific variables for each platform are shown in the corresponding deployment guides.

### Using a TOML File (Recommended)

For managing multiple environments or more complex setups, a TOML file is the recommended approach.

The application automatically looks for `config.toml` or `.otlp2parquet.toml` in the current directory. You can also specify a path via the `OTLP2PARQUET_CONFIG` environment variable.

**Example `config.toml`:**

```toml
# config.toml

# Request handling settings
[request]
max_payload_bytes = 10485760  # 10 MiB (10 * 1024 * 1024)

# Settings for the in-memory batch processor
[batch]
max_rows = 200_000
max_bytes = 134_217_728  # 128 MiB
max_age_secs = 10

# Settings for the storage backend
[storage]
backend = "s3"

[storage.s3]
bucket = "my-otlp-bucket"
region = "us-east-1"

# Settings for the server mode
[server]
listen_addr = "0.0.0.0:4318"
log_level = "info"
```

For a complete list of all possible settings, see the [example configuration file](../../config.example.toml) in the repository root.

#### Apache Iceberg Configuration

Apache Iceberg is available for Lambda and Server platforms only (not Cloudflare Workers). You can configure it via TOML or environment variables.

**Example TOML Configuration:**

```toml
[iceberg]
rest_uri = "https://s3tables.us-east-1.amazonaws.com/iceberg"
warehouse = "s3://my-iceberg-warehouse"
namespace = "otel.production"

# Optional settings with defaults
catalog_name = "rest"
staging_prefix = "data/incoming"
target_file_size_bytes = 536_870_912  # 512 MB
format_version = 2

# Optional: Custom table names (defaults shown)
[iceberg.tables]
logs = "otel_logs"
traces = "otel_traces"
metrics_gauge = "otel_metrics_gauge"
metrics_sum = "otel_metrics_sum"
metrics_histogram = "otel_metrics_histogram"
metrics_exponential_histogram = "otel_metrics_exponential_histogram"
metrics_summary = "otel_metrics_summary"
```

**Corresponding Environment Variables:**

All Iceberg settings can also be configured via environment variables with the `OTLP2PARQUET_ICEBERG_` prefix:

*   `OTLP2PARQUET_ICEBERG_REST_URI` - REST catalog endpoint (required)
*   `OTLP2PARQUET_ICEBERG_WAREHOUSE` - Warehouse location
*   `OTLP2PARQUET_ICEBERG_NAMESPACE` - Namespace for tables (dot-separated)
*   `OTLP2PARQUET_ICEBERG_CATALOG_NAME` - Catalog name (default: "rest")
*   `OTLP2PARQUET_ICEBERG_STAGING_PREFIX` - Staging prefix (default: "data/incoming")
*   `OTLP2PARQUET_ICEBERG_TARGET_FILE_SIZE_BYTES` - Target file size (default: 536870912)
*   `OTLP2PARQUET_ICEBERG_FORMAT_VERSION` - Format version (default: 2)
*   `OTLP2PARQUET_ICEBERG_TABLE_LOGS` - Logs table name
*   `OTLP2PARQUET_ICEBERG_TABLE_TRACES` - Traces table name
*   `OTLP2PARQUET_ICEBERG_TABLE_METRICS_GAUGE` - Gauge metrics table name
*   `OTLP2PARQUET_ICEBERG_TABLE_METRICS_SUM` - Sum metrics table name
*   `OTLP2PARQUET_ICEBERG_TABLE_METRICS_HISTOGRAM` - Histogram metrics table name
*   `OTLP2PARQUET_ICEBERG_TABLE_METRICS_EXPONENTIAL_HISTOGRAM` - Exponential histogram table name
*   `OTLP2PARQUET_ICEBERG_TABLE_METRICS_SUMMARY` - Summary metrics table name

Environment variables override TOML configuration. For more details on using Iceberg, see the [Apache Iceberg documentation](../storage/iceberg.md).

## Platform-Specific Defaults

Each platform has different defaults optimized for its constraints.

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
