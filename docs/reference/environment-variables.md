# Environment Variables Reference

Complete reference for all `OTLP2PARQUET_*` environment variables.

All environment variables use the `OTLP2PARQUET_` prefix and override values from `config.toml`.

## Platform Detection

These variables are automatically set by the runtime platform and used for auto-detection:

| Variable | Platforms | Set By | Description |
|----------|-----------|---------|-------------|
| `AWS_LAMBDA_FUNCTION_NAME` | Lambda | AWS | Lambda function name (auto-set by AWS) |
| `CF_WORKER` | Cloudflare | Cloudflare | Cloudflare Workers indicator (auto-set) |

**Platform detection logic:**
- If `AWS_LAMBDA_FUNCTION_NAME` is set → Lambda platform
- Else if `CF_WORKER` is set → Cloudflare Workers platform
- Else → Server platform (default)

## Storage Configuration

### Backend Selection

| Variable | Platforms | Required | Default | Description |
|----------|-----------|----------|---------|-------------|
| `OTLP2PARQUET_STORAGE_BACKEND` | All | No | Auto-detected | Storage backend: `s3`, `r2`, `fs` (filesystem) |

**Auto-detection rules:**
- Lambda: `s3` (S3 only)
- Cloudflare Workers: `r2` (R2 only)
- Server: `s3` (default, but configurable)

### S3 Storage

| Variable | Platforms | Required | Default | Description |
|----------|-----------|----------|---------|-------------|
| `OTLP2PARQUET_S3_BUCKET` | All | Yes* | - | S3 bucket name |
| `OTLP2PARQUET_S3_REGION` | All | Yes* | - | AWS region (e.g., `us-west-2`) |
| `OTLP2PARQUET_S3_ENDPOINT` | All | No | AWS default | Custom S3 endpoint (for MinIO, LocalStack) |

*Required when using S3 storage backend

### R2 Storage

| Variable | Platforms | Required | Default | Description |
|----------|-----------|----------|---------|-------------|
| `OTLP2PARQUET_R2_BUCKET` | Cloudflare | Yes* | - | R2 bucket name |
| `OTLP2PARQUET_R2_ACCOUNT_ID` | Cloudflare | Yes* | - | Cloudflare account ID |
| `OTLP2PARQUET_R2_ACCESS_KEY_ID` | Cloudflare | Yes* | - | R2 access key ID |
| `OTLP2PARQUET_R2_SECRET_ACCESS_KEY` | Cloudflare | Yes* | - | R2 secret access key |

*Required when using R2 storage backend

### Filesystem Storage

| Variable | Platforms | Required | Default | Description |
|----------|-----------|----------|---------|-------------|
| `OTLP2PARQUET_STORAGE_PATH` | Server | No | `./data` | Local filesystem path for Parquet files |

## Catalog Configuration (Iceberg)

### S3 Tables (Lambda)

| Variable | Platforms | Required | Default | Description |
|----------|-----------|----------|---------|-------------|
| `OTLP2PARQUET_ICEBERG_BUCKET_ARN` | Lambda | Yes* | - | S3 Tables bucket ARN (format: `arn:aws:s3tables:region:account:bucket/name`) |

*Required for S3 Tables catalog support on Lambda

**Example:**
```bash
OTLP2PARQUET_ICEBERG_BUCKET_ARN=arn:aws:s3tables:us-west-2:123456789012:bucket/my-otlp-bucket
```

### REST Catalog (Server)

| Variable | Platforms | Required | Default | Description |
|----------|-----------|----------|---------|-------------|
| `OTLP2PARQUET_ICEBERG_REST_URI` | Server | Yes* | - | REST catalog endpoint (e.g., `https://glue.us-west-2.amazonaws.com/iceberg` or Nessie URL) |
| `OTLP2PARQUET_ICEBERG_WAREHOUSE` | Server | Yes* | - | Warehouse identifier (Glue: account ID, Nessie: warehouse name) |
| `OTLP2PARQUET_ICEBERG_NAMESPACE` | Server | Yes* | - | Catalog namespace/database name (e.g., `otel`) |
| `OTLP2PARQUET_ICEBERG_DATA_LOCATION` | Server | No | - | S3 location for table data (e.g., `s3://my-bucket`) |
| `OTLP2PARQUET_ICEBERG_CATALOG_NAME` | Server | No | `default` | Catalog name |

*Required when using Iceberg REST catalog

### Catalog Advanced Settings

| Variable | Platforms | Required | Default | Description |
|----------|-----------|----------|---------|-------------|
| `OTLP2PARQUET_ICEBERG_STAGING_PREFIX` | All | No | `staging/` | Prefix for staging files before commit |
| `OTLP2PARQUET_ICEBERG_TARGET_FILE_SIZE_BYTES` | All | No | `134217728` | Target Parquet file size (128MB) |
| `OTLP2PARQUET_ICEBERG_FORMAT_VERSION` | All | No | `2` | Iceberg format version (1 or 2) |

### Table Names (Iceberg)

| Variable | Platforms | Required | Default | Description |
|----------|-----------|----------|---------|-------------|
| `OTLP2PARQUET_ICEBERG_TABLE_LOGS` | All | No | `logs` | Logs table name |
| `OTLP2PARQUET_ICEBERG_TABLE_TRACES` | All | No | `traces` | Traces table name |
| `OTLP2PARQUET_ICEBERG_TABLE_METRICS_GAUGE` | All | No | `metrics_gauge` | Gauge metrics table name |
| `OTLP2PARQUET_ICEBERG_TABLE_METRICS_SUM` | All | No | `metrics_sum` | Sum metrics table name |
| `OTLP2PARQUET_ICEBERG_TABLE_METRICS_HISTOGRAM` | All | No | `metrics_histogram` | Histogram metrics table name |
| `OTLP2PARQUET_ICEBERG_TABLE_METRICS_EXPONENTIAL_HISTOGRAM` | All | No | `metrics_exponential_histogram` | Exponential histogram table name |
| `OTLP2PARQUET_ICEBERG_TABLE_METRICS_SUMMARY` | All | No | `metrics_summary` | Summary metrics table name |

## Batch Configuration

| Variable | Platforms | Required | Default | Description |
|----------|-----------|----------|---------|-------------|
| `OTLP2PARQUET_BATCHING_ENABLED` | Server | No | `true` | Enable in-memory batching |
| `OTLP2PARQUET_BATCH_MAX_ROWS` | Server | No | `1000` | Maximum rows per batch before flush |
| `OTLP2PARQUET_BATCH_MAX_BYTES` | Server | No | `10485760` | Maximum bytes per batch (10MB) |
| `OTLP2PARQUET_BATCH_MAX_AGE_SECS` | Server | No | `300` | Maximum batch age in seconds (5 min) |

**Note:** Batching is only available on Server platform. Lambda and Cloudflare Workers process requests individually.

## Request Configuration

| Variable | Platforms | Required | Default | Description |
|----------|-----------|----------|---------|-------------|
| `OTLP2PARQUET_MAX_PAYLOAD_BYTES` | All | No | `10485760` | Maximum request payload size (10MB) |

## Parquet Configuration

| Variable | Platforms | Required | Default | Description |
|----------|-----------|----------|---------|-------------|
| `OTLP2PARQUET_PARQUET_ROW_GROUP_SIZE` | All | No | `1000` | Parquet row group size |

## Server Configuration

| Variable | Platforms | Required | Default | Description |
|----------|-----------|----------|---------|-------------|
| `OTLP2PARQUET_LISTEN_ADDR` | Server | No | `0.0.0.0:4318` | HTTP server listen address |
| `OTLP2PARQUET_LOG_LEVEL` | Server | No | `info` | Log level: `trace`, `debug`, `info`, `warn`, `error` |
| `OTLP2PARQUET_LOG_FORMAT` | Server | No | `text` | Log format: `text` or `json` |

**Note:** These variables only affect the Server platform. Lambda and Cloudflare Workers use platform-specific logging.

## Lambda Configuration

| Variable | Platforms | Required | Default | Description |
|----------|-----------|----------|---------|-------------|
| `OTLP2PARQUET_LAMBDA_INTEGRATED_ICEBERG` | Lambda | No | `true` | Enable integrated Iceberg support |
| `RUST_LOG` | Lambda | No | `info` | Rust logging level (Lambda-specific) |

## Authentication (Cloudflare Workers Only)

| Variable | Platforms | Required | Default | Description |
|----------|-----------|----------|---------|-------------|
| `OTLP2PARQUET_BASIC_AUTH_ENABLED` | Cloudflare | No | `false` | Enable HTTP Basic Authentication |
| `OTLP2PARQUET_BASIC_AUTH_USERNAME` | Cloudflare | Yes* | - | Basic auth username |
| `OTLP2PARQUET_BASIC_AUTH_PASSWORD` | Cloudflare | Yes* | - | Basic auth password |

*Required when `OTLP2PARQUET_BASIC_AUTH_ENABLED=true`

## Configuration Precedence

Environment variables have the highest precedence and override all other configuration sources:

1. **Highest Priority:** Environment variables (`OTLP2PARQUET_*`)
2. **Medium Priority:** `config.toml` file
3. **Lowest Priority:** Platform-specific defaults

## Examples

### Lambda with S3 Tables

```bash
# Auto-detected by CloudFormation
OTLP2PARQUET_STORAGE_BACKEND=s3
OTLP2PARQUET_S3_REGION=us-west-2
OTLP2PARQUET_ICEBERG_BUCKET_ARN=arn:aws:s3tables:us-west-2:123456789012:bucket/otlp-data
```

### Server with Nessie Catalog

```bash
OTLP2PARQUET_STORAGE_BACKEND=s3
OTLP2PARQUET_S3_BUCKET=my-data-bucket
OTLP2PARQUET_S3_REGION=us-west-2

OTLP2PARQUET_ICEBERG_REST_URI=http://nessie:19120/api/v1
OTLP2PARQUET_ICEBERG_WAREHOUSE=warehouse
OTLP2PARQUET_ICEBERG_NAMESPACE=otel
```

### Cloudflare Workers with R2 (Plain Parquet)

```bash
OTLP2PARQUET_STORAGE_BACKEND=r2
OTLP2PARQUET_R2_BUCKET=otlp-logs
OTLP2PARQUET_R2_ACCOUNT_ID=abc123
OTLP2PARQUET_R2_ACCESS_KEY_ID=***
OTLP2PARQUET_R2_SECRET_ACCESS_KEY=***
```

### Cloudflare Workers with R2 Data Catalog

```bash
OTLP2PARQUET_STORAGE_BACKEND=r2
OTLP2PARQUET_R2_BUCKET=otlp-data
OTLP2PARQUET_R2_ACCOUNT_ID=abc123
OTLP2PARQUET_R2_ACCESS_KEY_ID=***
OTLP2PARQUET_R2_SECRET_ACCESS_KEY=***
OTLP2PARQUET_CATALOG_TYPE=r2
```

## Related Documentation

- [Configuration Guide](../concepts/configuration.md) - Configuration file format
- [Catalog Types](./catalog-types.md) - Detailed catalog configuration
- [AWS Lambda Setup](../setup/aws-lambda.md) - Lambda deployment
- [Cloudflare Workers Setup](../setup/cloudflare.md) - Workers deployment
