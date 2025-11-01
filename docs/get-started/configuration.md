# Configuration Guide

`otlp2parquet` provides flexible configuration through multiple sources with a clear priority order. All platforms (Server, Lambda, Cloudflare Workers) use the same unified configuration system.

## Configuration Sources

Configuration is loaded from multiple sources in the following priority order (highest to lowest):

1. **Environment variables** (highest priority) - `OTLP2PARQUET_*` prefixed variables
2. **Config file from path** - `OTLP2PARQUET_CONFIG` environment variable
3. **Inline config content** - `OTLP2PARQUET_CONFIG_CONTENT` environment variable
4. **Default config files** - `./config.toml` or `./.otlp2parquet.toml`
5. **Platform defaults** (lowest priority) - Auto-detected based on runtime

## Platform Auto-Detection

The runtime platform is automatically detected based on environment variables:

- **Cloudflare Workers**: `CF_WORKER` environment variable present
- **AWS Lambda**: `AWS_LAMBDA_FUNCTION_NAME` environment variable present
- **Server**: Neither of the above (default mode)

Each platform has appropriate defaults for batch sizes, payload limits, and storage backends.

## Configuration via Environment Variables

All environment variables use the `OTLP2PARQUET_` prefix for consistency.

### Batch Configuration

Control how OTLP data is batched before writing to storage:

| Variable | Type | Default (Server/Lambda) | Default (Cloudflare) | Description |
|----------|------|------------------------|---------------------|-------------|
| `OTLP2PARQUET_BATCH_MAX_ROWS` | integer | 200,000 | 100,000 | Maximum rows per batch |
| `OTLP2PARQUET_BATCH_MAX_BYTES` | integer | 134,217,728 (128 MB) | 67,108,864 (64 MB) | Maximum bytes per batch |
| `OTLP2PARQUET_BATCH_MAX_AGE_SECS` | integer | 10 | 5 | Maximum age (seconds) before flush |
| `OTLP2PARQUET_BATCHING_ENABLED` | boolean | true | true | Enable/disable batching |

**Example:**
```bash
OTLP2PARQUET_BATCH_MAX_ROWS=100000
OTLP2PARQUET_BATCH_MAX_BYTES=67108864
OTLP2PARQUET_BATCH_MAX_AGE_SECS=5
OTLP2PARQUET_BATCHING_ENABLED=true
```

### Request Handling

Control HTTP request handling:

| Variable | Type | Default (Server) | Default (Lambda) | Default (Cloudflare) | Description |
|----------|------|------------------|------------------|---------------------|-------------|
| `OTLP2PARQUET_MAX_PAYLOAD_BYTES` | integer | 8,388,608 (8 MB) | 6,291,456 (6 MB) | 1,048,576 (1 MB) | Maximum HTTP payload size |

**Example:**
```bash
OTLP2PARQUET_MAX_PAYLOAD_BYTES=8388608
```

### Storage Backend

Configure the storage backend and credentials:

| Variable | Type | Default (Server) | Default (Lambda) | Default (Cloudflare) | Description |
|----------|------|------------------|------------------|---------------------|-------------|
| `OTLP2PARQUET_STORAGE_BACKEND` | string | fs | s3 | r2 | Storage type: `fs`, `s3`, or `r2` |

**Example:**
```bash
OTLP2PARQUET_STORAGE_BACKEND=s3
```

#### Filesystem Storage (backend=fs)

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `OTLP2PARQUET_STORAGE_PATH` | string | ./data | Local directory path |

**Example:**
```bash
OTLP2PARQUET_STORAGE_BACKEND=fs
OTLP2PARQUET_STORAGE_PATH=/var/lib/otlp2parquet/data
```

#### S3 Storage (backend=s3)

| Variable | Type | Required | Description |
|----------|------|----------|-------------|
| `OTLP2PARQUET_S3_BUCKET` | string | Yes | S3 bucket name |
| `OTLP2PARQUET_S3_REGION` | string | Yes | AWS region |
| `OTLP2PARQUET_S3_ENDPOINT` | string | No | Custom S3 endpoint (for S3-compatible services) |
| `AWS_ACCESS_KEY_ID` | string | No* | AWS access key (auto-discovered from IAM role if not provided) |
| `AWS_SECRET_ACCESS_KEY` | string | No* | AWS secret key (auto-discovered from IAM role if not provided) |

\*Required if not using IAM role authentication

**Example (with credentials):**
```bash
OTLP2PARQUET_STORAGE_BACKEND=s3
OTLP2PARQUET_S3_BUCKET=my-otlp-bucket
OTLP2PARQUET_S3_REGION=us-east-1
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

**Example (with IAM role - Lambda/EC2):**
```bash
OTLP2PARQUET_STORAGE_BACKEND=s3
OTLP2PARQUET_S3_BUCKET=my-otlp-bucket
OTLP2PARQUET_S3_REGION=us-east-1
# Credentials auto-discovered from IAM role
```

**Example (MinIO/S3-compatible):**
```bash
OTLP2PARQUET_STORAGE_BACKEND=s3
OTLP2PARQUET_S3_BUCKET=otlp-logs
OTLP2PARQUET_S3_REGION=us-east-1
OTLP2PARQUET_S3_ENDPOINT=http://minio:9000
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
```

#### R2 Storage (backend=r2)

| Variable | Type | Required | Description |
|----------|------|----------|-------------|
| `OTLP2PARQUET_R2_BUCKET` | string | Yes | R2 bucket name |
| `OTLP2PARQUET_R2_ACCOUNT_ID` | string | Yes | Cloudflare account ID |
| `OTLP2PARQUET_R2_ACCESS_KEY_ID` | string | Yes | R2 access key ID |
| `OTLP2PARQUET_R2_SECRET_ACCESS_KEY` | string | Yes | R2 secret access key |

**Example:**
```bash
OTLP2PARQUET_STORAGE_BACKEND=r2
OTLP2PARQUET_R2_BUCKET=my-r2-bucket
OTLP2PARQUET_R2_ACCOUNT_ID=abc123def456
OTLP2PARQUET_R2_ACCESS_KEY_ID=your_access_key
OTLP2PARQUET_R2_SECRET_ACCESS_KEY=your_secret_key
```

### Server-Specific Configuration

Only applicable when running in Server mode:

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `OTLP2PARQUET_LISTEN_ADDR` | string | 0.0.0.0:4318 | HTTP server listen address |
| `OTLP2PARQUET_LOG_LEVEL` | string | info | Log level: `trace`, `debug`, `info`, `warn`, `error` |
| `OTLP2PARQUET_LOG_FORMAT` | string | text | Log format: `text` or `json` |

**Example:**
```bash
OTLP2PARQUET_LISTEN_ADDR=0.0.0.0:4318
OTLP2PARQUET_LOG_LEVEL=info
OTLP2PARQUET_LOG_FORMAT=json
```

## Configuration via TOML File

For more complex configurations, you can use a TOML configuration file.

### Using a Config File

**Option 1: Default locations** (checked automatically)
```bash
# Place config in one of these locations:
./config.toml
./.otlp2parquet.toml
```

**Option 2: Specify file path**
```bash
OTLP2PARQUET_CONFIG=/path/to/config.toml
```

**Option 3: Inline config content** (useful for Lambda/Cloudflare)
```bash
OTLP2PARQUET_CONFIG_CONTENT='[batch]
max_rows = 100000
max_bytes = 67108864
'
```

### TOML File Format

See the [example configuration file](../../config.example.toml) for a complete reference.

**Basic example:**
```toml
[batch]
max_rows = 200_000
max_bytes = 134_217_728  # 128 MB
max_age_secs = 10
enabled = true

[request]
max_payload_bytes = 8_388_608  # 8 MB

[storage]
backend = "s3"

[storage.s3]
bucket = "my-otlp-bucket"
region = "us-east-1"

[server]
listen_addr = "0.0.0.0:4318"
log_level = "info"
log_format = "text"
```

**Multi-backend example:**
```toml
# Server defaults with filesystem storage
[storage]
backend = "fs"

[storage.fs]
path = "./data"

# Optional: S3 configuration (activate with OTLP2PARQUET_STORAGE_BACKEND=s3)
[storage.s3]
bucket = "my-otlp-bucket"
region = "us-east-1"
endpoint = "https://s3.custom-endpoint.com"  # Optional

# Optional: R2 configuration (activate with OTLP2PARQUET_STORAGE_BACKEND=r2)
[storage.r2]
bucket = "my-r2-bucket"
account_id = "your_account_id"
access_key_id = "your_access_key"
secret_access_key = "your_secret_key"
```

## Platform-Specific Defaults

Each platform has sensible defaults optimized for its constraints:

### Server (Docker/Kubernetes)
```toml
[batch]
max_rows = 200_000
max_bytes = 134_217_728  # 128 MB
max_age_secs = 10

[request]
max_payload_bytes = 8_388_608  # 8 MB

[storage]
backend = "fs"  # Filesystem by default
```

### AWS Lambda
```toml
[batch]
max_rows = 200_000
max_bytes = 134_217_728  # 128 MB
max_age_secs = 10

[request]
max_payload_bytes = 6_291_456  # 6 MB (Lambda limit consideration)

[storage]
backend = "s3"  # S3 required for Lambda
```

### Cloudflare Workers
```toml
[batch]
max_rows = 100_000
max_bytes = 67_108_864  # 64 MB (WASM memory constraints)
max_age_secs = 5

[request]
max_payload_bytes = 1_048_576  # 1 MB (Workers free tier limit)

[storage]
backend = "r2"  # R2 required for Workers
```

## Configuration Priority Example

Given the following configuration sources:

**Environment variable:**
```bash
OTLP2PARQUET_BATCH_MAX_ROWS=50000
```

**Config file (config.toml):**
```toml
[batch]
max_rows = 100000
max_bytes = 67108864
```

**Platform default (Server):**
```toml
[batch]
max_rows = 200000
```

**Result:**
- `max_rows = 50000` (from environment variable - highest priority)
- `max_bytes = 67108864` (from config file)
- `max_age_secs = 10` (from platform default)

## Validation

Configuration is validated at startup. Common validation errors:

### Missing Required Fields
```
Error: storage.s3.bucket is required for S3 backend
```
**Solution:** Set `OTLP2PARQUET_S3_BUCKET` or add to config file

### Invalid Values
```
Error: batch.max_rows must be greater than 0
```
**Solution:** Set a positive value for `OTLP2PARQUET_BATCH_MAX_ROWS`

### Platform Mismatch
```
Error: Lambda requires S3 storage backend, got: fs
```
**Solution:** Set `OTLP2PARQUET_STORAGE_BACKEND=s3` for Lambda deployments

## Best Practices

### Development
```bash
# Use small batches for faster iteration
OTLP2PARQUET_BATCH_MAX_ROWS=1000
OTLP2PARQUET_BATCH_MAX_AGE_SECS=1

# Enable debug logging
OTLP2PARQUET_LOG_LEVEL=debug
OTLP2PARQUET_LOG_FORMAT=text
```

### Production (Server/Docker)
```bash
# Optimize for throughput
OTLP2PARQUET_BATCH_MAX_ROWS=200000
OTLP2PARQUET_BATCH_MAX_BYTES=134217728
OTLP2PARQUET_BATCH_MAX_AGE_SECS=10

# Use JSON logging for centralized log systems
OTLP2PARQUET_LOG_LEVEL=info
OTLP2PARQUET_LOG_FORMAT=json
```

### Production (Lambda)
```bash
# Use IAM role authentication (recommended)
OTLP2PARQUET_STORAGE_BACKEND=s3
OTLP2PARQUET_S3_BUCKET=production-otlp-logs
OTLP2PARQUET_S3_REGION=us-east-1
# Credentials auto-discovered from Lambda execution role
```

### Production (Cloudflare Workers)
```bash
# Use Cloudflare Secrets for sensitive data
# Set via: wrangler secret put OTLP2PARQUET_R2_SECRET_ACCESS_KEY

OTLP2PARQUET_R2_BUCKET=production-otlp-logs
OTLP2PARQUET_R2_ACCOUNT_ID=your_account_id
OTLP2PARQUET_R2_ACCESS_KEY_ID=your_access_key
# R2_SECRET_ACCESS_KEY set via Cloudflare Secrets
```

## Troubleshooting

### Check Active Configuration

**Server mode:**
Configuration is validated and logged at startup. Check the logs for:
```
INFO otlp2parquet_server: Using filesystem storage at: ./data
INFO otlp2parquet_server: Batching enabled (max_rows=200000 max_bytes=134217728 max_age=10s)
INFO otlp2parquet_server: Max payload size set to 8388608 bytes
```

**Lambda:**
Check CloudWatch logs for configuration messages:
```
Lambda batching enabled (max_rows=200000 max_bytes=134217728 max_age=10s)
Lambda payload cap set to 6291456 bytes
```

**Cloudflare Workers:**
Use `wrangler tail` to see configuration messages:
```
Cloudflare batching enabled (max_rows=100000 max_bytes=67108864 max_age=5s)
```

### Common Issues

**Issue: Configuration not being applied**
- Check environment variable names have `OTLP2PARQUET_` prefix
- Verify config file path if using `OTLP2PARQUET_CONFIG`
- Remember: Environment variables override config file values

**Issue: Storage backend errors**
- Verify credentials are set correctly
- Check bucket/path permissions
- Ensure storage backend matches platform (S3 for Lambda, R2 for Cloudflare)

**Issue: Batching not working**
- Check `OTLP2PARQUET_BATCHING_ENABLED=true`
- Verify batch size limits are reasonable
- Monitor logs for flush events

## See Also

- [Example .env file](../../config.example.env)
- [Example TOML config](../../config.example.toml)
- [Docker Deployment](deployment/docker.md)
- [Lambda Deployment](deployment/aws-lambda.md)
- [Cloudflare Deployment](deployment/cloudflare.md)
