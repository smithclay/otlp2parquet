# Docker Deployment

One command: `docker-compose up`

## Default Setup (MinIO)

```bash
docker-compose up
```

Includes:
- otlp2parquet HTTP server (:4318)
- MinIO S3-compatible storage (:9000 API, :9001 console)
- Auto-created `otlp-logs` bucket

MinIO console: http://localhost:9001 (minioadmin/minioadmin)

## Cloud Storage

Override environment variables:

**AWS S3:**
```bash
OTLP2PARQUET_STORAGE_BACKEND=s3 \
OTLP2PARQUET_S3_BUCKET=my-otlp-bucket \
OTLP2PARQUET_S3_REGION=us-east-1 \
AWS_ACCESS_KEY_ID=xxx AWS_SECRET_ACCESS_KEY=xxx \
docker-compose up
```

**Cloudflare R2:**
```bash
OTLP2PARQUET_STORAGE_BACKEND=r2 \
OTLP2PARQUET_R2_BUCKET=my-r2-bucket \
OTLP2PARQUET_R2_ACCOUNT_ID=your_account_id \
OTLP2PARQUET_R2_ACCESS_KEY_ID=xxx \
OTLP2PARQUET_R2_SECRET_ACCESS_KEY=xxx \
docker-compose up
```

## Build from Source

```bash
git clone https://github.com/smithclay/otlp2parquet.git
cd otlp2parquet
docker-compose up --build
```

## Kubernetes

See [Kubernetes manifests](../../deploy/docker/kubernetes/) for details.

```bash
kubectl apply -f deploy/docker/kubernetes/
```

## Environment Variables

For complete configuration options, see the [Configuration Guide](../configuration.md).

### Key Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `OTLP2PARQUET_STORAGE_BACKEND` | `fs` | Storage backend: `fs`, `s3`, or `r2` |
| `OTLP2PARQUET_STORAGE_PATH` | `./data` | Filesystem storage path (when backend=fs) |
| `OTLP2PARQUET_S3_BUCKET` | `otlp-logs` | S3 bucket name (when backend=s3) |
| `OTLP2PARQUET_S3_REGION` | `us-east-1` | AWS region (when backend=s3) |
| `OTLP2PARQUET_S3_ENDPOINT` | - | Custom S3 endpoint (optional, for MinIO/S3-compatible) |
| `OTLP2PARQUET_LISTEN_ADDR` | `0.0.0.0:4318` | HTTP server listen address |
| `OTLP2PARQUET_LOG_LEVEL` | `info` | Log level: `trace`, `debug`, `info`, `warn`, `error` |
| `OTLP2PARQUET_LOG_FORMAT` | `text` | Log format: `text` or `json` |
| `OTLP2PARQUET_BATCH_MAX_ROWS` | `200000` | Maximum rows per batch |
| `OTLP2PARQUET_BATCH_MAX_BYTES` | `134217728` | Maximum bytes per batch (128 MB) |
| `OTLP2PARQUET_BATCH_MAX_AGE_SECS` | `10` | Maximum batch age (seconds) |
| `OTLP2PARQUET_BATCHING_ENABLED` | `true` | Enable/disable batching |

### AWS Credentials

| Variable | Default | Description |
|----------|---------|-------------|
| `AWS_ACCESS_KEY_ID` | `minioadmin` | AWS/MinIO access key |
| `AWS_SECRET_ACCESS_KEY` | `minioadmin` | AWS/MinIO secret key |

## Troubleshooting

**Port conflict:**
```bash
HTTP_PORT=8081 MINIO_API_PORT=9002 MINIO_CONSOLE_PORT=9003 docker-compose up
```

**View logs:**
```bash
docker-compose logs -f otlp2parquet
```

**Reset data:**
```bash
docker-compose down -v
```
