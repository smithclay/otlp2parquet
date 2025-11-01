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
S3_ENDPOINT= S3_REGION=us-east-1 \
AWS_ACCESS_KEY_ID=xxx AWS_SECRET_ACCESS_KEY=xxx \
docker-compose up
```

**Cloudflare R2:**
```bash
S3_ENDPOINT=https://ACCOUNT_ID.r2.cloudflarestorage.com \
AWS_ACCESS_KEY_ID=xxx AWS_SECRET_ACCESS_KEY=xxx \
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

| Variable | Default | Description |
|----------|---------|-------------|
| `STORAGE_BACKEND` | `s3` | Storage: `s3`, `r2`, `filesystem` |
| `S3_BUCKET` | `otlp-logs` | Bucket name |
| `S3_REGION` | `us-east-1` | AWS region |
| `S3_ENDPOINT` | `http://minio:9000` | S3 endpoint (blank for AWS) |
| `AWS_ACCESS_KEY_ID` | `minioadmin` | Access key |
| `AWS_SECRET_ACCESS_KEY` | `minioadmin` | Secret key |
| `HTTP_PORT` | `4318` | HTTP server port |
| `RUST_LOG` | `info` | Log level |

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
