# Docker Deployment Guide

Deploy `otlp2parquet` using Docker with support for multiple storage backends (filesystem, S3, R2).

## Quick Start

### Pull from GitHub Container Registry

```bash
# Pull latest image
docker pull ghcr.io/smithclay/otlp2parquet:latest

# Run with filesystem storage (data persisted in Docker volume)
docker run -d \
  --name otlp2parquet \
  -p 8080:8080 \
  -e STORAGE_BACKEND=filesystem \
  -e FILESYSTEM_ROOT=/data \
  -v otlp-data:/data \
  ghcr.io/smithclay/otlp2parquet:latest
```

### Test the endpoint

```bash
# Send a test OTLP log payload
curl -X POST http://localhost:8080/v1/logs \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @test-payload.pb
```

## Docker Compose (Recommended)

### Basic Setup (Filesystem)

```bash
# Download docker-compose.yml
curl -O https://raw.githubusercontent.com/smithclay/otlp2parquet/main/docker-compose.yml

# Start the service
docker-compose up -d

# View logs
docker-compose logs -f

# Stop the service
docker-compose down
```

### S3 Storage

Create `docker-compose.s3.yml`:

```yaml
version: '3.9'

services:
  otlp2parquet:
    image: ghcr.io/smithclay/otlp2parquet:latest
    ports:
      - "8080:8080"
    environment:
      STORAGE_BACKEND: s3
      S3_BUCKET: my-otlp-logs
      S3_REGION: us-east-1
      # Use IAM role or provide credentials
      AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
      AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
      RUST_LOG: info
    restart: unless-stopped
```

Deploy:

```bash
# Set AWS credentials
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret

# Start
docker-compose -f docker-compose.s3.yml up -d
```

### R2 Storage (Cloudflare)

Create `docker-compose.r2.yml`:

```yaml
version: '3.9'

services:
  otlp2parquet:
    image: ghcr.io/smithclay/otlp2parquet:latest
    ports:
      - "8080:8080"
    environment:
      STORAGE_BACKEND: r2
      R2_BUCKET: my-otlp-logs
      R2_ACCOUNT_ID: your-account-id
      R2_ACCESS_KEY_ID: ${R2_ACCESS_KEY_ID}
      R2_SECRET_ACCESS_KEY: ${R2_SECRET_ACCESS_KEY}
      RUST_LOG: info
    restart: unless-stopped
```

## Build from Source

```bash
# Clone the repository
git clone https://github.com/smithclay/otlp2parquet.git
cd otlp2parquet

# Build the image
docker build -t otlp2parquet:local .

# Run locally built image
docker run -d -p 8080:8080 \
  -e STORAGE_BACKEND=filesystem \
  -e FILESYSTEM_ROOT=/data \
  -v otlp-data:/data \
  otlp2parquet:local
```

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `STORAGE_BACKEND` | Yes | `filesystem` | Storage backend: `filesystem`, `s3`, or `r2` |
| `HTTP_PORT` | No | `8080` | HTTP server port |
| `HTTP_HOST` | No | `0.0.0.0` | HTTP server bind address |
| `RUST_LOG` | No | `info` | Log level: `trace`, `debug`, `info`, `warn`, `error` |

### Filesystem Backend

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `FILESYSTEM_ROOT` | Yes | `/data` | Root directory for Parquet files |

### S3 Backend

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `S3_BUCKET` | Yes | - | S3 bucket name |
| `S3_REGION` | Yes | - | AWS region (e.g., `us-east-1`) |
| `AWS_ACCESS_KEY_ID` | No* | - | AWS access key (*or use IAM role) |
| `AWS_SECRET_ACCESS_KEY` | No* | - | AWS secret key (*or use IAM role) |

### R2 Backend (Cloudflare)

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `R2_BUCKET` | Yes | - | R2 bucket name |
| `R2_ACCOUNT_ID` | Yes | - | Cloudflare account ID |
| `R2_ACCESS_KEY_ID` | Yes | - | R2 access key ID |
| `R2_SECRET_ACCESS_KEY` | Yes | - | R2 secret access key |

## Kubernetes Deployment

See [kubernetes/](./kubernetes/) directory for example manifests:

- `deployment.yaml` - Deployment with configurable replicas
- `service.yaml` - Service exposing port 8080
- `configmap.yaml` - Environment configuration
- `secret.yaml` - S3/R2 credentials

```bash
# Apply Kubernetes manifests
kubectl apply -f deploy/docker/kubernetes/

# Check status
kubectl get pods -l app=otlp2parquet

# View logs
kubectl logs -l app=otlp2parquet -f
```

## Health Checks

The Docker image includes a health check that runs every 30 seconds:

```bash
# Check container health
docker inspect --format='{{.State.Health.Status}}' otlp2parquet

# Manual health check
docker exec otlp2parquet /usr/local/bin/otlp2parquet --health-check
```

## Multi-Architecture Support

Images are built for both `amd64` and `arm64` architectures:

```bash
# Automatically pulls correct architecture
docker pull ghcr.io/smithclay/otlp2parquet:latest

# Explicitly specify architecture
docker pull --platform linux/arm64 ghcr.io/smithclay/otlp2parquet:latest
docker pull --platform linux/amd64 ghcr.io/smithclay/otlp2parquet:latest
```

## Troubleshooting

### Container fails to start

```bash
# Check logs
docker logs otlp2parquet

# Common issues:
# 1. Invalid storage backend configuration
# 2. Missing required environment variables
# 3. Port 8080 already in use
```

### Storage backend errors

```bash
# Filesystem: Check volume permissions
docker exec otlp2parquet ls -la /data

# S3: Verify credentials and bucket exists
docker exec otlp2parquet env | grep AWS

# R2: Verify account ID and credentials
docker exec otlp2parquet env | grep R2
```

### Performance tuning

```bash
# Increase memory limit
docker run -d -p 8080:8080 \
  --memory=1g \
  --cpus=2 \
  ghcr.io/smithclay/otlp2parquet:latest

# Adjust log level
docker run -d -p 8080:8080 \
  -e RUST_LOG=warn \
  ghcr.io/smithclay/otlp2parquet:latest
```

## Production Considerations

1. **Use a reverse proxy** (nginx, Traefik) for TLS termination
2. **Configure resource limits** (`--memory`, `--cpus`)
3. **Enable log aggregation** (FluentBit, Promtail)
4. **Monitor health checks** via orchestrator
5. **Use IAM roles** instead of access keys when possible
6. **Set appropriate `RUST_LOG` level** (default: `info`)

## Next Steps

- [Kubernetes examples](./kubernetes/)
- [AWS ECS deployment](./ecs/)
- [Monitoring with Prometheus](./monitoring/)
- [Main documentation](../../README.md)
