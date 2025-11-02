# Quickstart

This guide will help you get `otlp2parquet` up and running quickly on your preferred platform.

## Choose your platform:

### Docker (Local Development)

```bash
docker-compose up
```

MinIO console: http://localhost:9001 (minioadmin/minioadmin)

For more details, see the [Docker Deployment Guide](deployment/docker.md).

### Cloudflare Workers (Free Tier)

[![Deploy to Cloudflare Workers](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/smithclay/otlp2parquet)

Or: `cd crates/otlp2parquet-cloudflare && wrangler deploy`

For more details, see the [Cloudflare Workers Deployment Guide](deployment/cloudflare.md).

### AWS Lambda (Serverless)

```bash
cd crates/otlp2parquet-lambda
sam deploy --guided
```

For more details, see the [AWS Lambda Deployment Guide](deployment/aws-lambda.md).
