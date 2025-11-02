# Quickstart

Get `otlp2parquet` running on your preferred platform.

## Docker

This is the recommended setup for local development. It uses Docker Compose and MinIO to simulate a complete environment on your machine.

```bash
docker-compose up
```

After starting, the MinIO console is available at [http://localhost:9001](http://localhost:9001) (`minioadmin`/`minioadmin`).

For more details, see the [Docker Deployment Guide](deployment/docker.md).

## Cloudflare Workers

> **Note**: Due to CPU limitations on the free tier, running this Worker requires a paid Cloudflare plan.

[![Deploy to Cloudflare Workers](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/smithclay/otlp2parquet)

Alternatively, you can deploy manually from the command line:

```bash
cd crates/otlp2parquet-cloudflare && wrangler deploy
```

For more details, see the [Cloudflare Workers Deployment Guide](deployment/cloudflare.md).

## AWS Lambda

This method deploys the function to your AWS account using the AWS SAM CLI.

```bash
cd crates/otlp2parquet-lambda
sam deploy --guided
```

For more details, see the [AWS Lambda Deployment Guide](deployment/aws-lambda.md).
