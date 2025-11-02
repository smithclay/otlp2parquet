# Docker Deployment Guide

This guide provides a focused workflow for running `otlp2parquet` using Docker and Docker Compose, covering both local development and production-like scenarios.

## Prerequisites

*   **Docker and Docker Compose**: Ensure you have both installed and running on your system.

## 1. Local Development & Testing (with MinIO)

The recommended setup for local development uses Docker Compose to spin up the `otlp2parquet` service alongside a MinIO container for S3-compatible object storage.

### Workflow

1.  **Start the Services**:

    ```bash
    # This command builds the containers and starts both services.
    docker-compose up
    ```

    This includes:
    *   `otlp2parquet` HTTP server running on port `4318`.
    *   MinIO S3-compatible storage with an API on port `9000` and a web console on `9001`.
    *   An auto-created S3 bucket named `otlp-logs`.

2.  **Test the Service**:

    Send a test request to the local `otlp2parquet` container.

    ```bash
    curl -X POST http://localhost:4318/v1/logs \
      -H "Content-Type: application/x-protobuf" \
      --data-binary @testdata/logs.pb
    ```

3.  **Verify the Output**:

    You can verify that the Parquet file was created in two ways:

    *   **MinIO Console**: Open [http://localhost:9001](http://localhost:9001) in your browser and log in with `minioadmin`/`minioadmin` to see the `otlp-logs` bucket.
    *   **AWS CLI**: Use the AWS CLI configured for a local endpoint.

        ```bash
        aws s3 ls s3://otlp-logs/logs/ --recursive \
          --endpoint-url http://localhost:9000
        ```

### Managing the Local Environment

*   **View Logs**: Tail the logs from the running container.

    ```bash
    docker-compose logs -f otlp2parquet
    ```

*   **Reset Data**: To start fresh, take down the services and remove the data volume.

    ```bash
    docker-compose down -v
    ```

## 2. Production Deployment (Connecting to Cloud Storage)

For a production environment, you can run the same Docker container but configure it to connect to a managed cloud storage service like AWS S3 or Cloudflare R2.

This is done by overriding the default environment variables in the `docker-compose.yml` file.

**Example for AWS S3:**

```bash
OTLP2PARQUET_STORAGE_BACKEND=s3 \
OTLP2PARQUET_S3_BUCKET=my-production-s3-bucket \
OTLP2PARQUET_S3_REGION=us-east-1 \
AWS_ACCESS_KEY_ID=<your-aws-key> \
AWS_SECRET_ACCESS_KEY=<your-aws-secret> \
docker-compose up
```

**Example for Cloudflare R2:**

```bash
OTLP2PARQUET_STORAGE_BACKEND=r2 \
OTLP2PARQUET_R2_BUCKET=my-production-r2-bucket \
OTLP2PARQUET_R2_ACCOUNT_ID=<your-cf-account-id> \
OTLP2PARQUET_R2_ACCESS_KEY_ID=<your-r2-key> \
OTLP2PARQUET_R2_SECRET_ACCESS_KEY=<your-r2-secret> \
docker-compose up
```

### Building from Source

To build the Docker image from the latest source code instead of using a pre-built image, use the `--build` flag.

```bash
docker-compose up --build
```

## 3. Configuration

The Docker container is configured entirely through environment variables. Below are some of the key variables.

| Variable | Default | Description |
|----------|---------|-------------|
| `OTLP2PARQUET_STORAGE_BACKEND` | `fs` | Storage backend: `fs`, `s3`, or `r2`. |
| `OTLP2PARQUET_S3_BUCKET` | `otlp-logs` | The name of the S3 or R2 bucket. |
| `OTLP2PARQUET_S3_ENDPOINT` | - | Custom S3 endpoint for S3-compatible services like MinIO. |
| `OTLP2PARQUET_LISTEN_ADDR` | `0.0.0.0:4318` | The listen address for the HTTP server. |
| `OTLP2PARQUET_LOG_LEVEL` | `info` | Log level: `trace`, `debug`, `info`, `warn`, `error`. |

## 4. Kubernetes

For Kubernetes deployment, please refer to the provided [Kubernetes manifests](../../deploy/docker/kubernetes/) for detailed instructions and examples.

```
