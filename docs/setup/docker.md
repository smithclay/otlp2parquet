# Docker Setup

This guide shows how to run `otlp2parquet` using Docker and Docker Compose.

## Use Cases

This setup is a good fit for:
*   Local development and testing.
*   Self-hosting in a private cloud or on-premises.
*   Deployments where you prefer to manage your own infrastructure.

## Prerequisites

*   Docker and Docker Compose

## Quick Start

The quickest way to start is with Docker Compose, which runs `otlp2parquet` alongside a MinIO container for S3-compatible storage.

1.  **Start the Services**: This command builds the containers and starts the server and storage.
    ```bash
    docker-compose up
    ```
    This starts an `otlp2parquet` server on port `4318` and a MinIO container.

2.  **Send Test Data**: Send a test request to the local server.
    ```bash
    curl -X POST http://localhost:4318/v1/logs \
      -H "Content-Type: application/x-protobuf" \
      --data-binary @testdata/logs.pb
    ```

3.  **Verify the Output**: Check that the Parquet file was written to MinIO by opening the MinIO Console at `http://localhost:9001` (login: `minioadmin`/`minioadmin`).

## Configuration

The Docker container is configured via environment variables or a TOML file. See the [Configuration Guide](../concepts/configuration.md) for a complete overview.

## Common Tasks

*   **View Logs**: `docker-compose logs -f otlp2parquet`
*   **Reset Data**: `docker-compose down -v`
*   **Build from Source**: `docker-compose up --build`

## Production Considerations

For production, run the same Docker container but configure it to use a managed cloud storage service like AWS S3 or Cloudflare R2. Pass environment variables to `docker-compose` to override the defaults.

**Example for AWS S3:**
```bash
OTLP2PARQUET_STORAGE_BACKEND=s3 \
OTLP2PARQUET_S3_BUCKET=my-production-s3-bucket \
OTLP2PARQUET_S3_REGION=us-east-1 \
AWS_ACCESS_KEY_ID=<your-aws-key> \
AWS_SECRET_ACCESS_KEY=<your-aws-secret> \
docker-compose up
```

## Troubleshooting

*   **Port Conflicts**: If another service is using port `4318`, `9000`, or `9001`, change the `ports` mapping in the `docker-compose.yml` file.
*   **"Access Denied" to S3/R2**: Ensure your `AWS_` or `OTLP2PARQUET_R2_` environment variables (access keys, secrets, region) are correctly set and passed to the container.

## Next Steps

*   [**Sending Data**](../guides/sending-data.md)
*   [**Configuration Concepts**](../concepts/configuration.md)
