# Deploy

Run otlp2parquet locally or self-host it with Docker. The same server binary can write to local disk or S3-compatible object storage.

## Prerequisites

Install the CLI:

```bash
cargo install otlp2parquet
```

Or download from [GitHub Releases](https://github.com/smithclay/otlp2parquet/releases).

---

## Docker (Local / Self-Hosted)

Run locally for development or self-host in your infrastructure.

### Quick Start

```bash
docker-compose up
```

This starts:
- **otlp2parquet** on port 4318
- **MinIO** (S3-compatible storage) on ports 9000/9001

### Send test data

```bash
curl -X POST http://localhost:4318/v1/logs \
  -H "Content-Type: application/json" \
  -d '{"resourceLogs":[{"resource":{},"scopeLogs":[{"scope":{},"logRecords":[{"body":{"stringValue":"Hello"}}]}]}]}'
```

### Verify output

Open MinIO Console at `http://localhost:9001` (login: `minioadmin`/`minioadmin`).

??? note "Production with object storage"
    Point Docker at any S3-compatible storage:
    ```bash
    OTLP2PARQUET_STORAGE_BACKEND=s3 \
    OTLP2PARQUET_S3_BUCKET=my-prod-bucket \
    OTLP2PARQUET_S3_REGION=us-west-2 \
    OTLP2PARQUET_S3_ENDPOINT=https://object.example.com \
    docker-compose up
    ```

??? note "Common commands"
    - View logs: `docker-compose logs -f otlp2parquet`
    - Reset data: `docker-compose down -v`
    - Rebuild: `docker-compose up --build`

---

## Local Binary

Run the server directly:

```bash
otlp2parquet --config config.toml
```

Use `config.example.toml` as a starting point and customize the storage section for your environment.
