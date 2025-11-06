# Object Storage (Parquet)

This is the default storage layer for `otlp2parquet`. It writes your OpenTelemetry data into compressed Parquet files and organizes them in a simple, date-based hierarchy within your chosen object storage provider.

## Key Features

*   **Cost-Effective**: Stores data in the cheapest possible object storage tier.
*   **Highly Compressed**: Uses Parquet, Snappy compression, and dictionary encoding to dramatically reduce storage size (often 10-50x smaller than JSON).
*   **Portable**: Parquet is a universal, open format readable by a vast ecosystem of tools.
*   **Simple**: Files are organized in a predictable path structure that is easy to navigate manually or programmatically.

## How It Works

When `otlp2parquet` receives a batch of data, it:

1.  Converts the OTLP data into an Arrow RecordBatch.
2.  Compresses and writes the data to a Parquet file in a temporary location.
3.  Uploads the file to your object storage bucket with a unique name.

### File Path Structure

Files are partitioned by signal type, date, and a unique file ID. This makes it easy to scope queries to specific time ranges.

```
s3://<your-bucket>/<signal>/YYYY/MM/DD/HH/<uuid>.parquet
```

**Example:**

```
s3://my-telemetry-data/logs/2024/11/04/15/a1b2c3d4-e5f6-7890-1234-567890abcdef.parquet
```

This structure allows query engines like DuckDB to perform "partition pruning," where it only scans the folders that match your query's `WHERE` clause, dramatically speeding up queries.

## Configuration

Object storage is configured in your `config.toml` or via environment variables. You must choose a backend (`fs`, `s3`, or `r2`) and provide the necessary credentials and bucket information.

**Example for AWS S3:**

```toml
[storage]
backend = "s3"

[storage.s3]
bucket = "my-otlp-bucket"
region = "us-east-1"
```

See the [Configuration Guide](../configuration.md) for more details.

## Querying

To query data at this layer, you use a query engine that can read Parquet files directly from object storage. You typically use a glob pattern to select all files within a certain path.

**Example with DuckDB:**

```sql
-- Query all logs from a specific day
SELECT service_name, body
FROM read_parquet('s3://my-telemetry-data/logs/2024/11/04/**/*.parquet');
```

While simple, this approach can be slow if you have thousands of files. For better performance, we recommend using the [Apache Iceberg](iceberg.md) table format layer.
