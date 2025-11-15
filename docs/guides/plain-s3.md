# Guide: Plain S3 Storage

This guide shows how to configure `otlp2parquet` to write Parquet files directly to an S3 bucket without using the Apache Iceberg table format.

## Use Case

This approach is simpler than a full Iceberg setup and is a good choice for:
*   Simple data archival.
*   Environments where a table catalog is not needed.
*   Getting started quickly without configuring a catalog.

For most production use cases, we recommend using the [Apache Iceberg layer](../concepts/storage.md#apache-iceberg-tables) for its performance and data management features.

## Configuration

To enable plain S3 mode, ensure that no `OTLP2PARQUET_ICEBERG_*` variables are set. Instead, provide the storage backend, bucket, and region.

**Example Environment Variables:**
```bash
export OTLP2PARQUET_STORAGE_BACKEND=s3
export OTLP2PARQUET_S3_BUCKET=my-plain-s3-bucket
export OTLP2PARQUET_S3_REGION=us-east-1
# Plus AWS credentials
export AWS_ACCESS_KEY_ID=<your-aws-key>
export AWS_SECRET_ACCESS_KEY=<your-aws-secret>
```

## File Path Structure

Without Iceberg, files are written to a simple, date-based hierarchy that enables partition pruning.

```
s3://<your-bucket>/<signal>/YYYY/MM/DD/HH/<uuid>.parquet
```

**Example:**
```
s3://my-plain-s3-bucket/logs/2024/11/04/15/a1b2c3d4-e5f6-7890-1234-567890abcdef.parquet
```

## Querying

Query the data using a tool that can read Parquet files from S3. You must scan the file paths, typically with a glob pattern.

**Example with DuckDB:**
```sql
-- Query all logs from a specific day
SELECT service_name, body
FROM read_parquet('s3://my-plain-s3-bucket/logs/2024/11/04/**/*.parquet');
```
