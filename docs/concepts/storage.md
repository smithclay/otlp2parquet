# Storage Layers

This guide explains how `otlp2parquet` stores data.

`otlp2parquet` uses a two-layer model for data storage via the [icepick](https://crates.io/crates/icepick) library. The foundational layer is plain Parquet files in object storage. An optional, higher-level table format layer using Apache Iceberg can be added for better management and query performance.

| Layer | Description | Best For |
| :--- | :--- | :--- |
| [**Plain Parquet Files**](#plain-parquet-files) | Raw Parquet files organized by date in an object store (S3, R2, etc.). | Simple archiving, ad-hoc analysis, or when a formal table structure is not needed. |
| [**Apache Iceberg Tables**](#apache-iceberg-tables) | A transactional table layer on top of Parquet files, managed by a central catalog. | Production data lakes, BI tool integration, and for reliable, high-performance querying. |

---

## Plain Parquet Files

This is the default storage layer. It writes telemetry data into compressed Parquet files and organizes them in a simple, date-based hierarchy in your object storage provider.

### Key Features

*   **Cost-Effective**: Stores data in the cheapest possible object storage tier.
*   **Highly Compressed**: Uses Parquet, Snappy compression, and dictionary encoding to dramatically reduce storage size—often 10-50x smaller than JSON.
*   **Portable**: Parquet is a universal, open format readable by a vast ecosystem of tools.
*   **Simple**: Files are organized in a predictable path structure that is easy to navigate.

### File Path Structure

Files are partitioned by signal type and date, allowing query engines to prune data by scanning only relevant folders.

```
s3://<your-bucket>/<signal>/YYYY/MM/DD/HH/<uuid>.parquet
```

**Example:**
```
s3://my-telemetry-data/logs/2024/11/04/15/a1b2c3d4-e5f6-7890-1234-567890abcdef.parquet
```

### Querying Plain Parquet

To query this layer, use a tool that can read Parquet files from object storage. You typically use a glob pattern to select all files within a time range.

**Example with DuckDB:**
```sql
-- Query all logs from a specific day
SELECT service_name, body
FROM read_parquet('s3://my-telemetry-data/logs/2024/11/04/**/*.parquet');
```
While simple, this can be slow if you have thousands of files. For better performance, use the Apache Iceberg layer.

---

## Apache Iceberg Tables

Apache Iceberg is an open table format for huge analytic datasets. `otlp2parquet` uses it to provide the reliability and performance of a database while keeping your data in cheap object storage.

**For most production use cases, we strongly recommend enabling the Iceberg layer.**

### Why Use Iceberg?

*   **ACID Transactions**: Guarantees that data is never partially written. Readers always see a consistent version of the table.
*   **Schema Evolution**: Safely add, remove, or rename columns without rewriting old data.
*   **Faster Queries**: Tracks file-level statistics, allowing query engines to skip irrelevant files and read less data.
*   **Time Travel**: Query historical versions of your table.
*   **Cross-Engine Compatibility**: Use DuckDB, Spark, Trino, and other engines on the same data concurrently.

### How It Works

When Iceberg is enabled, writing data becomes a two-step operation handled by icepick:

1.  **Write Parquet File**: icepick converts Arrow RecordBatch to Parquet and writes to object storage
2.  **Commit to Catalog**: icepick atomically commits the new file's metadata to the Iceberg catalog

**Warn-and-Succeed Pattern:**
- Catalog operations are non-blocking
- If catalog commit fails, a warning is logged but the Parquet file is still written
- Data durability is prioritized over catalog consistency
- Ensures data is never lost even if catalog is temporarily unavailable

Query engines ask the catalog for a manifest of files, which is significantly faster than scanning directories.

### Supported Catalogs

`otlp2parquet` supports multiple Iceberg catalog types via icepick:

| Catalog | Platforms | Type | Use Case |
|---------|-----------|------|----------|
| **S3 Tables** | Lambda only | AWS-managed | Simplified ARN-based config, automatic compaction |
| **R2 Data Catalog** | Cloudflare only | Edge-native | WASM-compatible, zero egress fees |
| **Nessie** | Server only | Self-hosted | Git-like version control, Docker-friendly |
| **AWS Glue** | Server only | AWS-managed | REST catalog for server deployments |

For detailed setup and configuration of each catalog type, see the [Catalog Types Reference](../reference/catalog-types.md).

### Querying Iceberg Tables

Querying an Iceberg table is simpler and faster. You query the table name, and the engine uses the catalog to find the data.

**Example with DuckDB:**
```sql
-- Assumes DuckDB is configured to use an Iceberg catalog
SELECT service_name, body
FROM otel_logs
WHERE timestamp > now() - interval '1 hour';
```
This avoids slow file scanning and provides immediate, consistent results.

---

## Storage Backends (via OpenDAL)

The icepick library uses [Apache OpenDAL](https://opendal.apache.org/) to provide a unified abstraction over different object storage backends.

### Supported Backends

| Backend | Platforms | Use Case |
|---------|-----------|----------|
| **S3** | Lambda, Server | AWS deployments, broad ecosystem support |
| **R2** | Cloudflare, Server | Zero egress fees, edge-optimized |
| **Filesystem** | Server only | Local development, testing |
| **GCS** | Server only | Google Cloud deployments |
| **Azure Blob** | Server only | Azure deployments |

### Platform Constraints

- **Lambda**: S3 only (event-driven architecture)
- **Cloudflare Workers**: R2 only (WASM compatibility)
- **Server**: All backends supported (configurable)

### Configuration

Storage backend is configured via `OTLP2PARQUET_STORAGE_BACKEND` environment variable or `config.toml`:

```toml
[storage]
backend = "s3"

[storage.s3]
bucket = "my-data-bucket"
region = "us-west-2"
endpoint = "https://s3.amazonaws.com"  # Optional, for custom endpoints
```

For complete configuration reference, see [Environment Variables Reference](../reference/environment-variables.md).
