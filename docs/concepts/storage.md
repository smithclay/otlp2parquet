# Storage Layers

This guide explains how `otlp2parquet` stores data.

`otlp2parquet` uses a two-layer model for data storage. The foundational layer is plain Parquet files in object storage. An optional, higher-level table format layer using Apache Iceberg can be added for better management and query performance.

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

When Iceberg is enabled, writing data becomes a two-step, transactional operation:
1.  `otlp2parquet` writes a Parquet file to object storage.
2.  It then connects to an **Iceberg Catalog** and atomically commits the new file's path and statistics to the target table.

Query engines ask the catalog for a manifest of files, which is significantly faster than scanning directories.

### Supported Catalogs

`otlp2parquet` supports the standard [Iceberg REST Catalog protocol](https://iceberg.apache.org/spec/#rest-catalog-api), including:

*   AWS Glue Catalog
*   AWS S3 Tables
*   Tabular
*   Polaris

### Querying Iceberg Tables

Querying an Iceberg table is simpler and faster. You query the table name, and the engine uses the catalog to find the data.

**Example with DuckDB:**
```sql
-- Assumes DuckDB is configured to use an Iceberg catalog
SELECT service_name, body
FROM otel_logs
WHERE timestamp > now() - interval '1 hour';
```
This avoids slow file scanning and provides immediate, consistent results. For configuration details, see the [Configuration guide](configuration.md).
