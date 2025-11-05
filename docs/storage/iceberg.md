# Table Format (Apache Iceberg)

Apache Iceberg is an open table format designed for huge analytic datasets. `otlp2parquet` uses it as an optional layer on top of Parquet files to provide the reliability and performance of a traditional database, while keeping your data in cheap object storage.

## Why Use Iceberg?

While querying raw Parquet files works, it has limitations. Iceberg solves these by providing:

*   **ACID Transactions**: Ensures that data is never partially written or corrupted. Readers see a consistent version of the table.
*   **Schema Evolution**: Safely add, remove, or rename columns in your table without rewriting old data.
*   **Faster Queries**: Iceberg tracks file-level statistics, allowing query engines to skip irrelevant files and read less data.
*   **Time Travel**: Query historical versions of your table down to the exact microsecond a change was made.
*   **Cross-Engine Compatibility**: Use DuckDB, Spark, Trino, Flink, and other engines to work on the same data concurrently.

For most production use cases, **we strongly recommend enabling the Iceberg layer**.

## How It Works

When Iceberg is enabled, the data writing process becomes a two-step, transactional operation:

1.  `otlp2parquet` writes Parquet files to your object storage, just like the standard [Object Storage](object-storage.md) layer.
2.  It then connects to an **Iceberg Catalog** and atomically commits the new file paths, along with their statistics, to the target table.

This commit-based approach means that query engines don't need to scan directories to find relevant files. Instead, they ask the catalog for a manifest of files for a given query, which is significantly faster.

## Supported Catalogs

`otlp2parquet` uses the standard [Iceberg REST Catalog protocol](https://iceberg.apache.org/spec/#rest-catalog-api), which is supported by a growing number of services. This allows you to choose the catalog that best fits your ecosystem.

*   **AWS Glue Catalog**
*   **AWS S3 Tables** (New, simplified offering)
*   [Tabular](https://tabular.io/)
*   [Polaris](https://project-polaris.github.io/)
*   Other REST-compliant catalogs

## Configuration

To enable Iceberg, you configure it via environment variables. This is because the catalog integration is designed to be managed by the infrastructure (Lambda, Docker, etc.) rather than static application config.

**Minimal Example for AWS S3 Tables:**

```bash
# The REST endpoint for your catalog
export OTLP2PARQUET_ICEBERG_REST_URI="https://s3tables.us-east-1.amazonaws.com/iceberg"

# The S3 bucket where your Iceberg warehouse is located
export OTLP2PARQUET_ICEBERG_WAREHOUSE="s3://my-iceberg-warehouse"

# The namespace for your OTLP tables
export OTLP2PARQUET_ICEBERG_NAMESPACE="otel.production"
```

### Configuration Details

| Variable | Description | Required |
| :--- | :--- | :--- |
| `OTLP2PARQUET_ICEBERG_REST_URI` | The HTTPS endpoint of your Iceberg REST Catalog. | **Yes** |
| `OTLP2PARQUET_ICEBERG_WAREHOUSE`| The root S3 path for your Iceberg data. | **Yes** |
| `OTLP2PARQUET_ICEBERG_NAMESPACE`| The database or namespace for your tables (e.g., `otel.prod`). | No |
| `OTLP2PARQUET_ICEBERG_TABLE_*` | Overrides the default table names (e.g., `OTLP2PARQUET_ICEBERG_TABLE_LOGS`). | No |

### Error Handling

The Iceberg integration is designed to be **resilient**. If the catalog commit fails for any reason, `otlp2parquet` will log a warning but **will not fail the request**. The Parquet files will still be written to object storage. This ensures that your data ingestion pipeline is never blocked by a temporary catalog outage.

## Querying

Querying an Iceberg table is simpler and faster than scanning for files. You query the table name directly, and the engine uses the catalog to handle the rest.

**Example with DuckDB:**

```sql
-- Assumes DuckDB is configured to use an Iceberg catalog
SELECT service_name, body
FROM otel_logs
WHERE timestamp > now() - interval '1 hour';
```
