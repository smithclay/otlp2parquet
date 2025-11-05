# Storage & Querying

`otlp2parquet` offers two layers for storing and managing your observability data:

1.  **Object Storage (Parquet Files)**: The foundational layer where your OTLP data is written as highly-compressed Parquet files. This is the most cost-effective way to store large volumes of telemetry data.
2.  **Table Format (Apache Iceberg)**: An optional, higher-level abstraction that organizes your Parquet files into queryable tables with ACID transactions, schema evolution, and performance optimizations.

| Layer | Description | Best For |
| :--- | :--- | :--- |
| [**Object Storage**](object-storage.md) | Raw Parquet files organized by date in an object store (S3, R2, etc.). | Cost-effective long-term storage, ad-hoc analysis, or when you don't need a formal table structure. |
| [**Apache Iceberg**](iceberg.md) | A formal table layer on top of your Parquet files, managed by a central catalog. | Production data lakes, BI tool integration, and when you need reliable, transactional table management. |

## How They Work Together

You can use either layer independently or combine them:

*   **Object Storage Only**: `otlp2parquet` writes Parquet files to your bucket. You query them directly by scanning file paths.
*   **Object Storage + Iceberg**: `otlp2parquet` writes Parquet files and then commits them to an Iceberg table in the same transaction. Your query engine (like DuckDB, Spark, or Trino) uses the Iceberg catalog to find the right files, which is faster and more efficient.

Most production setups will benefit from using the **Apache Iceberg** layer for its management and performance features.
