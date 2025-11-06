# Querying Your Data

`otlp2parquet` produces standard Parquet files (optionally with Apache Iceberg support) that can be queried by a wide range of tools. The best way to query your data depends on which storage layer you are using.

For a detailed explanation of the storage layers and how to query them, please see the [Storage & Querying](../storage/index.md) section.

## Recommended Tool: DuckDB

[DuckDB](https://duckdb.org/) is a fast, in-process analytical database that is exceptionally well-suited for querying Parquet files directly from object storage. It is the recommended tool for most use cases.

All examples in our documentation use DuckDB. You can try out querying examples directly in the browser at [https://smithclay.github.io/otlp2parquet/query-demo/](https://smithclay.github.io/otlp2parquet/query-demo/).

## Querying Guides

*   **[Querying Parquet Files Directly](../storage/object-storage/#querying)**
    *   Choose this if you are using the basic object storage layer without a table format.

*   **[Querying via an Iceberg Catalog](../storage/iceberg/#querying)**
    *   Choose this if you have enabled the Apache Iceberg table layer for better performance and management.
