# otlp2parquet-iceberg

Apache Iceberg catalog integration for S3 Tables.

## Status: Work in Progress

This crate provides S3 Tables (Iceberg) catalog integration for committing Parquet files written by `otlp2parquet-storage` into Iceberg tables.

**Current Status**: Scaffolding complete, pending API stabilization.

### What's Implemented

- ✅ Configuration loading from environment variables
- ✅ S3 Tables catalog builder using official `iceberg-catalog-s3tables` crate
- ✅ DataFile metadata extraction from Parquet files (column stats, bounds, split offsets)
- ✅ IcebergCommitter structure for transaction-based commits

### Pending Work

- ⏳ **API Compatibility**: The `iceberg-catalog-s3tables` crate is not yet published to crates.io and the git version has unstable APIs
- ⏳ **Type Conversions**: Need to convert Parquet statistics to Iceberg `Datum` types
- ⏳ **Table Operations**: Update to match latest `Table` transaction API
- ⏳ **Testing**: Integration tests with S3 Tables

### Why Git Dependency?

The `iceberg-catalog-s3tables` crate exists in the Apache Iceberg Rust project but hasn't been published to crates.io yet. We're using a git dependency as a placeholder. Once published, we'll switch to a stable version.

### Architecture

This crate is **only** used by Lambda and Server runtimes - not Cloudflare Workers. This keeps the WASM binary size minimal.

```
otlp2parquet-storage (Parquet writing, OpenDAL)
         ↓
otlp2parquet-iceberg (S3 Tables catalog) ← THIS CRATE
         ↓
    ┌────┴────┐
    ↓         ↓
 lambda     server
```

## Configuration

Set these environment variables (following the `OTLP2PARQUET_` prefix convention):

```bash
# Required
OTLP2PARQUET_ICEBERG_REST_URI="https://s3tables.us-east-1.amazonaws.com/iceberg"
OTLP2PARQUET_ICEBERG_TABLE="otel_logs"

# Optional
OTLP2PARQUET_ICEBERG_WAREHOUSE="s3://my-warehouse"  # Warehouse location
OTLP2PARQUET_ICEBERG_NAMESPACE="otel.production"  # Dot-separated namespace
OTLP2PARQUET_ICEBERG_CATALOG_NAME="rest"  # Catalog name (default: "rest")
OTLP2PARQUET_ICEBERG_STAGING_PREFIX="data/incoming"  # Staging prefix (default: "data/incoming")
OTLP2PARQUET_ICEBERG_TARGET_FILE_SIZE_BYTES="536870912"  # Target file size (default: 512MB)
```

## Usage (Planned)

```rust
use otlp2parquet_iceberg::{
    create_s3tables_catalog, IcebergCommitter, IcebergTableIdentifier, S3TablesConfig,
};

// Load config from environment
let config = S3TablesConfig::from_env()?;

// Create catalog
let catalog = create_s3tables_catalog(&config).await?;

// Create table identifier
let table_ident = IcebergTableIdentifier::new(
    config.namespace_ident()?,
    config.table.clone(),
);

// Create committer
let committer = IcebergCommitter::new(catalog, table_ident, config);

// Commit Parquet files
committer.commit(&parquet_results).await?;
```

## References

- [S3 Tables Documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-buckets.html)
- [Apache Iceberg Rust](https://github.com/apache/iceberg-rust)
- [S3 Tables Iceberg REST API](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-open-source.html)
