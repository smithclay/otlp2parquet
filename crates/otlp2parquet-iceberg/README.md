# otlp2parquet-iceberg

Apache Iceberg catalog integration via REST API.

## Status: MVP Complete

This crate provides Iceberg REST catalog integration for committing Parquet files written by `otlp2parquet-storage` into Iceberg tables. Works with any Iceberg REST catalog including AWS S3 Tables, AWS Glue Data Catalog, Tabular, Polaris, and others.

### Features

- ✅ Generic Iceberg REST catalog integration (not S3 Tables specific)
- ✅ Configuration loading from environment variables
- ✅ Transaction-based commits with ACID guarantees
- ✅ Arrow to Iceberg schema conversion with field ID preservation
- ✅ DataFile metadata extraction from Parquet (column stats, bounds, row counts)
- ✅ Field ID mapping from Arrow metadata (`PARQUET:field_id`)
- ✅ Unpartitioned tables (MVP - partitioning planned for future)
- ✅ Warn-and-succeed error handling (catalog failures don't fail ingestion)
- ✅ Integrated into Lambda and Server runtimes

### Why Git Dependency?

The `iceberg` and `iceberg-catalog-rest` crates use a git dependency because we need features from the latest `main` branch that aren't yet published to crates.io. Once the next version is published, we'll switch to a stable crates.io version.

### Architecture

This crate is **only** used by Lambda and Server runtimes - not Cloudflare Workers. This keeps the WASM binary size minimal.

```
otlp2parquet-storage (Parquet writing, OpenDAL)
         ↓
otlp2parquet-iceberg (REST catalog) ← THIS CRATE
         ↓
    ┌────┴────┐
    ↓         ↓
 lambda     server

(Not used by Cloudflare Workers to keep WASM binary small)
```

## Configuration

Set these environment variables (following the `OTLP2PARQUET_` prefix convention):

```bash
# Required
OTLP2PARQUET_ICEBERG_REST_URI="https://s3tables.us-east-1.amazonaws.com/iceberg"

# Optional - Table names per signal type (defaults shown)
# Each signal type writes to its own table for optimal query performance
OTLP2PARQUET_ICEBERG_TABLE_LOGS="otel_logs"                                          # Logs table
OTLP2PARQUET_ICEBERG_TABLE_TRACES="otel_traces"                                      # Traces table
OTLP2PARQUET_ICEBERG_TABLE_METRICS_GAUGE="otel_metrics_gauge"                        # Gauge metrics
OTLP2PARQUET_ICEBERG_TABLE_METRICS_SUM="otel_metrics_sum"                            # Sum metrics
OTLP2PARQUET_ICEBERG_TABLE_METRICS_HISTOGRAM="otel_metrics_histogram"                # Histogram metrics
OTLP2PARQUET_ICEBERG_TABLE_METRICS_EXPONENTIAL_HISTOGRAM="otel_metrics_exponential_histogram"  # Exponential histogram metrics
OTLP2PARQUET_ICEBERG_TABLE_METRICS_SUMMARY="otel_metrics_summary"                    # Summary metrics

# Optional - Catalog settings
OTLP2PARQUET_ICEBERG_WAREHOUSE="s3://my-warehouse"           # Warehouse location
OTLP2PARQUET_ICEBERG_NAMESPACE="otel.production"             # Dot-separated namespace
OTLP2PARQUET_ICEBERG_CATALOG_NAME="rest"                     # Catalog name (default: "rest")
OTLP2PARQUET_ICEBERG_STAGING_PREFIX="data/incoming"          # Staging prefix (default: "data/incoming")
OTLP2PARQUET_ICEBERG_TARGET_FILE_SIZE_BYTES="536870912"      # Target file size (default: 512MB)
```

### Multi-Table Architecture

The integration writes each signal type to a dedicated table:
- **Logs** → `otel_logs` (default)
- **Traces** → `otel_traces` (default)
- **Metrics** → 5 separate tables by metric type:
  - Gauge → `otel_metrics_gauge`
  - Sum → `otel_metrics_sum`
  - Histogram → `otel_metrics_histogram`
  - Exponential Histogram → `otel_metrics_exponential_histogram`
  - Summary → `otel_metrics_summary`

This provides schema homogeneity within each table for optimal query performance and allows independent schema evolution per signal type.

## Usage

The Lambda and Server runtimes automatically initialize the Iceberg committer if the required environment variables are set. No code changes needed - just configure the environment variables.

### Manual Usage Example

```rust
use otlp2parquet_iceberg::{
    create_rest_catalog, IcebergCommitter, IcebergTableIdentifier, IcebergRestConfig,
};
use std::collections::HashMap;

// Load config from environment
let config = IcebergRestConfig::from_env()?;

// Create REST catalog
let catalog = create_rest_catalog(&config).await?;

// Build table map from config
let namespace = config.namespace_ident()?;
let mut tables = HashMap::new();
for (signal_key, table_name) in &config.tables {
    let table_ident = IcebergTableIdentifier::new(
        namespace.clone(),
        table_name.clone(),
    );
    tables.insert(signal_key.clone(), table_ident);
}

// Create committer
let committer = IcebergCommitter::new(catalog, tables, config);

// Commit Parquet files (writes already in S3/storage)
// For logs:
committer.commit_with_signal("logs", None, &parquet_results).await?;

// For traces:
committer.commit_with_signal("traces", None, &parquet_results).await?;

// For metrics (specify metric type):
committer.commit_with_signal("metrics", Some("gauge"), &parquet_results).await?;
```

### Error Handling

The integration uses a **warn-and-succeed** pattern:
- If Iceberg is not configured, the runtime continues without catalog integration
- If catalog operations fail, errors are logged as warnings but requests succeed
- Parquet files are always written to storage (S3/R2/FS) regardless of catalog status

This ensures that ingestion is never blocked by catalog availability issues.

## Catalog Compatibility

This crate uses the generic Iceberg REST catalog API, so it works with:

- **AWS S3 Tables**: `https://s3tables.<region>.amazonaws.com/iceberg`
- **AWS Glue Data Catalog**: `https://glue.<region>.amazonaws.com/iceberg`
- **Tabular**: `https://<org>.tabular.io/ws`
- **Polaris**: Self-hosted or Snowflake Polaris
- **Any REST-compatible catalog**: Following the Iceberg REST spec

## Future Enhancements

- **Partitioning**: Time-based partitioning (year/month/day/hour) to match storage layout
- **Schema Evolution**: Automatic schema updates when new fields are added
- **Table Creation**: Automatic table creation if it doesn't exist
- **Statistics**: More detailed Parquet statistics conversion to Iceberg Datum types
- **Compaction**: Integration with Iceberg compaction for optimizing file sizes

## References

- [Apache Iceberg Rust](https://github.com/apache/iceberg-rust)
- [Iceberg REST Catalog Spec](https://iceberg.apache.org/docs/latest/rest-catalog/)
- [AWS S3 Tables Documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-buckets.html)
