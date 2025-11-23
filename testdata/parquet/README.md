# Test Parquet Files

These Parquet files are generated from the OTLP protobuf test data in the parent directory.

## Generation

To regenerate these files:

```bash
cd examples/generate-parquet-testdata
cargo run
```

## Files

- **logs.parquet** - OpenTelemetry logs in Parquet format (81 rows)
- **metrics_gauge.parquet** - Gauge metrics (3 rows)
- **metrics_sum.parquet** - Sum/counter metrics (4 rows)
- **metrics_histogram.parquet** - Histogram metrics (3 rows)
- **metrics_exponential_histogram.parquet** - Exponential histogram metrics (2 rows)
- **metrics_summary.parquet** - Summary metrics (2 rows)
- **traces.parquet** - OpenTelemetry traces (19 rows)

## Schema

All files use the ClickHouse-compatible PascalCase schema defined in `otlp2parquet-core`:

- Logs: `crates/otlp2parquet-core/src/schema/logs.rs`
- Metrics: `crates/otlp2parquet-core/src/schema/metrics.rs` (separate schemas per metric type)
- Traces: `crates/otlp2parquet-core/src/schema/traces.rs`

## Compression

All files use Snappy compression for optimal balance between size and read performance.
