# S3 Tables Integrated Write Support

**Date:** 2025-11-06
**Status:** Approved
**Author:** Claude (brainstorming with user)

## Overview

Enable `otlp2parquet` to write Parquet data files directly to AWS S3 Tables with integrated Iceberg catalog commits in a single atomic operation. This eliminates the current two-bucket architecture (separate data bucket + S3 Tables catalog) in favor of a unified S3 Tables storage solution.

## Motivation

Current AWS Lambda + S3 Tables deployment uses two buckets:
- **DataBucket**: Regular S3 bucket storing Parquet files (written via S3 API)
- **TableBucket**: S3 Tables bucket storing only Iceberg metadata (written via REST API)

This architecture exists because we initially believed S3 Tables couldn't accept direct Parquet writes. However, research confirms that S3 Tables supports standard S3 operations (`PutObject`, `GetObject`) when writing to the table's warehouse location, just like Apache Spark does.

**Benefits of integrated writes:**
- Single bucket architecture (simpler, lower cost)
- Atomic write + commit operations
- Proper Iceberg table format with automatic maintenance
- On-demand table creation (no pre-defined schemas in CloudFormation)

## Architecture

### Current Flow (Two Buckets)
```
Lambda Handler
  ↓
otlp2parquet-storage (OpenDAL → DataBucket s3://data-bucket/logs/...)
  ↓
IcebergCommitter (REST API → S3 Tables catalog with file paths)
```

### New Flow (S3 Tables Integrated)
```
Lambda Handler
  ↓
IcebergWriter::write_and_commit()
  ├─→ LoadTable/CreateTable (REST API) → get warehouse location
  ├─→ OpenDAL S3 → write Parquet to warehouse location
  └─→ CommitTransaction (REST API) → register files atomically
```

### Module Reorganization

**Move `otlp2parquet-iceberg` crate into `otlp2parquet-storage`:**

```
otlp2parquet-storage/src/
  ├─ lib.rs
  ├─ opendal_storage.rs
  ├─ parquet.rs
  └─ iceberg/              # Moved from separate crate
      ├─ mod.rs            # Re-exports
      ├─ writer.rs         # NEW: IcebergWriter with write_and_commit
      ├─ catalog.rs        # Existing REST client
      ├─ arrow_convert.rs  # Existing schema conversion
      ├─ datafile_convert.rs
      ├─ http.rs
      ├─ validation.rs
      ├─ protocol/         # Iceberg protocol types
      └─ types/            # Iceberg data types
```

**Rationale:**
- All storage logic in one crate
- OpenDAL already available (no new dependency)
- Clear that Iceberg is a storage feature
- Easier to coordinate writes + catalog commits
- Reduces workspace complexity

## Component Design

### IcebergWriter API

```rust
pub struct IcebergWriter {
    catalog: Arc<IcebergCatalog<ReqwestHttpClient>>,
    storage: Arc<OpenDalStorage>,
    config: IcebergConfig,
}

impl IcebergWriter {
    /// Write Arrow batch to S3 Tables and commit to catalog atomically
    pub async fn write_and_commit(
        &self,
        signal_type: &str,
        metric_type: Option<&str>,
        arrow_batch: &RecordBatch,
    ) -> Result<WriteResult> {
        // 1. Get or create table
        let table = self.get_or_create_table(signal_type, metric_type).await?;

        // 2. Generate file path in warehouse location
        let file_path = self.generate_warehouse_path(&table, signal_type)?;

        // 3. Write Parquet using OpenDAL
        let write_result = self.write_parquet(&file_path, arrow_batch).await?;

        // 4. Build DataFile metadata
        let data_file = build_data_file(&write_result, &table.schema)?;

        // 5. Commit transaction to catalog
        self.catalog.commit_transaction(&table, vec![data_file]).await?;

        Ok(write_result)
    }
}
```

### On-Demand Table Creation

```rust
async fn get_or_create_table(
    &self,
    signal_type: &str,
    metric_type: Option<&str>
) -> Result<TableMetadata> {
    // Try to load existing table
    match self.catalog.load_table(signal_type, metric_type).await {
        Ok(table) => Ok(table),
        Err(_) => {
            // Create table with Arrow schema converted to Iceberg
            let schema = get_schema_for_signal(signal_type, metric_type)?;
            self.catalog.create_table(signal_type, metric_type, schema).await
        }
    }
}
```

### Warehouse Path Generation

```rust
fn generate_warehouse_path(
    &self,
    table: &TableMetadata,
    signal_type: &str,
    partition_values: &HashMap<String, String>,
) -> String {
    let base = &table.location; // e.g., s3://bucket/warehouse/namespace/table
    let partition_path = build_partition_path(partition_values);
    let filename = format!(
        "{}-{}.parquet",
        Utc::now().timestamp_nanos(),
        uuid::Uuid::new_v4()
    );
    format!("{}/data/{}/{}", base, partition_path, filename)
}
```

## Operating Modes

The system supports two modes based on configuration:

### Mode 1: S3 Tables (with Iceberg catalog)
```bash
OTLP2PARQUET_ICEBERG_REST_URI=https://s3tables.us-west-2.amazonaws.com/iceberg
OTLP2PARQUET_ICEBERG_WAREHOUSE=arn:aws:s3tables:us-west-2:123456789012:bucket/otlp2parquet
OTLP2PARQUET_ICEBERG_NAMESPACE=otel
# Writes to S3 Tables warehouse + commits to catalog
```

### Mode 2: Plain S3 (no catalog)
```bash
OTLP2PARQUET_S3_BUCKET=my-parquet-bucket
OTLP2PARQUET_S3_REGION=us-west-2
# Writes Parquet files to regular S3, no catalog integration
```

### Lambda Handler (Mode Detection)

```rust
enum Writer {
    Iceberg(IcebergWriter),
    PlainS3(ParquetWriter),
}

// Initialization - choose based on config
let writer = if env::var("OTLP2PARQUET_ICEBERG_REST_URI").is_ok() {
    Writer::Iceberg(IcebergWriter::from_env().await?)
} else {
    Writer::PlainS3(ParquetWriter::from_env().await?)
};

// Handler delegates appropriately
pub async fn handle_logs(event: LogsRequest, writer: &Writer) -> Result<()> {
    let arrow_batch = convert_otlp_to_arrow(&event)?;

    match writer {
        Writer::Iceberg(w) => w.write_and_commit("logs", None, &arrow_batch).await,
        Writer::PlainS3(w) => w.write_logs(&arrow_batch).await,
    }
}
```

## CloudFormation Changes

### S3 Tables Mode

**Add Namespace resource:**
```yaml
OtelNamespace:
  Type: AWS::S3Tables::Namespace
  Properties:
    TableBucketArn: !GetAtt OtelTableBucket.TableBucketARN
    Namespace:
      - otel
```

**Remove DataBucket** - No longer needed

**Update Environment Variables:**
```yaml
Environment:
  Variables:
    OTLP2PARQUET_ICEBERG_REST_URI: !Sub 'https://s3tables.${AWS::Region}.amazonaws.com/iceberg'
    OTLP2PARQUET_ICEBERG_WAREHOUSE: !GetAtt OtelTableBucket.TableBucketARN
    OTLP2PARQUET_ICEBERG_NAMESPACE: 'otel'
```

**Tables created on-demand** - Not in CloudFormation

### Plain S3 Mode

**Keep simple S3 bucket:**
```yaml
DataBucket:
  Type: AWS::S3::Bucket
  Properties:
    BucketName: !Sub 'otlp-data-${AWS::StackName}'
```

## Implementation Details

### Key Methods

**`IcebergWriter::write_and_commit()`:**
1. **LoadTable or CreateTable** - Get table metadata + warehouse location
2. **Generate path** - `{warehouse}/data/{partition}/file-{uuid}.parquet`
3. **Write Parquet** - Use OpenDAL to write to warehouse location
4. **Build DataFile** - Extract metadata (row count, size, stats)
5. **CommitTransaction** - Append data file to table via REST API

### Error Handling

- **LoadTable fails** → CreateTable with Arrow schema
- **Write fails** → Return error (no partial state)
- **Commit fails** → Log warning, file written but not cataloged (warn-and-succeed pattern)

### Dependencies

**Reuse existing:**
- OpenDAL (already in otlp2parquet-storage)
- reqwest (already in iceberg code for REST client)
- Arrow schemas (already available)

**No new dependencies needed** - Everything required is already in the workspace.

## Testing Strategy

### Existing Tests to Update
- Move iceberg crate tests to `otlp2parquet-storage/src/iceberg/`
- Update import paths after module move
- Existing tests already cover:
  - Arrow → Iceberg schema conversion
  - DataFile metadata building
  - REST API protocol
  - Mock HTTP client patterns

### New Tests Needed (Minimal)
- `IcebergWriter::write_and_commit()` integration test
- Warehouse path generation logic
- Mode detection (Iceberg vs PlainS3)

### Reuse Existing Test Infrastructure
- Mock HTTP client from iceberg tests
- Arrow test data from storage tests
- Parquet validation utilities

## Deployment Steps

1. Move iceberg code → `otlp2parquet-storage/src/iceberg/`
2. Implement `IcebergWriter` with `write_and_commit()`
3. Update Lambda handler to detect mode and use appropriate writer
4. Update CloudFormation - add Namespace, keep dual-mode support
5. Build & test locally with `cargo test`
6. Deploy to test stack
7. Run `./test.sh` to validate end-to-end
8. Verify tables created in S3 Tables console
9. Query data with DuckDB to validate

## Documentation Updates

- Update `docs/storage/aws-lambda-s3-tables.md` with new architecture
- Update `examples/aws-lambda-s3-tables/README.md`
- Document dual-mode operation (S3 Tables vs plain S3)

## Benefits

1. **Simpler architecture** - Single bucket instead of two
2. **Lower cost** - No separate data bucket needed
3. **Atomic operations** - Write + commit in single transaction
4. **Flexible schemas** - Tables created on-demand from Arrow schemas
5. **Better maintenance** - S3 Tables auto-compaction and optimization
6. **Query performance** - Iceberg metadata enables faster queries
7. **Dual-mode support** - Users can choose S3 Tables or plain S3

## Risks & Mitigations

**Risk:** S3 Tables warehouse location access restrictions
**Mitigation:** Confirmed via web research that S3 Tables supports `PutObject` to warehouse locations when using proper IAM permissions (`s3tables:PutTableData`)

**Risk:** Module move breaks existing code
**Mitigation:** This is pre-release; clean slate approach with no backward compatibility concerns

**Risk:** On-demand table creation failures
**Mitigation:** Graceful error handling with warn-and-succeed pattern

## Future Enhancements

- Partition pruning for better query performance
- Schema evolution support
- Table compaction triggers
- Metrics on catalog operations
