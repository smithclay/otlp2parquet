# Iceberg Manifest-Based Commit Implementation

**Date:** 2025-11-07
**Status:** Design Approved
**Related Issue:** [COMMIT_TRANSACTION_ISSUE.md](../../examples/aws-lambda-s3-tables/COMMIT_TRANSACTION_ISSUE.md)

## Problem Statement

AWS S3 Tables returns HTTP 404 "UnknownOperationException" when attempting to commit data files via the unsupported `/transactions/commit` endpoint. S3 Tables only supports 13 standard Iceberg REST API operations, specifically `updateTable` (POST `/v1/{prefix}/namespaces/{namespace}/tables/{table}`), not the multi-table transaction endpoint.

**Impact:**
- Tables are created successfully
- Parquet files are written to S3
- Files are NOT registered in catalog metadata
- Queries cannot discover the data

## Solution Overview

Implement proper Iceberg commit flow using the supported `updateTable` endpoint with manifest file generation.

### Current Flow (Broken)
```
Write Parquet → POST /transactions/commit with DataFiles → ❌ 404 Error
```

### New Flow (Standards-Compliant)
```
1. Write Parquet files to S3 (unchanged)
2. Load current table metadata via loadTable
3. Generate new snapshot ID (timestamp-based)
4. Create manifest file (Avro) listing data files
5. Write manifest to S3 metadata/ directory
6. Create manifest-list file (Avro) referencing manifest
7. Write manifest-list to S3 metadata/ directory
8. POST to updateTable with add-snapshot + set-snapshot-ref
9. On success: catalog updated ✅
   On failure: warn and continue (files are written)
```

## Design Decisions

### 1. Avro Library
**Choice:** Use `apache-avro` crate
**Rationale:** Battle-tested, handles edge cases, ~150-200KB compiled size is acceptable
**Trade-off:** Adds dependency vs custom implementation complexity

### 2. Testing Strategy
**Choice:** Manual Lambda deployment first, automated tests later
**Rationale:** Get end-to-end validation quickly, iterate on working solution
**Trade-off:** Slower initial feedback vs comprehensive test coverage upfront

### 3. Snapshot ID Generation
**Choice:** Timestamp-based (milliseconds since epoch)
**Rationale:** Simple, monotonic, human-readable, sufficient for Lambda use case
**Trade-off:** Collision risk in same millisecond vs UUID complexity

### 4. Error Handling
**Choice:** Warn and continue on catalog failures
**Rationale:** Preserves data, allows manual recovery, matches current behavior
**Trade-off:** Requires monitoring vs failing entire ingestion

### 5. Binary Size Impact
**Estimated Addition:** ~250KB (apache-avro ~150-200KB + manifest code ~50KB)
**Budget:** Well within 3MB WASM target

## Architecture

### Component Structure

#### New Module: `crates/otlp2parquet-storage/src/iceberg/manifest.rs`

**1. ManifestWriter**
- Input: `Vec<DataFile>`, snapshot_id
- Output: S3 URI to manifest file
- Converts DataFile → ManifestEntry
- Writes Avro file using apache-avro
- Schema: Iceberg manifest_entry spec

**2. ManifestListWriter**
- Input: manifest paths, snapshot metadata
- Output: S3 URI to manifest-list file
- Creates manifest-list entries with statistics
- Writes Avro manifest-list file
- Schema: Iceberg manifest_file spec

**3. File Naming Convention**
```
{table_location}/metadata/{uuid}-m0.avro                    # Manifest
{table_location}/metadata/snap-{snapshot_id}-1-{uuid}.avro  # Manifest-list
```

#### Modified: `crates/otlp2parquet-storage/src/iceberg/catalog.rs`

**New Method: `update_table()`**
```rust
pub async fn update_table(
    &self,
    table_name: &str,
    requirements: Vec<TableRequirement>,
    updates: Vec<TableUpdate>,
) -> Result<TableMetadata>
```
- Endpoint: `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}`
- Body: `CommitTableRequest { requirements, updates }`
- Handles optimistic concurrency

**Refactored: `commit_with_signal()`**
```rust
pub async fn commit_with_signal(...) -> Result<()> {
    // 1. Load current table metadata
    let metadata = self.load_table(table_name).await?;

    // 2. Generate snapshot ID
    let snapshot_id = Utc::now().timestamp_millis();

    // 3. Write manifest file
    let manifest_path = manifest_writer.write(
        snapshot_id,
        &data_files,
        &self.storage_operator,
        &table_location,
    ).await?;

    // 4. Write manifest-list file
    let manifest_list_path = manifest_list_writer.write(
        snapshot_id,
        &manifest_path,
        &data_files,
        &self.storage_operator,
        &table_location,
    ).await?;

    // 5. Commit via updateTable
    let requirements = vec![
        TableRequirement::AssertRefSnapshotId {
            ref_name: "main".to_string(),
            snapshot_id: metadata.current_snapshot_id,
        }
    ];

    let updates = vec![
        TableUpdate::AddSnapshot {
            snapshot: Snapshot {
                snapshot_id,
                timestamp_ms: snapshot_id,
                manifest_list: manifest_list_path,
                summary: [("operation".into(), "append".into())].into(),
                parent_snapshot_id: metadata.current_snapshot_id,
            }
        },
        TableUpdate::SetSnapshotRef {
            ref_name: "main".to_string(),
            snapshot_id,
            ref_type: "branch".to_string(),
        }
    ];

    match self.update_table(table_name, requirements, updates).await {
        Ok(_) => {
            info!("Successfully committed {} files to catalog", data_files.len());
            Ok(())
        }
        Err(e) => {
            warn!("Catalog commit failed: {}. Parquet files written but not cataloged.", e);
            Ok(())
        }
    }
}
```

**Removed:**
- `commit_transaction()` method (uses unsupported endpoint)
- Never worked with S3 Tables, clean removal

#### Updated: `crates/otlp2parquet-storage/src/iceberg/protocol/requests.rs`

**New Types:**
```rust
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CommitTableRequest {
    pub requirements: Vec<TableRequirement>,
    pub updates: Vec<TableUpdate>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum TableRequirement {
    AssertRefSnapshotId {
        #[serde(rename = "ref")]
        ref_name: String,
        #[serde(rename = "snapshot-id")]
        snapshot_id: Option<i64>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case")]
pub enum TableUpdate {
    AddSnapshot {
        snapshot: Snapshot,
    },
    SetSnapshotRef {
        #[serde(rename = "ref-name")]
        ref_name: String,
        #[serde(rename = "snapshot-id")]
        snapshot_id: i64,
        #[serde(rename = "type")]
        ref_type: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Snapshot {
    pub snapshot_id: i64,
    pub parent_snapshot_id: Option<i64>,
    pub timestamp_ms: i64,
    pub manifest_list: String,
    pub summary: HashMap<String, String>,
}
```

**Removed:**
- `CommitTransactionRequest` with `AppendFiles` variant
- `TableUpdate::AppendFiles` (not in Iceberg spec for updateTable)

## Data Structures

### Manifest Entry (Avro)
```rust
struct ManifestEntry {
    status: i32,              // 1 = ADDED, 2 = EXISTING, 0 = DELETED
    snapshot_id: Option<i64>, // Snapshot that added this file
    data_file: ManifestFile,
}

struct ManifestFile {
    file_path: String,
    file_format: String,
    partition: HashMap<String, String>,
    record_count: i64,
    file_size_in_bytes: i64,
    column_sizes: Option<HashMap<i32, i64>>,
    value_counts: Option<HashMap<i32, i64>>,
    null_value_counts: Option<HashMap<i32, i64>>,
    nan_value_counts: Option<HashMap<i32, i64>>,
    lower_bounds: Option<HashMap<i32, Vec<u8>>>,
    upper_bounds: Option<HashMap<i32, Vec<u8>>>,
}
```

### Manifest List Entry (Avro)
```rust
struct ManifestListEntry {
    manifest_path: String,
    manifest_length: i64,
    partition_spec_id: i32,      // 0 for unpartitioned
    added_snapshot_id: i64,
    added_data_files_count: i32,
    added_rows_count: i64,
}
```

## Avro Schemas

Embedded as constants in `manifest.rs`:

### Manifest Schema
```json
{
  "type": "record",
  "name": "manifest_entry",
  "fields": [
    {"name": "status", "type": "int"},
    {"name": "snapshot_id", "type": ["null", "long"], "default": null},
    {"name": "data_file", "type": {
      "type": "record",
      "name": "r2",
      "fields": [
        {"name": "file_path", "type": "string"},
        {"name": "file_format", "type": "string"},
        {"name": "partition", "type": {"type": "map", "values": "string"}},
        {"name": "record_count", "type": "long"},
        {"name": "file_size_in_bytes", "type": "long"},
        {"name": "column_sizes", "type": ["null", {"type": "map", "values": "long"}]},
        {"name": "value_counts", "type": ["null", {"type": "map", "values": "long"}]},
        {"name": "null_value_counts", "type": ["null", {"type": "map", "values": "long"}]},
        {"name": "nan_value_counts", "type": ["null", {"type": "map", "values": "long"}]},
        {"name": "lower_bounds", "type": ["null", {"type": "map", "values": "bytes"}]},
        {"name": "upper_bounds", "type": ["null", {"type": "map", "values": "bytes"}]}
      ]
    }}
  ]
}
```

### Manifest-List Schema
```json
{
  "type": "record",
  "name": "manifest_file",
  "fields": [
    {"name": "manifest_path", "type": "string"},
    {"name": "manifest_length", "type": "long"},
    {"name": "partition_spec_id", "type": "int"},
    {"name": "added_snapshot_id", "type": "long"},
    {"name": "added_data_files_count", "type": "int"},
    {"name": "added_rows_count", "type": "long"}
  ]
}
```

## Error Handling

**Principle:** Warn and continue on catalog failures

```rust
match catalog_commit(...).await {
    Ok(_) => info!("Committed to catalog"),
    Err(e) => {
        warn!("Catalog commit failed: {}. Parquet files written but not cataloged.", e);
        Ok(()) // Continue
    }
}
```

**Rationale:**
- Preserves data integrity (Parquet files always written)
- Allows manual recovery via metadata repair
- Matches existing behavior
- Enables monitoring without data loss

## Observability

**Logging:**
- DEBUG: Manifest file paths, Avro encoding details
- INFO: Snapshot IDs, file counts, commit success
- WARN: Catalog commit failures

**Metrics (future):**
- Commit success/failure rate
- Manifest generation latency
- Catalog update latency

## Dependencies

**New:**
```toml
[dependencies]
apache-avro = { version = "0.17", default-features = false, features = ["snappy"] }
uuid = { version = "1.0", features = ["v4"] }  # Already present
```

**Existing (no change):**
- opendal (storage)
- reqwest (HTTP client)
- serde/serde_json (serialization)

## Implementation Plan

1. **Add apache-avro dependency** to `otlp2parquet-storage/Cargo.toml`
2. **Create `manifest.rs`** module with ManifestWriter and ManifestListWriter
3. **Update `protocol/requests.rs`** with new request types
4. **Add `update_table()`** method to catalog.rs
5. **Refactor `commit_with_signal()`** to use manifest-based flow
6. **Remove `commit_transaction()`** and old types
7. **Test manually** with deployed Lambda
8. **Verify** files are queryable via Athena/DuckDB

## Testing Strategy

**Phase 1: Manual Integration Test**
1. Deploy Lambda with changes
2. Send test OTLP data via `./test.sh`
3. Check CloudWatch logs for successful commit
4. Query via Athena to verify files are cataloged
5. Use DuckDB to validate Parquet + Iceberg metadata

**Phase 2: Automated Tests (future)**
- Unit tests for manifest generation
- Mock catalog for commit flow testing
- Integration test with localstack S3 Tables (if available)

## Success Criteria

- [ ] Lambda deploys successfully with new code
- [ ] Parquet files written to S3
- [ ] Manifest files created in metadata/ directory
- [ ] Manifest-list files created in metadata/ directory
- [ ] `updateTable` returns HTTP 200
- [ ] Athena queries return ingested data
- [ ] No increase in binary size beyond 250KB
- [ ] CloudWatch logs show INFO-level commit success

## Future Improvements (Out of Scope)

- Retry logic with exponential backoff
- Batch multiple manifests into single manifest-list
- Manifest compaction (merge small manifests)
- Dead-letter queue for failed commits
- Comprehensive unit test coverage
- Performance benchmarks

## References

- [AWS S3 Tables Supported APIs](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-open-source.html)
- [Iceberg REST Catalog Spec](https://iceberg.apache.org/rest-catalog-spec/)
- [Iceberg Table Spec - Manifests](https://iceberg.apache.org/spec/#manifests)
- [Apache Avro Rust](https://docs.rs/apache-avro/latest/apache_avro/)
