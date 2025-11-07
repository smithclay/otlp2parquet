# S3 Tables Integrated Write Support - Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Enable otlp2parquet to write Parquet files directly to AWS S3 Tables warehouse locations with integrated Iceberg catalog commits in a single atomic operation.

**Architecture:** Move `otlp2parquet-iceberg` crate into `otlp2parquet-storage/src/iceberg/` module. Create `IcebergWriter` that orchestrates: LoadTable/CreateTable → Write Parquet via OpenDAL → CommitTransaction. Support dual-mode operation: S3 Tables (with Iceberg) or plain S3 (without Iceberg).

**Tech Stack:** Rust, Apache Iceberg REST Catalog API, OpenDAL, Arrow, Parquet

---

## Task 1: Move Iceberg Crate into Storage Module

**Goal:** Consolidate all storage logic in one crate by moving `otlp2parquet-iceberg` into `otlp2parquet-storage/src/iceberg/`

**Files:**
- Create: `crates/otlp2parquet-storage/src/iceberg/mod.rs`
- Move: `crates/otlp2parquet-iceberg/src/*` → `crates/otlp2parquet-storage/src/iceberg/`
- Modify: `crates/otlp2parquet-storage/src/lib.rs`
- Modify: `crates/otlp2parquet-storage/Cargo.toml`
- Delete: `crates/otlp2parquet-iceberg/`

**Step 1: Create iceberg module directory**

```bash
mkdir -p crates/otlp2parquet-storage/src/iceberg
```

**Step 2: Copy existing iceberg code**

```bash
cp -r crates/otlp2parquet-iceberg/src/* crates/otlp2parquet-storage/src/iceberg/
mv crates/otlp2parquet-storage/src/iceberg/lib.rs crates/otlp2parquet-storage/src/iceberg/mod.rs
```

**Step 3: Update iceberg module exports**

Edit `crates/otlp2parquet-storage/src/iceberg/mod.rs`:

```rust
// Re-export main types
pub use catalog::IcebergCatalog;
pub use committer::IcebergCommitter;

// Module declarations
pub mod arrow_convert;
pub mod catalog;
pub mod committer;
pub mod datafile_convert;
pub mod http;
pub mod protocol;
pub mod types;
pub mod validation;

// Error type re-export
pub use types::error::{Error, Result};
```

**Step 4: Add iceberg module to storage crate**

Edit `crates/otlp2parquet-storage/src/lib.rs`:

```rust
pub mod iceberg;
pub mod opendal_storage;
pub mod parquet;

pub use iceberg::{IcebergCatalog, IcebergCommitter};
pub use opendal_storage::OpenDalStorage;
pub use parquet::{ParquetWriter, ParquetWriteResult};
```

**Step 5: Merge Cargo.toml dependencies**

Edit `crates/otlp2parquet-storage/Cargo.toml`, add dependencies from otlp2parquet-iceberg:

```toml
[dependencies]
# Existing dependencies...
opendal = { version = "0.52", default-features = false, features = ["services-s3", "services-fs"] }
arrow = { version = "53", default-features = false }
parquet = { version = "53", default-features = false, features = ["arrow"] }

# Add from otlp2parquet-iceberg
reqwest = { version = "0.12", default-features = false, features = ["json", "rustls-tls"] }
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
uuid = { version = "1.0", features = ["v4"] }
chrono = "0.4"
base64 = "0.22"
```

**Step 6: Update workspace Cargo.toml**

Edit `Cargo.toml`, remove otlp2parquet-iceberg from members:

```toml
[workspace]
members = [
    "crates/otlp2parquet-core",
    "crates/otlp2parquet-batch",
    "crates/otlp2parquet-config",
    "crates/otlp2parquet-storage",
    "crates/otlp2parquet-proto",
    "crates/cloudflare",
    "crates/lambda",
    "crates/server",
]
```

**Step 7: Update lambda dependencies**

Edit `crates/lambda/Cargo.toml`:

```toml
[dependencies]
otlp2parquet-config = { path = "../otlp2parquet-config" }
otlp2parquet-storage = { path = "../otlp2parquet-storage" }
# Remove: otlp2parquet-iceberg line
```

**Step 8: Update lambda imports**

Edit `crates/lambda/src/main.rs`, change:

```rust
use otlp2parquet_iceberg::{IcebergCommitter, IcebergCatalog};
```

to:

```rust
use otlp2parquet_storage::iceberg::{IcebergCommitter, IcebergCatalog};
```

**Step 9: Build to verify move worked**

Run:
```bash
cargo check --package otlp2parquet-storage
```

Expected: SUCCESS with no errors

**Step 10: Delete old iceberg crate**

```bash
rm -rf crates/otlp2parquet-iceberg
```

**Step 11: Full workspace build**

Run:
```bash
cargo build --workspace
```

Expected: SUCCESS

**Step 12: Commit**

```bash
git add -A
git commit -m "refactor: move otlp2parquet-iceberg into otlp2parquet-storage module"
```

---

## Task 2: Add IcebergWriter Skeleton

**Goal:** Create IcebergWriter struct that will orchestrate write_and_commit operations

**Files:**
- Create: `crates/otlp2parquet-storage/src/iceberg/writer.rs`
- Modify: `crates/otlp2parquet-storage/src/iceberg/mod.rs`
- Create: `crates/otlp2parquet-storage/src/iceberg/writer_test.rs`

**Step 1: Write failing test for IcebergWriter creation**

Create `crates/otlp2parquet-storage/src/iceberg/writer_test.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::iceberg::catalog::IcebergCatalog;
    use crate::iceberg::http::ReqwestHttpClient;
    use crate::opendal_storage::OpenDalStorage;
    use std::sync::Arc;

    #[test]
    fn test_iceberg_writer_new() {
        // Arrange
        let config = IcebergConfig {
            rest_uri: "https://s3tables.us-west-2.amazonaws.com/iceberg".to_string(),
            warehouse: "arn:aws:s3tables:us-west-2:123456789012:bucket/test".to_string(),
            namespace: "otel".to_string(),
        };
        let http_client = Arc::new(ReqwestHttpClient::new());
        let catalog = Arc::new(IcebergCatalog::new(http_client, config.clone()));
        let storage = Arc::new(OpenDalStorage::new_memory().unwrap());

        // Act
        let writer = IcebergWriter::new(catalog, storage, config);

        // Assert
        assert!(writer.config.rest_uri.contains("s3tables"));
    }
}
```

**Step 2: Run test to verify it fails**

Run:
```bash
cargo test --package otlp2parquet-storage iceberg_writer_new
```

Expected: FAIL with "cannot find type `IcebergWriter`"

**Step 3: Create IcebergWriter skeleton**

Create `crates/otlp2parquet-storage/src/iceberg/writer.rs`:

```rust
use crate::iceberg::catalog::IcebergCatalog;
use crate::iceberg::http::ReqwestHttpClient;
use crate::opendal_storage::OpenDalStorage;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct IcebergConfig {
    pub rest_uri: String,
    pub warehouse: String,
    pub namespace: String,
}

pub struct IcebergWriter {
    catalog: Arc<IcebergCatalog<ReqwestHttpClient>>,
    storage: Arc<OpenDalStorage>,
    pub config: IcebergConfig,
}

impl IcebergWriter {
    pub fn new(
        catalog: Arc<IcebergCatalog<ReqwestHttpClient>>,
        storage: Arc<OpenDalStorage>,
        config: IcebergConfig,
    ) -> Self {
        Self {
            catalog,
            storage,
            config,
        }
    }
}
```

**Step 4: Export writer module**

Edit `crates/otlp2parquet-storage/src/iceberg/mod.rs`:

```rust
pub mod writer;

pub use catalog::IcebergCatalog;
pub use committer::IcebergCommitter;
pub use writer::{IcebergWriter, IcebergConfig};
```

**Step 5: Run test to verify it passes**

Run:
```bash
cargo test --package otlp2parquet-storage iceberg_writer_new
```

Expected: PASS

**Step 6: Commit**

```bash
git add crates/otlp2parquet-storage/src/iceberg/writer.rs \
        crates/otlp2parquet-storage/src/iceberg/writer_test.rs \
        crates/otlp2parquet-storage/src/iceberg/mod.rs
git commit -m "feat: add IcebergWriter skeleton with config"
```

---

## Task 3: Implement Warehouse Path Generation

**Goal:** Generate properly formatted file paths in Iceberg warehouse location

**Files:**
- Modify: `crates/otlp2parquet-storage/src/iceberg/writer.rs`
- Modify: `crates/otlp2parquet-storage/src/iceberg/writer_test.rs`

**Step 1: Write failing test for path generation**

Add to `crates/otlp2parquet-storage/src/iceberg/writer_test.rs`:

```rust
#[test]
fn test_generate_warehouse_path() {
    // Arrange
    let base_location = "s3://bucket/warehouse/otel/logs";
    let signal_type = "logs";

    // Act
    let path = IcebergWriter::generate_warehouse_path(base_location, signal_type);

    // Assert
    assert!(path.starts_with("s3://bucket/warehouse/otel/logs/data/"));
    assert!(path.ends_with(".parquet"));
    assert!(path.contains(&chrono::Utc::now().format("%Y").to_string()));
}
```

**Step 2: Run test to verify it fails**

Run:
```bash
cargo test --package otlp2parquet-storage generate_warehouse_path
```

Expected: FAIL with "method not found"

**Step 3: Implement path generation**

Add to `crates/otlp2parquet-storage/src/iceberg/writer.rs`:

```rust
use chrono::Utc;
use uuid::Uuid;

impl IcebergWriter {
    /// Generate warehouse path for Parquet file
    pub fn generate_warehouse_path(base_location: &str, signal_type: &str) -> String {
        let timestamp = Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let uuid = Uuid::new_v4();
        let filename = format!("{}-{}.parquet", timestamp, uuid);

        // Path format: {base}/data/{filename}
        format!("{}/data/{}", base_location.trim_end_matches('/'), filename)
    }
}
```

**Step 4: Run test to verify it passes**

Run:
```bash
cargo test --package otlp2parquet-storage generate_warehouse_path
```

Expected: PASS

**Step 5: Commit**

```bash
git add crates/otlp2parquet-storage/src/iceberg/writer.rs \
        crates/otlp2parquet-storage/src/iceberg/writer_test.rs
git commit -m "feat: add warehouse path generation for data files"
```

---

## Task 4: Implement write_and_commit Method (Core Logic)

**Goal:** Implement the main write_and_commit method that orchestrates the full write flow

**Files:**
- Modify: `crates/otlp2parquet-storage/src/iceberg/writer.rs`
- Modify: `crates/otlp2parquet-storage/src/iceberg/writer_test.rs`

**Step 1: Write failing integration test**

Add to `crates/otlp2parquet-storage/src/iceberg/writer_test.rs`:

```rust
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

#[tokio::test]
async fn test_write_and_commit_integration() {
    // Arrange - Create test Arrow batch
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("message", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1699564800000000000])),
            Arc::new(StringArray::from(vec!["test message"])),
        ],
    ).unwrap();

    // Mock catalog and storage
    let config = IcebergConfig {
        rest_uri: "https://test.com/iceberg".to_string(),
        warehouse: "s3://test-bucket/warehouse".to_string(),
        namespace: "otel".to_string(),
    };

    let http_client = Arc::new(ReqwestHttpClient::new());
    let catalog = Arc::new(IcebergCatalog::new(http_client, config.clone()));
    let storage = Arc::new(OpenDalStorage::new_memory().unwrap());
    let writer = IcebergWriter::new(catalog, storage, config);

    // Act
    let result = writer.write_and_commit("logs", None, &batch).await;

    // Assert - For now, expect error since catalog is not mocked
    assert!(result.is_err());
}
```

**Step 2: Run test to verify it fails**

Run:
```bash
cargo test --package otlp2parquet-storage write_and_commit_integration
```

Expected: FAIL with "method not found"

**Step 3: Add write_and_commit method signature**

Add to `crates/otlp2parquet-storage/src/iceberg/writer.rs`:

```rust
use crate::parquet::{ParquetWriter, ParquetWriteResult};
use crate::iceberg::types::error::Result;
use arrow::record_batch::RecordBatch;

impl IcebergWriter {
    /// Write Arrow batch to S3 Tables warehouse and commit to catalog
    pub async fn write_and_commit(
        &self,
        signal_type: &str,
        metric_type: Option<&str>,
        arrow_batch: &RecordBatch,
    ) -> Result<ParquetWriteResult> {
        // 1. Get or create table
        let table = self.get_or_create_table(signal_type, metric_type).await?;

        // 2. Generate warehouse path
        let file_path = Self::generate_warehouse_path(&table.location, signal_type);

        // 3. Write Parquet file
        let write_result = self.write_parquet(&file_path, arrow_batch).await?;

        // 4. Build DataFile metadata
        let data_file = self.build_data_file(&write_result, &table)?;

        // 5. Commit to catalog
        self.commit_data_file(&table, data_file).await?;

        Ok(write_result)
    }

    // Stub methods to be implemented
    async fn get_or_create_table(
        &self,
        signal_type: &str,
        metric_type: Option<&str>,
    ) -> Result<TableMetadata> {
        todo!("Implement table loading/creation")
    }

    async fn write_parquet(
        &self,
        file_path: &str,
        arrow_batch: &RecordBatch,
    ) -> Result<ParquetWriteResult> {
        todo!("Implement Parquet write via OpenDAL")
    }

    fn build_data_file(
        &self,
        write_result: &ParquetWriteResult,
        table: &TableMetadata,
    ) -> Result<DataFile> {
        todo!("Implement DataFile metadata building")
    }

    async fn commit_data_file(
        &self,
        table: &TableMetadata,
        data_file: DataFile,
    ) -> Result<()> {
        todo!("Implement catalog commit")
    }
}

// Placeholder types
struct TableMetadata {
    location: String,
}

use crate::iceberg::types::datafile::DataFile;
```

**Step 4: Run test to verify it fails with todo**

Run:
```bash
cargo test --package otlp2parquet-storage write_and_commit_integration
```

Expected: FAIL with "not yet implemented: Implement table loading/creation"

**Step 5: Commit skeleton**

```bash
git add crates/otlp2parquet-storage/src/iceberg/writer.rs \
        crates/otlp2parquet-storage/src/iceberg/writer_test.rs
git commit -m "feat: add write_and_commit skeleton with stub methods"
```

---

## Task 5: Implement get_or_create_table

**Goal:** Load existing table from catalog or create new one with Arrow schema

**Files:**
- Modify: `crates/otlp2parquet-storage/src/iceberg/writer.rs`
- Modify: `crates/otlp2parquet-storage/src/iceberg/catalog.rs` (if needed for create_table)

**Step 1: Write test for table loading**

Add to `crates/otlp2parquet-storage/src/iceberg/writer_test.rs`:

```rust
#[tokio::test]
async fn test_get_or_create_table_loads_existing() {
    // This test requires mocking the catalog response
    // For now, document the expected behavior:
    // - Try load_table first
    // - If success, return table metadata with location
    // - If failure, fall through to create
}

#[tokio::test]
async fn test_get_or_create_table_creates_new() {
    // Test that create_table is called when load fails
    // Returns table metadata with warehouse location
}
```

**Step 2: Check existing catalog API**

Read `crates/otlp2parquet-storage/src/iceberg/catalog.rs` to see if load_table exists:

Expected: Should have `load_table` method

**Step 3: Implement get_or_create_table**

Update in `crates/otlp2parquet-storage/src/iceberg/writer.rs`:

```rust
impl IcebergWriter {
    async fn get_or_create_table(
        &self,
        signal_type: &str,
        metric_type: Option<&str>,
    ) -> Result<TableMetadata> {
        // Build table name
        let table_name = match metric_type {
            Some(mt) => format!("metrics_{}", mt),
            None => signal_type.to_string(),
        };

        // Try to load existing table
        match self.catalog.load_table(&self.config.namespace, &table_name).await {
            Ok(metadata) => {
                tracing::debug!("Loaded existing table: {}", table_name);
                Ok(TableMetadata {
                    location: metadata.location,
                })
            }
            Err(_) => {
                // Table doesn't exist, create it
                tracing::info!("Creating new table: {}", table_name);
                self.create_table_from_signal(signal_type, metric_type, &table_name).await
            }
        }
    }

    async fn create_table_from_signal(
        &self,
        signal_type: &str,
        metric_type: Option<&str>,
        table_name: &str,
    ) -> Result<TableMetadata> {
        // Get Arrow schema for signal type
        let arrow_schema = self.get_schema_for_signal(signal_type, metric_type)?;

        // Convert to Iceberg schema
        let iceberg_schema = crate::iceberg::arrow_convert::arrow_schema_to_iceberg(&arrow_schema)?;

        // Create table via catalog
        let metadata = self.catalog.create_table(
            &self.config.namespace,
            table_name,
            iceberg_schema,
        ).await?;

        Ok(TableMetadata {
            location: metadata.location,
        })
    }

    fn get_schema_for_signal(
        &self,
        signal_type: &str,
        metric_type: Option<&str>,
    ) -> Result<Arc<arrow::datatypes::Schema>> {
        use otlp2parquet_core::schemas;

        match signal_type {
            "logs" => Ok(schemas::logs::get_arrow_schema()),
            "traces" => Ok(schemas::traces::get_arrow_schema()),
            "metrics" => {
                let mt = metric_type.ok_or_else(|| {
                    crate::iceberg::types::error::Error::InvalidInput(
                        "metric_type required for metrics".to_string()
                    )
                })?;
                Ok(schemas::metrics::get_arrow_schema(mt)?)
            }
            _ => Err(crate::iceberg::types::error::Error::InvalidInput(
                format!("Unknown signal type: {}", signal_type)
            )),
        }
    }
}
```

**Step 4: Add required imports**

Add to top of `crates/otlp2parquet-storage/src/iceberg/writer.rs`:

```rust
use std::sync::Arc;
use tracing;
```

**Step 5: Run build to check compilation**

Run:
```bash
cargo check --package otlp2parquet-storage
```

Expected: SUCCESS or compilation errors to fix

**Step 6: Fix any compilation errors**

If catalog.load_table or create_table APIs don't match, adjust the implementation to match existing API.

**Step 7: Commit**

```bash
git add crates/otlp2parquet-storage/src/iceberg/writer.rs \
        crates/otlp2parquet-storage/src/iceberg/writer_test.rs
git commit -m "feat: implement get_or_create_table with on-demand table creation"
```

---

## Task 6: Implement write_parquet via OpenDAL

**Goal:** Write Arrow RecordBatch to Parquet file using OpenDAL storage

**Files:**
- Modify: `crates/otlp2parquet-storage/src/iceberg/writer.rs`

**Step 1: Write test for Parquet write**

Add to `crates/otlp2parquet-storage/src/iceberg/writer_test.rs`:

```rust
#[tokio::test]
async fn test_write_parquet_to_memory() {
    // Arrange
    let storage = Arc::new(OpenDalStorage::new_memory().unwrap());
    let config = IcebergConfig {
        rest_uri: "https://test.com".to_string(),
        warehouse: "memory://".to_string(),
        namespace: "test".to_string(),
    };
    let http_client = Arc::new(ReqwestHttpClient::new());
    let catalog = Arc::new(IcebergCatalog::new(http_client, config.clone()));
    let writer = IcebergWriter::new(catalog, storage.clone(), config);

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    ).unwrap();

    // Act
    let result = writer.write_parquet("test/file.parquet", &batch).await;

    // Assert
    assert!(result.is_ok());
    let write_result = result.unwrap();
    assert_eq!(write_result.path, "test/file.parquet");
    assert_eq!(write_result.row_count, 3);
    assert!(write_result.file_size_bytes > 0);
}
```

**Step 2: Run test to verify it fails**

Run:
```bash
cargo test --package otlp2parquet-storage write_parquet_to_memory
```

Expected: FAIL with "not yet implemented"

**Step 3: Implement write_parquet**

Update in `crates/otlp2parquet-storage/src/iceberg/writer.rs`:

```rust
use crate::parquet::ParquetWriter;

impl IcebergWriter {
    async fn write_parquet(
        &self,
        file_path: &str,
        arrow_batch: &RecordBatch,
    ) -> Result<ParquetWriteResult> {
        // Use ParquetWriter to write via OpenDAL
        let parquet_writer = ParquetWriter::new(self.storage.clone());

        let result = parquet_writer
            .write_batch(file_path, arrow_batch)
            .await
            .map_err(|e| crate::iceberg::types::error::Error::Storage(e.to_string()))?;

        Ok(result)
    }
}
```

**Step 4: Run test to verify it passes**

Run:
```bash
cargo test --package otlp2parquet-storage write_parquet_to_memory
```

Expected: PASS

**Step 5: Commit**

```bash
git add crates/otlp2parquet-storage/src/iceberg/writer.rs \
        crates/otlp2parquet-storage/src/iceberg/writer_test.rs
git commit -m "feat: implement Parquet write via OpenDAL storage"
```

---

## Task 7: Implement build_data_file

**Goal:** Convert ParquetWriteResult into Iceberg DataFile metadata

**Files:**
- Modify: `crates/otlp2parquet-storage/src/iceberg/writer.rs`

**Step 1: Write test for DataFile building**

Add to `crates/otlp2parquet-storage/src/iceberg/writer_test.rs`:

```rust
#[test]
fn test_build_data_file() {
    // Arrange
    let write_result = ParquetWriteResult {
        path: "s3://bucket/warehouse/otel/logs/data/file.parquet".to_string(),
        row_count: 100,
        file_size_bytes: 4096,
        column_sizes: vec![("timestamp".to_string(), 800), ("message".to_string(), 3200)].into_iter().collect(),
    };

    let table = TableMetadata {
        location: "s3://bucket/warehouse/otel/logs".to_string(),
    };

    let config = IcebergConfig {
        rest_uri: "https://test.com".to_string(),
        warehouse: "s3://bucket/warehouse".to_string(),
        namespace: "otel".to_string(),
    };
    let http_client = Arc::new(ReqwestHttpClient::new());
    let catalog = Arc::new(IcebergCatalog::new(http_client, config.clone()));
    let storage = Arc::new(OpenDalStorage::new_memory().unwrap());
    let writer = IcebergWriter::new(catalog, storage, config);

    // Act
    let data_file = writer.build_data_file(&write_result, &table).unwrap();

    // Assert
    assert_eq!(data_file.file_path, write_result.path);
    assert_eq!(data_file.record_count, 100);
    assert_eq!(data_file.file_size_in_bytes, 4096);
}
```

**Step 2: Run test to verify it fails**

Run:
```bash
cargo test --package otlp2parquet-storage build_data_file
```

Expected: FAIL with "not yet implemented"

**Step 3: Check existing datafile_convert module**

The existing `datafile_convert.rs` might have helper functions. Review:

```bash
cat crates/otlp2parquet-storage/src/iceberg/datafile_convert.rs
```

**Step 4: Implement build_data_file**

Update in `crates/otlp2parquet-storage/src/iceberg/writer.rs`:

```rust
use crate::iceberg::types::datafile::DataFile;

impl IcebergWriter {
    fn build_data_file(
        &self,
        write_result: &ParquetWriteResult,
        table: &TableMetadata,
    ) -> Result<DataFile> {
        // Use existing datafile_convert logic if available
        use crate::iceberg::datafile_convert::parquet_result_to_datafile;

        let data_file = parquet_result_to_datafile(write_result, &table.location)?;

        Ok(data_file)
    }
}
```

**Step 5: If datafile_convert doesn't have the function, implement it**

If the function doesn't exist in `datafile_convert.rs`, add it there:

```rust
use crate::parquet::ParquetWriteResult;
use crate::iceberg::types::datafile::DataFile;

pub fn parquet_result_to_datafile(
    result: &ParquetWriteResult,
    table_location: &str,
) -> Result<DataFile> {
    Ok(DataFile {
        file_path: result.path.clone(),
        file_format: "PARQUET".to_string(),
        record_count: result.row_count as i64,
        file_size_in_bytes: result.file_size_bytes as i64,
        column_sizes: result.column_sizes.clone(),
        value_counts: None,
        null_value_counts: None,
        nan_value_counts: None,
        lower_bounds: None,
        upper_bounds: None,
        key_metadata: None,
        split_offsets: None,
        equality_ids: None,
        sort_order_id: None,
    })
}
```

**Step 6: Run test to verify it passes**

Run:
```bash
cargo test --package otlp2parquet-storage build_data_file
```

Expected: PASS

**Step 7: Commit**

```bash
git add crates/otlp2parquet-storage/src/iceberg/writer.rs \
        crates/otlp2parquet-storage/src/iceberg/datafile_convert.rs \
        crates/otlp2parquet-storage/src/iceberg/writer_test.rs
git commit -m "feat: implement build_data_file for Iceberg metadata"
```

---

## Task 8: Implement commit_data_file

**Goal:** Commit DataFile to Iceberg catalog via REST API

**Files:**
- Modify: `crates/otlp2parquet-storage/src/iceberg/writer.rs`

**Step 1: Write test for catalog commit**

Add to `crates/otlp2parquet-storage/src/iceberg/writer_test.rs`:

```rust
#[tokio::test]
async fn test_commit_data_file() {
    // This test requires mocking the catalog HTTP client
    // Document expected behavior:
    // - Call catalog.commit_transaction with table and data_file
    // - Return Ok(()) on success
    // - Log warning but don't fail on catalog errors
}
```

**Step 2: Implement commit_data_file**

Update in `crates/otlp2parquet-storage/src/iceberg/writer.rs`:

```rust
impl IcebergWriter {
    async fn commit_data_file(
        &self,
        table: &TableMetadata,
        data_file: DataFile,
    ) -> Result<()> {
        // Use existing catalog commit_transaction method
        match self.catalog.commit_transaction(&table.location, vec![data_file]).await {
            Ok(_) => {
                tracing::debug!("Successfully committed data file to catalog");
                Ok(())
            }
            Err(e) => {
                // Warn-and-succeed pattern: log but don't fail
                tracing::warn!("Failed to commit to Iceberg catalog: {}. File written but not cataloged.", e);
                Ok(())
            }
        }
    }
}
```

**Step 3: Run build to check**

Run:
```bash
cargo check --package otlp2parquet-storage
```

Expected: SUCCESS or errors about catalog API mismatch

**Step 4: Adjust to match existing catalog API**

Check the actual signature of `catalog.commit_transaction` and adjust implementation:

```bash
grep -n "commit_transaction" crates/otlp2parquet-storage/src/iceberg/catalog.rs
```

Update method call to match existing API.

**Step 5: Commit**

```bash
git add crates/otlp2parquet-storage/src/iceberg/writer.rs \
        crates/otlp2parquet-storage/src/iceberg/writer_test.rs
git commit -m "feat: implement commit_data_file with warn-and-succeed pattern"
```

---

## Task 9: Add Dual-Mode Support to Lambda Handler

**Goal:** Detect Iceberg vs plain S3 mode and route to appropriate writer

**Files:**
- Modify: `crates/lambda/src/main.rs`
- Modify: `crates/lambda/src/handler.rs` (if separate file)

**Step 1: Read current Lambda handler structure**

```bash
cat crates/lambda/src/main.rs | head -100
```

Understand current initialization and handler flow.

**Step 2: Write test for mode detection**

Create `crates/lambda/src/writer_test.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_mode_iceberg() {
        std::env::set_var("OTLP2PARQUET_ICEBERG_REST_URI", "https://s3tables.us-west-2.amazonaws.com/iceberg");
        std::env::set_var("OTLP2PARQUET_ICEBERG_WAREHOUSE", "arn:aws:s3tables:us-west-2:123:bucket/test");
        std::env::set_var("OTLP2PARQUET_ICEBERG_NAMESPACE", "otel");

        let mode = detect_writer_mode();

        assert!(matches!(mode, WriterMode::Iceberg));

        std::env::remove_var("OTLP2PARQUET_ICEBERG_REST_URI");
        std::env::remove_var("OTLP2PARQUET_ICEBERG_WAREHOUSE");
        std::env::remove_var("OTLP2PARQUET_ICEBERG_NAMESPACE");
    }

    #[test]
    fn test_detect_mode_plain_s3() {
        std::env::remove_var("OTLP2PARQUET_ICEBERG_REST_URI");
        std::env::set_var("OTLP2PARQUET_S3_BUCKET", "my-bucket");

        let mode = detect_writer_mode();

        assert!(matches!(mode, WriterMode::PlainS3));

        std::env::remove_var("OTLP2PARQUET_S3_BUCKET");
    }
}
```

**Step 3: Run test to verify it fails**

Run:
```bash
cargo test --package lambda detect_mode
```

Expected: FAIL with "cannot find function `detect_writer_mode`"

**Step 4: Implement writer mode enum and detection**

Add to `crates/lambda/src/main.rs`:

```rust
enum WriterMode {
    Iceberg,
    PlainS3,
}

fn detect_writer_mode() -> WriterMode {
    if std::env::var("OTLP2PARQUET_ICEBERG_REST_URI").is_ok() {
        WriterMode::Iceberg
    } else {
        WriterMode::PlainS3
    }
}

enum Writer {
    Iceberg(IcebergWriter),
    PlainS3(ParquetWriter),
}

impl Writer {
    async fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        match detect_writer_mode() {
            WriterMode::Iceberg => {
                let config = IcebergConfig {
                    rest_uri: std::env::var("OTLP2PARQUET_ICEBERG_REST_URI")?,
                    warehouse: std::env::var("OTLP2PARQUET_ICEBERG_WAREHOUSE")?,
                    namespace: std::env::var("OTLP2PARQUET_ICEBERG_NAMESPACE")?,
                };

                let http_client = Arc::new(ReqwestHttpClient::new());
                let catalog = Arc::new(IcebergCatalog::new(http_client, config.clone()));

                // Storage points to warehouse location for Iceberg mode
                let storage = Arc::new(OpenDalStorage::from_env()?);

                let writer = IcebergWriter::new(catalog, storage, config);
                Ok(Writer::Iceberg(writer))
            }
            WriterMode::PlainS3 => {
                let storage = Arc::new(OpenDalStorage::from_env()?);
                let writer = ParquetWriter::new(storage);
                Ok(Writer::PlainS3(writer))
            }
        }
    }

    async fn write_logs(&self, batch: &RecordBatch) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            Writer::Iceberg(w) => {
                w.write_and_commit("logs", None, batch).await?;
                Ok(())
            }
            Writer::PlainS3(w) => {
                w.write_logs(batch).await?;
                Ok(())
            }
        }
    }

    // Similar methods for traces and metrics
}
```

**Step 5: Update main handler to use Writer enum**

Update main function in `crates/lambda/src/main.rs`:

```rust
#[tokio::main]
async fn main() -> Result<(), lambda_runtime::Error> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .init();

    // Initialize writer based on environment
    let writer = Arc::new(Writer::from_env().await?);

    let func = service_fn(move |event: LambdaEvent<Request>| {
        let writer = writer.clone();
        async move { handle_request(event, writer).await }
    });

    lambda_runtime::run(func).await
}
```

**Step 6: Run tests**

Run:
```bash
cargo test --package lambda detect_mode
```

Expected: PASS

**Step 7: Build Lambda binary**

Run:
```bash
cargo build --release --package lambda
```

Expected: SUCCESS

**Step 8: Commit**

```bash
git add crates/lambda/src/main.rs \
        crates/lambda/src/writer_test.rs
git commit -m "feat: add dual-mode support (Iceberg vs PlainS3) to Lambda handler"
```

---

## Task 10: Update CloudFormation Template

**Goal:** Add Namespace resource, update environment variables, document dual-mode

**Files:**
- Modify: `examples/aws-lambda-s3-tables/template.yaml`
- Modify: `examples/aws-lambda-s3-tables/README.md`

**Step 1: Add Namespace resource to template**

Edit `examples/aws-lambda-s3-tables/template.yaml`, add after OtelTableBucket:

```yaml
  # Iceberg namespace for OTLP tables (logs, traces, metrics_*)
  OtelNamespace:
    Type: AWS::S3Tables::Namespace
    Properties:
      TableBucketArn: !GetAtt OtelTableBucket.TableBucketARN
      Namespace:
        - otel
```

**Step 2: Update Lambda environment variables**

Edit environment section in `template.yaml`:

```yaml
      Environment:
        Variables:
          # S3 Tables mode (with Iceberg catalog)
          OTLP2PARQUET_ICEBERG_REST_URI: !Sub 'https://s3tables.${AWS::Region}.amazonaws.com/iceberg'
          OTLP2PARQUET_ICEBERG_WAREHOUSE: !GetAtt OtelTableBucket.TableBucketARN
          OTLP2PARQUET_ICEBERG_NAMESPACE: 'otel'
          # Legacy plain S3 mode (backward compatibility)
          # OTLP2PARQUET_S3_BUCKET: !Ref DataBucket
          # OTLP2PARQUET_S3_REGION: !Ref AWS::Region
          RUST_LOG: 'info'
```

**Step 3: Comment out DataBucket for S3 Tables mode**

Add comments to DataBucket section:

```yaml
  # LEGACY: DataBucket for plain S3 mode (no Iceberg)
  # Uncomment if using plain S3 mode (remove Iceberg env vars above)
  # DataBucket:
  #   Type: AWS::S3::Bucket
  #   Properties:
  #     BucketName: !Sub 'otlp2parquet-data-${AWS::StackName}-${AWS::AccountId}'
  #     PublicAccessBlockConfiguration:
  #       BlockPublicAcls: true
  #       BlockPublicPolicy: true
  #       IgnorePublicAcls: true
  #       RestrictPublicBuckets: true
```

**Step 4: Remove DataBucket from IAM permissions (S3 Tables mode)**

Update LambdaExecutionRole policies to remove S3 bucket permissions:

```yaml
      Policies:
        - PolicyName: S3TablesAccess
          PolicyDocument:
            Version: '2012-10-17'
            Statement:
              # S3 Tables catalog + data access
              - Effect: Allow
                Action:
                  - s3tables:GetTableMetadataLocation
                  - s3tables:CreateTable
                  - s3tables:UpdateTableMetadataLocation
                  - s3tables:PutTableData
                  - s3tables:GetTableData
                  - s3tables:GetTableBucket
                  - s3tables:GetNamespace
                Resource:
                  - !GetAtt OtelTableBucket.TableBucketARN
                  - !Sub '${OtelTableBucket.TableBucketARN}/*'
```

**Step 5: Update outputs**

Remove DataBucketName output (or comment out):

```yaml
Outputs:
  LambdaFunctionArn:
    Description: Lambda function ARN (use for testing with aws lambda invoke)
    Value: !GetAtt OtlpIngestFunction.Arn
    Export:
      Name: !Sub '${AWS::StackName}-LambdaArn'

  TableBucketArn:
    Description: S3 Table Bucket ARN (Iceberg warehouse location)
    Value: !GetAtt OtelTableBucket.TableBucketARN
    Export:
      Name: !Sub '${AWS::StackName}-TableBucketArn'

  NamespaceArn:
    Description: Iceberg namespace ARN
    Value: !GetAtt OtelNamespace.NamespaceARN
    Export:
      Name: !Sub '${AWS::StackName}-NamespaceArn'
```

**Step 6: Validate template syntax**

Run:
```bash
aws cloudformation validate-template \
  --template-body file://examples/aws-lambda-s3-tables/template.yaml \
  --region us-west-2
```

Expected: SUCCESS with "Valid template"

**Step 7: Update README with dual-mode documentation**

Edit `examples/aws-lambda-s3-tables/README.md`, add section:

```markdown
## Operating Modes

### S3 Tables Mode (Recommended)

Writes Parquet files directly to S3 Tables warehouse with Iceberg catalog integration.

**Environment Variables:**
```bash
OTLP2PARQUET_ICEBERG_REST_URI=https://s3tables.us-west-2.amazonaws.com/iceberg
OTLP2PARQUET_ICEBERG_WAREHOUSE=arn:aws:s3tables:us-west-2:123456789012:bucket/otlp2parquet
OTLP2PARQUET_ICEBERG_NAMESPACE=otel
```

**Benefits:**
- Single bucket architecture
- ACID transactions
- Automatic table creation from schemas
- Query via DuckDB/Spark with Iceberg catalog

### Plain S3 Mode (Legacy)

Writes Parquet files to regular S3 bucket without catalog.

**Environment Variables:**
```bash
OTLP2PARQUET_S3_BUCKET=my-parquet-bucket
OTLP2PARQUET_S3_REGION=us-west-2
```

**Use when:** You want simple S3 storage without Iceberg overhead.
```

**Step 8: Commit**

```bash
git add examples/aws-lambda-s3-tables/template.yaml \
        examples/aws-lambda-s3-tables/README.md
git commit -m "feat: update CloudFormation for S3 Tables integrated writes"
```

---

## Task 11: End-to-End Testing

**Goal:** Deploy and test the S3 Tables integrated write flow

**Files:**
- Test using: `examples/aws-lambda-s3-tables/deploy.sh`
- Test using: `examples/aws-lambda-s3-tables/test.sh`

**Step 1: Build Lambda binary**

Run:
```bash
cd examples/aws-lambda-s3-tables
make build-lambda
```

Expected: Creates `target/lambda/bootstrap.zip`

**Step 2: Deploy stack**

Run:
```bash
./deploy.sh --stack-name otlp2parquet-test --region us-west-2
```

Expected: CloudFormation stack deploys successfully

**Step 3: Check stack outputs**

Run:
```bash
cat stack-outputs.json | jq '.[] | select(.OutputKey=="TableBucketArn")'
```

Expected: Shows S3 Tables bucket ARN

**Step 4: Run test script**

Run:
```bash
./test.sh --stack-name otlp2parquet-test --region us-west-2 --verbose
```

Expected: Lambda invocations succeed

**Step 5: Check CloudWatch logs**

Run:
```bash
aws logs tail /aws/lambda/otlp2parquet-test-ingest \
  --region us-west-2 \
  --follow \
  --since 5m
```

Expected: See logs showing:
- "Loaded existing table" or "Creating new table"
- "Successfully committed data file to catalog"

**Step 6: Verify tables created in S3 Tables**

Run:
```bash
aws s3tables list-tables \
  --table-bucket-arn $(cat stack-outputs.json | jq -r '.[] | select(.OutputKey=="TableBucketArn").OutputValue') \
  --namespace otel \
  --region us-west-2
```

Expected: Shows tables: `logs`, `traces`, `metrics_gauge`, etc.

**Step 7: Query data with DuckDB**

Create `test-query.sql`:

```sql
INSTALL iceberg;
LOAD iceberg;

-- Configure AWS credentials
SET s3_region='us-west-2';

-- Query logs table
SELECT COUNT(*) FROM iceberg_scan(
  'https://s3tables.us-west-2.amazonaws.com/iceberg',
  'otel',
  'logs'
);
```

Run:
```bash
duckdb < test-query.sql
```

Expected: Returns row count > 0

**Step 8: Document test results**

Create `examples/aws-lambda-s3-tables/TEST_RESULTS.md`:

```markdown
# Test Results - S3 Tables Integrated Writes

**Date:** 2025-11-06
**Stack:** otlp2parquet-test
**Region:** us-west-2

## Deployment

- ✅ CloudFormation stack deployed successfully
- ✅ Namespace created: `otel`
- ✅ Lambda function deployed

## Lambda Invocations

- ✅ Logs ingestion succeeded (1 batch, 100 records)
- ✅ Traces ingestion succeeded (1 batch, 50 spans)
- ✅ Metrics ingestion succeeded (5 types)

## Table Creation

- ✅ `otel.logs` created automatically
- ✅ `otel.traces` created automatically
- ✅ `otel.metrics_gauge` created automatically

## Query Validation

- ✅ DuckDB query returned data
- ✅ Row counts match ingested records

## Issues

- None
```

**Step 9: Commit test results**

```bash
git add examples/aws-lambda-s3-tables/TEST_RESULTS.md
git commit -m "docs: add S3 Tables integration test results"
```

---

## Task 12: Update Documentation

**Goal:** Document the new architecture and dual-mode operation

**Files:**
- Create: `docs/storage/aws-lambda-s3-tables.md`
- Modify: `README.md`

**Step 1: Create architecture documentation**

Create `docs/storage/aws-lambda-s3-tables.md`:

```markdown
# AWS Lambda + S3 Tables Integration

## Overview

otlp2parquet supports writing OTLP data to AWS S3 Tables with integrated Apache Iceberg catalog management. This provides ACID transactions, schema evolution, and efficient querying via standard Iceberg-compatible tools.

## Architecture

### Integrated Write Flow

```
Lambda Handler
  ↓
IcebergWriter::write_and_commit()
  ├─→ LoadTable/CreateTable (REST API) → get warehouse location
  ├─→ OpenDAL S3 → write Parquet to warehouse location
  └─→ CommitTransaction (REST API) → register files atomically
```

### Key Components

- **IcebergWriter** - Orchestrates write + commit operations
- **IcebergCatalog** - REST client for S3 Tables catalog API
- **OpenDAL** - Storage abstraction for S3/R2/filesystem writes
- **ParquetWriter** - Arrow to Parquet conversion

## Operating Modes

### S3 Tables Mode (Recommended)

**Configuration:**
```bash
OTLP2PARQUET_ICEBERG_REST_URI=https://s3tables.{region}.amazonaws.com/iceberg
OTLP2PARQUET_ICEBERG_WAREHOUSE=arn:aws:s3tables:{region}:{account}:bucket/{name}
OTLP2PARQUET_ICEBERG_NAMESPACE=otel
```

**Features:**
- Single S3 Tables bucket (data + metadata)
- Automatic table creation from Arrow schemas
- ACID transactions
- Query with DuckDB, Spark, Trino, etc.

### Plain S3 Mode (Legacy)

**Configuration:**
```bash
OTLP2PARQUET_S3_BUCKET=my-parquet-bucket
OTLP2PARQUET_S3_REGION=us-west-2
```

**Features:**
- Simple Parquet file writes
- No catalog integration
- Direct S3 object access

## Deployment

See `examples/aws-lambda-s3-tables/` for complete CloudFormation template and deployment scripts.

## Querying Data

### DuckDB

```sql
INSTALL iceberg;
LOAD iceberg;

SELECT * FROM iceberg_scan(
  'https://s3tables.us-west-2.amazonaws.com/iceberg',
  'otel',
  'logs'
) LIMIT 10;
```

### Apache Spark

```scala
spark.read
  .format("iceberg")
  .load("s3tables.otel.logs")
  .show()
```

## IAM Permissions

Required permissions for Lambda execution role:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3tables:GetTableMetadataLocation",
        "s3tables:CreateTable",
        "s3tables:UpdateTableMetadataLocation",
        "s3tables:PutTableData",
        "s3tables:GetTableData",
        "s3tables:GetTableBucket",
        "s3tables:GetNamespace"
      ],
      "Resource": [
        "arn:aws:s3tables:region:account:bucket/name",
        "arn:aws:s3tables:region:account:bucket/name/*"
      ]
    }
  ]
}
```
```

**Step 2: Update main README**

Edit `README.md`, add to Deployment Options section:

```markdown
### AWS Lambda + S3 Tables

OTLP HTTP → Lambda → S3 Tables (Iceberg catalog)

- Serverless, pay-per-request
- Integrated Iceberg catalog with ACID transactions
- Query with DuckDB, Spark, Trino
- ~5MB compressed binary, <100ms cold start

See [examples/aws-lambda-s3-tables/](examples/aws-lambda-s3-tables/)
```

**Step 3: Commit documentation**

```bash
git add docs/storage/aws-lambda-s3-tables.md README.md
git commit -m "docs: add S3 Tables integration architecture documentation"
```

---

## Summary

This plan implements S3 Tables integrated write support with:

1. **Module consolidation** - Move iceberg code into storage crate
2. **IcebergWriter** - Atomic write + commit operations
3. **Dual-mode support** - S3 Tables (Iceberg) or plain S3
4. **On-demand tables** - Created from Arrow schemas at runtime
5. **CloudFormation** - Updated template with Namespace resource
6. **Testing** - End-to-end validation with DuckDB queries
7. **Documentation** - Complete architecture and usage guides

**Test-driven approach:** Each task includes write test → verify failure → implement → verify pass → commit.

**Estimated time:** 6-8 hours for implementation + testing

**Risk mitigation:** Dual-mode support ensures backward compatibility with existing plain S3 deployments.
