# icepick Integration Design

**Date:** 2025-01-16
**Status:** Approved
**Author:** Claude (brainstorming session)

## Executive Summary

This document describes the complete replacement of `otlp2parquet-iceberg` and `otlp2parquet-storage` crates with the icepick library, a WASM-compatible Apache Iceberg client built on OpenDAL.

**Goals:**
- Enable Cloudflare Workers deployment with R2 Data Catalog support
- Reduce maintenance burden by replacing custom Iceberg implementation with battle-tested library
- Unify storage + catalog abstraction across all platforms
- Maintain existing functionality while simplifying architecture

**Key Changes:**
- Delete `otlp2parquet-iceberg` and `otlp2parquet-storage` crates (~2000 lines)
- Add icepick path dependency (located at `/Users/clay/workspace/icepick`)
- Create new `otlp2parquet-writer` crate as application-specific wrapper
- Breaking configuration changes (ARN-based S3 Tables, no backward compatibility)
- All platforms (Lambda, Server, Cloudflare Workers) use unified writer interface

---

## Table of Contents

1. [Current Architecture](#current-architecture)
2. [icepick Overview](#icepick-overview)
3. [Proposed Architecture](#proposed-architecture)
4. [Writer Abstraction Design](#writer-abstraction-design)
5. [Platform-Specific Initialization](#platform-specific-initialization)
6. [Configuration Migration](#configuration-migration)
7. [Schema Management](#schema-management)
8. [Write Workflow](#write-workflow)
9. [OpenDAL Version Alignment](#opendal-version-alignment)
10. [Testing Strategy](#testing-strategy)
11. [Migration Timeline](#migration-timeline)

---

## Current Architecture

### otlp2parquet-iceberg Crate

**Purpose:** Custom Iceberg REST catalog integration for committing Parquet files to Iceberg tables.

**Key Components:**
- `IcebergCatalog<T: HttpClient>` - Generic catalog client with pluggable auth
- `IcebergWriter<C: HttpClient>` - Combined Parquet writer + catalog committer
- `IcebergCommitter` - Post-write catalog committer for separate write/commit workflows
- Custom manifest generation (Avro format)
- Custom Arrow→Iceberg schema conversion
- AWS SigV4 authentication support for Glue/S3 Tables

**Dependencies:** reqwest, arrow, parquet, opendal, aws-sigv4, apache-avro

**Limitations:**
- Native-only (no WASM support)
- Custom Iceberg protocol implementation to maintain
- Separate from storage abstraction

### otlp2parquet-storage Crate

**Purpose:** Storage abstraction layer for Parquet writing and OpenDAL operations.

**Key Components:**
- `OpenDalStorage` - Thin wrapper around OpenDAL `Operator`
- `ParquetWriter` - Synchronous Parquet writer with hash computation
- Partition path generation (Hive-style time-based)
- Support for multiple backends (S3, R2, Filesystem, GCS, Azure)

**Limitations:**
- Separate from Iceberg concerns
- Duplicates some Parquet writing logic with iceberg crate

### Integration Points

**Lambda:** Two modes - "integrated" (IcebergWriter) vs "plain S3" (ParquetWriter + IcebergCommitter)
**Server:** Always "plain" mode with optional committer
**Cloudflare Workers:** No Iceberg support, Parquet-only

---

## icepick Overview

**Repository:** `/Users/clay/workspace/icepick`
**Description:** Experimental Apache Iceberg client for Rust with WASM support
**Size:** ~7,300 lines of Rust code

### Key Features

**Catalog Support:**
- AWS S3 Tables (native only) - Full Iceberg REST with SigV4 auth
- Cloudflare R2 Data Catalog (WASM + native) - Bearer token auth
- Direct Parquet writes (bypass Iceberg metadata)

**Core APIs:**
- `Catalog` trait with `S3TablesCatalog` and `R2Catalog` implementations
- `Table` - Loaded Iceberg table with schema, metadata, snapshots
- `Transaction` - Write transaction API for atomic commits
- `FileIO` - OpenDAL-based storage abstraction with AWS credential support
- `ParquetWriter` - Iceberg-aware writer with automatic statistics collection
- `arrow_to_parquet()` - Standalone Parquet writer

**Platform Support:**
- Native: S3 Tables, R2 Catalog, Direct Parquet
- WASM: R2 Catalog, Direct Parquet

**Dependencies:** opendal 0.54 (upgraded), arrow 55.2, parquet 55.2, apache-avro 0.21, reqwest

### Design Philosophy

- Simple API over builder-heavy patterns
- WASM-first design for serverless environments
- Minimal dependencies for small binary size
- Trait-based catalog abstraction
- Integrated FileIO management (catalogs create FileIO internally)

---

## Proposed Architecture

### High-Level Structure

```
otlp2parquet/
├── crates/
│   ├── otlp2parquet-core/          # Schemas, encoding (add iceberg schemas)
│   ├── otlp2parquet-writer/        # NEW: Application wrapper around icepick
│   ├── otlp2parquet-config/        # Configuration (update for new config)
│   ├── otlp2parquet-lambda/        # Lambda (refactor to use writer)
│   ├── otlp2parquet-server/        # Server (refactor to use writer)
│   ├── otlp2parquet-cloudflare/    # Cloudflare Workers (add Iceberg support)
│   ├── otlp2parquet-storage/       # DELETE
│   └── otlp2parquet-iceberg/       # DELETE
└── icepick/ (external at ../icepick)
```

### Dependency Graph

```
                    ┌─────────────┐
                    │   icepick   │ (external path dependency)
                    └──────┬──────┘
                           │
                    ┌──────▼──────────────────┐
                    │ otlp2parquet-writer    │ (NEW)
                    │ - OtlpWriter trait     │
                    │ - IcepickWriter impl   │
                    │ - Platform detection   │
                    │ - Partition paths      │
                    └──────┬──────────────────┘
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
   ┌────▼─────┐      ┌─────▼──────┐    ┌─────▼──────────┐
   │  Lambda  │      │   Server   │    │   Cloudflare   │
   └──────────┘      └────────────┘    └────────────────┘
```

### What Gets Deleted

- `crates/otlp2parquet-iceberg/` - Entire crate (~1200 LOC)
- `crates/otlp2parquet-storage/` - Entire crate (~800 LOC)
- Custom Iceberg REST client code
- Custom manifest generation code
- Custom Arrow→Iceberg schema conversion
- OpenDAL wrapper code

### What Gets Created

**`crates/otlp2parquet-writer/`** - New abstraction layer containing:
- Writer trait and implementation wrapping icepick
- Partition path generation (ported from storage crate)
- Platform-specific catalog initialization
- Signal type → table name mapping
- Table caching and auto-creation
- Warn-and-succeed error handling

---

## Writer Abstraction Design

### Core Trait

```rust
pub trait OtlpWriter: Send + Sync {
    async fn write_batch(
        &self,
        signal: SignalType,
        batch: RecordBatch,
        timestamp: DateTime<Utc>,
    ) -> Result<WriteResult>;

    async fn write_batches(
        &self,
        signal: SignalType,
        batches: Vec<RecordBatch>,
        timestamp: DateTime<Utc>,
    ) -> Result<WriteResult>;
}

pub struct WriteResult {
    pub path: String,
    pub file_size: u64,
    pub row_count: u64,
    pub committed_to_catalog: bool,
}
```

### IcepickWriter Implementation

```rust
pub struct IcepickWriter {
    catalog: Option<Arc<dyn icepick::Catalog>>,
    config: WriterConfig,
    table_cache: Arc<RwLock<HashMap<String, icepick::Table>>>,
}

pub struct WriterConfig {
    pub base_path: String,
    pub namespace: String,
    pub table_prefix: Option<String>,
    pub warn_on_catalog_failure: bool,
    pub partition_by_service: bool,
}
```

### Partition Path Generation

Ported from `otlp2parquet-storage`:

```rust
pub fn generate_partition_path(
    signal: SignalType,
    metric_type: Option<&str>,
    service: &str,
    timestamp: DateTime<Utc>,
    content_hash: &str,
) -> String {
    // Returns: {signal}/{metric_type}/{service}/year={y}/month={m}/day={d}/hour={h}/{ts}-{hash}.parquet
}
```

### Signal-to-Table Mapping

```rust
pub fn table_name_for_signal(signal: SignalType, metric_type: Option<&str>) -> String {
    match signal {
        SignalType::Logs => "logs",
        SignalType::Traces => "traces",
        SignalType::Metrics => match metric_type {
            Some("gauge") => "metrics_gauge",
            Some("sum") => "metrics_sum",
            Some("histogram") => "metrics_histogram",
            Some("exponential_histogram") => "metrics_exponential_histogram",
            Some("summary") => "metrics_summary",
            _ => "metrics",
        }
    }
}
```

---

## Platform-Specific Initialization

### Platform Detection

```rust
enum Platform {
    CloudflareWorkers,  // CF_WORKER env var present
    AwsLambda,          // AWS_LAMBDA_FUNCTION_NAME env var present
    Server,             // Neither
}

fn detect_platform() -> Platform {
    if env::var("CF_WORKER").is_ok() {
        Platform::CloudflareWorkers
    } else if env::var("AWS_LAMBDA_FUNCTION_NAME").is_ok() {
        Platform::AwsLambda
    } else {
        Platform::Server
    }
}
```

### Cloudflare Workers Initialization

```rust
async fn init_cloudflare_catalog(config: &Config) -> Result<Option<Arc<dyn Catalog>>> {
    if !config.iceberg.enabled {
        return Ok(None);
    }

    let account_id = env::var("OTLP2PARQUET_R2_ACCOUNT_ID")?;
    let bucket_name = env::var("OTLP2PARQUET_R2_BUCKET_NAME")?;
    let api_token = env::var("OTLP2PARQUET_R2_API_TOKEN")?;

    let catalog = icepick::R2Catalog::new(
        "otlp2parquet",
        account_id,
        bucket_name,
        api_token,
    ).await?;

    Ok(Some(Arc::new(catalog)))
}
```

### Lambda Initialization

```rust
async fn init_lambda_catalog(config: &Config) -> Result<Option<Arc<dyn Catalog>>> {
    if !config.iceberg.enabled {
        return Ok(None);
    }

    let arn = env::var("OTLP2PARQUET_S3_TABLES_ARN")?;

    let catalog = icepick::S3TablesCatalog::from_arn(
        "otlp2parquet",
        arn,
    ).await?;

    Ok(Some(Arc::new(catalog)))
}
```

### Server Initialization

```rust
async fn init_server_catalog(config: &Config) -> Result<Option<Arc<dyn Catalog>>> {
    if !config.iceberg.enabled {
        return Ok(None);
    }

    // Try S3 Tables first
    if let Ok(arn) = env::var("OTLP2PARQUET_S3_TABLES_ARN") {
        let catalog = icepick::S3TablesCatalog::from_arn("otlp2parquet", arn).await?;
        return Ok(Some(Arc::new(catalog)));
    }

    // Try R2
    if let (Ok(account_id), Ok(bucket_name), Ok(api_token)) = (
        env::var("OTLP2PARQUET_R2_ACCOUNT_ID"),
        env::var("OTLP2PARQUET_R2_BUCKET_NAME"),
        env::var("OTLP2PARQUET_R2_API_TOKEN"),
    ) {
        let catalog = icepick::R2Catalog::new("otlp2parquet", account_id, bucket_name, api_token).await?;
        return Ok(Some(Arc::new(catalog)));
    }

    // No catalog configured
    Ok(None)
}
```

---

## Configuration Migration

### New Configuration Format

**config.toml:**
```toml
[writer]
base_path = "s3://my-bucket/otlp-data"
namespace = "otel"
warn_on_catalog_failure = true
partition_by_service = true

[iceberg]
enabled = true

# S3 Tables (Lambda/Server with AWS)
s3_tables_arn = "arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket"

# R2 (Cloudflare Workers/Server with R2)
r2_account_id = "cloudflare-account-id"
r2_bucket_name = "my-bucket"
# r2_api_token comes from env var only (secret)
```

### Environment Variables

```bash
# Writer config
OTLP2PARQUET_BASE_PATH="s3://bucket/prefix"
OTLP2PARQUET_NAMESPACE="otel"
OTLP2PARQUET_WARN_ON_CATALOG_FAILURE="true"
OTLP2PARQUET_PARTITION_BY_SERVICE="true"

# Iceberg config
OTLP2PARQUET_ICEBERG_ENABLED="true"

# S3 Tables (Lambda/Server)
OTLP2PARQUET_S3_TABLES_ARN="arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket"

# R2 (Cloudflare Workers/Server)
OTLP2PARQUET_R2_ACCOUNT_ID="cloudflare-account-id"
OTLP2PARQUET_R2_BUCKET_NAME="my-bucket"
OTLP2PARQUET_R2_API_TOKEN="secret-token"

# Platform detection (auto-set)
CF_WORKER="true"
AWS_LAMBDA_FUNCTION_NAME="my-function"

# AWS credentials (standard)
AWS_REGION="us-west-2"
AWS_ACCESS_KEY_ID="..."
AWS_SECRET_ACCESS_KEY="..."
```

### Removed Configuration (Breaking Changes)

**OLD (removed, no backward compatibility):**
```toml
[iceberg]
rest_uri = "https://glue.us-west-2.amazonaws.com/iceberg"
warehouse = "123456789012"
catalog_name = "..."
data_location = "s3://bucket/..."
```

**NEW (replacement):**
```toml
[iceberg]
s3_tables_arn = "arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket"
```

**Migration:** Users must convert from Glue/warehouse config to ARN format. ARN embeds region, account ID, and bucket name.

---

## Schema Management

### Challenge

icepick does not provide automatic Arrow→Iceberg schema conversion. We must build Iceberg schemas manually that match our existing Arrow schemas.

### Solution

Create `otlp2parquet-core/src/iceberg_schemas.rs`:

```rust
use icepick::spec::{Schema, NestedField, Type, PrimitiveType};

pub fn logs_schema() -> Schema {
    Schema::builder()
        .with_fields(vec![
            NestedField::required_field(1, "Timestamp".to_string(),
                Type::Primitive(PrimitiveType::Timestamptz)),
            NestedField::required_field(2, "TraceId".to_string(),
                Type::Primitive(PrimitiveType::String)),
            NestedField::required_field(3, "SpanId".to_string(),
                Type::Primitive(PrimitiveType::String)),
            NestedField::optional_field(4, "Body".to_string(),
                Type::Primitive(PrimitiveType::String)),
            // ... all other fields from ClickHouse schema
        ])
        .build()
        .expect("valid schema")
}

pub fn traces_schema() -> Schema { /* ... */ }
pub fn metrics_gauge_schema() -> Schema { /* ... */ }
pub fn metrics_sum_schema() -> Schema { /* ... */ }
pub fn metrics_histogram_schema() -> Schema { /* ... */ }
pub fn metrics_exponential_histogram_schema() -> Schema { /* ... */ }
pub fn metrics_summary_schema() -> Schema { /* ... */ }

pub fn schema_for_signal(signal: SignalType, metric_type: Option<&str>) -> Schema {
    match signal {
        SignalType::Logs => logs_schema(),
        SignalType::Traces => traces_schema(),
        SignalType::Metrics => match metric_type {
            Some("gauge") => metrics_gauge_schema(),
            Some("sum") => metrics_sum_schema(),
            Some("histogram") => metrics_histogram_schema(),
            Some("exponential_histogram") => metrics_exponential_histogram_schema(),
            Some("summary") => metrics_summary_schema(),
            _ => panic!("Unknown metric type: {:?}", metric_type),
        }
    }
}
```

### Schema Consistency

**Unit tests** validate that Iceberg schemas are compatible with Arrow schemas:

```rust
#[test]
fn logs_schemas_are_compatible() {
    let iceberg_schema = iceberg_schemas::logs_schema();
    let arrow_from_iceberg = icepick::arrow_convert::schema_to_arrow(&iceberg_schema).unwrap();
    let arrow_canonical = crate::schemas::logs::schema();

    assert_eq!(arrow_from_iceberg.fields().len(), arrow_canonical.fields().len());

    for (iceberg_field, canonical_field) in
        arrow_from_iceberg.fields().iter().zip(arrow_canonical.fields().iter())
    {
        assert_eq!(iceberg_field.name(), canonical_field.name());
        assert_eq!(iceberg_field.data_type(), canonical_field.data_type());
    }
}
```

### Field IDs

Iceberg requires sequential field IDs starting from 1. When building schemas, assign IDs manually and sequentially.

---

## Write Workflow

### High-Level Flow

1. **Generate partition path** using timestamp + content hash
2. **Get or load table** from catalog (with caching)
3. **Write Parquet file** using `icepick::ParquetWriter`
4. **Commit transaction** to catalog (with warn-and-succeed)
5. **Return WriteResult** with path, size, row count, catalog status

### Detailed Implementation

```rust
impl OtlpWriter for IcepickWriter {
    async fn write_batch(
        &self,
        signal: SignalType,
        batch: RecordBatch,
        timestamp: DateTime<Utc>,
    ) -> Result<WriteResult> {
        // 1. Generate partition path
        let service = extract_service_name(&batch)?;
        let metric_type = if signal == SignalType::Metrics {
            Some(extract_metric_type(&batch)?)
        } else {
            None
        };

        let content_hash = compute_batch_hash(&batch)?;
        let partition_path = generate_partition_path(
            signal,
            metric_type.as_deref(),
            &service,
            timestamp,
            &content_hash,
        );

        let file_path = format!("{}/{}", self.config.base_path, partition_path);

        // 2. Get or load table
        let table = match &self.catalog {
            Some(catalog) => {
                let table_name = table_name_for_signal(signal, metric_type.as_deref());
                self.get_or_create_table(catalog, &table_name, signal, metric_type.as_deref()).await?
            },
            None => {
                return self.write_plain_parquet(batch, &file_path).await;
            }
        };

        // 3. Write Parquet
        let iceberg_schema = table.metadata().current_schema()?.clone();
        let mut writer = icepick::writer::ParquetWriter::new(iceberg_schema)?;
        writer.write_batch(&batch)?;

        let data_file = writer.finish(table.file_io(), file_path.clone()).await?;

        // 4. Commit transaction
        let committed = self.commit_transaction(table, vec![data_file]).await;

        // 5. Return result
        Ok(WriteResult {
            path: file_path,
            file_size: data_file.file_size_in_bytes(),
            row_count: data_file.record_count(),
            committed_to_catalog: committed,
        })
    }
}
```

### Table Caching

```rust
async fn get_or_create_table(
    &self,
    catalog: &Arc<dyn Catalog>,
    table_name: &str,
    signal: SignalType,
    metric_type: Option<&str>,
) -> Result<Table> {
    // Check cache
    {
        let cache = self.table_cache.read().await;
        if let Some(table) = cache.get(table_name) {
            return Ok(table.clone());
        }
    }

    // Load or create table
    let namespace = NamespaceIdent::new(vec![self.config.namespace.clone()]);
    let table_ident = TableIdent::new(namespace.clone(), table_name.to_string());

    let table = match catalog.load_table(&table_ident).await {
        Ok(table) => table,
        Err(_) => self.create_table(catalog, &namespace, table_name, signal, metric_type).await?
    };

    // Cache the table
    {
        let mut cache = self.table_cache.write().await;
        cache.insert(table_name.to_string(), table.clone());
    }

    Ok(table)
}
```

### Warn-and-Succeed Pattern

```rust
async fn commit_transaction(&self, table: Table, data_files: Vec<DataFile>) -> bool {
    let txn = table.transaction().append(data_files);

    match txn.commit(self.catalog.as_ref().unwrap().as_ref()).await {
        Ok(()) => {
            tracing::debug!("Successfully committed to Iceberg catalog");
            true
        },
        Err(e) => {
            if self.config.warn_on_catalog_failure {
                tracing::warn!(
                    error = %e,
                    table = %table.identifier(),
                    "Failed to commit to Iceberg catalog, but Parquet file was written"
                );
                false
            } else {
                tracing::error!(error = %e, table = %table.identifier(),
                    "Failed to commit to Iceberg catalog");
                false
            }
        }
    }
}
```

### Error Handling Philosophy

1. **Parquet write failure** → Hard fail (propagate error)
2. **Catalog commit failure + warn_on_catalog_failure=true** → Warn, return success with `committed_to_catalog=false`
3. **Catalog commit failure + warn_on_catalog_failure=false** → Error, return success (file written)
4. **Table creation failure** → Hard fail

**Rationale:** Parquet file is the source of truth. Iceberg metadata is an optimization. If file is written, ingestion succeeded.

---

## OpenDAL Version Alignment

**Status:** Already completed - icepick upgraded to OpenDAL 0.54

**Original plan:**
- icepick was on OpenDAL 0.51
- otlp2parquet is on OpenDAL 0.54
- Decision: Upgrade icepick to match otlp2parquet

**Current state:**
- icepick now uses OpenDAL 0.54
- No version conflicts
- All crates use same OpenDAL version

**Testing:** Verify icepick test suite passes with OpenDAL 0.54.

---

## Testing Strategy

### Unit Tests

**In `otlp2parquet-writer`:**

1. Schema compatibility tests
2. Partition path generation tests
3. Table name mapping tests
4. Configuration parsing tests

**In `otlp2parquet-core`:**

1. Iceberg schema builder tests
2. Arrow↔Iceberg compatibility tests

### Integration Tests

**In workspace `tests/`:**

1. Plain Parquet mode (no catalog)
2. R2 Catalog mode (requires credentials)
3. S3 Tables Catalog mode (requires AWS credentials)
4. Warn-on-catalog-failure mode
5. Table auto-creation tests

### End-to-End Tests

1. Lambda E2E: `make test-e2e-lambda`
2. Server E2E: `make test-e2e-server`
3. Cloudflare Workers E2E: `make test-e2e-cloudflare`

### Binary Size Validation

**Critical for WASM:**

```bash
make wasm-full
make wasm-size
# Must be < 3MB compressed
```

**Success criteria:** Compressed WASM ≤ 2.8MB

### Makefile Targets

```makefile
.PHONY: test-writer
test-writer:
	cargo test -p otlp2parquet-writer

.PHONY: test-integration
test-integration:
	cargo test --test integration -- --ignored

.PHONY: test-migration
test-migration: test-writer test-integration test-e2e-lambda test-e2e-server
	@echo "All migration tests passed"
```

---

## Migration Timeline

### Phase 1: Preparation (Day 1)
- ✅ OpenDAL 0.54 already upgraded in icepick
- Create feature branch: `feat/icepick-integration`
- Write design documents

### Phase 2: Schema Migration (Days 2-3)
- Create `otlp2parquet-core/src/iceberg_schemas.rs`
- Build Iceberg schemas for all 7 signal types
- Add compatibility tests

### Phase 3: Writer Crate (Days 4-7)
- Create `crates/otlp2parquet-writer/`
- Implement core types and trait
- Port partition path generation
- Implement platform initialization
- Implement write workflow
- Add unit tests

### Phase 4: Platform Integration (Days 8-11)
- Refactor Lambda
- Refactor Server
- Refactor Cloudflare Workers
- Update dependencies

### Phase 5: Configuration (Day 12)
- Update `otlp2parquet-config`
- Update example configs
- Update documentation

### Phase 6: Cleanup (Day 13)
- Delete old crates
- Remove references
- Run clippy

### Phase 7: Testing (Days 14-16)
- Unit tests
- Integration tests
- E2E tests
- Binary size validation

### Phase 8: Documentation (Day 17)
- Update AGENTS.md
- Update README
- Migration guide

**Total timeline:** ~17 days

---

## Success Criteria

- ✅ All tests pass (unit, integration, E2E)
- ✅ WASM binary size < 3MB compressed
- ✅ Zero clippy warnings
- ✅ Cloudflare Workers gains Iceberg support
- ✅ Code reduction: ~2000 lines removed
- ✅ Documentation complete

---

## Risks & Mitigation

**Risk 1: icepick has bugs**
- Mitigation: Extensive testing, contribute fixes upstream

**Risk 2: Binary size exceeds 3MB**
- Mitigation: Continuous monitoring, feature-gate if needed

**Risk 3: Schema incompatibilities**
- Mitigation: Comprehensive unit tests

**Risk 4: Production issues**
- Mitigation: Staging deployment, monitoring, git revert ready

---

## Conclusion

This migration replaces ~2000 lines of custom Iceberg/storage code with a unified icepick-based abstraction, enabling WASM deployment with R2 Data Catalog while simplifying the codebase and reducing maintenance burden.
