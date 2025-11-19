# DuckDB Iceberg Read Failure - Debugging Summary

**Date:** 2025-11-18
**Status:** RESOLVED - Architectural simplification completed

## Resolution (2025-11-18)

**Decision:** Remove all Iceberg code from otlp2parquet. Pass only Arrow batches to icepick.

**Rationale:** The root cause was maintaining dual schema definitions (Arrow + Iceberg). This created:
- Schema drift risks
- Type mapping conflicts (UInt32 vs Int64, Binary vs FixedSizeBinary)
- 760+ lines of unnecessary code
- Statistics collection failures

**Changes Made:**
- ✅ Deleted `iceberg_schemas.rs` (758 lines)
- ✅ Deleted `arrow_to_iceberg.rs` (conversion logic)
- ✅ Removed all catalog operations from writer
- ✅ Removed Arrow-Iceberg compatibility tests
- ✅ Simplified to pure Arrow architecture

**New Architecture:**
```
otlp2parquet: OTLP → Arrow (with field_id metadata)
     ↓
icepick: Arrow → Parquet
     ↓
TODO: icepick will derive Iceberg + catalog from Arrow
```

**What Works Now:**
- ✅ Parquet file writing via icepick
- ✅ Arrow schemas (single source of truth)
- ✅ All 69 tests passing
- ✅ Clean, simple codebase

**What's Stubbed:**
- ❌ Catalog integration (will be handled by icepick)
- ❌ Table auto-creation (will be handled by icepick)
- ❌ Statistics collection (will be handled by icepick)

---

# Original Investigation (For Reference)

## Issue Description

DuckDB fails to read Iceberg tables created by otlp2parquet with the following error:

```
INTERNAL Error: Calling GetValueInternal on a value that is NULL
```

Stack trace points to `ManifestFileReader::CreateVectorMappingEyRNS_25MultiFileColumnDefinitionE` in DuckDB's Iceberg reader.

## Investigation Timeline

### Phase 1: Field ID Metadata (COMPLETED ✅)

**Initial Hypothesis:** List element fields missing `PARQUET:field_id` metadata

**Files Modified:**
- `crates/otlp2parquet-core/src/schema/traces.rs` - Added field_id to List element fields (5001-5007)
- `crates/otlp2parquet-core/src/schema/metrics.rs` - Added field_id to List element fields (8003-10004)
- `crates/otlp2parquet-core/src/schema/logs.rs` - Changed TraceId/SpanId from FixedSizeBinary to Binary
- `crates/otlp2parquet-core/src/otlp/logs/to_arrow.rs` - Updated builders from FixedSizeBinaryBuilder to BinaryBuilder
- `crates/otlp2parquet-core/src/otlp/metrics/to_arrow.rs` - Created `list_array_with_field()` helper

**Result:** All tests passed, field IDs correctly written to Parquet files, but DuckDB still failed with same error.

### Phase 2: Statistics Discovery (ROOT CAUSE IDENTIFIED 🎯)

**Investigation:**
- Downloaded actual Parquet files from S3 Tables
- Inspected Parquet metadata with PyArrow - field_id metadata present ✅
- Downloaded and parsed Avro manifest files
- **Discovery:** Manifest `data_file` entries have ALL statistics set to NULL:
  ```json
  {
    "column_sizes": null,
    "value_counts": null,
    "null_value_counts": null,
    "lower_bounds": null,
    "upper_bounds": null
  }
  ```

**Root Cause:** DuckDB's manifest reader expects statistics to be populated, but we're writing NULL for all stats fields.

**Why Statistics Are Missing:**

Our current implementation in `crates/otlp2parquet-writer/src/writer.rs:252-263` uses:

```rust
icepick::arrow_to_parquet(batch, &file_path, table.file_io()).await?;

// Manual DataFile creation WITHOUT statistics
let data_file = DataFile::builder()
    .with_file_path(&partition_path)
    .with_file_format("PARQUET")
    .with_record_count(row_count)
    .with_file_size_in_bytes(0)  // Missing
    .build()?;
```

The `arrow_to_parquet()` function is a simple convenience API that:
1. Writes Parquet file directly to S3
2. Does NOT collect statistics
3. Returns nothing

In contrast, `icepick::writer::ParquetWriter`:
1. Uses `StatsCollector` to track column sizes, value counts, null counts, bounds
2. Returns a fully-populated `DataFile` with statistics

### Phase 3: Attempted Fix (BLOCKED ❌)

**Approach:** Switch from `arrow_to_parquet()` to `ParquetWriter`

**Implementation:**
```rust
let iceberg_schema = otlp2parquet_core::iceberg_schemas::schema_for_signal(signal_type, metric_type);
let mut parquet_writer = ParquetWriter::new(iceberg_schema)?;
parquet_writer.write_batch(batch)?;
let data_file = parquet_writer.finish(table.file_io(), file_path).await?;
```

**New Error:**
```
Failed to write batch: Invalid input: Expected Int64Array
```

**Problem:** Schema mismatch
- Our Arrow `RecordBatch` uses the schema from `otlp2parquet_core::schema::*` (optimized for S3 Tables)
- `ParquetWriter::new(iceberg_schema)` converts Iceberg schema → Arrow schema internally
- These two Arrow schemas don't match (type differences)

## Technical Details

### Parquet Statistics Fields

Required by DuckDB's Iceberg reader:
- `column_sizes` - Map of field_id → byte size in column
- `value_counts` - Map of field_id → number of values (including nulls)
- `null_value_counts` - Map of field_id → number of null values
- `lower_bounds` - Map of field_id → min value bytes
- `upper_bounds` - Map of field_id → max value bytes

### icepick Architecture

```
icepick::writer::ParquetWriter
├── Creates StatsCollector from Iceberg Schema
├── Converts Iceberg Schema → Arrow Schema internally
├── Writes RecordBatch to Parquet (in-memory buffer)
├── StatsCollector tracks statistics during write
└── Returns DataFile with populated statistics

icepick::arrow_to_parquet()
├── Simple convenience function
├── Writes RecordBatch to Parquet
├── Uploads to storage
└── Returns nothing (no statistics)
```

## Architectural Simplification: otlp2parquet knows NOTHING about Parquet/Iceberg

**The Right Pattern:** Single Arrow schema with field_id metadata, icepick handles everything else

**Current (OVERCOMPLICATED):**
```
otlp2parquet maintains:
├── Arrow schemas (otlp2parquet_core::schema::*)
└── Iceberg schemas (otlp2parquet_core::iceberg_schemas::*)

Then passes BOTH to icepick, which gets confused about which is source of truth
```

**Proposed (SIMPLE):**
```
otlp2parquet maintains:
└── Arrow schemas ONLY (with PARQUET:field_id metadata)

icepick derives everything from Arrow:
├── Reads Arrow schema from RecordBatch
├── Derives Iceberg schema from Arrow schema + field_id metadata
├── Writes Parquet files
├── Collects statistics
└── Creates catalog entries
```

**Why This Is Better:**
1. ✅ Single source of truth - just Arrow schemas
2. ✅ otlp2parquet is purely OTLP → Arrow conversion (no Parquet/Iceberg knowledge)
3. ✅ No parallel schema maintenance (no drift between Arrow and Iceberg)
4. ✅ Clear separation: otlp2parquet = telemetry expert, icepick = storage expert
5. ✅ Field IDs preserved via Arrow metadata (already working in our schemas)

**Ideal API:**
```rust
// otlp2parquet: OTLP → Arrow (knows nothing about Parquet/Iceberg)
let batch = parse_otlp_to_arrow(request, SignalType::Logs)?;

// icepick: Arrow → Parquet + Iceberg (derives Iceberg schema from Arrow)
let data_file = icepick::arrow_to_parquet(&batch, path, file_io)
    .finish_data_file()  // Uses batch.schema(), derives Iceberg from field_id metadata
    .await?;
```

**What needs to change in icepick:**
1. Add `arrow_schema_to_iceberg()` function that:
   - Reads `PARQUET:field_id` from Arrow field metadata
   - Maps Arrow types → Iceberg types (UInt32→int, Binary→binary, List→list, etc.)
   - Preserves field names, nullability, and metadata
2. Modify `finish_data_file()` to:
   - Read `RecordBatch.schema()` (the actual Arrow schema being written)
   - Derive Iceberg schema from Arrow using `arrow_schema_to_iceberg()`
   - Create `StatsCollector` using the Arrow schema (not derived from Iceberg)
   - Collect statistics matching actual Arrow types in the RecordBatch
3. Remove `.with_iceberg_schema()` - no longer needed

**What needs to change in otlp2parquet:**
1. Delete `crates/otlp2parquet-core/src/iceberg_schemas.rs` entirely (500+ lines removed!)
2. Keep only Arrow schemas in `crates/otlp2parquet-core/src/schema/*.rs` (already have field_id metadata)
3. Update writer to just call `.finish_data_file()` without passing Iceberg schema
4. Remove all `use otlp2parquet_core::iceberg_schemas` imports

**Result:**
- otlp2parquet: ~500 lines of code deleted
- Simpler mental model: OTLP → Arrow, that's it
- No schema drift issues (single source of truth)
- icepick becomes the Parquet/Iceberg expert (proper separation of concerns)

## Path Forward

**ISSUE:** icepick's `.with_iceberg_schema()` creates a `StatsCollector` that expects Arrow types to match what would be generated from the Iceberg schema. But our Arrow schemas are independently designed.

**Recommended Fix (modify icepick):**

Change `StatsCollector` to work with the RecordBatch's actual Arrow schema, using the Iceberg schema only for field ID mapping:

```rust
// In icepick/src/writer/stats.rs
impl StatsCollector {
    // OLD: pub fn new(iceberg_schema: &Schema) -> Self
    // Creates Arrow schema from Iceberg, expects RecordBatch to match

    // NEW: Accept both schemas
    pub fn new_with_mapping(
        arrow_schema: &ArrowSchema,      // From RecordBatch - source of truth for types
        iceberg_schema: Option<&Schema>  // For field ID mapping only
    ) -> Self {
        // Collect stats based on actual Arrow types
        // Map to Iceberg field IDs if schema provided
    }
}
```

Then in `arrow_to_parquet.rs`:
```rust
pub fn with_iceberg_schema(mut self, schema: &Schema) -> Self {
    // Use RecordBatch's schema for stats collection, Iceberg schema for field IDs
    let arrow_schema = self.batch.schema();
    self.stats_collector = Some(StatsCollector::new_with_mapping(&arrow_schema, Some(schema)));
    self
}
```

**Alternative (workaround in otlp2parquet):**

If modifying icepick isn't feasible immediately:
1. Don't use `.with_iceberg_schema().finish_data_file()`
2. Manually build DataFile with statistics computed from our Arrow RecordBatch
3. This works but duplicates stats collection logic

**DO NOT:**
- Try to force our Arrow schemas to match what icepick generates from Iceberg
- This defeats the purpose of having optimized Arrow schemas for S3 Tables
- Wrong architectural direction (Arrow should be the contract, not derived from Iceberg)

## Files to Review Tomorrow

1. `/tmp/icepick-inspect/src/writer/stats.rs` - Statistics collector implementation
2. `/tmp/icepick-inspect/src/writer/parquet.rs` - ParquetWriter usage
3. `crates/otlp2parquet-writer/src/writer.rs:232-308` - Our write implementation
4. `crates/otlp2parquet-core/src/schema/*.rs` - Our Arrow schemas
5. `crates/otlp2parquet-core/src/iceberg_schemas.rs` - Our Iceberg schemas

## Test Environment

Stack deployed: `otlp-nov17b` (us-west-2)
- S3 Tables bucket: `arn:aws:s3tables:us-west-2:156280089524:bucket/otlp2parquet-otlp-nov17b-156280089524`
- Lambda: `otlp-nov17b-ingest`
- Current error: "Expected Int64Array" (from ParquetWriter schema mismatch)

## Current Code State (Updated 2025-11-18)

**Committed Changes:**
- ✅ Field ID metadata added to all List element fields in traces/metrics schemas
- ✅ Logs schema changed from FixedSizeBinary → Binary for TraceId/SpanId
- ✅ All Cargo.toml files updated to icepick commit `ef55ba32110435e71c55c99f74f3d94c98def408`
- ✅ Writer updated to use `.with_iceberg_schema().finish_data_file()` API
- ✅ All 64 unit tests passing, clippy clean

**Current Issue:**
- ❌ Lambda deployment fails with "Expected Int64Array" error
- ❌ DuckDB cannot read tables (no data written due to Lambda failure)

**Root Cause - Schema Architecture Mismatch:**

The new icepick API works as follows:
```rust
icepick::arrow_to_parquet(batch, path, file_io)
    .with_iceberg_schema(table.schema()?)  // <-- Passes Iceberg schema
    .finish_data_file()                     // <-- Creates StatsCollector from Iceberg schema
```

The problem:
1. `StatsCollector::new(iceberg_schema)` converts Iceberg schema → Arrow schema internally
2. StatsCollector then expects the RecordBatch to have types matching this generated Arrow schema
3. Our RecordBatch uses independently-designed Arrow schemas from `otlp2parquet_core::schema::*`
4. Type mismatch causes "Expected Int64Array" (e.g., our UInt32 TraceFlags vs Iceberg's Int64)

**Why This Happened:**
- We designed our Arrow schemas independently, optimized for S3 Tables (Binary vs FixedSizeBinary, etc.)
- Iceberg schemas were created later to match our Arrow schemas
- But icepick's Arrow→Iceberg conversion uses different type mappings
- Result: Two different Arrow schemas that don't align

**Lambda Deployment:**
- Stack `otlp-nov17b` deployed but failing at runtime
- All test invocations return 500 "internal storage failure"

## References

- DuckDB Iceberg extension: Uses Iceberg manifest statistics for query planning
- Iceberg spec: Statistics are optional but DuckDB appears to require them
- icepick commit: `b69c0f60eb0933cabc45e5e593cf404d6d051b97`
- icepick repo: `https://github.com/smithclay/icepick.git`
