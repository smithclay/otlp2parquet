# icepick Integration - Detailed Implementation Plan

**Date:** 2025-01-16
**Related:** [Design Document](./2025-01-16-icepick-integration-design.md)

## Overview

This document provides a detailed, task-by-task implementation plan for replacing `otlp2parquet-iceberg` and `otlp2parquet-storage` with icepick.

**Estimated timeline:** 17 days
**Strategy:** Incremental development with continuous testing

---

## Phase 1: Preparation & Setup (Day 1)

### Tasks

- [x] OpenDAL 0.54 upgrade in icepick (already completed)
- [ ] Create feature branch: `git checkout -b feat/icepick-integration`
- [ ] Write design document (this file and design.md)
- [ ] Commit design documents

### Commands

```bash
git checkout -b feat/icepick-integration
git add docs/plans/
git commit -m "docs: add icepick integration design and plan"
```

### Success Criteria

- ✅ Feature branch created
- ✅ Design documents committed
- ✅ Team review completed (if applicable)

---

## Phase 2: Schema Migration (Days 2-3)

### Day 2: Create Iceberg Schema Module

**Tasks:**

1. Create `crates/otlp2parquet-core/src/iceberg_schemas.rs`
2. Add module export to `crates/otlp2parquet-core/src/lib.rs`
3. Implement `logs_schema()` function
4. Implement `traces_schema()` function
5. Add unit test for logs schema
6. Add unit test for traces schema

**Implementation details:**

- Use `icepick::spec::{Schema, NestedField, Type, PrimitiveType}`
- Field IDs start at 1 and increment sequentially
- Match existing ClickHouse column names (PascalCase)
- Mark required vs optional fields correctly

**Test strategy:**

```rust
#[test]
fn test_logs_schema_compatibility() {
    let iceberg = iceberg_schemas::logs_schema();
    let arrow_from_iceberg = icepick::arrow_convert::schema_to_arrow(&iceberg).unwrap();
    let arrow_canonical = crate::schemas::logs::schema();

    assert_eq!(arrow_from_iceberg.fields().len(), arrow_canonical.fields().len());
    // Compare each field...
}
```

**Success criteria:**

- ✅ Schemas build without errors
- ✅ Unit tests pass
- ✅ Field count matches Arrow schemas

### Day 3: Complete All Metric Schemas

**Tasks:**

1. Implement `metrics_gauge_schema()`
2. Implement `metrics_sum_schema()`
3. Implement `metrics_histogram_schema()`
4. Implement `metrics_exponential_histogram_schema()`
5. Implement `metrics_summary_schema()`
6. Implement `schema_for_signal()` helper function
7. Add unit tests for all metric schemas

**Success criteria:**

- ✅ All 7 schemas implemented (logs, traces, 5 metrics)
- ✅ All compatibility tests pass
- ✅ `cargo test -p otlp2parquet-core` passes

---

## Phase 3: Create otlp2parquet-writer Crate (Days 4-7)

### Day 4: Crate Setup & Core Types

**Tasks:**

1. Create directory: `mkdir -p crates/otlp2parquet-writer/src`
2. Create `Cargo.toml` with dependencies
3. Create `src/lib.rs` with module structure
4. Define `OtlpWriter` trait
5. Define `WriteResult` struct
6. Define `WriterConfig` struct
7. Define `IcepickWriter` struct skeleton

**Cargo.toml template:**

```toml
[package]
name = "otlp2parquet-writer"
version = "0.1.0"
edition = "2021"

[dependencies]
icepick = { path = "../../../icepick", default-features = false }
otlp2parquet-core = { path = "../otlp2parquet-core" }
arrow = { version = "55", default-features = false }
anyhow = "1"
chrono = "0.4"
thiserror = "2"
tracing = "0.1"
tokio = { version = "1", features = ["sync"] }
async-trait = "0.1"

[dev-dependencies]
tokio = { version = "1", features = ["full"] }
```

**Success criteria:**

- ✅ Crate compiles
- ✅ Core types defined
- ✅ No clippy warnings

### Day 5: Partition Path & Table Mapping

**Tasks:**

1. Create `src/partition.rs` module
2. Port `generate_partition_path()` from storage crate
3. Add unit tests for partition path generation
4. Create `src/table_mapping.rs` module
5. Implement `table_name_for_signal()`
6. Add unit tests for table mapping
7. Port helper functions (service name extraction, hash computation)

**Port from:**
- `crates/otlp2parquet-storage/src/partition.rs`

**Success criteria:**

- ✅ Partition paths match old format exactly
- ✅ All partition tests pass
- ✅ Table name mapping correct for all signals

### Day 6: Platform Detection & Initialization

**Tasks:**

1. Create `src/platform.rs` module
2. Implement `Platform` enum
3. Implement `detect_platform()` function
4. Create `src/init.rs` module
5. Implement `initialize_writer()` entry point
6. Implement `init_cloudflare_catalog()`
7. Implement `init_lambda_catalog()`
8. Implement `init_server_catalog()`
9. Add unit tests for platform detection
10. Add integration tests for initialization (with mocks)

**Success criteria:**

- ✅ Platform detection works correctly
- ✅ Each platform initializes correct catalog type
- ✅ No catalog mode (plain Parquet) works

### Day 7: Write Workflow Implementation

**Tasks:**

1. Create `src/writer.rs` module
2. Implement `IcepickWriter::new()`
3. Implement `write_batch()` method
4. Implement `write_batches()` method
5. Implement `get_or_create_table()` helper
6. Implement `create_table()` helper
7. Implement `commit_transaction()` helper
8. Implement `write_plain_parquet()` for no-catalog mode
9. Add unit tests for writer methods
10. Add integration tests for full write workflow

**Success criteria:**

- ✅ Write workflow complete
- ✅ Table caching works
- ✅ Warn-and-succeed pattern implemented
- ✅ All writer tests pass

---

## Phase 4: Platform Integration (Days 8-11)

### Day 8: Lambda Refactor

**Tasks:**

1. Update `crates/otlp2parquet-lambda/Cargo.toml`:
   - Remove `otlp2parquet-storage`
   - Remove `otlp2parquet-iceberg`
   - Add `otlp2parquet-writer`
   - Add `icepick` with s3-tables feature
2. Remove old writer code:
   - Delete `src/writer.rs` (old enum-based writer)
   - Delete `src/iceberg_writer.rs`
   - Delete `src/plain_s3_writer.rs`
3. Update `src/handler.rs`:
   - Replace writer initialization
   - Use `OtlpWriter` trait
   - Remove mode-specific logic
4. Update configuration loading
5. Run `cargo build -p otlp2parquet-lambda`
6. Fix compilation errors

**Success criteria:**

- ✅ Lambda builds successfully
- ✅ Simplified to single code path
- ✅ No clippy warnings

### Day 9: Lambda Testing

**Tasks:**

1. Update Lambda unit tests
2. Run existing Lambda integration tests
3. Update `examples/aws-lambda-s3-tables/` for new config
4. Test deployment with LocalStack (if available)
5. Manual testing with real S3 event

**Commands:**

```bash
cargo test -p otlp2parquet-lambda
make build-lambda
./examples/aws-lambda-s3-tables/test.sh
```

**Success criteria:**

- ✅ All Lambda tests pass
- ✅ Lambda builds with `make build-lambda`
- ✅ Example deployment works

### Day 10: Server Refactor

**Tasks:**

1. Update `crates/otlp2parquet-server/Cargo.toml`:
   - Remove `otlp2parquet-storage`
   - Remove `otlp2parquet-iceberg`
   - Add `otlp2parquet-writer`
   - Add `icepick` with s3-tables and r2-catalog features
2. Update `src/main.rs`:
   - Replace initialization logic
   - Use `OtlpWriter` trait
3. Update HTTP handlers:
   - `src/handlers/logs.rs`
   - `src/handlers/traces.rs`
   - `src/handlers/metrics.rs`
4. Remove old writer/committer code
5. Run `cargo build -p otlp2parquet-server`
6. Fix compilation errors

**Success criteria:**

- ✅ Server builds successfully
- ✅ All handlers updated
- ✅ No clippy warnings

### Day 11: Server & Cloudflare Workers Testing

**Server tasks:**

1. Update server unit tests
2. Run server integration tests
3. Manual testing with example OTLP data
4. Test multi-backend support (S3, R2, Filesystem)

**Cloudflare Workers tasks:**

1. Update `crates/otlp2parquet-cloudflare/Cargo.toml`:
   - Add `otlp2parquet-writer`
   - Add `icepick` with wasm and r2-catalog features
2. Update `src/lib.rs`:
   - Add R2 Data Catalog support
   - Use `OtlpWriter` trait
3. Build WASM: `make wasm-full`
4. Check binary size: `make wasm-size`
5. Test with Miniflare or Cloudflare CLI

**Success criteria:**

- ✅ Server tests pass
- ✅ Cloudflare Workers builds
- ✅ WASM binary < 3MB compressed
- ✅ R2 Data Catalog integration works

---

## Phase 5: Configuration Migration (Day 12)

### Tasks

1. Update `crates/otlp2parquet-config/Cargo.toml`
2. Update `src/lib.rs`:
   - Remove `IcebergRestConfig` struct
   - Add `WriterConfig` struct
   - Update `from_env()` methods
   - Remove old env var mappings
3. Update `config.toml.example`:
   - Replace `[iceberg]` section with new format
   - Document ARN-based config
   - Remove deprecated settings
4. Update all example configs:
   - `examples/aws-lambda-s3-tables/config.toml`
   - `examples/server/config.toml`
5. Run `cargo build` to verify config changes
6. Update environment variable documentation

**Success criteria:**

- ✅ Config crate builds
- ✅ All examples use new config format
- ✅ No references to old config remain

---

## Phase 6: Cleanup (Day 13)

### Tasks

1. Delete `crates/otlp2parquet-storage/`:
   ```bash
   git rm -rf crates/otlp2parquet-storage
   ```
2. Delete `crates/otlp2parquet-iceberg/`:
   ```bash
   git rm -rf crates/otlp2parquet-iceberg
   ```
3. Update workspace `Cargo.toml`:
   - Remove deleted crates from `[workspace.members]`
   - Add `otlp2parquet-writer` to `[workspace.members]`
4. Search for remaining references:
   ```bash
   grep -r "otlp2parquet-storage" .
   grep -r "otlp2parquet-iceberg" .
   ```
5. Remove any leftover imports or dependencies
6. Run `cargo clean`
7. Run `cargo build --all`
8. Fix any compilation errors
9. Run `cargo clippy --all`
10. Fix all clippy warnings

**Success criteria:**

- ✅ Old crates deleted
- ✅ No references to old crates
- ✅ Full workspace builds
- ✅ Zero clippy warnings

---

## Phase 7: Testing & Validation (Days 14-16)

### Day 14: Unit & Integration Tests

**Tasks:**

1. Run all unit tests:
   ```bash
   cargo test --all
   ```
2. Fix any failing tests
3. Run integration tests:
   ```bash
   cargo test --test integration -- --ignored
   ```
4. Add missing test coverage:
   - Edge cases in partition path generation
   - Error handling in writer
   - Catalog failure scenarios
5. Run tests with features:
   ```bash
   cargo test --all-features
   ```

**Success criteria:**

- ✅ All unit tests pass
- ✅ All integration tests pass
- ✅ Test coverage for critical paths

### Day 15: E2E & Platform Tests

**Tasks:**

1. Lambda E2E test:
   ```bash
   KEEP_CONTAINERS=1 make test-e2e-lambda
   ```
2. Server E2E test:
   ```bash
   make test-e2e-server
   ```
3. Test with real OTLP data:
   - Send logs, traces, metrics to server
   - Verify Parquet files created
   - Verify correct partitioning
   - Verify Iceberg catalog entries (if enabled)
4. Test multi-backend support:
   - S3 storage
   - R2 storage
   - Filesystem storage
5. Test catalog modes:
   - S3 Tables catalog
   - R2 Data Catalog
   - No catalog (plain Parquet)

**Success criteria:**

- ✅ All E2E tests pass
- ✅ Manual testing successful
- ✅ All storage backends work
- ✅ All catalog modes work

### Day 16: Binary Size & Performance

**Tasks:**

1. Build WASM with optimizations:
   ```bash
   make wasm-full
   ```
2. Check compressed size:
   ```bash
   make wasm-size
   ```
3. Profile binary size if > 3MB:
   ```bash
   make wasm-profile
   ```
4. Optimize if needed:
   - Feature-gate unnecessary code
   - Check for duplicate dependencies
   - Review Cargo.toml features
5. Performance benchmarking:
   - Compare write throughput (before/after)
   - Measure latency (before/after)
   - Check memory usage
6. Validate metrics:
   - WASM size < 3MB ✅
   - Performance comparable or better ✅

**Success criteria:**

- ✅ WASM binary ≤ 2.8MB compressed
- ✅ Performance within 5% of baseline
- ✅ Memory usage acceptable

---

## Phase 8: Documentation (Day 17)

### Tasks

1. Update `AGENTS.md`:
   - Document new architecture
   - Update crate descriptions
   - Remove references to deleted crates
   - Add icepick integration details
2. Update `README.md`:
   - Update configuration section
   - Update deployment instructions
   - Add R2 Data Catalog documentation
3. Create migration guide:
   - `docs/migration/icepick-integration.md`
   - Document breaking changes
   - Provide config conversion examples
   - Include troubleshooting section
4. Update deployment docs:
   - Lambda deployment with new config
   - Server deployment examples
   - Cloudflare Workers with R2 Catalog
5. Add architecture diagrams (optional):
   - Before/after diagrams
   - Data flow diagrams
6. Update changelog:
   - Add breaking changes section
   - Document new features (R2 Catalog for CF Workers)
   - List removed crates

**Success criteria:**

- ✅ All documentation updated
- ✅ Migration guide complete
- ✅ Examples work as documented
- ✅ No broken links

---

## Phase 9: Final Review & Merge (Day 18)

### Tasks

1. Final smoke test:
   ```bash
   cargo clean
   cargo build --all
   cargo test --all
   cargo clippy --all
   make wasm-full
   ```
2. Review all changes:
   ```bash
   git diff main...feat/icepick-integration
   ```
3. Verify no debug code or TODOs left
4. Run pre-commit hooks:
   ```bash
   make pre-commit
   ```
5. Write comprehensive commit message
6. Push to remote:
   ```bash
   git push origin feat/icepick-integration
   ```
7. Create pull request (if using PR workflow)
8. Tag release after merge:
   ```bash
   git tag -a v2.0.0 -m "feat: replace iceberg/storage with icepick integration"
   git push origin v2.0.0
   ```

**Success criteria:**

- ✅ All checks pass
- ✅ PR approved (if applicable)
- ✅ Merged to main
- ✅ Release tagged

---

## Risk Mitigation Checklist

### Before Starting Implementation

- [ ] Review icepick documentation thoroughly
- [ ] Verify icepick test suite passes with OpenDAL 0.54
- [ ] Backup current working state

### During Implementation

- [ ] Commit frequently with conventional commit messages
- [ ] Run tests after each major change
- [ ] Monitor binary size continuously (for WASM)
- [ ] Keep feature branch synced with main

### Before Merge

- [ ] All tests pass on CI (if applicable)
- [ ] Code review completed
- [ ] Documentation reviewed
- [ ] Migration guide tested by another developer

### After Merge

- [ ] Deploy to staging environment
- [ ] Monitor for 24 hours
- [ ] Validate production metrics
- [ ] Keep previous release tag for quick rollback

---

## Success Metrics

| Metric | Target | Status |
|--------|--------|--------|
| Unit tests passing | 100% | ⏳ |
| Integration tests passing | 100% | ⏳ |
| E2E tests passing | 100% | ⏳ |
| WASM binary size | < 3MB | ⏳ |
| Clippy warnings | 0 | ⏳ |
| Code removed | ~2000 lines | ⏳ |
| Performance regression | < 5% | ⏳ |
| Documentation coverage | 100% | ⏳ |

---

## Rollback Plan

If critical issues are discovered after merge:

1. **Immediate rollback:**
   ```bash
   git revert <merge-commit>
   git push origin main
   ```

2. **Redeploy previous version:**
   - Checkout previous tag
   - Build and deploy
   - Verify functionality

3. **Root cause analysis:**
   - Document the issue
   - Create tracking issue
   - Plan fix on feature branch

4. **Re-attempt merge:**
   - Fix issues
   - Re-test thoroughly
   - Merge when stable

---

## Notes

- This is a **breaking change** requiring major version bump (v2.0.0)
- No backward compatibility with old configuration
- Users must migrate config to ARN-based format
- Timeline is estimated; adjust based on actual progress
- Prioritize correctness over speed
- Test thoroughly at each phase before proceeding

---

## Appendix: Key Commands

```bash
# Setup
git checkout -b feat/icepick-integration

# Build & Test
cargo build --all
cargo test --all
cargo clippy --all

# WASM
make wasm-full
make wasm-size

# E2E
make test-e2e-lambda
make test-e2e-server

# Pre-commit
make pre-commit

# Cleanup
git rm -rf crates/otlp2parquet-storage crates/otlp2parquet-iceberg
cargo clean

# Merge
git push origin feat/icepick-integration
git tag -a v2.0.0 -m "feat: icepick integration"
git push origin v2.0.0
```
