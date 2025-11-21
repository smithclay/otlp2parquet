# Unified Smoke Test Framework - Design Document

**Date:** 2025-11-21
**Status:** Approved
**Goal:** Consolidate server/Docker tests with cloud platform tests into a unified smoke test framework

## Motivation

Currently, testing is split across two different patterns:

1. **Server/Docker tests:** Bash script (`scripts/test-e2e.sh`) + Rust tests (`tests/e2e_docker.rs`) with direct localhost assertions
2. **Cloud platform tests:** Unified `SmokeTestHarness` trait in `tests/smoke/` for Lambda and Workers

This split creates:
- **Code duplication:** Server and cloud tests verify similar things (Parquet files, catalog state) with different code
- **Poor ergonomics:** Different commands, different patterns, harder to add test coverage consistently

**Primary Goals:**
- **Developer ergonomics:** Single command to run all platform tests
- **Code reuse:** Shared verification logic across all platforms
- **Quality consistency:** Same test coverage depth for server, Lambda, and Workers

## Design Overview

### File Structure

```
tests/
  smoke/
    mod.rs           # Trait + shared types (existing, enhanced)
    server.rs        # NEW: ServerHarness implementation
    lambda.rs        # Existing, minor updates
    workers.rs       # Existing, minor updates
  smoke.rs           # NEW: Unified test runner with all test cases
  e2e.rs             # Keep as-is (unit-level integration tests)
```

**Removed:**
- `tests/e2e_docker.rs` - Replaced by unified `tests/smoke.rs`
- `scripts/test-e2e.sh` - Replaced by cargo commands

### Feature Flags

```bash
# Server tests (requires Docker)
smoke-server

# Cloud tests (requires credentials)
smoke-lambda
smoke-workers
```

### Test Invocation

```bash
# Run all platforms (requires Docker + cloud credentials)
cargo test --test smoke --features smoke-server,smoke-lambda,smoke-workers

# Server only (local Docker)
cargo test --test smoke --features smoke-server

# Cloud only
cargo test --test smoke --features smoke-lambda,smoke-workers

# Single platform
cargo test --test smoke --features smoke-lambda
```

## ServerHarness Implementation

### Responsibilities

1. **Deploy:** Start Docker Compose with MinIO + REST catalog (catalog mode) or MinIO only (plain Parquet mode)
2. **Health checks:** Wait for services to be ready before returning
3. **Send signals:** POST OTLP data to `http://localhost:4318`
4. **Verify execution:** Check server logs for errors via Docker logs API
5. **DuckDB verification:** Configure verifier with MinIO endpoint + REST catalog
6. **Cleanup:** Stop and remove containers

### Core Structure

```rust
pub struct ServerHarness {
    catalog_mode: CatalogMode,
    compose_project_name: String, // Unique per test for isolation
    namespace: String,
}

impl ServerHarness {
    pub async fn new(catalog_mode: CatalogMode) -> Result<Self> {
        // Generate unique project name for isolation
        let project_name = format!("otlp2parquet-test-{}", uuid::Uuid::new_v4().simple());

        Ok(Self {
            catalog_mode,
            compose_project_name: project_name,
            namespace: "otel".to_string(),
        })
    }
}
```

### Docker Compose Management

```rust
async fn deploy(&self) -> Result<DeploymentInfo> {
    let services = match self.catalog_mode {
        CatalogMode::Enabled => vec!["minio", "rest", "otlp2parquet"],
        CatalogMode::None => vec!["minio", "otlp2parquet"],
    };

    // Start with unique project name for isolation
    Command::new("docker")
        .args(["compose", "-p", &self.compose_project_name, "up", "-d"])
        .args(&services)
        .env("CATALOG_MODE", if self.catalog_mode == CatalogMode::Enabled {
            "iceberg"
        } else {
            "none"
        })
        .output().await?;

    // Health check loop
    self.wait_for_health("http://localhost:4318/health").await?;

    Ok(DeploymentInfo {
        endpoint: "http://localhost:4318".to_string(),
        catalog_endpoint: "http://localhost:8181".to_string(),
        bucket: "otlp".to_string(),
        resource_ids: HashMap::from([
            ("compose_project".to_string(), self.compose_project_name.clone())
        ]),
    })
}
```

### Cleanup

```rust
async fn cleanup(&self) -> Result<()> {
    // Always clean up, even if tests fail
    let _ = Command::new("docker")
        .args(["compose", "-p", &self.compose_project_name, "down", "-v"])
        .output().await;
    Ok(())
}
```

## Unified Test Structure

### Test Matrix

Each test case runs against all enabled platforms, both catalog modes:
- Server + Iceberg
- Server + Plain Parquet
- Lambda + Iceberg (S3 Tables)
- Lambda + Plain Parquet
- Workers + R2 Data Catalog
- Workers + Plain Parquet

Total: **18 test cases** (3 test scenarios × 3 platforms × 2 catalog modes)

### Test Pattern

```rust
// Helper macro to generate tests for all platforms
macro_rules! smoke_test {
    ($test_name:ident, $test_fn:expr) => {
        #[cfg(feature = "smoke-server")]
        mod server {
            use super::*;

            #[tokio::test]
            async fn $test_name_with_catalog() -> Result<()> {
                let harness = ServerHarness::new(CatalogMode::Enabled).await?;
                $test_fn(&harness).await
            }

            #[tokio::test]
            async fn $test_name_without_catalog() -> Result<()> {
                let harness = ServerHarness::new(CatalogMode::None).await?;
                $test_fn(&harness).await
            }
        }

        #[cfg(feature = "smoke-lambda")]
        mod lambda {
            use super::*;

            #[tokio::test]
            async fn $test_name_with_catalog() -> Result<()> {
                let harness = LambdaHarness::from_env().await?;
                $test_fn(&harness).await
            }

            #[tokio::test]
            async fn $test_name_without_catalog() -> Result<()> {
                // TODO: Implement plain Parquet mode for Lambda
                Ok(())
            }
        }

        #[cfg(feature = "smoke-workers")]
        mod workers {
            use super::*;

            #[tokio::test]
            async fn $test_name_with_catalog() -> Result<()> {
                let harness = WorkersHarness::from_env(CatalogMode::Enabled)?;
                $test_fn(&harness).await
            }

            #[tokio::test]
            async fn $test_name_without_catalog() -> Result<()> {
                let harness = WorkersHarness::from_env(CatalogMode::None)?;
                $test_fn(&harness).await
            }
        }
    };
}

// Define test logic once, runs everywhere
async fn test_logs_pipeline(harness: &dyn SmokeTestHarness) -> Result<()> {
    let info = harness.deploy().await?;
    harness.send_signals(&info.endpoint).await?;

    let status = harness.verify_execution().await?;
    assert!(status.is_healthy(), "Found errors: {:?}", status.sample_errors);

    let verifier = harness.duckdb_verifier();
    let report = verifier.verify("otel").await?;
    assert!(report.row_counts.get("otel_logs").unwrap_or(&0) > &0);

    harness.cleanup().await?;
    Ok(())
}

// Generate all platform variants
smoke_test!(logs_pipeline, test_logs_pipeline);
smoke_test!(metrics_pipeline, test_metrics_pipeline);
smoke_test!(traces_pipeline, test_traces_pipeline);
```

## DuckDB Verification Updates

### CatalogType Enum Changes

```rust
pub enum CatalogType {
    StandardREST,  // Generic Iceberg REST catalog (apache/iceberg-rest-fixture)
    S3Tables,      // AWS S3 Tables
    R2Catalog,     // Cloudflare R2 Data Catalog
    PlainParquet,  // NEW: No catalog, direct Parquet files
}
```

### Verification Logic

```rust
impl DuckDBVerifier {
    fn generate_verification_script(&self, namespace: &str) -> Result<String> {
        let mut script = String::new();

        // Install extensions
        script.push_str("INSTALL iceberg; LOAD iceberg;\n");

        // Configure storage credentials
        self.add_storage_credentials(&mut script)?;

        // Attach catalog OR direct Parquet scanning
        match self.catalog_type {
            CatalogType::StandardREST => {
                script.push_str(&format!(
                    "ATTACH '{}' AS cat (TYPE ICEBERG, ...);\n",
                    self.catalog_endpoint
                ));
                script.push_str(&format!(
                    "SELECT COUNT(*) FROM cat.{}.otel_logs;\n",
                    namespace
                ));
            }
            CatalogType::PlainParquet => {
                // Direct Parquet scanning without catalog
                let parquet_path = self.get_parquet_scan_path()?;
                script.push_str(&format!(
                    "SELECT COUNT(*) FROM read_parquet('{}');\n",
                    parquet_path
                ));
            }
            // S3Tables, R2Catalog same as before
        }

        Ok(script)
    }

    fn get_parquet_scan_path(&self) -> Result<String> {
        match &self.storage_config.backend {
            StorageBackend::S3 { bucket, endpoint, .. } => {
                if let Some(_) = endpoint {
                    // MinIO: s3://bucket/logs/**/*.parquet
                    Ok(format!("s3://{}/logs/**/*.parquet", bucket))
                } else {
                    // Real S3
                    Ok(format!("s3://{}/logs/**/*.parquet", bucket))
                }
            }
            StorageBackend::R2 { bucket, .. } => {
                Ok(format!("r2://{}/logs/**/*.parquet", bucket))
            }
        }
    }
}
```

**Key Insight:** Plain Parquet mode verification uses direct DuckDB Parquet scanning with glob patterns instead of Iceberg catalog attachment.

## Migration Path

### Changes

1. **Add new files:**
   - `tests/smoke/server.rs` - ServerHarness implementation
   - `tests/smoke.rs` - Unified test cases

2. **Update existing:**
   - `tests/smoke/mod.rs` - Add `CatalogType::PlainParquet`, update DuckDB verifier
   - `tests/smoke/lambda.rs` - Minor updates for consistency
   - `tests/smoke/workers.rs` - Minor updates for consistency

3. **Remove old files:**
   - `tests/e2e_docker.rs` - Replaced by unified smoke tests
   - `scripts/test-e2e.sh` - Replaced by cargo commands
   - `scripts/verify-duckdb.sh` - Optional (DuckDB verification built into harness)

### Makefile Updates

```makefile
test-smoke: ## Run smoke tests for all platforms (requires Docker + credentials)
    cargo test --test smoke --features smoke-server,smoke-lambda,smoke-workers

test-smoke-server: ## Run server smoke tests only (requires Docker)
    cargo test --test smoke --features smoke-server

test-e2e: test-smoke-server ## Alias for backwards compatibility

test-e2e-iceberg: test-smoke-server ## Alias (both modes now run by default)
```

### CI Updates

Update `.github/workflows/*.yml`:
- Replace `./scripts/test-e2e.sh` → `cargo test --test smoke --features smoke-server`
- Replace `TEST_ICEBERG=1 ./scripts/test-e2e.sh` → Same command (both modes always run)

## Implementation Checklist

- [ ] Add `tests/smoke/server.rs` with ServerHarness
- [ ] Add `tests/smoke.rs` with unified test cases and macro
- [ ] Update `tests/smoke/mod.rs` with PlainParquet support
- [ ] Add `CatalogMode` enum to shared types
- [ ] Implement DuckDB plain Parquet scanning
- [ ] Update Lambda harness for plain Parquet mode
- [ ] Update Workers harness (already supports both modes)
- [ ] Delete `tests/e2e_docker.rs`
- [ ] Delete `scripts/test-e2e.sh`
- [ ] Update Makefile targets
- [ ] Update CI workflows
- [ ] Test all 18 test combinations
- [ ] Verify DuckDB verification for all modes
- [ ] Update documentation

## Success Criteria

1. ✅ Single command runs all platform tests: `cargo test --test smoke --features smoke-server,smoke-lambda,smoke-workers`
2. ✅ Adding new test case automatically creates 6 test variants (3 platforms × 2 modes)
3. ✅ All existing e2e_docker.rs test coverage preserved
4. ✅ DuckDB verification works for all platform+mode combinations
5. ✅ Cleanup handles failures gracefully across all platforms

## Benefits

- **Consistency:** Same test patterns, verification depth across all platforms
- **Ergonomics:** Single command, feature flags for flexibility
- **Maintainability:** Add test case once, runs everywhere
- **Rigor:** DuckDB verification mandatory for all platforms
- **Simplicity:** Pure Rust, no bash orchestration needed
