# otlp2parquet Testing Guide

Comprehensive testing guide for all test types across all platforms.

## Test Hierarchy

```
Unit Tests (fast, isolated)
  ↓
Docker Integration Tests (local, full-stack)
  ↓
Platform Smoke Tests (CI-only, real cloud)
```

## Test Types

### 1. Unit Tests
**Location**: Inline `#[cfg(test)]` modules across workspace crates
**Purpose**: Fast, isolated component testing
**Run**: `make test`

**Coverage**:
- Format conversion (protobuf ↔ JSON ↔ Arrow)
- Schema validation
- Batching logic
- Configuration parsing
- Platform detection

**Example**:
```bash
make test                    # All unit tests
make test-verbose            # With output
cargo test -p otlp2parquet-core  # Specific crate
```

### 2. Docker Integration Tests
**Location**: `tests/e2e_docker.rs`
**Purpose**: Full-stack validation with local services
**Run**: `make test-e2e-iceberg`

**Architecture**:
```
docker-compose up (MinIO + REST catalog + otlp2parquet)
  ↓
Send OTLP HTTP requests (logs, metrics, traces)
  ↓
Verify Parquet files in MinIO
  ↓
Verify Iceberg tables in REST catalog
  ↓
DuckDB verification (row counts, schema)
```

**Commands**:
```bash
make test-e2e                # Core tests only (skip Iceberg)
make test-e2e-iceberg        # Core + Iceberg catalog tests
make test-e2e-debug          # Preserve containers for debugging
```

**Services**:
- **MinIO** (localhost:9000): S3-compatible storage
- **REST Catalog** (localhost:8181): Nessie Iceberg catalog
- **otlp2parquet** (localhost:4318): Server binary

### 3. Platform Smoke Tests (CI-Only)
**Location**: `tests/smoke.rs` + `tests/smoke/`
**Purpose**: Real cloud platform validation
**Run**: `make smoke-lambda` or `make smoke-workers`

#### Lambda + S3 Tables
**Test flow**:
1. Deploy CloudFormation stack (Lambda + S3 Tables)
2. Send OTLP signals to Function URL
3. Verify CloudWatch Logs (no errors)
4. DuckDB verification (S3 Tables catalog)
5. Cleanup stack

**Commands**:
```bash
# Prerequisites
export AWS_REGION=us-west-2
export SMOKE_TEST_DEPLOYMENT_BUCKET=my-bucket
make build-lambda

# Run tests
make smoke-lambda

# Or with cargo
cargo test --test smoke --features smoke-lambda -- lambda
```

**See**: [docs/testing/lambda-smoke-tests.md](lambda-smoke-tests.md)

#### Cloudflare Workers + R2
**Test flow**:
1. Deploy Workers script (wrangler)
2. Send OTLP signals to Workers URL
3. Verify Workers logs (no errors)
4. DuckDB verification (R2 Catalog, if configured)
5. Cleanup Workers script

**Commands**:
```bash
# Prerequisites
export CLOUDFLARE_API_TOKEN=...
export CLOUDFLARE_ACCOUNT_ID=...
make wasm-compress

# Run tests
make smoke-workers

# Or with cargo
cargo test --test smoke --features smoke-workers -- workers
```

## Quick Reference

### Local Development (No Cloud)
```bash
make test-full    # Unit tests + Docker integration
```

### CI Complete Suite
```bash
make test-ci      # Unit + Docker + Lambda + Workers
```

### Individual Test Suites
```bash
make test                  # Unit tests only
make test-e2e-iceberg      # Docker integration
make smoke-lambda          # Lambda smoke tests
make smoke-workers         # Workers smoke tests
make smoke-all             # All smoke tests
```

## Makefile Targets

| Target | Description | Cloud Required |
|--------|-------------|----------------|
| `test` | Unit tests across all crates | No |
| `test-e2e` | Docker integration (core) | No |
| `test-e2e-iceberg` | Docker integration (full) | No |
| `test-e2e-debug` | Docker with container preservation | No |
| `test-all` | Unit + Docker core | No |
| `test-full` | Unit + Docker full | No |
| `smoke-lambda` | Lambda + S3 Tables | AWS |
| `smoke-workers` | Workers + R2 | Cloudflare |
| `smoke-all` | All platform smoke tests | AWS + CF |
| `test-ci` | Complete test suite | AWS + CF |

## CI/CD Integration

### GitHub Actions Workflows

#### `.github/workflows/ci.yml`
**Runs on**: Every PR and push to main
**Jobs**:
- Format check (`make fmt-check`)
- Clippy lints (`make clippy`)
- Unit tests (`make test`)
- Build all platforms (`make build-release`)
- Docker integration (`make test-e2e-iceberg`)
- WASM size check (`make wasm-compress`)

#### `.github/workflows/smoke-tests.yml`
**Runs on**:
- Push to main branch (always)
- PRs with `test-lambda` or `test-workers` labels
- Manual workflow dispatch

**Jobs**:
- **lambda-smoke**: Lambda + S3 Tables tests
- **workers-smoke**: Cloudflare Workers + R2 tests

**Conditional execution**: Only runs when credentials available (main branch or labeled PRs)

## Test Data

### Location
`testdata/` directory (committed to repo)

### Generation
```bash
uvx python scripts/generate_testdata.py
```

**Generates**:
- Logs: `logs.pb`, `logs.json`, `logs.jsonl`
- Metrics: All 5 types × 3 formats (protobuf, JSON, JSONL)
- Traces: `traces.pb`, `traces.json`, `traces.jsonl`
- Invalid data: `testdata/invalid/` for negative tests

### Usage
All tests use the same canonical fixtures via `TestDataSet::load()` (smoke tests) or `include_bytes!()` (unit tests).

## DuckDB Verification

### Purpose
Universal verification layer across all catalogs:
- Docker: Nessie REST catalog + MinIO
- Lambda: S3 Tables catalog + AWS S3
- Workers: R2 Data Catalog + Cloudflare R2

### How It Works
```sql
-- 1. Connect to storage (S3/R2/MinIO)
CREATE SECRET storage_secret (TYPE S3, ...);

-- 2. Attach Iceberg catalog
ATTACH 's3://warehouse/' AS iceberg_catalog (
    TYPE ICEBERG,
    ENDPOINT 'http://catalog:8181'
);

-- 3. Verify tables
SELECT table_name FROM iceberg_catalog.otel.information_schema.tables;
SELECT COUNT(*) FROM iceberg_catalog.otel.otel_logs;
```

### Scripts
- `scripts/verify-duckdb.sh`: Docker integration verification
- `tests/smoke/mod.rs`: Rust DuckDB verifier for smoke tests

## Platform-Specific Testing

### Server (Default)
**Unit tests**: `cargo test`
**Integration**: Docker e2e tests
**Backends tested**: MinIO (local), S3 (manual)
**Catalog tested**: Nessie REST (local)

### Lambda
**Build**: `make build-lambda`
**Integration**: Smoke tests (CI-only)
**Backends tested**: AWS S3
**Catalog tested**: S3 Tables

### Cloudflare Workers
**Build**: `make wasm-compress`
**Integration**: Smoke tests (CI-only)
**Backends tested**: Cloudflare R2
**Catalog tested**: R2 Data Catalog (optional)

## Debugging

### Docker Integration Tests
```bash
# Preserve containers
KEEP_CONTAINERS=1 make test-e2e-iceberg

# View logs
docker compose logs otlp2parquet
docker compose logs rest

# Inspect MinIO
docker compose exec minio mc ls minio/otlp

# Manual DuckDB verification
./scripts/verify-duckdb.sh
```

### Lambda Smoke Tests
```bash
# View CloudFormation stack
aws cloudformation describe-stacks --stack-name smoke-lambda-xyz

# View Lambda logs
aws logs tail /aws/lambda/smoke-lambda-xyz-ingest --follow

# List S3 Tables
aws s3tables list-tables --table-bucket-arn arn:...
```

### Workers Smoke Tests
```bash
# View Workers logs
wrangler tail smoke-workers-xyz

# List R2 objects
wrangler r2 object list otlp-smoke
```

## Cost Considerations

### Free Tier Compatible
- ✅ Unit tests (local)
- ✅ Docker integration tests (local)
- ✅ Workers smoke tests (free tier: 100K requests/day)

### Costs Money
- 💰 Lambda smoke tests (minimal: $0.20/GB-second)
- 💰 S3 Tables (storage + metadata)

### CI Optimization
- Smoke tests only run on main or labeled PRs
- Stack cleanup after tests (no lingering resources)
- Short Lambda timeout (60s) to prevent runaway costs

## Pre-Commit Checks

```bash
make pre-commit   # Format + Clippy + Tests
make fmt          # Format code
make clippy       # Lint code
make test-full    # Unit + Docker tests
```

## Best Practices

1. **Local development**: Use `make test-full` (unit + Docker)
2. **Before PR**: Run `make pre-commit`
3. **Cloud testing**: Add `test-lambda` or `test-workers` label to PR
4. **Debugging**: Use `KEEP_CONTAINERS=1` for Docker tests
5. **Cost control**: Smoke tests auto-cleanup, but verify manually if tests fail
