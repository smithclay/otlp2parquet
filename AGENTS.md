# otlp2parquet - Agentic Coding Instructions
**Universal OTel Logs, Metrics & Traces Ingestion Pipeline (Rust)**

## Mission
Build a Rust workspace that ingests OpenTelemetry logs, metrics, and traces via OTLP HTTP (protobuf/JSON/JSONL), converts to Arrow RecordBatch, writes Parquet files to object storage with optional Apache Iceberg catalog integration. Must compile to <3MB compressed WASM for Cloudflare Workers free plan AND native binary for AWS Lambda.

---

## Critical Constraints

1. **Binary Size: <3MB compressed for CF Workers**
   - Strip ALL unnecessary features
   - Use `default-features = false` everywhere
   - Profile with `twiggy` to track bloat
   - Target: 2.5MB compressed to have headroom

2. **Schema: ClickHouse-compatible**
   - Use exact column names from ClickHouse OTel exporter
   - PascalCase naming convention
   - Extract common resource attributes to dedicated columns
   - Metrics: Separate schema per metric type (Gauge, Sum, Histogram, ExponentialHistogram, Summary)
   - See schema specification below

3. **Platform Detection: Auto-detect at runtime**
   - CF Workers: `CF_WORKER` env var
   - Lambda: `AWS_LAMBDA_FUNCTION_NAME` env var
   - Server (default): neither present

4. **Storage: Apache OpenDAL unified abstraction**
   - Server (default): S3, R2, Filesystem, GCS, Azure (configurable via env vars)
   - Lambda: S3 only (event-driven constraint)
   - CF Workers: R2 only (WASM constraint)
   - **Philosophy**: Leverage mature external abstractions vs NIH

5. **Configuration: TOML + Environment Variables**
   - `config.toml` for structured config (server, batch, storage, iceberg)
   - Environment variables with `OTLP2PARQUET_` prefix override TOML
   - Platform-specific defaults auto-detected at runtime

6. **Workspace Architecture**
   - `otlp2parquet-core` - Schema definitions and core types
   - `otlp2parquet-batch` - In-memory batching logic
   - `otlp2parquet-config` - Configuration parsing and defaults
   - `otlp2parquet-writer` - Unified writer abstraction with icepick integration
   - `otlp2parquet-proto` - OTLP protobuf definitions
   - Platform-specific: `otlp2parquet-cloudflare`, `otlp2parquet-lambda`, `otlp2parquet-server`
   - External dependency: `icepick` - Iceberg/Parquet writer library (not in workspace)

   **Architecture Change**: Previously used separate `otlp2parquet-storage` and `otlp2parquet-iceberg` crates. Now consolidated into single `otlp2parquet-writer` crate that uses `icepick` for all Parquet writing and optional catalog operations.

## Supported Signals

### ✅ Logs
- Full OTLP logs ingestion (protobuf, JSON, JSONL)
- Single Parquet schema per batch
- Partition: `logs/{service}/year={year}/month={month}/day={day}/hour={hour}/file.parquet`

### ✅ Metrics
- Full OTLP metrics ingestion (protobuf, JSON, JSONL)
- **5 separate schemas** - one per metric type for optimal query performance
  - Gauge - instant measurements
  - Sum - cumulative/delta aggregations with temporality
  - Histogram - distributions with explicit buckets
  - ExponentialHistogram - distributions with exponential buckets
  - Summary - quantile-based distributions
- Partition: `metrics/{type}/{service}/year={year}/month={month}/day={day}/hour={hour}/file.parquet`
- Each metric type written to its own file for schema homogeneity

### ✅ Traces
- Full OTLP traces ingestion (protobuf, JSON, JSONL)
- Single Parquet schema per batch with ClickHouse-compatible fields
- Partition: `traces/{service}/year={year}/month={month}/day={day}/hour={hour}/file.parquet`
- Includes spans with events, links, attributes, and status

## Apache Iceberg Integration

**Optional layer on top of Parquet files** - Provides ACID transactions, schema evolution, and faster queries via `icepick` library
- **Implementation**: Uses [icepick](https://crates.io/crates/icepick) for unified Parquet writing and catalog operations
- **Platforms**:
  - Lambda: S3 Tables catalog (ARN-based configuration)
  - Server: Nessie, Glue, or plain Parquet (REST catalog API)
  - Cloudflare Workers: R2 Data Catalog (WASM-compatible)
- **Configuration**: Via `config.toml` `[iceberg]` section or `OTLP2PARQUET_ICEBERG_*` env vars
- **Behavior**: Atomic write-and-commit via icepick (Parquet file + catalog metadata)
- **Resilience**: Warn-and-succeed pattern - catalog failures log warnings but Parquet files are still written
- **Tables**: One per schema (logs, traces, 5 metric types)

### AWS S3 Tables (Lambda - Recommended)

AWS S3 Tables provides a fully-managed Iceberg catalog service with simplified ARN-based configuration:

**Configuration:**
- `OTLP2PARQUET_ICEBERG_BUCKET_ARN`: S3 Tables bucket ARN
  - Format: `arn:aws:s3tables:region:account-id:bucket/bucket-name`
  - Example: `arn:aws:s3tables:us-west-2:123456789012:bucket/my-otlp-bucket`
- Auto-detected when ARN is provided
- No REST endpoint configuration needed

**IAM Permissions Required:**
- `s3tables:GetTable`, `s3tables:GetTableMetadataLocation`
- `s3tables:CreateTable`, `s3tables:UpdateTableMetadataLocation`
- `s3tables:GetNamespace`, `s3tables:CreateNamespace`
- `s3:PutObject`, `s3:GetObject`, `s3:DeleteObject`, `s3:ListBucket`

**Benefits:**
- Simplified configuration (just ARN)
- Managed service with automatic compaction
- Native S3 integration
- CloudFormation support

### AWS Glue Iceberg REST Catalog (Server - Alternative)

AWS Glue provides a fully-managed Iceberg REST catalog for self-hosted server deployments:

**Configuration Requirements:**
- `OTLP2PARQUET_ICEBERG_REST_URI`: `https://glue.<region>.amazonaws.com/iceberg`
- `OTLP2PARQUET_ICEBERG_WAREHOUSE`: AWS account ID (Glue catalog ID)
- `OTLP2PARQUET_ICEBERG_NAMESPACE`: Glue database name (e.g., `otel`)
- `OTLP2PARQUET_ICEBERG_DATA_LOCATION`: S3 bucket for data (e.g., `s3://my-data-bucket`)

**IAM Permissions Required:**
- `glue:GetDatabase`, `glue:GetTable`, `glue:CreateTable`, `glue:UpdateTable`
- `lakeformation:GetDataAccess` for Lake Formation integration
- Standard S3 permissions for data bucket

## Notes for AI Agent

### Development Workflow
- **Use Makefile targets** - Comprehensive build automation is available
  - `make dev` for quick iterations
  - `make pre-commit` before committing
  - `make wasm-full` for complete WASM pipeline
  - `make help` to see all targets
- **Run pre-commit hooks** - Use `make pre-commit` or `uvx pre-commit run --all-files`
- **Fix clippy warnings** - Zero warnings policy enforced (`make clippy`)

### Commit Message Guidelines
- **Use Conventional Commits** - All commit messages MUST follow the [Conventional Commits](https://www.conventionalcommits.org/) specification
- **Format**: `<type>[optional scope]: <description>`
  - Examples: `feat: add histogram support`, `fix: resolve memory leak in parser`, `docs: update deployment guide`
- **Common types**:
  - `feat`: New feature
  - `fix`: Bug fix
  - `docs`: Documentation only changes
  - `style`: Formatting, missing semicolons, etc (no code change)
  - `refactor`: Code change that neither fixes a bug nor adds a feature
  - `perf`: Performance improvement
  - `test`: Adding or updating tests
  - `build`: Changes to build system or dependencies
  - `ci`: Changes to CI configuration
  - `chore`: Other changes that don't modify src or test files
- **Enforcement**: Pre-commit hook validates commit message format automatically

### Code Quality & Performance
- **Prioritize binary size** over features
- **Test size continuously** - use `make wasm-size` after changes
- **Use `default-features = false`** everywhere
- **Minimize allocations** in hot path
- **Prefer `&str` over `String`** where possible
- **Use `Arc` for shared data** (schema, config)
- **Profile before optimizing** - `make wasm-profile` to measure
- **HTTP client**: `reqwest` used for both WASM and native (unified)
- **Iceberg REST**: Thin custom client to minimize dependencies and binary size

### Logging & Error Handling
- **Use tracing macros exclusively** - `tracing::info!()`, `tracing::warn!()`, `tracing::error!()`, `tracing::debug!()`
- **NEVER use println!/eprintln!** in production code (lambda, server, iceberg crates)
  - Exception: Test code, build scripts, and examples are OK
  - Rationale: Production needs structured logging for CloudWatch/observability
- **Avoid .expect()/.unwrap()** in production code paths
  - Use `?` operator with `.context()` for error propagation
  - Exception: Test setup code and infallible operations are OK
  - Rationale: Panics crash Lambda functions and break production services
- **Structured logging** - All production logging must use tracing for proper observability
  - `info!()` - Normal operational events (startup, shutdown, config)
  - `warn!()` - Degraded operations that succeed (catalog failures with fallback)
  - `error!()` - Request failures, validation errors, storage errors
  - `debug!()` - Verbose diagnostic information (not shown in production by default)
- **Panic-free production** - All errors should be propagated with Result types, not panicked
- **Error context** - Use `.context()` or `.map_err()` to add meaningful error messages

### Build & Deployment
- **Document tradeoffs** made for size
- **Test all feature combinations** - `make check` and `make test` cover all platforms
- **WASM optimization flags** - Already configured in Makefile with nontrapping-float-to-int

### Working with icepick Integration

The `otlp2parquet-writer` crate provides a unified `OtlpWriter` trait implemented by `IcepickWriter`. This abstraction handles:

**Core Functionality:**
- Parquet file writing via icepick's Arrow → Parquet conversion
- Optional catalog commits (S3 Tables, Nessie, R2 Data Catalog)
- Platform-specific catalog initialization
- Warn-and-succeed pattern for catalog failures

**Platform-Specific Initialization:**
- **Lambda**: S3 Tables catalog detected via `OTLP2PARQUET_ICEBERG_BUCKET_ARN` environment variable
- **Cloudflare Workers**: R2 Data Catalog when `OTLP2PARQUET_CATALOG_TYPE="r2"` is set
- **Server**: Configurable catalog (Nessie REST, Glue, or plain Parquet)

**Error Handling Pattern:**
- Catalog operations are non-blocking (warn-and-succeed)
- Write failures are logged but don't prevent Parquet file creation
- Data durability prioritized over catalog consistency
- Parquet files always written first, catalog commit is best-effort

**Testing:**
- E2E tests (core): `make test-e2e` - Tests plain Parquet writing
- E2E tests (Iceberg): `make test-e2e-iceberg` - Tests with Nessie catalog
- WASM build: `make wasm-full` - Full WASM pipeline with optimizations
- WASM size check: `make wasm-size` - Verify binary stays under 3MB limit

**WASM Constraints:**
- S3 Tables catalog NOT available on WASM (Lambda-only, uses native AWS SDK)
- R2 Data Catalog IS available on WASM (Cloudflare Workers, WASM-compatible)
- Plain Parquet writing always available on all platforms
