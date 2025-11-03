# otlp2parquet - Agentic Coding Instructions
**Universal OTel Logs, Metrics & Traces Ingestion Pipeline (Rust)**

## Mission
Build a Rust binary that ingests OpenTelemetry logs, metrics, and traces via OTLP HTTP (protobuf/JSON/JSONL), converts to Arrow RecordBatch, writes Parquet files to object storage. Must compile to <3MB compressed WASM for Cloudflare Workers free plan AND native binary for AWS Lambda.

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

### Build & Deployment
- **Document tradeoffs** made for size
- **Test all feature combinations** - `make check` and `make test` cover all platforms
- **WASM optimization flags** - Already configured in Makefile with nontrapping-float-to-int
