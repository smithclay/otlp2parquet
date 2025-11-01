# otlp2parquet - Agentic Coding Instructions
**Universal OTel Log Ingestion Pipeline (Rust)**

## Mission
Build a Rust binary that ingests OpenTelemetry logs via OTLP HTTP (protobuf), converts to Arrow RecordBatch, writes Parquet files to object storage. Must compile to <3MB compressed WASM for Cloudflare Workers free plan AND native binary for AWS Lambda.

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


## Notes for AI Agent

### Development Workflow
- **Use Makefile targets** - Comprehensive build automation is available
  - `make dev` for quick iterations
  - `make pre-commit` before committing
  - `make wasm-full` for complete WASM pipeline
  - `make help` to see all targets
- **Run pre-commit hooks** - Use `make pre-commit` or `uvx pre-commit run --all-files`
- **Fix clippy warnings** - Zero warnings policy enforced (`make clippy`)

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
