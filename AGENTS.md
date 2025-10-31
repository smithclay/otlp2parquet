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

---

## Architecture

**Philosophy (Fred Brooks):** "Conceptual integrity is the most important consideration in system design."

The architecture separates **essence** (pure OTLP→Parquet conversion) from **accident** (platform I/O, networking, runtime). We use **Apache OpenDAL** as a mature, battle-tested storage abstraction instead of building custom implementations.

### Core Principle: Default + Constrained Runtimes

**Server mode** is the default, full-featured implementation. Lambda and Cloudflare are **constrained runtime** special cases that use the same core processing logic but have platform-specific limitations.

```
┌─────────────────────────────────────────┐
│  Platform-Specific Entry Points         │
│  ├─ Server (default): Axum HTTP server │
│  │   Full-featured, multi-backend       │
│  ├─ Lambda: lambda_runtime::run()       │
│  │   Event-driven, S3 only             │
│  └─ Cloudflare: #[event(fetch)]        │
│      WASM-constrained, R2 only          │
└─────────────────────────────────────────┘
                  ↓
┌─────────────────────────────────────────┐
│  Protocol Layer (HTTP handlers)         │
│  └─ Parse HTTP request → OTLP bytes    │
└─────────────────────────────────────────┘
                  ↓
┌─────────────────────────────────────────┐
│  Core Processing (PURE - no I/O)       │
│  process_otlp_logs(bytes) -> bytes      │
│  ├─ Parse OTLP protobuf ✅              │
│  ├─ Convert to Arrow RecordBatch ✅     │
│  ├─ Write Parquet (Snappy) ✅           │
│  └─ Generate partition path ✅          │
└─────────────────────────────────────────┘
                  ↓
┌─────────────────────────────────────────┐
│  Unified Storage Layer (OpenDAL)        │
│  ├─ S3 (Lambda, Server)                │
│  ├─ R2 (Cloudflare, Server)            │
│  ├─ Filesystem (Server)                │
│  └─ GCS, Azure, etc. (Server-ready)    │
└─────────────────────────────────────────┘
```

---

## Cargo Workspace Structure

```
otlp2parquet/
├── Cargo.toml                # Workspace root
├── crates/
│   ├── otlp2parquet-core/    # ✅ PURE platform-agnostic logic
│   │   ├── otlp/             # ✅ OTLP→Arrow conversion
│   │   ├── parquet/          # ✅ Parquet writing + partitioning
│   │   └── schema.rs         # ✅ Arrow schema (15 fields)
│   ├── otlp2parquet-runtime/ # Platform adapters
│   │   ├── server.rs         # ✅ Default mode (Axum + multi-backend)
│   │   ├── lambda.rs         # ✅ Event-driven (OpenDAL S3)
│   │   ├── cloudflare.rs     # ✅ WASM mode (OpenDAL R2)
│   │   └── opendal_storage.rs # ✅ Unified storage abstraction
│   └── otlp2parquet-proto/   # ✅ Generated protobuf (v1.3.2)
│       └── proto/            # ✅ OpenTelemetry proto files
└── src/
    └── main.rs               # ✅ Platform-specific entry points
```

**Key Change:** Adopted Apache OpenDAL for unified storage - leverages mature external abstractions vs NIH syndrome.

---

## Dependency Configuration (CRITICAL)

### Cargo.toml - Aggressive Size Optimization

```toml
[workspace]
members = ["crates/*"]

[profile.release]
opt-level = "z"           # Optimize for size
lto = true                # Link-time optimization
codegen-units = 1         # Better optimization
strip = true              # Strip symbols
panic = "abort"           # Smaller panic handler

[profile.release.package."*"]
opt-level = "z"

[dependencies]
# Arrow/Parquet - MINIMAL features only
arrow = { version = "53", default-features = false, features = [
    "ipc"                 # Arrow IPC format (smaller than full)
] }
parquet = { version = "53", default-features = false, features = [
    "arrow",              # Arrow integration
    "snap",               # Snappy only (smallest compressor)
] }

# OTLP Protocol - minimal
prost = { version = "0.13", default-features = false, features = ["std"] }

# Platform-specific (behind features)
worker = { version = "0.4", optional = true }
aws-lambda-runtime = { version = "0.13", optional = true }

# Core utilities - minimal
tokio = { version = "1", default-features = false, features = ["rt", "macros"] }
serde = { version = "1", default-features = false, features = ["derive"] }
anyhow = "1"
uuid = { version = "1", default-features = false, features = ["v4", "fast-rng"] }

[features]
default = []
cloudflare = ["worker"]
lambda = ["aws-lambda-runtime"]

[build-dependencies]
prost-build = "0.13"
```

---

## Schema Definition (ClickHouse-Compatible)

```rust
// crates/core/src/schema.rs

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use std::sync::Arc;

pub fn otel_logs_schema() -> Schema {
    Schema::new(vec![
        // Timestamps - nanosecond precision, UTC
        Field::new(
            "Timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new(
            "ObservedTimestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),

        // Trace context
        Field::new("TraceId", DataType::FixedSizeBinary(16), false),
        Field::new("SpanId", DataType::FixedSizeBinary(8), false),
        Field::new("TraceFlags", DataType::UInt32, false),

        // Severity
        Field::new("SeverityText", DataType::Utf8, false),
        Field::new("SeverityNumber", DataType::Int32, false),

        // Body
        Field::new("Body", DataType::Utf8, false),

        // Resource attributes - extracted common fields
        Field::new("ServiceName", DataType::Utf8, false),
        Field::new("ServiceNamespace", DataType::Utf8, true),
        Field::new("ServiceInstanceId", DataType::Utf8, true),

        // Scope
        Field::new("ScopeName", DataType::Utf8, false),
        Field::new("ScopeVersion", DataType::Utf8, true),

        // Remaining attributes as Map<String, String>
        Field::new(
            "ResourceAttributes",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Utf8, true),
                    ].into()),
                    false,
                )),
                false,
            ),
            false,
        ),
        Field::new(
            "LogAttributes",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Utf8, true),
                    ].into()),
                    false,
                )),
                false,
            ),
            false,
        ),
    ])
}

// Common resource attribute keys to extract
pub const EXTRACTED_RESOURCE_ATTRS: &[&str] = &[
    "service.name",
    "service.namespace",
    "service.instance.id",
];
```

---

## Core Implementation Priorities

### Phase 1: Foundation ✅ COMPLETED

1. **Project Setup** ✅
   ```bash
   cargo new --lib otlp2parquet
   cd otlp2parquet
   # Set up workspace structure
   # Configure Cargo.toml with size optimizations
   ```

2. **Generate OTLP Protobuf Code** ✅
   ```rust
   // build.rs
   fn main() {
       prost_build::Config::new()
           .compile_protos(
               &["proto/opentelemetry/proto/collector/logs/v1/logs_service.proto"],
               &["proto/"],
           )
           .unwrap();
   }
   ```

3. **Define Arrow Schema** ✅
   - Implemented `otel_logs_schema()` function (15 fields)
   - Tests passing

4. **Platform Detection** ✅ (via conditional compilation in main.rs)
   ```rust
   // crates/runtime/src/lib.rs
   pub enum Platform {
       CloudflareWorkers,
       Lambda,
       Server,
   }

   pub fn detect() -> Platform {
       if std::env::var("CF_WORKER").is_ok() {
           Platform::CloudflareWorkers
       } else if std::env::var("AWS_LAMBDA_FUNCTION_NAME").is_ok() {
           Platform::Lambda
       } else {
           Platform::Server
       }
   }
   ```

### Phase 2: Core Processing ✅ COMPLETED

5. **OTLP → Arrow Conversion** ✅
   ```rust
   // crates/core/src/otlp/to_arrow.rs

   use arrow::array::*;
   use arrow::record_batch::RecordBatch;
   use opentelemetry_proto::collector::logs::v1::ExportLogsServiceRequest;

   pub struct ArrowConverter {
       schema: Schema,
       // Builders for each column
       timestamp_builder: TimestampNanosecondBuilder,
       severity_text_builder: StringBuilder,
       // ... etc
   }

   impl ArrowConverter {
       pub fn new() -> Self {
           // Initialize with schema
       }

       pub fn add_log_record(&mut self, record: &LogRecord) -> Result<()> {
           // Extract fields and append to builders
           // Handle attribute extraction
       }

       pub fn finish(self) -> Result<RecordBatch> {
           // Build RecordBatch from builders
       }
   }
   ```

6. **Minimal Parquet Writer** ✅
   ```rust
   // crates/core/src/parquet/writer.rs

   use parquet::arrow::ArrowWriter;
   use parquet::file::properties::WriterProperties;

   pub fn write_parquet(batch: RecordBatch) -> Result<Vec<u8>> {
       let mut buffer = Vec::new();

       let props = WriterProperties::builder()
           .set_compression(parquet::basic::Compression::SNAPPY)
           .set_dictionary_enabled(true)
           .build();

       let mut writer = ArrowWriter::try_new(
           &mut buffer,
           batch.schema(),
           Some(props),
       )?;

       writer.write(&batch)?;
       writer.close()?;

       Ok(buffer)
   }
   ```

7. **Partition Path Generation** ✅
   ```rust
   // crates/core/src/parquet/partition.rs

   pub fn generate_path(
       service_name: &str,
       timestamp: i64,
   ) -> String {
       let dt = /* convert timestamp to datetime */;
       format!(
           "logs/{}/year={}/month={:02}/day={:02}/hour={:02}/{}-{}.parquet",
           service_name,
           dt.year(),
           dt.month(),
           dt.day(),
           dt.hour(),
           uuid::Uuid::new_v4(),
           timestamp
       )
   }
   ```

### Phase 3: Storage Layer ✅ COMPLETED (OpenDAL)

**ARCHITECTURE DECISION:** Use Apache OpenDAL for unified storage abstraction.

**Philosophy:** Leverage mature, battle-tested external abstractions rather than building custom implementations.

We use **Apache OpenDAL (v0.54+)** as a unified storage layer across all platforms:
- **Cloudflare Workers:** OpenDAL S3 service → R2 (via S3-compatible endpoint)
- **AWS Lambda:** OpenDAL S3 service → S3 (replaces aws-sdk-s3)
- **Server (default):** OpenDAL supports S3, R2, Fs, GCS, Azure (configurable via env vars)

**Benefits:**
- ✅ Unified API reduces code duplication (3 implementations → 1)
- ✅ Battle-tested by 600+ GitHub projects (Apache graduated project)
- ✅ Handles S3/R2 compatibility quirks automatically
- ✅ Minimal binary size impact (+17KB / +2.4% for WASM)
- ✅ Zero-cost abstractions with proper async support per platform
- ✅ Future-proof: Easy to add GCS, Azure, etc. (40+ backends supported)

**Validation Results:**
- WASM size: 703KB → 720KB compressed (+17KB) ✅ Well under 3MB limit
- All platforms compile successfully ✅
- Tests passing ✅
- See `OPENDAL_VALIDATION_RESULTS.md` for full analysis

8. **OpenDAL Unified Storage** ✅
   ```rust
   // crates/runtime/src/opendal_storage.rs
   use opendal::{services, Operator};

   pub struct OpenDalStorage {
       operator: Operator,
   }

   impl OpenDalStorage {
       // S3 (for Lambda)
       pub fn new_s3(bucket: &str, region: &str, ...) -> Result<Self> {
           let builder = services::S3::default()
               .bucket(bucket)
               .region(region);
           let operator = Operator::new(builder)?.finish();
           Ok(Self { operator })
       }

       // R2 (for Cloudflare Workers)
       pub fn new_r2(bucket: &str, account_id: &str, ...) -> Result<Self> {
           let endpoint = format!("https://{}.r2.cloudflarestorage.com", account_id);
           Self::new_s3(bucket, "auto", Some(&endpoint), ...)
       }

       // Filesystem (for Server)
       pub fn new_fs(root: &str) -> Result<Self> {
           let builder = services::Fs::default().root(root);
           let operator = Operator::new(builder)?.finish();
           Ok(Self { operator })
       }

       pub async fn write(&self, path: &str, data: Vec<u8>) -> Result<()> {
           self.operator.write(path, data).await?;
           Ok(())
       }

       pub async fn read(&self, path: &str) -> Result<Vec<u8>> {
           let data = self.operator.read(path).await?;
           Ok(data.to_vec())
       }
   }
   ```

9. **Dependency Configuration** ✅
   ```toml
   # Cargo.toml - workspace dependencies
   opendal = { version = "0.54", default-features = false }

   # Platform-specific features (crates/otlp2parquet-runtime/Cargo.toml)
   [features]
   cloudflare = ["worker", "opendal/services-s3"]
   lambda = ["lambda_runtime", "opendal/services-s3"]
   server = ["opendal/services-fs", "opendal/services-s3", "axum", "tracing"]
   ```

10. **Removed Dependencies** 🗑️
    - ❌ `aws-sdk-s3` (replaced by OpenDAL S3)
    - ❌ `aws-config` (OpenDAL handles credentials automatically)
    - Result: Smaller binaries, faster compile times

### Phase 4: Protocol Handlers 🚧 IN PROGRESS

**Core Function Available:** `otlp2parquet_core::process_otlp_logs(bytes) -> Result<Vec<u8>>`

11. **HTTP Handler** ✅ (All platforms)
    ```rust
    // crates/core/src/http.rs

    pub async fn handle_otlp_http(
        body: Vec<u8>,
        storage: Box<dyn Storage>,
    ) -> Result<Response> {
        // Parse protobuf
        let request: ExportLogsServiceRequest =
            prost::Message::decode(&body[..])?;

        // Convert to Arrow
        let mut converter = ArrowConverter::new();
        for log in request.resource_logs {
            converter.add_log_record(&log)?;
        }
        let batch = converter.finish()?;

        // Write Parquet
        let parquet_bytes = write_parquet(batch)?;

        // Generate path
        let path = generate_path(/* ... */);

        // Store
        storage.write(&path, &parquet_bytes).await?;

        // Return success
        Ok(Response::success())
    }
    ```

### Phase 5: Platform Adapters

13. **Cloudflare Workers Entry**
    ```rust
    // crates/runtime/src/cloudflare/mod.rs

    use worker::*;

    #[event(fetch)]
    async fn main(req: Request, env: Env, _ctx: Context) -> Result<Response> {
        let bucket = env.bucket("LOGS_BUCKET")?;
        let storage = Box::new(R2Storage::new(bucket));

        match (req.method(), req.path().as_str()) {
            (Method::Post, "/v1/logs") => {
                let body = req.bytes().await?;
                handle_otlp_http(body, storage).await
            }
            _ => Response::error("Not Found", 404),
        }
    }
    ```

14. **Lambda Entry**
    ```rust
    // crates/runtime/src/lambda/mod.rs

    use aws_lambda_runtime::{service_fn, LambdaEvent};

    #[tokio::main]
    async fn main() -> Result<(), Error> {
        let config = aws_config::load_from_env().await;
        let s3_client = aws_sdk_s3::Client::new(&config);
        let storage = Arc::new(S3Storage::new(s3_client, bucket));

        lambda_runtime::run(service_fn(|event: LambdaEvent<Request>| {
            handler(event, storage.clone())
        })).await
    }
    ```

15. **Universal Main**
    ```rust
    // src/main.rs

    fn main() {
        match runtime::detect() {
            Platform::CloudflareWorkers => {
                // Entry handled by worker macro
            }
            Platform::Lambda => {
                runtime::lambda::main()
            }
            Platform::Server => {
                runtime::server::main()
            }
        }
    }
    ```

---

## Phase 6: Input Format Support

**Current Status:** Protobuf only ✅

### Supported Input Formats

**Currently Implemented:**
- ✅ **Protobuf (Binary)** - `application/x-protobuf`
  - OTLP's native format
  - Most efficient (smallest, fastest)
  - Fully implemented via `prost::Message::decode()`

**TODO - OTLP Compliance:**
- [ ] **JSON** - `application/json`
  - Required by OTLP specification
  - Use `prost-serde` for automatic protobuf ↔ JSON conversion
  - Parse `Content-Type` header and route to appropriate decoder
  - Larger payloads but easier debugging/testing

**TODO - Bonus Features:**
- [ ] **JSONL (Newline-Delimited JSON)**
  - Not part of OTLP spec, but common for bulk log ingestion
  - Parse each line as a separate `LogRecord`
  - Useful for importing from log files or streaming pipelines
  - Example: `{"resourceLogs":[...]}\n{"resourceLogs":[...]}\n`

### Implementation Plan

1. **Add JSON Support** (OTLP-compliant)
   ```toml
   # Cargo.toml
   prost-serde = { version = "0.13", optional = true }
   ```

   ```rust
   // crates/core/src/otlp/mod.rs
   pub enum InputFormat {
       Protobuf,
       Json,
       Jsonl,
   }

   pub fn parse_otlp_logs(bytes: &[u8], format: InputFormat) -> Result<ExportLogsServiceRequest> {
       match format {
           InputFormat::Protobuf => {
               ExportLogsServiceRequest::decode(bytes)
           }
           InputFormat::Json => {
               serde_json::from_slice(bytes)
           }
           InputFormat::Jsonl => {
               // Parse line-by-line and merge into single request
               parse_jsonl(bytes)
           }
       }
   }
   ```

2. **Update HTTP Handlers**
   ```rust
   // Detect format from Content-Type header
   let format = match content_type {
       "application/x-protobuf" => InputFormat::Protobuf,
       "application/json" => InputFormat::Json,
       "application/x-ndjson" | "application/jsonl" => InputFormat::Jsonl,
       _ => return error("Unsupported Content-Type"),
   };
   ```

3. **Size Impact Analysis**
   - `prost-serde`: ~50KB to binary
   - `serde_json`: Already included (used for responses)
   - Net impact: ~50KB for OTLP compliance ✅ Acceptable

---

## Size Optimization Checklist

After implementation, profile and optimize:

### Using Makefile (Recommended)

```bash
# Full WASM pipeline: build → optimize → compress → profile
make wasm-full

# Show WASM sizes and check 3MB limit
make wasm-size

# Profile binary size with twiggy
make wasm-profile
```

### Manual Commands

1. **Build with optimizations**
   ```bash
   cargo build --release --target wasm32-unknown-unknown --no-default-features --features cloudflare
   wasm-opt -Oz --enable-bulk-memory --enable-nontrapping-float-to-int \
     -o optimized.wasm target/wasm32-unknown-unknown/release/otlp2parquet.wasm
   gzip -9 optimized.wasm
   ls -lh optimized.wasm.gz  # Must be <3MB
   ```

2. **Profile with twiggy**
   ```bash
   cargo install twiggy
   twiggy top -n 20 target/wasm32-unknown-unknown/release/otlp2parquet.wasm
   # Identify largest dependencies
   ```

3. **If over 3MB, eliminate features:**
   - Use Arrow IPC instead of Parquet for CF Workers
   - Strip more arrow features
   - Consider custom protobuf parser (no prost)

4. **Feature flags for builds**
   ```bash
   # CF Workers - minimal (or use: make build-cloudflare)
   cargo build --release --target wasm32-unknown-unknown \
     --no-default-features --features cloudflare

   # Lambda - full featured (or use: make build-lambda)
   cargo build --release --no-default-features --features lambda
   ```

---

## Development Tooling

### Makefile - Build Automation

The project includes a comprehensive Makefile with Rust-idiomatic targets for all common development tasks.

**Quick Reference:**
```bash
make help          # Show all available targets
make dev           # Quick check + test (fast feedback loop)
make pre-commit    # Run fmt, clippy, test before committing
make ci            # Full CI pipeline locally
make wasm-full     # Complete WASM pipeline: build → optimize → compress → profile
```

**Installation:**
```bash
# Install all required tools automatically
make install-tools

# Or manually install prerequisites
rustup toolchain install stable
rustup component add rustfmt clippy
rustup target add wasm32-unknown-unknown
brew install binaryen  # macOS (provides wasm-opt)
cargo install twiggy   # WASM binary profiler
```

See `Makefile` for the complete list of targets.

### uv - Fast Python Package Manager

This project uses [uv](https://github.com/astral-sh/uv) for managing Python-based development tools. uv is an extremely fast Python package installer and resolver written in Rust.

**Installation:**
```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Verify installation
uv --version
```

**Usage:**
```bash
# Run a tool without installing it globally (recommended)
uvx pre-commit run --all-files

# Install a package in a virtual environment
uv pip install pre-commit

# Run a Python script with automatic dependency management
uv run script.py
```

### Pre-commit Hooks

Pre-commit hooks ensure code quality and consistency before commits. The project uses:

**Rust Hooks:**
- `cargo fmt` - Format Rust code with rustfmt
- `cargo clippy` - Lint Rust code with clippy (zero warnings policy)

**General Hooks:**
- YAML/TOML validation
- Trailing whitespace removal
- End-of-file fixer
- Large file detection
- Merge conflict detection

**Setup:**
```bash
# Install pre-commit hooks (one-time setup)
uvx pre-commit install

# Run hooks manually on all files
uvx pre-commit run --all-files

# Run hooks on staged files only
uvx pre-commit run

# Update hook versions
uvx pre-commit autoupdate
```

**Configuration:**
See `.pre-commit-config.yaml` for the complete hook configuration.

**Note:** Hooks run automatically on `git commit`. If a hook fails:
1. Review the changes made by auto-fixers (fmt)
2. Fix any issues reported by linters (clippy)
3. Stage the changes and commit again

---

## Testing Strategy

1. **Unit tests**: OTLP parsing, Arrow conversion, schema validation
2. **Integration tests**: End-to-end with mock storage
3. **Size tests**: Assert binary size < 3MB compressed
4. **Load tests**: Use `otel-cli` or custom generator
5. **Platform tests**:
   - Miniflare for CF Workers
   - cargo-lambda for Lambda

---

## Deployment

### Cloudflare Workers
```bash
# wrangler.toml
name = "otlp2parquet"
main = "build/worker.wasm"
compatibility_date = "2025-01-01"

[[r2_buckets]]
binding = "LOGS_BUCKET"
bucket_name = "otel-logs"

# Deploy
wrangler deploy
```

### AWS Lambda
```bash
# Build for Lambda
cargo lambda build --release --features lambda

# Deploy
cargo lambda deploy --iam-role arn:aws:iam::...

# Create function URL
aws lambda create-function-url-config \
  --function-name otlp2parquet \
  --auth-type NONE
```

---

## Success Criteria

**Core Functionality:**
- [x] Parses OTLP protobuf correctly ✅
- [ ] Parses OTLP JSON (OTLP spec compliance)
- [ ] Parses JSONL (bonus feature)
- [x] Generates ClickHouse-compatible Parquet ✅
- [x] Platform detection works ✅
- [x] End-to-end test passes ✅

**Platform Support:**
- [x] Cloudflare Workers implementation ✅
- [x] AWS Lambda implementation ✅
- [x] Server mode implementation (default) ✅
- [x] Writes to R2 (CF Workers) ✅
- [x] Writes to S3 (Lambda) ✅
- [x] Multi-backend storage (Server: S3/R2/Filesystem) ✅

**Performance & Size:**
- [ ] CF Workers binary <3MB compressed
- [ ] Lambda binary compiles and deploys
- [ ] HTTP endpoint handles 1000 req/s
- [ ] Parquet files queryable in DuckDB

**Architecture Quality:**
- [x] Core is PURE (no side effects) ✅
- [x] Partition paths in runtime layer ✅
- [x] Single, clear API ✅
- [x] No information loss in pipeline ✅

---

## Key Files to Generate

Priority order for AI agent:

1. `Cargo.toml` - workspace config with size optimizations
2. `crates/core/src/schema.rs` - Arrow schema definition
3. `build.rs` - protobuf generation
4. `crates/core/src/otlp/to_arrow.rs` - OTLP→Arrow conversion
5. `crates/core/src/parquet/writer.rs` - minimal Parquet writer
6. `crates/core/src/storage/mod.rs` - storage trait
7. `crates/runtime/src/cloudflare/mod.rs` - CF Workers entry
8. `crates/runtime/src/lambda/mod.rs` - Lambda entry
9. `src/main.rs` - universal entry point

---

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
