# otlp2parquet - Agentic Coding Instructions
**Universal OTel Log Ingestion Pipeline (Rust)**

## Mission
Build a Rust binary that ingests OpenTelemetry logs via OTLP (HTTP/gRPC), converts to Arrow RecordBatch, writes Parquet files to object storage. Must compile to <3MB compressed WASM for Cloudflare Workers free plan AND native binary for AWS Lambda.

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
   - Standalone: neither present

4. **Storage: Trait-based abstraction**
   - R2 for CF Workers
   - S3 for Lambda
   - Common interface

---

## Architecture

```
┌─────────────────────────────────────────┐
│  Platform Detection (runtime)           │
│  ├─ Cloudflare Workers → R2             │
│  ├─ Lambda → S3                         │
│  └─ Standalone → local/testing          │
└─────────────────────────────────────────┘
                  ↓
┌─────────────────────────────────────────┐
│  Protocol Layer                         │
│  ├─ HTTP: /v1/logs (protobuf/json)     │
│  └─ gRPC: LogsService/Export (Lambda)   │
└─────────────────────────────────────────┘
                  ↓
┌─────────────────────────────────────────┐
│  Core Processing (platform-agnostic)    │
│  ├─ Parse OTLP protobuf                 │
│  ├─ Convert to Arrow RecordBatch        │
│  ├─ Write Parquet (minimal features)    │
│  └─ Generate partition path             │
└─────────────────────────────────────────┘
                  ↓
┌─────────────────────────────────────────┐
│  Storage Trait                          │
│  ├─ R2Client (worker crate)             │
│  └─ S3Client (aws-sdk-s3)               │
└─────────────────────────────────────────┘
```

---

## Cargo Workspace Structure

```
otlp2parquet/
├── Cargo.toml                # Workspace root
├── crates/
│   ├── otlp2parquet-core/    # Platform-agnostic logic
│   │   ├── otlp/             # OTLP parsing
│   │   ├── parquet/          # Parquet writing (minimal)
│   │   └── storage/          # Storage trait
│   ├── otlp2parquet-runtime/ # Platform adapters
│   │   ├── cloudflare/       # CF Workers
│   │   ├── lambda/           # AWS Lambda
│   │   └── standalone/       # Local dev
│   └── otlp2parquet-proto/   # Generated protobuf
└── src/
    └── main.rs               # Universal entry point
```

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
tonic = { version = "0.12", default-features = false, optional = true, features = [
    "transport",          # Lambda only
    "codegen",
    "prost"
] }

# Platform-specific (behind features)
worker = { version = "0.4", optional = true }
aws-lambda-runtime = { version = "0.13", optional = true }
aws-sdk-s3 = { version = "1", optional = true, default-features = false, features = [
    "rustls"              # Smaller than native-tls
] }

# Core utilities - minimal
tokio = { version = "1", default-features = false, features = ["rt", "macros"] }
serde = { version = "1", default-features = false, features = ["derive"] }
anyhow = "1"
uuid = { version = "1", default-features = false, features = ["v4", "fast-rng"] }

[features]
default = []
cloudflare = ["worker"]
lambda = ["tonic", "aws-lambda-runtime", "aws-sdk-s3"]
grpc = ["tonic"]            # Can be disabled for size

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

### Phase 1: Foundation (Do First)

1. **Project Setup**
   ```bash
   cargo new --lib otlp2parquet
   cd otlp2parquet
   # Set up workspace structure
   # Configure Cargo.toml with size optimizations
   ```

2. **Generate OTLP Protobuf Code**
   ```rust
   // build.rs
   fn main() {
       tonic_build::configure()
           .build_server(true)
           .compile(
               &["proto/opentelemetry/proto/collector/logs/v1/logs_service.proto"],
               &["proto/"],
           )
           .unwrap();
   }
   ```

3. **Define Arrow Schema**
   - Implement `otel_logs_schema()` function
   - Test schema creation

4. **Platform Detection**
   ```rust
   // crates/runtime/src/lib.rs
   pub enum Platform {
       CloudflareWorkers,
       Lambda,
       Standalone,
   }

   pub fn detect() -> Platform {
       if std::env::var("CF_WORKER").is_ok() {
           Platform::CloudflareWorkers
       } else if std::env::var("AWS_LAMBDA_FUNCTION_NAME").is_ok() {
           Platform::Lambda
       } else {
           Platform::Standalone
       }
   }
   ```

### Phase 2: Core Processing

5. **OTLP → Arrow Conversion**
   ```rust
   // crates/core/src/otlp/to_arrow.rs

   use arrow::array::*;
   use arrow::record_batch::RecordBatch;
   use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;

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

6. **Minimal Parquet Writer**
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

7. **Partition Path Generation**
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

### Phase 3: Storage Layer

8. **Storage Trait**
   ```rust
   // crates/core/src/storage/mod.rs

   #[async_trait::async_trait]
   pub trait Storage: Send + Sync {
       async fn write(&self, path: &str, data: &[u8]) -> Result<()>;
   }
   ```

9. **R2 Implementation** (CF Workers)
   ```rust
   // crates/runtime/src/cloudflare/storage.rs

   use worker::*;

   pub struct R2Storage {
       bucket: Bucket,
   }

   #[async_trait::async_trait]
   impl Storage for R2Storage {
       async fn write(&self, path: &str, data: &[u8]) -> Result<()> {
           self.bucket.put(path, data).await?;
           Ok(())
       }
   }
   ```

10. **S3 Implementation** (Lambda)
    ```rust
    // crates/runtime/src/lambda/storage.rs

    use aws_sdk_s3::Client;

    pub struct S3Storage {
        client: Client,
        bucket: String,
    }

    #[async_trait::async_trait]
    impl Storage for S3Storage {
        async fn write(&self, path: &str, data: &[u8]) -> Result<()> {
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(path)
                .body(data.to_vec().into())
                .send()
                .await?;
            Ok(())
        }
    }
    ```

### Phase 4: Protocol Handlers

11. **HTTP Handler** (Both platforms)
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

12. **gRPC Handler** (Lambda only, optional)
    ```rust
    // crates/runtime/src/lambda/grpc.rs

    #[cfg(feature = "grpc")]
    use tonic::{Request, Response, Status};

    pub struct LogsServiceImpl {
        storage: Arc<dyn Storage>,
    }

    #[tonic::async_trait]
    impl LogsService for LogsServiceImpl {
        async fn export(
            &self,
            request: Request<ExportLogsServiceRequest>,
        ) -> Result<Response<ExportLogsServiceResponse>, Status> {
            // Similar to HTTP handler
        }
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
            Platform::Standalone => {
                runtime::standalone::main()
            }
        }
    }
    ```

---

## Size Optimization Checklist

After implementation, profile and optimize:

1. **Build with optimizations**
   ```bash
   cargo build --release --target wasm32-unknown-unknown
   wasm-opt -Oz -o optimized.wasm target/wasm32-unknown-unknown/release/otlp2parquet.wasm
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
   - Remove `tonic` from CF Workers build (HTTP only)
   - Use Arrow IPC instead of Parquet for CF Workers
   - Strip more arrow features
   - Consider custom protobuf parser (no prost)

4. **Feature flags for builds**
   ```bash
   # CF Workers - minimal
   cargo build --release --target wasm32-unknown-unknown \
     --no-default-features --features cloudflare

   # Lambda - full featured
   cargo build --release --target x86_64-unknown-linux-gnu \
     --features lambda,grpc
   ```

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

- [ ] CF Workers binary <3MB compressed
- [ ] Lambda binary compiles and deploys
- [ ] Parses OTLP protobuf correctly
- [ ] Generates ClickHouse-compatible Parquet
- [ ] Writes to R2 (CF) and S3 (Lambda)
- [ ] Platform detection works
- [ ] HTTP endpoint handles 1000 req/s
- [ ] Parquet files queryable in DuckDB
- [ ] End-to-end test passes

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

- **Prioritize binary size** over features
- **Test size continuously** - don't discover at the end
- **Use `default-features = false`** everywhere
- **Minimize allocations** in hot path
- **Prefer `&str` over `String`** where possible
- **Use `Arc` for shared data** (schema, config)
- **Profile before optimizing** - measure don't guess
- **Document tradeoffs** made for size
