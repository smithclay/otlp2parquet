# otlp2parquet

Universal OpenTelemetry Log Ingestion Pipeline

A Rust-based service that ingests OpenTelemetry logs via OTLP (HTTP/gRPC) and writes them as Parquet files to object storage. Designed to compile to both:

- **WASM** (<3MB compressed) for Cloudflare Workers (free tier)
- **Native binary** for AWS Lambda

## Features

- OTLP HTTP/gRPC endpoint for log ingestion
- ClickHouse-compatible Parquet schema
- Multi-platform support (Cloudflare Workers, AWS Lambda, Standalone)
- Time-based Hive partitioning
- Minimal binary size (<3MB compressed for WASM)
- R2 and S3 storage backends

## Architecture

**Philosophy (Fred Brooks):** "Conceptual integrity is the most important consideration in system design."

The project separates **essence** (OTLP→Parquet conversion) from **accident** (platform I/O). Each platform uses its native idioms:

- **Cloudflare Workers:** Single-threaded JavaScript-style execution (worker crate runtime)
- **Lambda:** Uses lambda_runtime's provided tokio
- **Standalone:** Simple blocking I/O with std::fs and std::net

```
┌─────────────────────────────────────────┐
│  Platform-Specific Entry Points         │
│  ├─ CF Workers: #[event(fetch)]         │
│  ├─ Lambda: lambda_runtime::run()       │
│  └─ Standalone: blocking HTTP server    │
└─────────────────────────────────────────┘
                  ↓
┌─────────────────────────────────────────┐
│  Protocol Layer (TODO)                  │
│  └─ HTTP: POST /v1/logs (protobuf)     │
└─────────────────────────────────────────┘
                  ↓
┌─────────────────────────────────────────┐
│  Core Processing (PURE - no I/O)       │
│  ├─ process_otlp_logs(bytes) -> bytes  │
│  ├─ Parse OTLP protobuf ✅              │
│  ├─ Convert to Arrow RecordBatch ✅     │
│  ├─ Write Parquet (Snappy) ✅           │
│  └─ Generate partition path ✅          │
└─────────────────────────────────────────┘
                  ↓
┌─────────────────────────────────────────┐
│  Platform-Specific Storage              │
│  ├─ R2Storage (async, worker runtime)  │
│  ├─ S3Storage (async, lambda tokio)    │
│  └─ LocalStorage (blocking, std::fs)   │
└─────────────────────────────────────────┘
```

## Workspace Structure

```
otlp2parquet/
├── Cargo.toml                # Workspace root
├── crates/
│   ├── otlp2parquet-core/    # ✅ Platform-agnostic logic (PURE)
│   │   ├── otlp/             # ✅ OTLP→Arrow conversion
│   │   ├── parquet/          # ✅ Parquet writing + partitioning
│   │   └── schema.rs         # ✅ Arrow schema (15 fields)
│   ├── otlp2parquet-runtime/ # 🚧 Platform adapters
│   │   ├── cloudflare.rs     # ✅ R2Storage (async)
│   │   ├── lambda.rs         # ✅ S3Storage (async)
│   │   └── standalone.rs     # ✅ LocalStorage (blocking)
│   └── otlp2parquet-proto/   # ✅ Generated protobuf (v1.3.2)
│       └── proto/            # ✅ OTLP proto files
└── src/
    └── main.rs               # ✅ Platform-specific entry points
```

**Note:** No shared Storage trait - each platform uses its native idioms directly.

## Building

### Cloudflare Workers (WASM)

```bash
# Install target
rustup target add wasm32-unknown-unknown

# Build with minimal features
cargo build --release \
  --target wasm32-unknown-unknown \
  --no-default-features \
  --features cloudflare

# Optimize
wasm-opt -Oz -o optimized.wasm target/wasm32-unknown-unknown/release/otlp2parquet.wasm
gzip -9 optimized.wasm

# Check size (must be <3MB)
ls -lh optimized.wasm.gz
```

### AWS Lambda

```bash
# Install cargo-lambda
cargo install cargo-lambda

# Build
cargo lambda build --release --features lambda

# Deploy
cargo lambda deploy
```

### Standalone (Development)

```bash
cargo build --release --features standalone
./target/release/otlp2parquet
```

## Development Status

**Current Phase:** Core Implementation Complete

### ✅ Completed (Phase 1-2)

- [x] Workspace structure created
- [x] Cargo.toml with size optimizations
- [x] Arrow schema definition (15 fields, ClickHouse-compatible)
- [x] OTLP protobuf integration (v1.3.2, code generation configured)
- [x] OTLP → Arrow conversion (ArrowConverter with all fields)
- [x] Parquet writer implementation (Snappy compression, minimal features)
- [x] Partition path generation (Hive-style time partitioning)
- [x] Platform-specific storage implementations (R2, S3, Local)
- [x] Brooks architecture refactor (pure core, platform-native runtimes)
- [x] Core processing function (`process_otlp_logs`)
- [x] CI/CD with protoc installation
- [x] Pre-commit hooks (fmt, clippy)

### 🚧 In Progress (Phase 3-4)

- [ ] HTTP protocol handlers
- [ ] Cloudflare Workers entry point (`#[event(fetch)]`)
- [ ] Lambda handler implementation
- [ ] Standalone HTTP server

### 📋 Planned (Phase 5)

- [ ] Binary size optimization and profiling
- [ ] End-to-end integration tests
- [ ] Load testing
- [ ] Deployment configurations

See [CLAUDE.md](./CLAUDE.md) for detailed implementation instructions.

## Size Optimization

Target: <3MB compressed WASM

Current optimizations:
- `opt-level = "z"` (size optimization)
- LTO enabled
- `default-features = false` for all dependencies
- Minimal feature flags
- Snappy compression only
- Strip symbols

## Schema

ClickHouse-compatible schema with PascalCase naming:

- Timestamps (Timestamp, ObservedTimestamp)
- Trace context (TraceId, SpanId, TraceFlags)
- Severity (SeverityText, SeverityNumber)
- Body
- Extracted attributes (ServiceName, ServiceNamespace, ServiceInstanceId)
- Scope (ScopeName, ScopeVersion)
- Maps (ResourceAttributes, LogAttributes)

## License

MIT OR Apache-2.0
