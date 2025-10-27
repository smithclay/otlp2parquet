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

## Workspace Structure

```
otlp2parquet/
├── Cargo.toml                # Workspace root
├── crates/
│   ├── otlp2parquet-core/    # Platform-agnostic logic
│   │   ├── otlp/             # OTLP parsing
│   │   ├── parquet/          # Parquet writing
│   │   └── storage/          # Storage trait
│   ├── otlp2parquet-runtime/ # Platform adapters
│   │   ├── cloudflare/       # CF Workers
│   │   ├── lambda/           # AWS Lambda
│   │   └── standalone/       # Local dev
│   └── otlp2parquet-proto/   # Generated protobuf
└── src/
    └── main.rs               # Universal entry point
```

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

**Current Phase:** Initial Setup

- [x] Workspace structure created
- [x] Cargo.toml with size optimizations
- [x] Arrow schema definition
- [x] Storage trait
- [ ] OTLP protobuf integration
- [ ] OTLP → Arrow conversion
- [ ] Parquet writer implementation
- [ ] HTTP handler
- [ ] Platform adapters
- [ ] Binary size optimization
- [ ] Testing

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
