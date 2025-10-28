# otlp2parquet

OpenTelemetry ingestion pipeline that writes ClickHouse-compatible Parquet files to object storage.

**Built for multi-platform serverless deployment:** Compiles to <3MB WASM for Cloudflare Workers (free tier) or native binary for AWS Lambda.

## Why otlp2parquet?

- **Minimal footprint:** <3MB compressed WASM binary fits Cloudflare Workers free tier
- **ClickHouse-compatible:** Direct Parquet schema compatibility for seamless querying
- **Multi-platform:** Single codebase deploys to Cloudflare Workers, AWS Lambda, or standalone servers
- **Time-partitioned:** Automatic Hive-style partitioning for efficient querying

## Quick Start

> **One-click deployment coming soon:** Cloudflare Workers button + AWS CloudFormation template

For now, see [Development Setup](#development-setup) for manual installation.

## Usage

Once deployed, send OpenTelemetry logs to the `/v1/logs` endpoint:

### Send Logs (OTLP Protobuf)

```bash
# Using otel-cli (recommended)
otel-cli logs \
  --endpoint https://your-deployment.workers.dev/v1/logs \
  --protocol http/protobuf \
  --body "Application started successfully"

# Or with curl (raw protobuf)
curl -X POST https://your-deployment.workers.dev/v1/logs \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @logs.pb
```

### Send Logs (JSON)

> JSON support planned - protobuf only for now

```bash
# Coming soon
curl -X POST https://your-deployment.workers.dev/v1/logs \
  -H "Content-Type: application/json" \
  -d '{
    "resourceLogs": [{
      "resource": {
        "attributes": [{"key": "service.name", "value": {"stringValue": "my-service"}}]
      },
      "scopeLogs": [{
        "logRecords": [{
          "timeUnixNano": "1234567890000000000",
          "severityText": "INFO",
          "body": {"stringValue": "Hello from my app"}
        }]
      }]
    }]
  }'
```

### Query Results

Parquet files are written to object storage with Hive-style partitioning:

```
logs/{service_name}/year={yyyy}/month={mm}/day={dd}/hour={hh}/{uuid}-{timestamp}.parquet
```

Query with DuckDB:

```sql
-- Install DuckDB httpfs extension (one-time)
INSTALL httpfs;
LOAD httpfs;

-- Configure S3/R2 credentials
SET s3_endpoint='<your-endpoint>';
SET s3_access_key_id='<key>';
SET s3_secret_access_key='<secret>';

-- Query logs
SELECT
  Timestamp,
  ServiceName,
  SeverityText,
  Body
FROM read_parquet('s3://your-bucket/logs/my-service/year=2025/month=01/**/*.parquet')
WHERE Timestamp > NOW() - INTERVAL 1 HOUR
ORDER BY Timestamp DESC
LIMIT 100;
```

## API Reference

### POST /v1/logs

Ingest OpenTelemetry logs via OTLP protocol.

**Request:**
- **Content-Type:** `application/x-protobuf` (JSON coming soon)
- **Body:** OTLP `ExportLogsServiceRequest` protobuf message

**Response:**
- **200 OK:** Logs successfully ingested and written to storage
- **400 Bad Request:** Invalid protobuf or malformed request
- **500 Internal Server Error:** Storage or processing error

**Environment Variables:**

| Variable | Platform | Description |
|----------|----------|-------------|
| `LOGS_BUCKET` | Cloudflare Workers | R2 bucket name |
| `AWS_REGION` | Lambda | S3 region (default: `us-east-1`) |
| `BUCKET_NAME` | Lambda | S3 bucket name |
| `STORAGE_PATH` | Standalone | Local filesystem path |

## How It Works

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

**Workspace Structure:**

```
otlp2parquet/
├── crates/
│   ├── otlp2parquet-core/    # ✅ Platform-agnostic logic (PURE)
│   │   ├── otlp/             # ✅ OTLP→Arrow conversion
│   │   ├── parquet/          # ✅ Parquet writing + partitioning
│   │   └── schema.rs         # ✅ Arrow schema (15 fields)
│   ├── otlp2parquet-runtime/ # Platform adapters
│   │   ├── cloudflare.rs     # ✅ R2Storage (async)
│   │   ├── lambda.rs         # ✅ S3Storage (async)
│   │   └── standalone.rs     # ✅ LocalStorage (blocking)
│   └── otlp2parquet-proto/   # ✅ Generated protobuf (v1.3.2)
└── src/main.rs               # ✅ Platform detection
```

**Schema:**

ClickHouse-compatible schema with PascalCase naming (15 fields):

- **Timestamps:** Timestamp, ObservedTimestamp (nanosecond precision, UTC)
- **Trace context:** TraceId, SpanId, TraceFlags
- **Severity:** SeverityText, SeverityNumber
- **Body:** Log message content
- **Extracted attributes:** ServiceName, ServiceNamespace, ServiceInstanceId
- **Scope:** ScopeName, ScopeVersion
- **Maps:** ResourceAttributes, LogAttributes (remaining key-value pairs)

<details>
<summary><b>Development Setup</b></summary>

### Prerequisites

```bash
# Install Rust toolchain
rustup toolchain install stable
rustup component add rustfmt clippy
rustup target add wasm32-unknown-unknown

# Install wasm-opt (required for WASM optimization)
# macOS:
brew install binaryen

# Linux (Ubuntu/Debian):
sudo apt install binaryen

# Install development tools (optional but recommended)
cargo install twiggy          # WASM binary profiler
curl -LsSf https://astral.sh/uv/install.sh | sh  # uv for Python tools

# Setup pre-commit hooks
uvx pre-commit install
```

### Quick Start with Makefile

```bash
# Show all available commands
make help

# Quick development check (fast)
make dev

# Format and lint
make fmt
make clippy

# Run tests
make test

# Build for specific platform
make build-standalone
make build-lambda
make build-cloudflare

# Full WASM pipeline: build → optimize → compress → profile
make wasm-full
```

### Building

#### Using Makefile (Recommended)

```bash
# Cloudflare Workers - full WASM pipeline
make wasm-full

# AWS Lambda
make build-lambda

# Standalone server
make build-standalone

# Run pre-commit checks before committing
make pre-commit

# Run full CI locally
make ci
```

#### Manual Build Commands

**Cloudflare Workers (WASM):**

```bash
# Build with minimal features
cargo build --release \
  --target wasm32-unknown-unknown \
  --no-default-features \
  --features cloudflare

# Optimize
wasm-opt -Oz --enable-bulk-memory --enable-nontrapping-float-to-int \
  -o optimized.wasm target/wasm32-unknown-unknown/release/otlp2parquet.wasm

# Compress and check size (must be <3MB)
gzip -9 optimized.wasm
ls -lh optimized.wasm.gz
```

**AWS Lambda:**

```bash
# Install cargo-lambda (optional, for local testing)
cargo install cargo-lambda

# Build
cargo build --release --no-default-features --features lambda

# Or with gRPC support
cargo build --release --no-default-features --features lambda,grpc
```

**Standalone (Development):**

```bash
cargo build --release --no-default-features --features standalone
./target/release/otlp2parquet
```

### Size Optimization

Target: <3MB compressed WASM

Current optimizations:
- `opt-level = "z"` (size optimization)
- LTO enabled
- `default-features = false` for all dependencies
- Minimal feature flags
- Snappy compression only
- Strip symbols

Profile with twiggy to identify bloat:
```bash
make wasm-profile
```

</details>

## Status & Roadmap

**Current Phase:** Core Implementation Complete

### ✅ Completed (Phase 1-3)

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

### 🚧 In Progress (Phase 4-5)

- [ ] HTTP protocol handlers
- [ ] Cloudflare Workers entry point (`#[event(fetch)]`)
- [ ] Lambda handler implementation
- [ ] Standalone HTTP server
- [ ] Binary size optimization and profiling
- [ ] End-to-end integration tests

### 📋 Planned (Phase 6+)

- [ ] JSON input format support (OTLP spec compliance)
- [ ] JSONL support (bonus feature)
- [ ] One-click Cloudflare deployment
- [ ] CloudFormation template for Lambda
- [ ] Load testing
- [ ] gRPC support (Lambda, optional)

See [CLAUDE.md](./CLAUDE.md) for detailed implementation instructions and architecture decisions.

## Troubleshooting

### Binary Size Exceeds 3MB

```bash
# Profile binary to identify bloat
make wasm-profile

# Check feature flags
cargo tree --features cloudflare --edges features

# Verify optimizations in Cargo.toml
grep -A 5 "\[profile.release\]" Cargo.toml
```

### OTLP Protobuf Parse Errors

Ensure you're sending valid OTLP v1.3.2 format:
```bash
# Verify with otel-cli
otel-cli logs --protocol http/protobuf --dry-run
```

### Storage Write Failures

**Cloudflare Workers:**
- Verify R2 bucket binding in `wrangler.toml`
- Check bucket permissions

**AWS Lambda:**
- Verify IAM role has `s3:PutObject` permission
- Check `AWS_REGION` and `BUCKET_NAME` environment variables

**Standalone:**
- Verify `STORAGE_PATH` directory exists and is writable

## License

MIT OR Apache-2.0
