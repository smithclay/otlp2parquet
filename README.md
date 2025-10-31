# otlp2parquet

OpenTelemetry logs → Parquet files on object storage. ClickHouse-compatible schema.

**Multi-platform:** Docker (local/K8s), Cloudflare Workers (WASM), AWS Lambda (serverless).

## Quick Start

**Choose your platform:**

### Docker (Local Development)

```bash
docker-compose up
```

MinIO console: http://localhost:9001 (minioadmin/minioadmin)

### Cloudflare Workers (Free Tier)

[![Deploy to Cloudflare Workers](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/smithclay/otlp2parquet)

Or: `wrangler deploy` ([guide](deploy/cloudflare/README.md))

### AWS Lambda (Serverless)

```bash
sam deploy --guided
```

([guide](deploy/lambda/README.md))

---

## Usage

Send logs:

```bash
curl -X POST http://localhost:8080/v1/logs \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @logs.pb
```

Query with DuckDB:

```sql
INSTALL httpfs; LOAD httpfs;
SET s3_endpoint='localhost:9000'; SET s3_url_style='path'; SET s3_use_ssl=false;
SET s3_access_key_id='minioadmin'; SET s3_secret_access_key='minioadmin';

SELECT Timestamp, ServiceName, SeverityText, Body
FROM read_parquet('s3://otlp-logs/logs/**/*.parquet')
ORDER BY Timestamp DESC LIMIT 10;
```

Files stored as: `logs/{service}/year={yyyy}/month={mm}/day={dd}/hour={hh}/{uuid}.parquet`

## How It Works

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
│  Protocol Layer (HTTP)                  │
│  ├─ POST /v1/logs (protobuf) ✅         │
│  ├─ GET /health (health check) ✅       │
│  └─ GET /ready (readiness) ✅           │
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
│  Unified Storage (Apache OpenDAL)        │
│  ├─ S3 (Lambda, Server)                │
│  ├─ R2 (Cloudflare, Server)            │
│  ├─ Filesystem (Server)                │
│  └─ GCS, Azure, etc. (Server-ready)    │
└─────────────────────────────────────────┘
```

**Architecture Highlights:**
- **Server is Default:** Full-featured mode with Axum HTTP server, structured logging, graceful shutdown
- **Unified Storage:** Apache OpenDAL provides consistent API across all platforms
- **Pure Core:** OTLP processing is deterministic with no I/O dependencies
- **Platform-Native:** Each runtime uses its native async model (worker, tokio)
- **Binary Size:** WASM compressed to 720KB (~24% of 3MB limit)

**Workspace Structure:**

```
otlp2parquet/
├── crates/
│   ├── otlp2parquet-core/     # ✅ Platform-agnostic logic (PURE)
│   │   ├── otlp/              # ✅ OTLP→Arrow conversion
│   │   ├── parquet/           # ✅ Parquet writing + partitioning
│   │   └── schema.rs          # ✅ Arrow schema (15 fields)
│   ├── otlp2parquet-runtime/  # Platform adapters + OpenDAL storage
│   │   ├── opendal_storage.rs # ✅ Unified storage abstraction
│   │   ├── server.rs          # ✅ Default mode (Axum + multi-backend)
│   │   ├── lambda.rs          # ✅ Lambda handler (OpenDAL S3)
│   │   └── cloudflare.rs      # ✅ CF Workers handler (OpenDAL S3→R2)
│   └── otlp2parquet-proto/    # ✅ Generated protobuf (v1.3.2)
└── src/main.rs                # ✅ Platform detection
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
make build-server
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

# Server mode (default)
make build-server

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
```

**Server Mode (Default - Docker/Kubernetes/Development):**

```bash
# Build
cargo build --release --no-default-features --features server

# Run with filesystem storage (default)
./target/release/otlp2parquet

# Run with S3 storage
STORAGE_BACKEND=s3 \
S3_BUCKET=my-logs-bucket \
S3_REGION=us-east-1 \
./target/release/otlp2parquet

# Run with R2 storage
STORAGE_BACKEND=r2 \
R2_BUCKET=my-r2-bucket \
R2_ACCOUNT_ID=your_account_id \
R2_ACCESS_KEY_ID=your_key_id \
R2_SECRET_ACCESS_KEY=your_secret \
./target/release/otlp2parquet

# Docker deployment example
docker build -t otlp2parquet .
docker run -p 8080:8080 \
  -e STORAGE_BACKEND=s3 \
  -e S3_BUCKET=my-logs-bucket \
  -e S3_REGION=us-east-1 \
  -e LOG_FORMAT=json \
  otlp2parquet
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

**Current Phase:** OpenDAL Migration Complete ✅

### ✅ Completed (Phase 1-5)

- [x] Workspace structure created
- [x] Cargo.toml with size optimizations
- [x] Arrow schema definition (15 fields, ClickHouse-compatible)
- [x] OTLP protobuf integration (v1.3.2, code generation configured)
- [x] OTLP → Arrow conversion (ArrowConverter with all fields)
- [x] Parquet writer implementation (Snappy compression, minimal features)
- [x] Partition path generation (Hive-style time partitioning)
- [x] **Apache OpenDAL unified storage layer**
- [x] HTTP protocol handlers (all platforms)
- [x] Cloudflare Workers entry point (`#[event(fetch)]`) with OpenDAL S3→R2
- [x] Lambda handler implementation with OpenDAL S3
- [x] Standalone async HTTP server with OpenDAL Fs
- [x] Binary size optimization (WASM: 1006KB compressed, 33% of 3MB limit)
- [x] CI/CD with protoc installation
- [x] Pre-commit hooks (fmt, clippy)

### 🔄 Recent Changes (Phase 2 - OpenDAL Migration)

- **Unified Storage:** Migrated from platform-specific implementations to Apache OpenDAL
- **Removed Dependencies:** Eliminated `aws-sdk-s3` and `aws-config` (replaced by OpenDAL)
- **Async Everywhere:** Standalone now uses tokio for API consistency
- **Code Reduction:** -913 lines of code, cleaner architecture
- **Binary Size:** Maintained excellent WASM size (<3MB compressed)

### 🚀 Latest (Phase 6 - Easy Button Deployments)

- [x] **Docker multi-arch images** (amd64, arm64) on GitHub Container Registry
- [x] **Cloudflare Workers deployment** with wrangler.toml and deploy button
- [x] **AWS Lambda SAM template** with guided deployment
- [x] Comprehensive deployment guides for all platforms
- [x] docker-compose examples (filesystem, S3, R2)
- [x] CI/CD workflow for Docker image builds

### 📋 Planned (Phase 7+)

- [ ] JSON input format support (OTLP spec compliance)
- [ ] JSONL support (bonus feature)
- [ ] Kubernetes manifests and Helm chart
- [ ] Load testing and performance benchmarks
- [ ] Grafana dashboards for monitoring
- [ ] Integration tests with real OTLP clients

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

**Server Mode:**
- **Filesystem:** Verify `STORAGE_PATH` directory exists and is writable
- **S3:** Verify AWS credentials and S3 bucket permissions
- **R2:** Verify R2 credentials and bucket permissions
- Check `/health` and `/ready` endpoints for diagnostics

## License

MIT OR Apache-2.0
