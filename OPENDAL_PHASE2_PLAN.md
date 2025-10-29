# Phase 2: OpenDAL Full Migration Implementation Plan

**Status:** Ready to Execute
**Estimated Time:** 1-2 days
**Risk Level:** Low (validated in Phase 1)
**Target Branch:** `claude/migrate-to-opendal-011CUawCBsmZLoNBAsnRbqLu`

---

## Overview

This plan details the step-by-step migration from platform-specific storage implementations to a unified OpenDAL-based architecture across all three platforms: Cloudflare Workers, AWS Lambda, and Standalone.

### Phase 1 Results Recap
- ✅ Binary size impact: +17KB (+2.4%) - acceptable
- ✅ WASM compilation: confirmed working
- ✅ All platforms build successfully
- ✅ Tests passing

---

## Migration Strategy

### Approach: Incremental Platform-by-Platform

We'll migrate one platform at a time, testing thoroughly before moving to the next:

1. **Standalone** (easiest, most testable locally)
2. **AWS Lambda** (moderate complexity)
3. **Cloudflare Workers** (most complex due to WASM)

**Rationale:** Start with the simplest platform to validate the pattern, then apply to more complex ones.

### Feature Flag Strategy

Keep both implementations temporarily during migration:
- Old: `cloudflare`, `lambda`, `standalone` (current)
- New: `opendal-cloudflare`, `opendal-lambda`, `opendal-standalone` (new)
- After validation: remove old flags and rename new ones

---

## Detailed Implementation Plan

### Step 1: Update Architecture Documentation ✏️

**File:** `CLAUDE.md`

**Changes:**
```markdown
## Storage Layer (UPDATED)

**Philosophy:** Use mature, battle-tested abstractions over custom implementations.

We use Apache OpenDAL (v0.54+) as a unified storage layer across all platforms:
- **Cloudflare Workers:** OpenDAL S3 service → R2 (S3-compatible endpoint)
- **AWS Lambda:** OpenDAL S3 service → S3 (replaces aws-sdk-s3)
- **Standalone:** OpenDAL Fs service → local filesystem

**Benefits:**
- Unified API reduces code duplication (3 implementations → 1)
- Battle-tested by 600+ projects
- Handles S3/R2 quirks automatically
- Minimal binary size impact (+17KB / +2.4%)
- Zero-cost abstractions with correct async per platform

**Dependency Configuration:**
```toml
opendal = { version = "0.54", default-features = false }
# Platform-specific features:
# - services-s3 (for CF Workers, Lambda)
# - services-fs (for Standalone)
```

### Step 2: Refactor Standalone Runtime 🔧

**Priority:** HIGH (easiest to test locally)

**File:** `crates/otlp2parquet-runtime/src/standalone.rs`

**Current Implementation:**
```rust
pub struct FilesystemStorage {
    base_path: PathBuf,
}

impl FilesystemStorage {
    pub fn write(&self, path: &str, data: &[u8]) -> Result<()> {
        // Uses std::fs directly
    }
}
```

**New Implementation:**
```rust
use crate::opendal_storage::OpenDalStorage;

pub struct StandaloneRuntime {
    storage: OpenDalStorage,
}

impl StandaloneRuntime {
    pub fn new(base_path: &str) -> Result<Self> {
        let storage = OpenDalStorage::new_fs(base_path)?;
        Ok(Self { storage })
    }
}

// In run() function:
pub fn run() -> Result<()> {
    let storage_path = std::env::var("STORAGE_PATH")
        .unwrap_or_else(|_| "./data".to_string());

    let runtime = StandaloneRuntime::new(&storage_path)?;

    // Use runtime.storage.write() in request handlers
}
```

**Changes Required:**
1. Replace `FilesystemStorage` with `OpenDalStorage::new_fs()`
2. Update `handle_request()` to use async storage API
3. Convert standalone server to use async (tokio::net instead of std::net)
4. Update all `storage.write()` calls to `.await`

**Testing:**
```bash
# Build standalone with OpenDAL
cargo build --release --no-default-features --features opendal-standalone

# Run local server
STORAGE_PATH=./test-data ./target/release/otlp2parquet

# Test with curl
curl -X POST http://localhost:8080/v1/logs \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @test-otlp-payload.bin
```

### Step 3: Refactor AWS Lambda Runtime 🔧

**Priority:** MEDIUM (needs AWS credentials for full testing)

**File:** `crates/otlp2parquet-runtime/src/lambda.rs`

**Current Implementation:**
```rust
use aws_sdk_s3::Client;

pub struct S3Storage {
    client: Client,
    bucket: String,
}

impl S3Storage {
    pub async fn write(&self, path: &str, data: Vec<u8>) -> Result<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(path)
            .body(data.into())
            .send()
            .await?;
        Ok(())
    }
}
```

**New Implementation:**
```rust
use crate::opendal_storage::OpenDalStorage;

pub async fn run() -> Result<(), Error> {
    let bucket = std::env::var("LOGS_BUCKET")
        .unwrap_or_else(|_| "otlp-logs".to_string());
    let region = std::env::var("AWS_REGION")
        .unwrap_or_else(|_| "us-east-1".to_string());

    // OpenDAL automatically uses AWS credentials from environment
    let storage = Arc::new(
        OpenDalStorage::new_s3(&bucket, &region, None, None, None)?
    );

    lambda_runtime::run(service_fn(move |event| {
        let storage = storage.clone();
        async move { handle_request(event, storage).await }
    }))
    .await
}
```

**Key Changes:**
1. Remove `S3Storage` struct entirely
2. Replace `aws-sdk-s3::Client` with `OpenDalStorage::new_s3()`
3. Remove AWS SDK initialization code
4. OpenDAL handles AWS credential discovery automatically (IAM roles, env vars)

**Environment Variables:**
- `LOGS_BUCKET` - S3 bucket name
- `AWS_REGION` - AWS region
- AWS credentials via IAM role (preferred) or `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`

**Testing:**
```bash
# Build Lambda with OpenDAL
cargo lambda build --release --features opendal-lambda

# Local testing with cargo-lambda
cargo lambda watch

# Invoke locally
cargo lambda invoke --data-file test-event.json
```

### Step 4: Refactor Cloudflare Workers Runtime 🔧

**Priority:** HIGH (most critical - WASM constraints)

**File:** `crates/otlp2parquet-runtime/src/cloudflare.rs`

**Current Implementation:**
```rust
use worker::Bucket;

pub struct R2Storage {
    bucket: Bucket,
}

impl R2Storage {
    pub async fn write(&self, path: &str, data: Vec<u8>) -> Result<()> {
        self.bucket
            .put(path, data)
            .execute()
            .await?;
        Ok(())
    }
}
```

**New Implementation - Option A (OpenDAL via Environment):**
```rust
use crate::opendal_storage::OpenDalStorage;

pub async fn handle_otlp_request(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    // Get R2 credentials from environment
    let bucket = env.var("R2_BUCKET")?.to_string();
    let account_id = env.var("R2_ACCOUNT_ID")?.to_string();
    let access_key_id = env.var("R2_ACCESS_KEY_ID")?.to_string();
    let secret_access_key = env.secret("R2_SECRET_ACCESS_KEY")?
        .to_string();

    let storage = OpenDalStorage::new_r2(
        &bucket,
        &account_id,
        &access_key_id,
        &secret_access_key,
    ).map_err(|e| worker::Error::RustError(format!("Storage init: {}", e)))?;

    // Rest of handler unchanged
}
```

**New Implementation - Option B (Hybrid - Keep Worker Bucket):**
```rust
// Keep using worker::Bucket for R2 in CF Workers
// Only use OpenDAL for Lambda/Standalone
// Rationale: worker crate is optimized for CF Workers runtime
```

**Decision Point:**
- **Option A** (Full OpenDAL): More consistent, but requires R2 credentials in env
- **Option B** (Hybrid): Keeps worker crate's optimized R2 bindings

**Recommendation:** Start with **Option B** (hybrid), migrate to Option A only if needed.

**Testing:**
```bash
# Build WASM with OpenDAL
cargo build --release --target wasm32-unknown-unknown \
  --no-default-features --features cloudflare

# Test with Miniflare
npm install -g miniflare
miniflare --wasm target/wasm32-unknown-unknown/release/otlp2parquet.wasm

# Test request
curl -X POST http://localhost:8787/v1/logs \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @test-payload.bin
```

### Step 5: Update Feature Flags 🏗️

**File:** `Cargo.toml` and `crates/otlp2parquet-runtime/Cargo.toml`

**Current State:**
```toml
[features]
cloudflare = ["worker"]
lambda = ["lambda_runtime", "aws-config", "aws-sdk-s3"]
standalone = []
```

**New State (transitional):**
```toml
[features]
# Old (deprecated)
cloudflare-legacy = ["worker"]
lambda-legacy = ["lambda_runtime", "aws-config", "aws-sdk-s3"]
standalone-legacy = []

# New (recommended)
cloudflare = ["worker", "opendal-s3"]  # or keep worker-only for hybrid
lambda = ["lambda_runtime", "opendal-s3"]
standalone = ["opendal-fs"]

# OpenDAL features
opendal-s3 = ["opendal/services-s3"]
opendal-fs = ["opendal/services-fs"]
```

**Final State (after validation):**
```toml
[features]
default = ["standalone"]
cloudflare = ["worker"]  # or ["worker", "opendal-s3"] if full migration
lambda = ["lambda_runtime", "opendal-s3"]
standalone = ["opendal-fs"]

# Remove: aws-config, aws-sdk-s3 from dependencies (Lambda only uses OpenDAL)
```

### Step 6: Update Main Entry Point 🚪

**File:** `src/main.rs`

**Current:**
```rust
#[cfg(feature = "cloudflare")]
fn main() {
    // Empty - entry via worker macro
}

#[cfg(feature = "lambda")]
#[tokio::main]
async fn main() -> Result<(), Error> {
    otlp2parquet_runtime::lambda::run().await
}

#[cfg(feature = "standalone")]
fn main() -> anyhow::Result<()> {
    otlp2parquet_runtime::standalone::run()
}
```

**New (if standalone becomes async):**
```rust
#[cfg(feature = "cloudflare")]
fn main() {
    // Empty - entry via worker macro
}

#[cfg(feature = "lambda")]
#[tokio::main]
async fn main() -> Result<(), Error> {
    otlp2parquet_runtime::lambda::run().await
}

#[cfg(feature = "standalone")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    otlp2parquet_runtime::standalone::run().await
}
```

### Step 7: Remove Old Dependencies 🗑️

**Files:** `Cargo.toml`, `crates/otlp2parquet-runtime/Cargo.toml`

**Dependencies to Remove:**
```toml
# Remove these after Lambda migration complete:
aws-config = { version = "1", default-features = false, features = ["rustls"] }
aws-sdk-s3 = { version = "1", default-features = false, features = ["rustls"] }
```

**Keep:**
```toml
# Keep for Lambda events
lambda_runtime = { version = "0.14" }
aws_lambda_events = { version = "0.18" }

# Keep for CF Workers
worker = { version = "0.4" }

# New unified storage
opendal = { version = "0.54", default-features = false }
```

**Size Impact Check:**
```bash
# Before removing aws-sdk-s3
cargo build --release --features lambda
ls -lh target/release/otlp2parquet

# After removing aws-sdk-s3
cargo build --release --features lambda
ls -lh target/release/otlp2parquet

# Expect: smaller binary (aws-sdk-s3 is heavy)
```

---

## Testing Strategy

### Unit Tests

**New test file:** `crates/otlp2parquet-runtime/tests/opendal_integration.rs`

```rust
#[cfg(feature = "opendal-fs")]
#[tokio::test]
async fn test_standalone_write_read() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = OpenDalStorage::new_fs(temp_dir.path().to_str().unwrap()).unwrap();

    let test_data = b"test parquet data".to_vec();
    storage.write("test.parquet", test_data.clone()).await.unwrap();

    let read_data = storage.read("test.parquet").await.unwrap();
    assert_eq!(test_data, read_data);
}

#[cfg(feature = "opendal-s3")]
#[tokio::test]
#[ignore] // Requires AWS credentials
async fn test_lambda_s3_write() {
    let bucket = std::env::var("TEST_S3_BUCKET").unwrap();
    let storage = OpenDalStorage::new_s3(&bucket, "us-east-1", None, None, None).unwrap();

    let test_data = b"test data".to_vec();
    let path = format!("test/{}.parquet", uuid::Uuid::new_v4());
    storage.write(&path, test_data).await.unwrap();

    assert!(storage.exists(&path).await.unwrap());
}
```

### Integration Tests

**Per platform:**

1. **Standalone:**
   ```bash
   # Start server
   cargo run --release --features standalone

   # Send OTLP request
   cargo run --example generate-otlp-payload
   curl -X POST http://localhost:8080/v1/logs \
     -H "Content-Type: application/x-protobuf" \
     --data-binary @test-payload.bin

   # Verify parquet file created
   ls -lh ./data/logs/test-service/year=2025/month=10/day=29/
   ```

2. **Lambda:**
   ```bash
   # Local testing
   cargo lambda watch --features lambda
   cargo lambda invoke --data-file test-event.json

   # Deploy to AWS
   cargo lambda deploy --features lambda
   aws lambda invoke --function-name otlp2parquet output.json
   ```

3. **Cloudflare Workers:**
   ```bash
   # Build WASM
   cargo build --release --target wasm32-unknown-unknown \
     --features cloudflare

   # Test locally with Miniflare
   miniflare --wasm target/wasm32-unknown-unknown/release/otlp2parquet.wasm

   # Deploy to Cloudflare
   wrangler deploy
   ```

### Binary Size Verification

**Critical for Cloudflare Workers:**

```bash
# Build WASM
make build-cloudflare  # or equivalent cargo build

# Optimize and compress
make wasm-full

# Verify size
make wasm-size

# Expected output:
# Uncompressed: ~3.1MB
# Compressed (gzip -9): ~720KB (must be < 3MB)
```

### End-to-End Tests

**Test matrix:**

| Platform | Storage | Request Type | Expected Result |
|----------|---------|--------------|----------------|
| Standalone | Filesystem | POST /v1/logs | Parquet file created locally |
| Lambda | S3 | POST /v1/logs | Parquet file in S3 bucket |
| CF Workers | R2 | POST /v1/logs | Parquet file in R2 bucket |
| All | All | GET /health | 200 OK response |

---

## Rollback Plan

If issues arise during migration:

### Quick Rollback (per platform)

```bash
# Revert to old feature flags
cargo build --release --features lambda-legacy  # instead of lambda
cargo build --release --features standalone-legacy  # instead of standalone
```

### Full Rollback

```bash
git revert <commit-hash>
git push origin claude/migrate-to-opendal-011CUawCBsmZLoNBAsnRbqLu
```

### Hybrid Approach (fallback)

Keep OpenDAL for Lambda + Standalone, revert Cloudflare Workers to worker crate:
- Benefit: Still get 2/3 platform unification
- Trade-off: Maintains one platform-specific implementation

---

## Success Criteria

### Must Have ✅

- [ ] All three platforms compile successfully
- [ ] WASM binary size < 3MB compressed
- [ ] All existing tests pass
- [ ] Manual testing successful on all platforms
- [ ] No regression in functionality
- [ ] Documentation updated

### Nice to Have 🎯

- [ ] Reduced binary size vs current (remove aws-sdk-s3)
- [ ] Faster compile times (fewer dependencies)
- [ ] Additional OpenDAL storage backends easy to add
- [ ] Unified error handling across platforms

### Performance Targets 📊

- [ ] OTLP processing latency unchanged (within 5%)
- [ ] Storage write latency unchanged (within 10%)
- [ ] Memory usage unchanged (within 10%)

---

## Timeline

### Day 1: Core Migration
- **Morning:** Steps 1-2 (Documentation + Standalone refactor)
- **Afternoon:** Step 3 (Lambda refactor)
- **Evening:** Testing Standalone + Lambda

### Day 2: WASM + Polish
- **Morning:** Step 4 (Cloudflare Workers decision + refactor)
- **Afternoon:** Steps 5-7 (Feature flags, cleanup, dependency removal)
- **Evening:** Full integration testing, binary size validation

### Day 3 (Buffer): Testing + Documentation
- Final testing across all platforms
- Update README, examples, deployment guides
- Create PR for review

---

## Risk Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| WASM size > 3MB | Low | High | Already validated at 720KB; monitor with `make wasm-size` |
| CF Workers runtime incompatibility | Low | High | Hybrid approach: keep worker crate for R2 |
| Lambda credential issues | Medium | Medium | Document credential setup; test with IAM roles |
| Performance regression | Low | Medium | Benchmark before/after; OpenDAL is zero-cost |
| Breaking API changes | Low | Low | No public API changes, only internal refactor |

---

## Documentation Updates

### Files to Update:

1. **CLAUDE.md** - Architecture section (storage layer)
2. **README.md** - Add OpenDAL configuration examples
3. **Cargo.toml** - Update feature descriptions
4. **Deployment guides** - Update for each platform:
   - `docs/deploy-cloudflare.md`
   - `docs/deploy-lambda.md`
   - `docs/deploy-standalone.md`

### New Documentation:

1. **MIGRATION_GUIDE.md** - For users upgrading from old version
2. **STORAGE_BACKENDS.md** - How to add new OpenDAL backends (GCS, Azure, etc.)

---

## Next Actions

Once this plan is approved:

1. Create new branch: `feature/opendal-full-migration` (or continue on validation branch)
2. Start with Step 1 (Documentation) + Step 2 (Standalone)
3. Test thoroughly after each step
4. Commit incrementally with clear messages
5. Final integration testing before merging

---

## Questions to Resolve

Before starting implementation:

1. **Cloudflare Workers:** Full OpenDAL or hybrid approach?
   - **Recommendation:** Start hybrid (keep worker crate), migrate later if needed

2. **Standalone async conversion:** Should we convert standalone server to async?
   - **Recommendation:** Yes, for API consistency (all platforms use async storage)

3. **Dependency cleanup timing:** Remove aws-sdk-s3 immediately or after full validation?
   - **Recommendation:** After Lambda testing successful, remove immediately

4. **Feature flag naming:** Keep temporary `-legacy` flags or switch immediately?
   - **Recommendation:** Direct switch if confident, otherwise keep legacy for 1 version

---

**Ready to execute? Let's start with Step 1 + Step 2 (Standalone platform)!**
