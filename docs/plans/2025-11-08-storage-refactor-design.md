# Storage Refactor Design - Separate Essence from Accident

**Date:** 2025-11-08
**Goal:** Refactor `otlp2parquet-storage` to separate pure storage abstraction from platform-specific construction logic, applying Fred Brooks principles of conceptual integrity.

## Problem Statement

Current architecture mixes abstraction levels in `otlp2parquet-storage`:
- Pure storage abstraction (`from_operator()`, `read()`, `write()`)
- Platform-specific constructors (`new_s3()`, `new_r2()`, `new_fs()`)
- Iceberg catalog integration (`IcebergWriter`, `IcebergCommitter`)
- Platform-specific quirks (S3 Tables URL encoding, AWS SigV4 auth)

This violates Brooks' principle of **conceptual integrity** and makes it hard to:
- Keep Workers binary <3MB (can't exclude AWS SDK deps)
- Support multiple Iceberg catalog types (S3 Tables, Polaris, generic)
- Test storage independently of platform concerns

## Core Insight: Iceberg ≠ Storage

Iceberg is **catalog metadata**. Storage is S3/R2/Filesystem via OpenDAL. These are orthogonal concerns that should be separated.

## Design Principles

### Brooks Principles
- **Conceptual Integrity** - Each crate has single, clear purpose
- **Essence vs Accident** - Core abstractions separate from platform quirks
- **No Second-System Effect** - Resist urge to make storage crate do everything
- **Surgical Refactoring** - Platform logic moves to edges, core stays clean

### Rust Idioms
- **Trait abstraction** - `HttpClient` trait for pluggable auth
- **Feature flags** - Heavy deps (AWS SDK) behind `aws-sigv4` feature
- **Builder pattern** - Runtime crates provide construction helpers
- **Dependency injection** - `from_operator()` accepts configured dependencies

## Refactored Architecture

### Crate Structure

```
crates/
├── otlp2parquet-storage/          # Pure storage abstraction
│   ├── opendal_storage.rs         # ONLY from_operator() + read/write
│   ├── parquet_writer.rs          # ONLY Parquet serialization
│   └── partition.rs               # ONLY path generation logic
│
├── otlp2parquet-iceberg/          # Catalog layer (platform-agnostic)
│   ├── catalog.rs                 # IcebergCatalog<C: HttpClient>
│   ├── http.rs                    # HttpClient trait + ReqwestHttpClient
│   ├── aws.rs                     # AwsSigV4HttpClient (feature-gated)
│   ├── writer.rs                  # IcebergWriter (write + commit)
│   └── committer.rs               # IcebergCommitter (post-write commit)
│
├── otlp2parquet-lambda/           # AWS Lambda assembly
│   ├── storage.rs                 # build_s3_operator()
│   └── iceberg.rs                 # build_s3_tables_catalog()
│
├── otlp2parquet-server/           # Server assembly
│   ├── storage.rs                 # build_operator() - multi-backend
│   └── iceberg.rs                 # build_catalog() - multi-catalog
│
└── otlp2parquet-cloudflare/       # Workers assembly
    └── storage.rs                 # build_r2_operator()
                                   # NO iceberg dependency
```

### 1. `otlp2parquet-storage` - Pure Abstraction

**What it does:**
- Wraps OpenDAL `Operator` with convenient methods
- Provides Parquet serialization utilities
- Generates partition paths

**What it doesn't do:**
- Construct platform-specific operators (S3/R2/Fs)
- Know about Iceberg catalogs
- Handle authentication

**API:**
```rust
pub struct OpenDalStorage {
    operator: Operator,
}

impl OpenDalStorage {
    pub fn from_operator(operator: Operator) -> Self;
    pub fn operator(&self) -> &Operator;
    pub async fn write(&self, path: &str, data: Vec<u8>) -> Result<()>;
    pub async fn read(&self, path: &str) -> Result<Buffer>;
    pub async fn exists(&self, path: &str) -> Result<bool>;
    pub async fn list(&self, path: &str) -> Result<Vec<String>>;
}
```

**Removed:**
- `new_s3()`, `new_r2()`, `new_fs()` constructors
- All `#[cfg(feature = "services-*")]` conditional compilation
- Iceberg-related code

**Dependencies:**
```toml
[dependencies]
opendal = { version = "0.50", default-features = false }
anyhow = "1.0"
```

### 2. `otlp2parquet-iceberg` - Catalog Layer

**What it does:**
- Implements Iceberg REST catalog protocol
- Provides pluggable HTTP client abstraction
- Handles table commits and metadata

**What it doesn't do:**
- Know about specific platforms (Lambda vs Server)
- Construct storage operators
- Make deployment decisions

**API:**
```rust
#[async_trait]
pub trait HttpClient: Send + Sync {
    async fn post(&self, url: &str, body: Vec<u8>, headers: Vec<(String, String)>) -> Result<Response>;
    async fn get(&self, url: &str, headers: Vec<(String, String)>) -> Result<Response>;
}

pub struct ReqwestHttpClient { ... }  // Simple, no auth

#[cfg(feature = "aws-sigv4")]
pub struct AwsSigV4HttpClient { ... }  // AWS SigV4 signing

pub struct IcebergCatalog<C: HttpClient> {
    client: C,
    rest_uri: String,
    prefix: String,
    namespace: NamespaceIdent,
    tables: HashMap<String, String>,
}

pub struct IcebergWriter { ... }     // Integrated write + commit
pub struct IcebergCommitter { ... }  // Post-write commit
```

**Features:**
```toml
[features]
default = []
aws-sigv4 = ["aws-sdk-s3", "aws-sigv4"]  # Heavy deps, optional
```

**Platform-specific auth strategies:**
- `ReqwestHttpClient` - No auth (generic REST catalogs)
- `AwsSigV4HttpClient` - AWS SigV4 (S3 Tables, feature-gated)
- Future: `BearerTokenHttpClient` - OAuth (Polaris, Tabular)

### 3. `otlp2parquet-lambda` - AWS Assembly

**Responsibilities:**
- Construct S3 operator with Lambda-appropriate defaults
- Build S3 Tables catalog with platform-specific quirks
- Wire together storage + catalog into Writer

**New files:**

`storage.rs`:
```rust
pub fn build_s3_operator(config: &S3Config) -> Result<Operator> {
    // S3 operator construction
    // IAM role credentials (recommended)
}
```

`iceberg.rs`:
```rust
pub async fn build_s3_tables_catalog(config: &IcebergConfig) -> Result<IcebergCatalog<AwsSigV4HttpClient>> {
    // S3 Tables quirk: URL-encoded warehouse ARN as prefix
    let prefix = urlencoding::encode(&config.warehouse).to_string();

    // Create AWS SigV4 HTTP client
    let http_client = AwsSigV4HttpClient::new(&region).await?;

    IcebergCatalog::new(http_client, rest_uri, prefix, namespace, tables)
}
```

**Dependencies:**
```toml
otlp2parquet-iceberg = { path = "../otlp2parquet-iceberg", features = ["aws-sigv4"] }
```

### 4. `otlp2parquet-server` - Multi-Backend Assembly

**Responsibilities:**
- Support multiple storage backends (S3, R2, Filesystem)
- Support multiple catalog types (S3 Tables, Polaris, Generic)
- Auto-detect catalog type from URI

**New files:**

`storage.rs`:
```rust
pub fn build_operator(config: &RuntimeConfig) -> Result<Operator> {
    match config.storage.backend {
        StorageBackend::Fs => build_fs_operator(config.storage.fs)?,
        StorageBackend::S3 => build_s3_operator(config.storage.s3)?,
        StorageBackend::R2 => build_r2_operator(config.storage.r2)?,
    }
}
```

`iceberg.rs`:
```rust
pub enum CatalogType {
    S3Tables,    // AWS S3 Tables (needs SigV4)
    Polaris,     // Snowflake Polaris (bearer token)
    Generic,     // Generic REST catalog (no auth)
}

impl CatalogType {
    pub fn from_uri(uri: &str) -> Self {
        // Auto-detect from URI pattern
    }
}

pub async fn build_catalog(config: &IcebergConfig) -> Result<Arc<IcebergCommitter>> {
    match config.catalog_type {
        CatalogType::S3Tables => {
            let http_client = AwsSigV4HttpClient::new(&region).await?;
            // ...
        }
        CatalogType::Polaris | CatalogType::Generic => {
            let http_client = ReqwestHttpClient::new()?;
            // ...
        }
    }
}
```

**Dependencies:**
```toml
[dependencies]
otlp2parquet-iceberg = { path = "../otlp2parquet-iceberg", features = ["aws-sigv4"], optional = true }

[features]
default = ["iceberg"]
iceberg = ["dep:otlp2parquet-iceberg"]
```

### 5. `otlp2parquet-cloudflare` - WASM-Optimized Assembly

**Responsibilities:**
- Construct R2 operator (Cloudflare-specific endpoint)
- Stay under 3MB compressed binary size
- NO Iceberg support (binary size constraint)

**New file:**

`storage.rs`:
```rust
pub fn build_r2_operator(config: &R2Config) -> Result<Operator> {
    let endpoint = format!("https://{}.r2.cloudflarestorage.com", config.account_id);

    let builder = services::S3::default()
        .bucket(&config.bucket)
        .region("auto")
        .endpoint(&endpoint)
        .access_key_id(&config.access_key_id)
        .secret_access_key(&config.secret_access_key);

    Operator::new(builder)?.finish()
}
```

**Dependencies:**
```toml
[dependencies]
otlp2parquet-storage = { path = "../otlp2parquet-storage" }
# NO otlp2parquet-iceberg - keeps binary small

opendal = { version = "0.50", default-features = false, features = ["services-s3"] }
```

## Migration Strategy

### Phase 1: Prepare `otlp2parquet-iceberg`
1. Extract Iceberg code from storage crate
2. Add `HttpClient` trait abstraction
3. Implement `ReqwestHttpClient` (simple)
4. Implement `AwsSigV4HttpClient` (feature-gated)
5. Update `IcebergCatalog` to use generic `HttpClient`

### Phase 2: Update Runtime Crates
1. Add `storage.rs` to lambda/server/cloudflare with builder functions
2. Update initialization code to use builders
3. Move S3 Tables quirks to lambda `iceberg.rs`

### Phase 3: Clean `otlp2parquet-storage`
1. Remove `new_s3()`, `new_r2()`, `new_fs()` from `OpenDalStorage`
2. Keep only `from_operator()`
3. Update tests to use `from_operator()`
4. Remove Iceberg dependencies

### Phase 4: Verify
1. Build lambda - should include aws-sigv4 feature
2. Build server - should support all backends
3. Build cloudflare - should be <3MB compressed
4. Run integration tests

## Benefits

### Conceptual Clarity
- **Storage crate:** "I wrap OpenDAL operators"
- **Iceberg crate:** "I speak Iceberg REST protocol with pluggable auth"
- **Runtime crates:** "I assemble components for my platform"

### Binary Size Control
- Workers excludes Iceberg entirely (no catalog overhead)
- Lambda includes only S3 + SigV4 dependencies
- Server can optionally exclude Iceberg with feature flag

### Extensibility
- New catalog types (Polaris, Tabular, Unity) add new `HttpClient` impls
- New storage backends (GCS, Azure) add builder functions in runtime crates
- Platform-specific quirks stay isolated in runtime crates

### Testability
- Storage tests mock `Operator` (no S3 needed)
- Iceberg tests mock `HttpClient` (no catalog needed)
- Runtime tests mock both layers independently

## Open Questions

None - design validated and approved.

## References

- Fred Brooks, "The Mythical Man-Month" - Conceptual Integrity
- Fred Brooks, "No Silver Bullet" - Essence vs Accident
- Rust API Guidelines - Builder Pattern, Feature Flags, Trait Objects
