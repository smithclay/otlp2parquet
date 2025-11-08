# Storage Refactor Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Separate storage abstraction from platform-specific construction logic, moving Iceberg to dedicated crate with pluggable auth

**Architecture:**
- `otlp2parquet-storage` becomes pure OpenDAL wrapper (from_operator only)
- New `otlp2parquet-iceberg` crate for catalog layer with trait-based HTTP client
- Runtime crates (lambda/server) own assembly logic via builder functions

**Tech Stack:** Rust, OpenDAL, Apache Iceberg REST, AWS SigV4, reqwest

---

## Phase 1: Create otlp2parquet-iceberg Crate

### Task 1.1: Create new iceberg crate structure

**Files:**
- Create: `crates/otlp2parquet-iceberg/Cargo.toml`
- Create: `crates/otlp2parquet-iceberg/src/lib.rs`

**Step 1: Create Cargo.toml**

```toml
[package]
name = "otlp2parquet-iceberg"
version = "0.1.0"
edition = "2021"

[dependencies]
anyhow = "1.0"
async-trait = "0.1"
reqwest = { version = "0.12", default-features = false, features = ["json", "rustls-tls"] }
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
arrow = { version = "53", default-features = false }
parquet = { version = "53", default-features = false }
opendal = { version = "0.50", default-features = false }
tracing = "0.1"
chrono = "0.4"
uuid = { version = "1.0", features = ["v4"] }
blake3 = "1.5"
url = "2.5"

# AWS dependencies (optional, feature-gated)
aws-config = { version = "1.0", optional = true }
aws-credential-types = { version = "1.0", optional = true }
aws-sigv4 = { version = "1.0", optional = true }

[target.'cfg(not(target_arch = "wasm32"))'.dependencies]
futures-util = "0.3"
urlencoding = "2.1"
tokio = { version = "1", features = ["macros"] }

[dev-dependencies]
tokio = { version = "1", features = ["full"] }

[features]
default = []
aws-sigv4 = ["dep:aws-config", "dep:aws-credential-types", "dep:aws-sigv4"]
```

**Step 2: Create placeholder lib.rs**

```rust
//! Apache Iceberg REST catalog integration
//!
//! Platform-agnostic catalog layer with pluggable HTTP auth

pub mod http;

pub use http::{HttpClient, HttpResponse, ReqwestHttpClient};

#[cfg(feature = "aws-sigv4")]
pub mod aws;

#[cfg(feature = "aws-sigv4")]
pub use aws::AwsSigV4HttpClient;
```

**Step 3: Create http module placeholder**

Create: `crates/otlp2parquet-iceberg/src/http.rs`

```rust
//! HTTP client abstraction

use anyhow::Result;
use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl HttpResponse {
    pub fn is_success(&self) -> bool {
        self.status >= 200 && self.status < 300
    }

    pub fn body_string(&self) -> Result<String> {
        String::from_utf8(self.body.clone())
            .map_err(|e| anyhow::anyhow!("Invalid UTF-8: {}", e))
    }

    pub fn json<T: serde::de::DeserializeOwned>(&self) -> Result<T> {
        serde_json::from_slice(&self.body)
            .map_err(|e| anyhow::anyhow!("Failed to parse JSON: {}", e))
    }
}

#[async_trait]
pub trait HttpClient: Send + Sync {
    async fn request(
        &self,
        method: &str,
        url: &str,
        headers: Vec<(String, String)>,
        body: Option<Vec<u8>>,
    ) -> Result<HttpResponse>;

    async fn get(&self, url: &str, headers: Vec<(String, String)>) -> Result<HttpResponse> {
        self.request("GET", url, headers, None).await
    }

    async fn post(&self, url: &str, headers: Vec<(String, String)>, body: Vec<u8>) -> Result<HttpResponse> {
        self.request("POST", url, headers, Some(body)).await
    }
}

/// Simple reqwest-based HTTP client (no authentication)
pub struct ReqwestHttpClient {
    client: reqwest::Client,
}

impl ReqwestHttpClient {
    pub fn new() -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to create client: {}", e))?;
        Ok(Self { client })
    }
}

#[async_trait]
impl HttpClient for ReqwestHttpClient {
    async fn request(
        &self,
        method: &str,
        url: &str,
        headers: Vec<(String, String)>,
        body: Option<Vec<u8>>,
    ) -> Result<HttpResponse> {
        use anyhow::Context;

        let mut builder = self.client.request(
            method.parse().context("Invalid HTTP method")?,
            url
        );

        for (name, value) in &headers {
            builder = builder.header(name, value);
        }

        if let Some(body_bytes) = body {
            builder = builder.body(body_bytes);
        }

        let response = builder.send().await.context("HTTP request failed")?;

        let status = response.status().as_u16();
        let response_headers: Vec<(String, String)> = response
            .headers()
            .iter()
            .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();
        let body = response.bytes().await.context("Failed to read body")?.to_vec();

        Ok(HttpResponse { status, headers: response_headers, body })
    }
}
```

**Step 4: Commit**

```bash
git add crates/otlp2parquet-iceberg
git commit -m "feat: create otlp2parquet-iceberg crate skeleton"
```

---

### Task 1.2: Move existing iceberg code to new crate

**Files:**
- Move: `crates/otlp2parquet-storage/src/iceberg/*` → `crates/otlp2parquet-iceberg/src/`
- Modify: `crates/otlp2parquet-iceberg/src/lib.rs`

**Step 1: Copy iceberg directory**

Run: `cp -r crates/otlp2parquet-storage/src/iceberg/* crates/otlp2parquet-iceberg/src/`

**Step 2: Replace http.rs with simple version**

(Already created in previous task - keep the simple one)

**Step 3: Update lib.rs to re-export all modules**

Modify: `crates/otlp2parquet-iceberg/src/lib.rs`

```rust
//! Apache Iceberg REST catalog integration

pub mod arrow_convert;
pub mod catalog;
pub mod datafile_convert;
pub mod http;
#[cfg(not(target_arch = "wasm32"))]
pub mod init;
#[cfg(not(target_arch = "wasm32"))]
pub mod manifest;
pub mod protocol;
pub mod types;
pub mod validation;
pub mod writer;

// Re-export main types
pub use catalog::{IcebergCatalog, NamespaceIdent};
pub use http::{HttpClient, HttpResponse, ReqwestHttpClient};
pub use writer::{IcebergConfig, IcebergWriter};

#[cfg(not(target_arch = "wasm32"))]
pub use manifest::{ManifestWriter, ManifestListWriter};

use std::collections::HashMap;
use std::env;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;

// Include IcebergRestConfig and IcebergCommitter from old mod.rs
// (copy from otlp2parquet-storage/src/iceberg/mod.rs lines 122-412)
```

**Step 4: Update imports in all moved files**

Fix imports in each file to use `crate::` instead of `super::` or `use crate::iceberg::`

Example for `catalog.rs`:
```rust
use super::http::{HttpClient, HttpResponse};  // Keep this
use super::protocol::{...};  // Keep this
use super::types::{...};  // Keep this
```

**Step 5: Add iceberg dependency to workspace**

Modify: `Cargo.toml` (workspace root)

```toml
[workspace]
members = [
    "crates/otlp2parquet-core",
    "crates/otlp2parquet-batch",
    "crates/otlp2parquet-config",
    "crates/otlp2parquet-storage",
    "crates/otlp2parquet-iceberg",  # Add this
    "crates/otlp2parquet-proto",
    "crates/otlp2parquet-lambda",
    "crates/otlp2parquet-server",
    "crates/otlp2parquet-cloudflare",
]
```

**Step 6: Test build**

Run: `cargo build -p otlp2parquet-iceberg`
Expected: BUILD SUCCESS (may have warnings about unused imports)

**Step 7: Commit**

```bash
git add -A
git commit -m "feat: move iceberg code to dedicated crate"
```

---

### Task 1.3: Create AWS SigV4 HTTP client (feature-gated)

**Files:**
- Create: `crates/otlp2parquet-iceberg/src/aws.rs`

**Step 1: Create AWS SigV4 client**

```rust
//! AWS SigV4-authenticated HTTP client for S3 Tables

use crate::http::{HttpClient, HttpResponse};
use anyhow::{Context, Result};
use async_trait::async_trait;
use aws_credential_types::provider::ProvideCredentials;
use aws_sigv4::http_request::{sign, SignableBody, SignableRequest, SigningSettings};
use aws_sigv4::sign::v4;

/// HTTP client that signs requests with AWS SigV4
pub struct AwsSigV4HttpClient {
    client: reqwest::Client,
    credentials_provider: aws_credential_types::provider::SharedCredentialsProvider,
    region: String,
}

impl AwsSigV4HttpClient {
    /// Create new AWS SigV4 client
    ///
    /// Loads credentials from environment (Lambda execution role, EC2 instance profile, etc.)
    pub async fn new(region: &str) -> Result<Self> {
        let config = aws_config::from_env().load().await;
        let credentials_provider = config
            .credentials_provider()
            .ok_or_else(|| anyhow::anyhow!("No AWS credentials provider available"))?;

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to create reqwest client: {}", e))?;

        Ok(Self {
            client,
            credentials_provider,
            region: region.to_string(),
        })
    }
}

#[async_trait]
impl HttpClient for AwsSigV4HttpClient {
    async fn request(
        &self,
        method: &str,
        url: &str,
        mut headers: Vec<(String, String)>,
        body: Option<Vec<u8>>,
    ) -> Result<HttpResponse> {
        // Get AWS credentials
        let credentials = self
            .credentials_provider
            .provide_credentials()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get AWS credentials: {}", e))?;

        // Build signable request
        let parsed_url = url.parse::<url::Url>().context("Failed to parse URL")?;

        let signable_headers: Vec<(&str, &str)> = headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect();

        let signable_body = if let Some(ref body_bytes) = body {
            SignableBody::Bytes(body_bytes)
        } else {
            SignableBody::Bytes(&[])
        };

        let signable_request = SignableRequest::new(
            method,
            parsed_url.as_str(),
            signable_headers.into_iter(),
            signable_body,
        )
        .expect("Failed to create signable request");

        // Build identity from credentials
        let identity = aws_credential_types::Credentials::new(
            credentials.access_key_id(),
            credentials.secret_access_key(),
            credentials.session_token().map(String::from),
            None,
            "lambda-execution-role",
        )
        .into();

        // Configure signing params
        let signing_params = v4::SigningParams::builder()
            .identity(&identity)
            .region(&self.region)
            .name("s3tables")
            .time(std::time::SystemTime::now())
            .settings(SigningSettings::default())
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to build signing params: {}", e))?
            .into();

        // Sign the request
        let (signing_instructions, _signature) = sign(signable_request, &signing_params)
            .map_err(|e| anyhow::anyhow!("Failed to sign request: {}", e))?
            .into_parts();

        // Apply signing headers
        for (name, value) in signing_instructions.headers() {
            headers.push((name.to_string(), value.to_string()));
        }

        // Build and execute reqwest request
        let mut request_builder = self
            .client
            .request(method.parse().context("Invalid HTTP method")?, url);

        for (name, value) in &headers {
            request_builder = request_builder.header(name, value);
        }

        if let Some(body_bytes) = body {
            request_builder = request_builder.body(body_bytes);
        }

        let response = request_builder
            .send()
            .await
            .context("HTTP request failed")?;

        // Extract response
        let status = response.status().as_u16();
        let response_headers: Vec<(String, String)> = response
            .headers()
            .iter()
            .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();
        let body = response
            .bytes()
            .await
            .context("Failed to read response body")?
            .to_vec();

        Ok(HttpResponse {
            status,
            headers: response_headers,
            body,
        })
    }
}
```

**Step 2: Update lib.rs to export AWS client**

Modify: `crates/otlp2parquet-iceberg/src/lib.rs`

Add at top:
```rust
#[cfg(feature = "aws-sigv4")]
pub mod aws;

#[cfg(feature = "aws-sigv4")]
pub use aws::AwsSigV4HttpClient;
```

**Step 3: Test build with feature**

Run: `cargo build -p otlp2parquet-iceberg --features aws-sigv4`
Expected: BUILD SUCCESS

**Step 4: Commit**

```bash
git add crates/otlp2parquet-iceberg/src/aws.rs crates/otlp2parquet-iceberg/src/lib.rs
git commit -m "feat: add AWS SigV4 HTTP client (feature-gated)"
```

---

## Phase 2: Update otlp2parquet-storage

### Task 2.1: Add iceberg dependency and remove iceberg code

**Files:**
- Modify: `crates/otlp2parquet-storage/Cargo.toml`
- Delete: `crates/otlp2parquet-storage/src/iceberg/` (entire directory)
- Modify: `crates/otlp2parquet-storage/src/lib.rs`

**Step 1: Update Cargo.toml**

```toml
[dependencies]
# Add iceberg dependency
otlp2parquet-iceberg = { path = "../otlp2parquet-iceberg", optional = true }

# Keep existing deps...
opendal = { version = "0.50", default-features = false }
anyhow = "1.0"
# ... rest

[features]
default = []
# Keep existing features
services-s3 = ["opendal/services-s3"]
services-fs = ["opendal/services-fs"]
# Add iceberg feature
iceberg = ["dep:otlp2parquet-iceberg"]
```

**Step 2: Remove iceberg directory**

Run: `rm -rf crates/otlp2parquet-storage/src/iceberg`

**Step 3: Update lib.rs to re-export from otlp2parquet-iceberg**

Modify: `crates/otlp2parquet-storage/src/lib.rs`

```rust
pub mod opendal_storage;
pub mod parquet_writer;
pub mod partition;

// Re-export iceberg if feature enabled
#[cfg(feature = "iceberg")]
pub use otlp2parquet_iceberg as iceberg;

// Re-export commonly used types
pub use opendal_storage::OpenDalStorage;
pub use parquet_writer::{
    set_parquet_row_group_size, writer_properties, Blake3Hash, ParquetWriteResult, ParquetWriter,
};

// Re-export iceberg types for backward compatibility
#[cfg(feature = "iceberg")]
pub use otlp2parquet_iceberg::{IcebergCatalog, IcebergCommitter};
```

**Step 4: Test build**

Run: `cargo build -p otlp2parquet-storage --features iceberg`
Expected: BUILD SUCCESS

**Step 5: Commit**

```bash
git add -A
git commit -m "refactor: move iceberg to separate crate, re-export for compatibility"
```

---

### Task 2.2: Remove platform-specific constructors from OpenDalStorage

**Files:**
- Modify: `crates/otlp2parquet-storage/src/opendal_storage.rs`

**Step 1: Remove new_s3, new_r2, new_fs methods**

Remove these methods entirely (lines 25-81 in current file):
- `new_s3()`
- `new_r2()`
- `new_fs()`

Keep only:
- `from_operator()`
- `operator()`
- `write()`
- `read()`
- `exists()`
- `list()`

**Step 2: Remove feature gates from Cargo.toml**

Modify: `crates/otlp2parquet-storage/Cargo.toml`

Remove these features:
```toml
# Remove these
services-s3 = ["opendal/services-s3"]
services-fs = ["opendal/services-fs"]
```

**Step 3: Update tests**

Modify test at bottom of file to use `from_operator()`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use opendal::{services, Operator};

    #[tokio::test]
    async fn test_opendal_fs_basic() -> anyhow::Result<()> {
        let temp_dir = std::env::temp_dir();
        let test_root = temp_dir.join("opendal_test");

        // Build operator manually
        let builder = services::Fs::default().root(test_root.to_str().unwrap());
        let operator = Operator::new(builder)?.finish();

        // Use from_operator instead of new_fs
        let storage = OpenDalStorage::from_operator(operator);

        let test_data = b"Hello, OpenDAL!".to_vec();
        storage.write("test.txt", test_data.clone()).await?;

        let read_data = storage.read("test.txt").await?;
        assert_eq!(test_data, read_data.to_vec());

        assert!(storage.exists("test.txt").await?);
        assert!(!storage.exists("nonexistent.txt").await?);

        Ok(())
    }
}
```

**Step 4: Test build**

Run: `cargo test -p otlp2parquet-storage`
Expected: All tests PASS

**Step 5: Commit**

```bash
git add crates/otlp2parquet-storage
git commit -m "refactor: remove platform-specific constructors from OpenDalStorage"
```

---

## Phase 3: Update Lambda Runtime

### Task 3.1: Add storage builder to lambda

**Files:**
- Create: `crates/otlp2parquet-lambda/src/storage.rs`

**Step 1: Create storage.rs**

```rust
//! S3 storage initialization for AWS Lambda

use anyhow::Result;
use opendal::{Operator, services};
use otlp2parquet_config::S3Config;

/// Build S3 operator for Lambda
///
/// Uses IAM role credentials (Lambda execution role)
pub fn build_s3_operator(config: &S3Config) -> Result<Operator> {
    let mut builder = services::S3::default()
        .bucket(&config.bucket)
        .region(&config.region);

    if let Some(endpoint) = &config.endpoint {
        builder = builder.endpoint(endpoint);
    }

    // Credentials auto-discovered from Lambda execution role
    let operator = Operator::new(builder)?.finish();
    Ok(operator)
}
```

**Step 2: Add module to lib.rs**

Modify: `crates/otlp2parquet-lambda/src/lib.rs`

Add at top:
```rust
mod storage;
mod handlers;
mod response;

use storage::build_s3_operator;
```

**Step 3: Update Writer::from_env to use builder**

In `lib.rs`, find `Writer::from_env()` method and update PlainS3 branch:

```rust
WriterMode::PlainS3 => {
    let s3_config = config.storage.s3.as_ref()
        .ok_or_else(|| Error::from("S3 config required"))?;

    // Use builder instead of OpenDalStorage::new_s3
    let operator = build_s3_operator(s3_config)
        .map_err(|e| Error::from(format!("Storage init failed: {}", e)))?;
    let storage = Arc::new(OpenDalStorage::from_operator(operator));

    let parquet_writer = Arc::new(ParquetWriter::new(storage.operator().clone()));

    // ... rest of PlainS3 initialization
}
```

**Step 4: Test build**

Run: `cargo build -p otlp2parquet-lambda`
Expected: BUILD SUCCESS

**Step 5: Commit**

```bash
git add crates/otlp2parquet-lambda/src/storage.rs crates/otlp2parquet-lambda/src/lib.rs
git commit -m "feat: add S3 storage builder for Lambda"
```

---

### Task 3.2: Add iceberg builder to lambda

**Files:**
- Create: `crates/otlp2parquet-lambda/src/iceberg.rs`
- Modify: `crates/otlp2parquet-lambda/Cargo.toml`

**Step 1: Update Cargo.toml to use aws-sigv4 feature**

```toml
[dependencies]
otlp2parquet-iceberg = { path = "../otlp2parquet-iceberg", features = ["aws-sigv4"] }
# ... keep other deps
```

**Step 2: Create iceberg.rs**

```rust
//! S3 Tables Iceberg catalog initialization for AWS Lambda

use anyhow::Result;
use otlp2parquet_iceberg::{AwsSigV4HttpClient, IcebergCatalog, NamespaceIdent};
use std::collections::HashMap;
use std::sync::Arc;

pub struct IcebergConfig {
    pub rest_uri: String,
    pub warehouse: String,
    pub namespace: String,
}

/// Build S3 Tables-specific Iceberg catalog
///
/// Handles S3 Tables quirks:
/// - URL-encoded warehouse ARN as prefix
/// - SigV4 authentication
pub async fn build_s3_tables_catalog(config: &IcebergConfig) -> Result<Arc<IcebergCatalog<AwsSigV4HttpClient>>> {
    // Extract region from URI
    let region = extract_region(&config.rest_uri)?;

    // S3 Tables quirk: prefix must be URL-encoded warehouse ARN
    let prefix = urlencoding::encode(&config.warehouse).to_string();
    println!("Using S3 Tables prefix (URL-encoded warehouse ARN): {}", prefix);

    // Parse namespace
    let namespace_parts: Vec<String> = config.namespace
        .split('.')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    let namespace = NamespaceIdent::from_vec(namespace_parts)?;

    // Build table map
    let mut tables = HashMap::new();
    tables.insert("logs".to_string(), "logs".to_string());
    tables.insert("traces".to_string(), "traces".to_string());
    tables.insert("metrics:gauge".to_string(), "metrics_gauge".to_string());
    tables.insert("metrics:sum".to_string(), "metrics_sum".to_string());
    tables.insert("metrics:histogram".to_string(), "metrics_histogram".to_string());
    tables.insert("metrics:exponential_histogram".to_string(), "metrics_exponential_histogram".to_string());
    tables.insert("metrics:summary".to_string(), "metrics_summary".to_string());

    // Create AWS SigV4 HTTP client
    let http_client = AwsSigV4HttpClient::new(&region).await?;

    let catalog = IcebergCatalog::new(
        http_client,
        config.rest_uri.clone(),
        prefix,
        namespace,
        tables,
    );

    Ok(Arc::new(catalog))
}

fn extract_region(uri: &str) -> Result<String> {
    // Extract region from S3 Tables URI
    // e.g., https://s3tables.us-west-2.amazonaws.com/iceberg -> us-west-2
    uri.split('.')
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("Invalid S3 Tables URI: {}", uri))
        .map(|s| s.to_string())
}
```

**Step 3: Add module to lib.rs**

Modify: `crates/otlp2parquet-lambda/src/lib.rs`

```rust
mod storage;
mod iceberg;  // Add this
mod handlers;
mod response;

use storage::build_s3_operator;
use iceberg::build_s3_tables_catalog;  // Add this
```

**Step 4: Update Iceberg mode in Writer::from_env**

In the `WriterMode::Iceberg` branch:

```rust
WriterMode::Iceberg => {
    let s3_config = config.storage.s3.as_ref()
        .ok_or_else(|| Error::from("S3 config required"))?;

    // Build storage operator
    let operator = build_s3_operator(s3_config)
        .map_err(|e| Error::from(format!("Storage init failed: {}", e)))?;
    let storage = Arc::new(OpenDalStorage::from_operator(operator));

    // Build Iceberg catalog with S3 Tables quirks
    let iceberg_config = iceberg::IcebergConfig {
        rest_uri: std::env::var("OTLP2PARQUET_ICEBERG_REST_URI")?,
        warehouse: std::env::var("OTLP2PARQUET_ICEBERG_WAREHOUSE")?,
        namespace: std::env::var("OTLP2PARQUET_ICEBERG_NAMESPACE")?,
    };
    let catalog = build_s3_tables_catalog(&iceberg_config).await?;

    let writer = Arc::new(IcebergWriter::new(catalog, storage, iceberg_config));

    Ok(Writer::Iceberg { writer, passthrough: PassthroughBatcher::default() })
}
```

**Step 5: Test build**

Run: `cargo build -p otlp2parquet-lambda`
Expected: BUILD SUCCESS

**Step 6: Commit**

```bash
git add crates/otlp2parquet-lambda
git commit -m "feat: add S3 Tables iceberg builder for Lambda"
```

---

## Phase 4: Update Server Runtime

### Task 4.1: Add storage builder to server

**Files:**
- Create: `crates/otlp2parquet-server/src/storage.rs`

**Step 1: Create storage.rs**

```rust
//! Multi-backend storage initialization for server

use anyhow::Result;
use opendal::{Operator, services};
use otlp2parquet_config::{RuntimeConfig, StorageBackend};

/// Build OpenDAL operator based on runtime config
pub fn build_operator(config: &RuntimeConfig) -> Result<Operator> {
    match config.storage.backend {
        StorageBackend::Fs => {
            let fs_config = config.storage.fs.as_ref()
                .ok_or_else(|| anyhow::anyhow!("Filesystem config required"))?;

            let builder = services::Fs::default().root(&fs_config.path);
            let operator = Operator::new(builder)?.finish();
            Ok(operator)
        }
        StorageBackend::S3 => {
            let s3_config = config.storage.s3.as_ref()
                .ok_or_else(|| anyhow::anyhow!("S3 config required"))?;

            let mut builder = services::S3::default()
                .bucket(&s3_config.bucket)
                .region(&s3_config.region);

            if let Some(endpoint) = &s3_config.endpoint {
                builder = builder.endpoint(endpoint);
            }

            let operator = Operator::new(builder)?.finish();
            Ok(operator)
        }
        StorageBackend::R2 => {
            let r2_config = config.storage.r2.as_ref()
                .ok_or_else(|| anyhow::anyhow!("R2 config required"))?;

            let endpoint = format!("https://{}.r2.cloudflarestorage.com", r2_config.account_id);

            let builder = services::S3::default()
                .bucket(&r2_config.bucket)
                .region("auto")
                .endpoint(&endpoint)
                .access_key_id(&r2_config.access_key_id)
                .secret_access_key(&r2_config.secret_access_key);

            let operator = Operator::new(builder)?.finish();
            Ok(operator)
        }
    }
}
```

**Step 2: Add module to lib.rs**

Modify: `crates/otlp2parquet-server/src/lib.rs`

```rust
mod storage;  // Add this
mod init;
mod handlers;
// ...

pub use storage::build_operator;  // Re-export
```

**Step 3: Update init.rs to use builder**

Modify: `crates/otlp2parquet-server/src/init.rs`

```rust
use crate::storage::build_operator;

pub(crate) fn init_storage(config: &RuntimeConfig) -> Result<Arc<OpenDalStorage>> {
    info!("Initializing storage backend: {}", config.storage.backend);

    let operator = build_operator(config)?;
    let storage = Arc::new(OpenDalStorage::from_operator(operator));

    Ok(storage)
}
```

**Step 4: Test build**

Run: `cargo build -p otlp2parquet-server`
Expected: BUILD SUCCESS

**Step 5: Commit**

```bash
git add crates/otlp2parquet-server/src/storage.rs crates/otlp2parquet-server/src/init.rs crates/otlp2parquet-server/src/lib.rs
git commit -m "feat: add multi-backend storage builder for server"
```

---

## Phase 5: Verify All Platforms

### Task 5.1: Build all platforms

**Step 1: Build lambda**

Run: `cargo build -p otlp2parquet-lambda`
Expected: SUCCESS

**Step 2: Build server**

Run: `cargo build -p otlp2parquet-server`
Expected: SUCCESS

**Step 3: Build storage with iceberg**

Run: `cargo build -p otlp2parquet-storage --features iceberg`
Expected: SUCCESS

**Step 4: Build iceberg with aws-sigv4**

Run: `cargo build -p otlp2parquet-iceberg --features aws-sigv4`
Expected: SUCCESS

**Step 5: Run all tests**

Run: `cargo test --workspace`
Expected: All tests PASS

**Step 6: Final commit**

```bash
git add -A
git commit -m "refactor: complete storage/iceberg separation"
```

---

## Verification Checklist

- [ ] Lambda builds successfully
- [ ] Server builds successfully
- [ ] Storage crate has no platform-specific constructors
- [ ] Iceberg crate builds with and without aws-sigv4 feature
- [ ] All tests pass
- [ ] No circular dependencies between crates

---

## Rollback Plan

If issues arise:
1. Revert to commit before Phase 1
2. Run: `git reset --hard <commit-hash>`
3. Review error messages and adjust plan
