# Platform Consistency Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Create shared `otlp2parquet-handlers` crate to eliminate ~300 LOC of duplication and unify error handling across Cloudflare, Lambda, and Server platforms.

**Architecture:** New handlers crate with shared `OtlpError` type and `process_logs/traces/metrics` functions. Platforms become thin adapters that convert to/from platform-specific error types. Critical constraint: maintain Cloudflare WASM binary under 3MB compressed (currently 2.01 MB).

**Tech Stack:** Rust workspace crate, Arrow, otlp2parquet-core/batch/writer dependencies

---

## Task 1: Create handlers crate structure

**Files:**
- Create: `crates/otlp2parquet-handlers/Cargo.toml`
- Create: `crates/otlp2parquet-handlers/src/lib.rs`
- Create: `crates/otlp2parquet-handlers/src/error.rs`
- Modify: `Cargo.toml` (workspace members)

**Step 1: Create crate directory**

Run: `mkdir -p crates/otlp2parquet-handlers/src`

**Step 2: Write Cargo.toml for handlers crate**

Create: `crates/otlp2parquet-handlers/Cargo.toml`

```toml
[package]
name = "otlp2parquet-handlers"
version.workspace = true
edition.workspace = true
authors.workspace = true
license.workspace = true
repository.workspace = true

[dependencies]
otlp2parquet-core = { path = "../otlp2parquet-core", default-features = false }
otlp2parquet-batch = { path = "../otlp2parquet-batch", default-features = false }
otlp2parquet-writer = { path = "../otlp2parquet-writer", default-features = false }

[dev-dependencies]
tokio = { version = "1", features = ["macros", "rt"] }
```

**Step 3: Add to workspace members**

Modify: `Cargo.toml` line 11 (after `otlp2parquet-server`)

Add:
```toml
    "crates/otlp2parquet-handlers",
```

**Step 4: Create lib.rs stub**

Create: `crates/otlp2parquet-handlers/src/lib.rs`

```rust
//! Shared signal processing and error handling for OTLP ingestion
//!
//! This crate provides unified error types and signal processing functions
//! used across Cloudflare Workers, Lambda, and Server platforms.

pub mod error;

pub use error::OtlpError;
```

**Step 5: Verify crate compiles**

Run: `cargo check -p otlp2parquet-handlers`
Expected: SUCCESS with no errors

**Step 6: Commit**

```bash
git add Cargo.toml crates/otlp2parquet-handlers/
git commit -m "feat: create otlp2parquet-handlers crate scaffold"
```

---

## Task 2: Implement OtlpError type with tests

**Files:**
- Create: `crates/otlp2parquet-handlers/src/error.rs`

**Step 1: Write failing test for status codes**

Create: `crates/otlp2parquet-handlers/src/error.rs`

```rust
/// Core error classification for OTLP ingestion
#[derive(Debug, Clone)]
pub enum OtlpError {
    // 400-level: Client errors
    InvalidRequest { message: String, hint: Option<String> },
    PayloadTooLarge { size: usize, limit: usize },

    // 500-level: Server errors
    ConversionFailed { signal: String, message: String },
    StorageFailed { message: String },
    CatalogFailed { message: String },
    InternalError { message: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_status_codes() {
        let err = OtlpError::InvalidRequest {
            message: "test".into(),
            hint: None,
        };
        assert_eq!(err.status_code(), 400);

        let err = OtlpError::PayloadTooLarge { size: 1000, limit: 500 };
        assert_eq!(err.status_code(), 413);

        let err = OtlpError::ConversionFailed {
            signal: "logs".into(),
            message: "failed".into(),
        };
        assert_eq!(err.status_code(), 500);

        let err = OtlpError::StorageFailed {
            message: "failed".into(),
        };
        assert_eq!(err.status_code(), 502);

        let err = OtlpError::CatalogFailed {
            message: "failed".into(),
        };
        assert_eq!(err.status_code(), 502);

        let err = OtlpError::InternalError {
            message: "failed".into(),
        };
        assert_eq!(err.status_code(), 500);
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p otlp2parquet-handlers`
Expected: FAIL with "no method named `status_code`"

**Step 3: Implement status_code method**

Add to `crates/otlp2parquet-handlers/src/error.rs` after the enum:

```rust
impl OtlpError {
    /// HTTP status code for this error
    pub fn status_code(&self) -> u16 {
        match self {
            Self::InvalidRequest { .. } => 400,
            Self::PayloadTooLarge { .. } => 413,
            Self::ConversionFailed { .. } => 500,
            Self::StorageFailed { .. } => 502,
            Self::CatalogFailed { .. } => 502,
            Self::InternalError { .. } => 500,
        }
    }
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test -p otlp2parquet-handlers`
Expected: PASS

**Step 5: Write failing test for error_type**

Add to tests module:

```rust
    #[test]
    fn test_error_types() {
        let err = OtlpError::InvalidRequest {
            message: "test".into(),
            hint: None,
        };
        assert_eq!(err.error_type(), "InvalidRequest");

        let err = OtlpError::PayloadTooLarge { size: 1000, limit: 500 };
        assert_eq!(err.error_type(), "PayloadTooLarge");

        let err = OtlpError::ConversionFailed {
            signal: "logs".into(),
            message: "failed".into(),
        };
        assert_eq!(err.error_type(), "ConversionFailed");

        let err = OtlpError::StorageFailed {
            message: "failed".into(),
        };
        assert_eq!(err.error_type(), "StorageFailed");
    }
```

**Step 6: Run test to verify it fails**

Run: `cargo test -p otlp2parquet-handlers test_error_types`
Expected: FAIL with "no method named `error_type`"

**Step 7: Implement error_type method**

Add to impl block:

```rust
    /// Error type string for responses
    pub fn error_type(&self) -> &'static str {
        match self {
            Self::InvalidRequest { .. } => "InvalidRequest",
            Self::PayloadTooLarge { .. } => "PayloadTooLarge",
            Self::ConversionFailed { .. } => "ConversionFailed",
            Self::StorageFailed { .. } => "StorageFailed",
            Self::CatalogFailed { .. } => "CatalogFailed",
            Self::InternalError { .. } => "InternalError",
        }
    }
```

**Step 8: Run test to verify it passes**

Run: `cargo test -p otlp2parquet-handlers test_error_types`
Expected: PASS

**Step 9: Write failing test for message method**

Add to tests module:

```rust
    #[test]
    fn test_error_messages() {
        let err = OtlpError::InvalidRequest {
            message: "bad data".into(),
            hint: Some("check format".into()),
        };
        assert_eq!(err.message(), "bad data");

        let err = OtlpError::PayloadTooLarge { size: 1000, limit: 500 };
        assert!(err.message().contains("1000"));
        assert!(err.message().contains("500"));

        let err = OtlpError::ConversionFailed {
            signal: "logs".into(),
            message: "schema mismatch".into(),
        };
        assert!(err.message().contains("logs"));
        assert!(err.message().contains("schema mismatch"));
    }
```

**Step 10: Run test to verify it fails**

Run: `cargo test -p otlp2parquet-handlers test_error_messages`
Expected: FAIL with "no method named `message`"

**Step 11: Implement message method**

Add to impl block:

```rust
    /// Human-readable message
    pub fn message(&self) -> String {
        match self {
            Self::InvalidRequest { message, .. } => message.clone(),
            Self::PayloadTooLarge { size, limit } =>
                format!("Payload size {} bytes exceeds limit of {} bytes", size, limit),
            Self::ConversionFailed { signal, message } =>
                format!("Failed to convert {} to Arrow: {}", signal, message),
            Self::StorageFailed { message } =>
                format!("Storage operation failed: {}", message),
            Self::CatalogFailed { message } =>
                format!("Catalog operation failed: {}", message),
            Self::InternalError { message } => message.clone(),
        }
    }
```

**Step 12: Run test to verify it passes**

Run: `cargo test -p otlp2parquet-handlers test_error_messages`
Expected: PASS

**Step 13: Write failing test for hint method**

Add to tests module:

```rust
    #[test]
    fn test_error_hints() {
        let err = OtlpError::InvalidRequest {
            message: "bad data".into(),
            hint: Some("check format".into()),
        };
        assert_eq!(err.hint(), Some("check format".to_string()));

        let err = OtlpError::PayloadTooLarge { size: 1000, limit: 500 };
        let hint = err.hint().unwrap();
        assert!(hint.contains("OTLP2PARQUET_MAX_PAYLOAD_BYTES"));

        let err = OtlpError::StorageFailed {
            message: "failed".into(),
        };
        assert_eq!(err.hint(), None);
    }
```

**Step 14: Run test to verify it fails**

Run: `cargo test -p otlp2parquet-handlers test_error_hints`
Expected: FAIL with "no method named `hint`"

**Step 15: Implement hint method**

Add to impl block:

```rust
    /// Optional hint for fixing the error
    pub fn hint(&self) -> Option<String> {
        match self {
            Self::InvalidRequest { hint, .. } => hint.clone(),
            Self::PayloadTooLarge { .. } =>
                Some("Reduce batch size or increase OTLP2PARQUET_MAX_PAYLOAD_BYTES".into()),
            _ => None,
        }
    }
```

**Step 16: Run all tests to verify they pass**

Run: `cargo test -p otlp2parquet-handlers`
Expected: PASS (all 4 tests)

**Step 17: Commit**

```bash
git add crates/otlp2parquet-handlers/src/error.rs
git commit -m "feat: implement OtlpError with status codes and messages"
```

---

## Task 3: Create ProcessingResult and ProcessorConfig types

**Files:**
- Create: `crates/otlp2parquet-handlers/src/processor.rs`
- Modify: `crates/otlp2parquet-handlers/src/lib.rs`

**Step 1: Write test for ProcessingResult**

Create: `crates/otlp2parquet-handlers/src/processor.rs`

```rust
/// Result of processing a signal request
#[derive(Debug, Clone, PartialEq)]
pub struct ProcessingResult {
    pub paths_written: Vec<String>,
    pub records_processed: usize,
    pub batches_flushed: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_processing_result_creation() {
        let result = ProcessingResult {
            paths_written: vec!["path1".to_string(), "path2".to_string()],
            records_processed: 100,
            batches_flushed: 2,
        };

        assert_eq!(result.paths_written.len(), 2);
        assert_eq!(result.records_processed, 100);
        assert_eq!(result.batches_flushed, 2);
    }
}
```

**Step 2: Run test to verify it passes**

Run: `cargo test -p otlp2parquet-handlers test_processing_result_creation`
Expected: PASS

**Step 3: Add ProcessorConfig type**

Add to `crates/otlp2parquet-handlers/src/processor.rs`:

```rust
/// Configuration for signal processing
pub struct ProcessorConfig<'a> {
    pub catalog: Option<&'a dyn otlp2parquet_writer::icepick::catalog::Catalog>,
    pub namespace: &'a str,
    pub snapshot_timestamp_ms: Option<i64>,
}
```

**Step 4: Export from lib.rs**

Modify: `crates/otlp2parquet-handlers/src/lib.rs`

Add:
```rust
pub mod processor;

pub use processor::{ProcessingResult, ProcessorConfig};
```

**Step 5: Verify compilation**

Run: `cargo check -p otlp2parquet-handlers`
Expected: SUCCESS

**Step 6: Commit**

```bash
git add crates/otlp2parquet-handlers/src/processor.rs crates/otlp2parquet-handlers/src/lib.rs
git commit -m "feat: add ProcessingResult and ProcessorConfig types"
```

---

## Task 4: Implement process_logs function

**Files:**
- Modify: `crates/otlp2parquet-handlers/src/processor.rs`

**Step 1: Write failing test for process_logs with invalid request**

Add to tests module in `processor.rs`:

```rust
    #[tokio::test]
    async fn test_process_logs_invalid_request() {
        use otlp2parquet_core::InputFormat;

        let invalid_data = b"not valid otlp data";
        let config = ProcessorConfig {
            catalog: None,
            namespace: "test",
            snapshot_timestamp_ms: None,
        };

        let result = process_logs(invalid_data, InputFormat::Protobuf, config).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.error_type(), "InvalidRequest");
        assert!(err.message().contains("Failed to parse OTLP logs request"));
    }
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p otlp2parquet-handlers test_process_logs_invalid_request`
Expected: FAIL with "cannot find function `process_logs`"

**Step 3: Implement process_logs function**

Add to `crates/otlp2parquet-handlers/src/processor.rs` before tests:

```rust
use crate::error::OtlpError;
use otlp2parquet_core::{otlp, InputFormat, SignalType};
use otlp2parquet_writer::WriteBatchRequest;

/// Process OTLP logs request
pub async fn process_logs(
    body: &[u8],
    format: InputFormat,
    config: ProcessorConfig<'_>,
) -> Result<ProcessingResult, OtlpError> {
    // Parse OTLP request
    let request = otlp::parse_otlp_request(body, format).map_err(|e| {
        OtlpError::InvalidRequest {
            message: format!("Failed to parse OTLP logs request: {}", e),
            hint: Some(
                "Ensure the request body contains valid OTLP protobuf, JSON, or JSONL format."
                    .into(),
            ),
        }
    })?;

    // Split by service name
    let per_service_requests = otlp::logs::split_request_by_service(request);

    // Convert to Arrow via PassthroughBatcher
    let passthrough = otlp2parquet_batch::PassthroughBatcher::default();
    let mut batches = Vec::new();
    let mut total_records = 0;

    for subset in per_service_requests {
        let batch = passthrough.ingest(&subset).map_err(|e| {
            OtlpError::ConversionFailed {
                signal: "logs".into(),
                message: e.to_string(),
            }
        })?;
        total_records += batch.metadata.record_count;
        batches.push(batch);
    }

    // Write batches
    let mut paths = Vec::new();
    for batch in batches {
        for record_batch in &batch.batches {
            let path = otlp2parquet_writer::write_batch(WriteBatchRequest {
                catalog: config.catalog,
                namespace: config.namespace,
                batch: record_batch,
                signal_type: SignalType::Logs,
                metric_type: None,
                service_name: &batch.metadata.service_name,
                timestamp_micros: batch.metadata.first_timestamp_nanos,
                snapshot_timestamp_ms: config.snapshot_timestamp_ms,
            })
            .await
            .map_err(|e| OtlpError::StorageFailed {
                message: e.to_string(),
            })?;

            paths.push(path);
        }
    }

    Ok(ProcessingResult {
        paths_written: paths,
        records_processed: total_records,
        batches_flushed: batches.len(),
    })
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test -p otlp2parquet-handlers test_process_logs_invalid_request`
Expected: PASS

**Step 5: Write integration test with real OTLP data**

Add to tests module:

```rust
    #[tokio::test]
    async fn test_process_logs_valid_request() {
        use otlp2parquet_core::InputFormat;

        // Read test data
        let test_data = std::fs::read("../../testdata/logs.pb").expect("Failed to read testdata");

        // Initialize storage for plain Parquet mode
        let temp_dir = std::env::temp_dir().join("otlp2parquet-handlers-test");
        std::fs::create_dir_all(&temp_dir).ok();

        let runtime_config = otlp2parquet_config::RuntimeConfig {
            storage: otlp2parquet_config::StorageConfig::Filesystem {
                root: temp_dir.to_string_lossy().to_string(),
            },
            catalog: None,
            ..Default::default()
        };

        otlp2parquet_writer::initialize_storage(&runtime_config)
            .expect("Failed to initialize storage");

        let config = ProcessorConfig {
            catalog: None,
            namespace: "test",
            snapshot_timestamp_ms: None,
        };

        let result = process_logs(&test_data, InputFormat::Protobuf, config)
            .await
            .expect("Failed to process logs");

        assert!(result.records_processed > 0);
        assert!(result.paths_written.len() > 0);
        assert_eq!(result.batches_flushed, result.paths_written.len());

        // Cleanup
        std::fs::remove_dir_all(&temp_dir).ok();
    }
```

**Step 6: Run test to verify it passes**

Run: `cargo test -p otlp2parquet-handlers test_process_logs_valid_request`
Expected: PASS

**Step 7: Export process_logs from lib.rs**

Modify: `crates/otlp2parquet-handlers/src/lib.rs`

Add to `pub use processor::` line:
```rust
pub use processor::{ProcessingResult, ProcessorConfig, process_logs};
```

**Step 8: Verify all tests pass**

Run: `cargo test -p otlp2parquet-handlers`
Expected: PASS (all tests)

**Step 9: Commit**

```bash
git add crates/otlp2parquet-handlers/src/processor.rs crates/otlp2parquet-handlers/src/lib.rs
git commit -m "feat: implement process_logs with error handling"
```

---

## Task 5: Implement process_traces function

**Files:**
- Modify: `crates/otlp2parquet-handlers/src/processor.rs`
- Modify: `crates/otlp2parquet-handlers/src/lib.rs`

**Step 1: Write failing test for process_traces**

Add to tests module:

```rust
    #[tokio::test]
    async fn test_process_traces_invalid_request() {
        use otlp2parquet_core::InputFormat;

        let invalid_data = b"not valid otlp data";
        let config = ProcessorConfig {
            catalog: None,
            namespace: "test",
            snapshot_timestamp_ms: None,
        };

        let result = process_traces(invalid_data, InputFormat::Protobuf, config).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.error_type(), "InvalidRequest");
        assert!(err.message().contains("Failed to parse OTLP traces request"));
    }
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p otlp2parquet-handlers test_process_traces_invalid_request`
Expected: FAIL with "cannot find function `process_traces`"

**Step 3: Implement process_traces function**

Add to `processor.rs` after `process_logs`:

```rust
/// Process OTLP traces request
pub async fn process_traces(
    body: &[u8],
    format: InputFormat,
    config: ProcessorConfig<'_>,
) -> Result<ProcessingResult, OtlpError> {
    // Parse OTLP traces request
    let request = otlp::traces::parse_otlp_trace_request(body, format).map_err(|e| {
        OtlpError::InvalidRequest {
            message: format!("Failed to parse OTLP traces request: {}", e),
            hint: Some(
                "Ensure the request body contains valid OTLP protobuf, JSON, or JSONL format."
                    .into(),
            ),
        }
    })?;

    // Split by service name
    let per_service_requests = otlp::traces::split_request_by_service(request);

    let mut paths = Vec::new();
    let mut spans_processed = 0;

    for subset in per_service_requests {
        let (batches, metadata) =
            otlp::traces::TraceArrowConverter::convert(&subset).map_err(|e| {
                OtlpError::ConversionFailed {
                    signal: "traces".into(),
                    message: e.to_string(),
                }
            })?;

        if batches.is_empty() || metadata.span_count == 0 {
            continue;
        }

        spans_processed += metadata.span_count;

        // Write traces
        for batch in &batches {
            let path = otlp2parquet_writer::write_batch(WriteBatchRequest {
                catalog: config.catalog,
                namespace: config.namespace,
                batch,
                signal_type: SignalType::Traces,
                metric_type: None,
                service_name: metadata.service_name.as_ref(),
                timestamp_micros: metadata.first_timestamp_nanos,
                snapshot_timestamp_ms: config.snapshot_timestamp_ms,
            })
            .await
            .map_err(|e| OtlpError::StorageFailed {
                message: e.to_string(),
            })?;

            paths.push(path);
        }
    }

    Ok(ProcessingResult {
        paths_written: paths,
        records_processed: spans_processed,
        batches_flushed: paths.len(),
    })
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test -p otlp2parquet-handlers test_process_traces_invalid_request`
Expected: PASS

**Step 5: Write integration test with real trace data**

Add to tests module:

```rust
    #[tokio::test]
    async fn test_process_traces_valid_request() {
        use otlp2parquet_core::InputFormat;

        // Read test data
        let test_data = std::fs::read("../../testdata/traces.pb").expect("Failed to read testdata");

        // Initialize storage
        let temp_dir = std::env::temp_dir().join("otlp2parquet-handlers-test-traces");
        std::fs::create_dir_all(&temp_dir).ok();

        let runtime_config = otlp2parquet_config::RuntimeConfig {
            storage: otlp2parquet_config::StorageConfig::Filesystem {
                root: temp_dir.to_string_lossy().to_string(),
            },
            catalog: None,
            ..Default::default()
        };

        otlp2parquet_writer::initialize_storage(&runtime_config)
            .expect("Failed to initialize storage");

        let config = ProcessorConfig {
            catalog: None,
            namespace: "test",
            snapshot_timestamp_ms: None,
        };

        let result = process_traces(&test_data, InputFormat::Protobuf, config)
            .await
            .expect("Failed to process traces");

        assert!(result.records_processed > 0);
        assert!(result.paths_written.len() > 0);

        // Cleanup
        std::fs::remove_dir_all(&temp_dir).ok();
    }
```

**Step 6: Run test to verify it passes**

Run: `cargo test -p otlp2parquet-handlers test_process_traces_valid_request`
Expected: PASS

**Step 7: Export process_traces from lib.rs**

Modify: `crates/otlp2parquet-handlers/src/lib.rs`

Update pub use line:
```rust
pub use processor::{ProcessingResult, ProcessorConfig, process_logs, process_traces};
```

**Step 8: Commit**

```bash
git add crates/otlp2parquet-handlers/src/processor.rs crates/otlp2parquet-handlers/src/lib.rs
git commit -m "feat: implement process_traces with error handling"
```

---

## Task 6: Implement process_metrics function

**Files:**
- Modify: `crates/otlp2parquet-handlers/src/processor.rs`
- Modify: `crates/otlp2parquet-handlers/src/lib.rs`

**Step 1: Write failing test for process_metrics**

Add to tests module:

```rust
    #[tokio::test]
    async fn test_process_metrics_invalid_request() {
        use otlp2parquet_core::InputFormat;

        let invalid_data = b"not valid otlp data";
        let config = ProcessorConfig {
            catalog: None,
            namespace: "test",
            snapshot_timestamp_ms: None,
        };

        let result = process_metrics(invalid_data, InputFormat::Protobuf, config).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.error_type(), "InvalidRequest");
        assert!(err.message().contains("Failed to parse OTLP metrics request"));
    }
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p otlp2parquet-handlers test_process_metrics_invalid_request`
Expected: FAIL with "cannot find function `process_metrics`"

**Step 3: Implement process_metrics function**

Add to `processor.rs` after `process_traces`:

```rust
/// Process OTLP metrics request
pub async fn process_metrics(
    body: &[u8],
    format: InputFormat,
    config: ProcessorConfig<'_>,
) -> Result<ProcessingResult, OtlpError> {
    // Parse OTLP metrics request
    let request = otlp::metrics::parse_otlp_request(body, format).map_err(|e| {
        OtlpError::InvalidRequest {
            message: format!("Failed to parse OTLP metrics request: {}", e),
            hint: Some(
                "Ensure the request body contains valid OTLP protobuf, JSON, or JSONL format."
                    .into(),
            ),
        }
    })?;

    // Split by service name
    let per_service_requests = otlp::metrics::split_request_by_service(request);
    let converter = otlp::metrics::ArrowConverter::new();

    let mut paths = Vec::new();
    let mut total_data_points = 0;

    for subset in per_service_requests {
        let (batches_by_type, metadata) = converter.convert(subset).map_err(|e| {
            OtlpError::ConversionFailed {
                signal: "metrics".into(),
                message: e.to_string(),
            }
        })?;

        // Count total data points
        total_data_points += metadata.gauge_count
            + metadata.sum_count
            + metadata.histogram_count
            + metadata.exponential_histogram_count
            + metadata.summary_count;

        // Write each metric type
        for (metric_type, batch) in batches_by_type {
            // Extract service name and timestamp from batch
            use arrow::array::{Array, StringArray, TimestampNanosecondArray};

            let service_name = if let Some(array) = batch.column(1).as_any().downcast_ref::<StringArray>() {
                for idx in 0..array.len() {
                    if array.is_valid(idx) {
                        let value = array.value(idx);
                        if !value.is_empty() {
                            value.to_string()
                        } else {
                            otlp::common::UNKNOWN_SERVICE_NAME.to_string()
                        }
                    } else {
                        otlp::common::UNKNOWN_SERVICE_NAME.to_string()
                    }
                }
                otlp::common::UNKNOWN_SERVICE_NAME.to_string()
            } else {
                otlp::common::UNKNOWN_SERVICE_NAME.to_string()
            };

            let timestamp_nanos = if let Some(array) = batch
                .column(0)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
            {
                let mut min_value = i64::MAX;
                for idx in 0..array.len() {
                    if array.is_valid(idx) {
                        let value = array.value(idx);
                        if value < min_value {
                            min_value = value;
                        }
                    }
                }
                if min_value != i64::MAX {
                    min_value
                } else {
                    0
                }
            } else {
                0
            };

            let path = otlp2parquet_writer::write_batch(WriteBatchRequest {
                catalog: config.catalog,
                namespace: config.namespace,
                batch: &batch,
                signal_type: SignalType::Metrics,
                metric_type: Some(&metric_type),
                service_name: &service_name,
                timestamp_micros: timestamp_nanos,
                snapshot_timestamp_ms: config.snapshot_timestamp_ms,
            })
            .await
            .map_err(|e| OtlpError::StorageFailed {
                message: e.to_string(),
            })?;

            paths.push(path);
        }
    }

    Ok(ProcessingResult {
        paths_written: paths,
        records_processed: total_data_points,
        batches_flushed: paths.len(),
    })
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test -p otlp2parquet-handlers test_process_metrics_invalid_request`
Expected: PASS

**Step 5: Write integration test with real metrics data**

Add to tests module:

```rust
    #[tokio::test]
    async fn test_process_metrics_valid_request() {
        use otlp2parquet_core::InputFormat;

        // Read test data
        let test_data = std::fs::read("../../testdata/metrics_gauge.pb")
            .expect("Failed to read testdata");

        // Initialize storage
        let temp_dir = std::env::temp_dir().join("otlp2parquet-handlers-test-metrics");
        std::fs::create_dir_all(&temp_dir).ok();

        let runtime_config = otlp2parquet_config::RuntimeConfig {
            storage: otlp2parquet_config::StorageConfig::Filesystem {
                root: temp_dir.to_string_lossy().to_string(),
            },
            catalog: None,
            ..Default::default()
        };

        otlp2parquet_writer::initialize_storage(&runtime_config)
            .expect("Failed to initialize storage");

        let config = ProcessorConfig {
            catalog: None,
            namespace: "test",
            snapshot_timestamp_ms: None,
        };

        let result = process_metrics(&test_data, InputFormat::Protobuf, config)
            .await
            .expect("Failed to process metrics");

        assert!(result.records_processed > 0);
        assert!(result.paths_written.len() > 0);

        // Cleanup
        std::fs::remove_dir_all(&temp_dir).ok();
    }
```

**Step 6: Run test to verify it passes**

Run: `cargo test -p otlp2parquet-handlers test_process_metrics_valid_request`
Expected: PASS

**Step 7: Export process_metrics from lib.rs**

Modify: `crates/otlp2parquet-handlers/src/lib.rs`

Update pub use line:
```rust
pub use processor::{ProcessingResult, ProcessorConfig, process_logs, process_traces, process_metrics};
```

**Step 8: Run all tests**

Run: `cargo test -p otlp2parquet-handlers`
Expected: PASS (all tests)

**Step 9: Commit**

```bash
git add crates/otlp2parquet-handlers/src/processor.rs crates/otlp2parquet-handlers/src/lib.rs
git commit -m "feat: implement process_metrics with error handling"
```

---

## Task 7: Migrate Cloudflare to use shared handlers

**Files:**
- Modify: `crates/otlp2parquet-cloudflare/Cargo.toml`
- Modify: `crates/otlp2parquet-cloudflare/src/handlers.rs`

**Step 1: Add otlp2parquet-handlers dependency**

Modify: `crates/otlp2parquet-cloudflare/Cargo.toml`

Add to `[dependencies]`:
```toml
otlp2parquet-handlers = { path = "../otlp2parquet-handlers", default-features = false }
```

**Step 2: Verify compilation**

Run: `cargo check -p otlp2parquet-cloudflare --target wasm32-unknown-unknown`
Expected: SUCCESS

**Step 3: Create convert_to_worker_error helper**

Modify: `crates/otlp2parquet-cloudflare/src/handlers.rs`

Add after imports:

```rust
use otlp2parquet_handlers::{process_logs as process_logs_handler, OtlpError, ProcessorConfig};

/// Convert OtlpError to worker::Error
fn convert_to_worker_error(err: OtlpError, request_id: &str) -> worker::Error {
    let status_code = err.status_code();
    let error_response = errors::ErrorResponse {
        error: err.error_type().to_string(),
        message: err.message(),
        details: err.hint(),
        request_id: Some(request_id.to_string()),
    };

    let error_json = serde_json::to_string(&error_response)
        .unwrap_or_else(|_| r#"{"error":"internal error","code":"SERIALIZATION_FAILED"}"#.to_string());
    worker::Error::RustError(format!("{}:{}", status_code, error_json))
}
```

**Step 4: Replace handle_logs_request with shared processor**

Modify: `crates/otlp2parquet-cloudflare/src/handlers.rs`

Replace the entire `handle_logs_request` function with:

```rust
/// Handle logs request
pub async fn handle_logs_request(
    body_bytes: &[u8],
    format: otlp2parquet_core::InputFormat,
    content_type: Option<&str>,
    catalog: Option<&Arc<dyn otlp2parquet_writer::icepick::catalog::Catalog>>,
    namespace: Option<&str>,
    catalog_enabled: bool,
    request_id: &str,
) -> Result<Response> {
    let current_time_ms = worker::Date::now().as_millis() as i64;

    let result = process_logs_handler(
        body_bytes,
        format,
        ProcessorConfig {
            catalog: catalog.map(|c| c.as_ref()),
            namespace: namespace.unwrap_or("default"),
            snapshot_timestamp_ms: Some(current_time_ms),
        },
    )
    .await
    .map_err(|e| {
        console_error!(
            "[{}] Failed to process logs (format: {:?}, content-type: {:?}): {:?}",
            request_id,
            format,
            content_type,
            e.message()
        );
        convert_to_worker_error(e, request_id)
    })?;

    let response_body = json!({
        "status": "ok",
        "records_processed": result.records_processed,
        "flush_count": result.batches_flushed,
        "partitions": result.paths_written,
        "catalog_enabled": catalog_enabled,
    });

    Response::from_json(&response_body)
}
```

**Step 5: Remove old imports that are no longer needed**

Remove from imports:
```rust
use otlp2parquet_batch::{LogSignalProcessor, PassthroughBatcher};
```

**Step 6: Run tests**

Run: `cargo test -p otlp2parquet-cloudflare`
Expected: PASS

**Step 7: Build WASM**

Run: `make build-cloudflare`
Expected: SUCCESS

**Step 8: Check WASM size**

Run: `make wasm-size`
Expected: Compressed size < 3MB (ideally < 2.5MB)

**Step 9: Commit**

```bash
git add crates/otlp2parquet-cloudflare/
git commit -m "refactor(cloudflare): migrate logs handler to shared processor"
```

---

## Task 8: Migrate Cloudflare traces handler

**Files:**
- Modify: `crates/otlp2parquet-cloudflare/src/handlers.rs`

**Step 1: Add process_traces import**

Modify imports in `crates/otlp2parquet-cloudflare/src/handlers.rs`:

```rust
use otlp2parquet_handlers::{
    process_logs as process_logs_handler,
    process_traces as process_traces_handler,
    OtlpError, ProcessorConfig
};
```

**Step 2: Replace handle_traces_request**

Replace the entire `handle_traces_request` function:

```rust
/// Handle traces request
pub async fn handle_traces_request(
    body_bytes: &[u8],
    format: otlp2parquet_core::InputFormat,
    content_type: Option<&str>,
    catalog: Option<&Arc<dyn otlp2parquet_writer::icepick::catalog::Catalog>>,
    namespace: Option<&str>,
    catalog_enabled: bool,
    request_id: &str,
) -> Result<Response> {
    let current_time_ms = worker::Date::now().as_millis() as i64;

    let result = process_traces_handler(
        body_bytes,
        format,
        ProcessorConfig {
            catalog: catalog.map(|c| c.as_ref()),
            namespace: namespace.unwrap_or("default"),
            snapshot_timestamp_ms: Some(current_time_ms),
        },
    )
    .await
    .map_err(|e| {
        console_error!(
            "[{}] Failed to process traces (format: {:?}, content-type: {:?}): {:?}",
            request_id,
            format,
            content_type,
            e.message()
        );
        convert_to_worker_error(e, request_id)
    })?;

    let response_body = json!({
        "status": "ok",
        "spans_processed": result.records_processed,
        "partitions": result.paths_written,
        "catalog_enabled": catalog_enabled,
    });

    Response::from_json(&response_body)
}
```

**Step 3: Run tests**

Run: `cargo test -p otlp2parquet-cloudflare`
Expected: PASS

**Step 4: Build and check WASM size**

Run: `make build-cloudflare && make wasm-size`
Expected: SUCCESS, size < 3MB

**Step 5: Commit**

```bash
git add crates/otlp2parquet-cloudflare/src/handlers.rs
git commit -m "refactor(cloudflare): migrate traces handler to shared processor"
```

---

## Task 9: Migrate Cloudflare metrics handler

**Files:**
- Modify: `crates/otlp2parquet-cloudflare/src/handlers.rs`

**Step 1: Add process_metrics import**

Modify imports:

```rust
use otlp2parquet_handlers::{
    process_logs as process_logs_handler,
    process_traces as process_traces_handler,
    process_metrics as process_metrics_handler,
    OtlpError, ProcessorConfig
};
```

**Step 2: Replace handle_metrics_request**

Replace the entire `handle_metrics_request` function:

```rust
/// Handle metrics request
pub async fn handle_metrics_request(
    body_bytes: &[u8],
    format: otlp2parquet_core::InputFormat,
    content_type: Option<&str>,
    catalog: Option<&Arc<dyn otlp2parquet_writer::icepick::catalog::Catalog>>,
    namespace: Option<&str>,
    catalog_enabled: bool,
    request_id: &str,
) -> Result<Response> {
    let current_time_ms = worker::Date::now().as_millis() as i64;

    let result = process_metrics_handler(
        body_bytes,
        format,
        ProcessorConfig {
            catalog: catalog.map(|c| c.as_ref()),
            namespace: namespace.unwrap_or("default"),
            snapshot_timestamp_ms: Some(current_time_ms),
        },
    )
    .await
    .map_err(|e| {
        console_error!(
            "[{}] Failed to process metrics (format: {:?}, content-type: {:?}): {:?}",
            request_id,
            format,
            content_type,
            e.message()
        );
        convert_to_worker_error(e, request_id)
    })?;

    let response_body = json!({
        "status": "ok",
        "data_points_processed": result.records_processed,
        "partitions": result.paths_written,
        "catalog_enabled": catalog_enabled,
    });

    Response::from_json(&response_body)
}
```

**Step 3: Remove helper functions that are now unused**

Delete the `extract_service_name` and `extract_first_timestamp` functions (they're now in the shared processor).

**Step 4: Run tests**

Run: `cargo test -p otlp2parquet-cloudflare`
Expected: PASS

**Step 5: Build and verify WASM size**

Run: `make build-cloudflare && make wasm-size`
Expected: SUCCESS, size < 3MB

**Step 6: Run full clippy check**

Run: `make clippy`
Expected: PASS with zero warnings

**Step 7: Commit**

```bash
git add crates/otlp2parquet-cloudflare/src/handlers.rs
git commit -m "refactor(cloudflare): migrate metrics handler to shared processor"
```

---

## Task 10: Migrate Lambda handlers

**Files:**
- Modify: `crates/otlp2parquet-lambda/Cargo.toml`
- Modify: `crates/otlp2parquet-lambda/src/handlers.rs`

**Step 1: Add otlp2parquet-handlers dependency**

Modify: `crates/otlp2parquet-lambda/Cargo.toml`

Add to `[dependencies]`:
```toml
otlp2parquet-handlers = { path = "../otlp2parquet-handlers", default-features = false }
```

**Step 2: Replace process_logs function**

Modify: `crates/otlp2parquet-lambda/src/handlers.rs`

Add import:
```rust
use otlp2parquet_handlers::{process_logs as process_logs_handler, ProcessorConfig};
```

Replace the entire `process_logs` async function:

```rust
async fn process_logs(
    body: &[u8],
    format: otlp2parquet_core::InputFormat,
    state: &LambdaState,
) -> HttpResponseData {
    match process_logs_handler(
        body,
        format,
        ProcessorConfig {
            catalog: state.catalog.as_deref(),
            namespace: &state.namespace,
            snapshot_timestamp_ms: None,
        },
    )
    .await
    {
        Ok(res) => HttpResponseData::json(
            200,
            json!({
                "status": "ok",
                "records_processed": res.records_processed,
                "flush_count": res.batches_flushed,
                "partitions": res.paths_written,
            })
            .to_string(),
        ),
        Err(e) => {
            tracing::error!("Failed to process logs: {}", e.message());
            HttpResponseData::json(
                e.status_code(),
                json!({
                    "error": e.error_type(),
                    "message": e.message(),
                })
                .to_string(),
            )
        }
    }
}
```

**Step 3: Remove old imports**

Remove:
```rust
use otlp2parquet_batch::{LogSignalProcessor, PassthroughBatcher};
```

**Step 4: Run tests**

Run: `cargo test -p otlp2parquet-lambda`
Expected: PASS

**Step 5: Commit**

```bash
git add crates/otlp2parquet-lambda/
git commit -m "refactor(lambda): migrate logs handler to shared processor"
```

---

## Task 11: Migrate Lambda traces and metrics handlers

**Files:**
- Modify: `crates/otlp2parquet-lambda/src/handlers.rs`

**Step 1: Add remaining imports**

Add to imports:
```rust
use otlp2parquet_handlers::{
    process_logs as process_logs_handler,
    process_traces as process_traces_handler,
    process_metrics as process_metrics_handler,
    ProcessorConfig,
};
```

**Step 2: Replace process_traces function**

Find and replace the `process_traces` function:

```rust
async fn process_traces(
    body: &[u8],
    format: otlp2parquet_core::InputFormat,
    state: &LambdaState,
) -> HttpResponseData {
    match process_traces_handler(
        body,
        format,
        ProcessorConfig {
            catalog: state.catalog.as_deref(),
            namespace: &state.namespace,
            snapshot_timestamp_ms: None,
        },
    )
    .await
    {
        Ok(res) => HttpResponseData::json(
            200,
            json!({
                "status": "ok",
                "spans_processed": res.records_processed,
                "partitions": res.paths_written,
            })
            .to_string(),
        ),
        Err(e) => {
            tracing::error!("Failed to process traces: {}", e.message());
            HttpResponseData::json(
                e.status_code(),
                json!({
                    "error": e.error_type(),
                    "message": e.message(),
                })
                .to_string(),
            )
        }
    }
}
```

**Step 3: Replace process_metrics function**

Replace the `process_metrics` function:

```rust
async fn process_metrics(
    body: &[u8],
    format: otlp2parquet_core::InputFormat,
    state: &LambdaState,
) -> HttpResponseData {
    match process_metrics_handler(
        body,
        format,
        ProcessorConfig {
            catalog: state.catalog.as_deref(),
            namespace: &state.namespace,
            snapshot_timestamp_ms: None,
        },
    )
    .await
    {
        Ok(res) => HttpResponseData::json(
            200,
            json!({
                "status": "ok",
                "data_points_processed": res.records_processed,
                "partitions": res.paths_written,
            })
            .to_string(),
        ),
        Err(e) => {
            tracing::error!("Failed to process metrics: {}", e.message());
            HttpResponseData::json(
                e.status_code(),
                json!({
                    "error": e.error_type(),
                    "message": e.message(),
                })
                .to_string(),
            )
        }
    }
}
```

**Step 4: Run tests**

Run: `cargo test -p otlp2parquet-lambda`
Expected: PASS

**Step 5: Build Lambda binary**

Run: `make build-lambda`
Expected: SUCCESS

**Step 6: Run full test suite**

Run: `make test`
Expected: PASS (all tests)

**Step 7: Commit**

```bash
git add crates/otlp2parquet-lambda/src/handlers.rs
git commit -m "refactor(lambda): migrate traces and metrics handlers to shared processor"
```

---

## Task 12: Final verification and documentation

**Files:**
- Modify: `ARCHITECTURE_REVIEW.md`
- Create: `docs/plans/2025-01-22-platform-consistency-design.md` commit

**Step 1: Run full clippy check**

Run: `make clippy`
Expected: PASS with zero warnings

**Step 2: Run all tests**

Run: `make test`
Expected: PASS (all tests)

**Step 3: Build all platforms**

Run: `make build-release`
Expected: SUCCESS for server, lambda, cloudflare

**Step 4: Verify WASM size one final time**

Run: `make wasm-compress && make wasm-size`
Expected: Compressed size < 3MB (record the exact size)

**Step 5: Update ARCHITECTURE_REVIEW.md**

Modify: `ARCHITECTURE_REVIEW.md`

Update Issue #3 status:
```markdown
### 3. **Duplication: Signal Processing Logic Across 3 Platforms**
**Priority: HIGH - Will cause maintenance burden**
**Status: ✅ RESOLVED** (2025-01-22)

Created `otlp2parquet-handlers` crate with shared `process_logs/traces/metrics` functions.
Eliminated ~300 LOC of duplication. All platforms now use shared error handling and processing logic.
```

**Step 6: Commit design document**

```bash
git add docs/plans/2025-01-22-platform-consistency-design.md
git commit -m "docs: add platform consistency design"
```

**Step 7: Update ARCHITECTURE_REVIEW.md**

```bash
git add ARCHITECTURE_REVIEW.md
git commit -m "docs: mark signal processing duplication as resolved"
```

**Step 8: Create final summary commit**

Run: `git log --oneline --since="1 day ago"`

Review the commits and create summary:

```bash
git commit --allow-empty -m "feat: unify platform handlers and error handling

Summary of changes:
- Created otlp2parquet-handlers crate with shared OtlpError and signal processors
- Migrated Cloudflare Workers to use shared handlers (eliminated ~100 LOC)
- Migrated Lambda to use shared handlers (eliminated ~100 LOC)
- Eliminated ~300 LOC total duplication
- Unified error messages and status codes across platforms
- WASM binary size: [record actual size] (within 3MB budget)

Resolves architecture review Issue #3.
"
```

---

## Success Criteria Checklist

After completing all tasks, verify:

- [ ] `cargo test` passes all tests
- [ ] `make clippy` shows zero warnings
- [ ] `make build-release` succeeds for all platforms
- [ ] WASM compressed size < 3MB (run `make wasm-size`)
- [ ] All platforms use `OtlpError` for error handling
- [ ] All platforms use `process_logs/traces/metrics` functions
- [ ] Error messages are consistent across platforms
- [ ] ~300 LOC eliminated from platform handlers
- [ ] Architecture review updated

---

## Rollback Plan

If WASM size exceeds 3MB during Task 7-9:

1. Revert Cloudflare migration commits
2. Add feature flags to `otlp2parquet-handlers/Cargo.toml`:
   ```toml
   [features]
   detailed-errors = []
   rich-hints = []
   ```
3. Make error hints conditional on `rich-hints` feature
4. Cloudflare uses `default-features = false`
5. If still too large, keep duplication for Cloudflare only
