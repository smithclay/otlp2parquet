# Platform Consistency Design

**Date:** 2025-01-22
**Status:** Approved
**Context:** Architecture review identified inconsistencies and duplication across platform handlers

## Problem Statement

The three platform implementations (Cloudflare Workers, Lambda, Server) have ~300 LOC of duplicated signal processing logic and inconsistent error handling:

**Error Handling Inconsistencies:**
- Cloudflare: Well-structured `OtlpErrorKind` with detailed messages
- Lambda: Ad-hoc JSON error strings
- Server: `anyhow::Error` with context chains

**Signal Processing Duplication:**
- Each platform reimplements: parse → split → convert → write
- Bug fixes and schema changes require updating 3+ places
- Inconsistent error messages across platforms

**Design Constraints:**
- Cloudflare WASM binary must stay under 3MB compressed (currently 2.01 MB)
- Preserve platform-specific idioms (worker::Error, anyhow, Axum)
- Maintain existing features (metrics in server, timestamp handling in WASM)

## Solution: Shared Core with Platform Adapters

Create `otlp2parquet-handlers` crate containing:
1. **Shared error types** - Common error classification
2. **Signal processors** - Unified parse/convert/write logic
3. **Platform adapters** - Thin conversion layer for each platform

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   Platform Entry Points                     │
│  ┌───────────┐    ┌──────────┐    ┌──────────────────┐    │
│  │ Cloudflare│    │  Lambda  │    │      Server      │    │
│  │  Worker   │    │ Function │    │   (Axum HTTP)    │    │
│  └─────┬─────┘    └────┬─────┘    └────────┬─────────┘    │
│        │               │                    │               │
│        │ Convert       │ Convert            │ Convert       │
│        │ to/from       │ to/from            │ to/from       │
│        │ worker::Error │ HttpResponseData   │ AppError      │
│        │               │                    │               │
└────────┼───────────────┼────────────────────┼───────────────┘
         │               │                    │
         └───────────────┴────────────────────┘
                         │
         ┌───────────────▼──────────────────┐
         │   otlp2parquet-handlers          │
         │                                  │
         │  ┌────────────────────────────┐ │
         │  │  OtlpError (shared)        │ │
         │  │  - InvalidRequest          │ │
         │  │  - PayloadTooLarge         │ │
         │  │  - ConversionFailed        │ │
         │  │  - StorageFailed           │ │
         │  │  - CatalogFailed           │ │
         │  └────────────────────────────┘ │
         │                                  │
         │  ┌────────────────────────────┐ │
         │  │  Signal Processors         │ │
         │  │  - process_logs()          │ │
         │  │  - process_traces()        │ │
         │  │  - process_metrics()       │ │
         │  └────────────────────────────┘ │
         └──────────────────────────────────┘
                         │
         ┌───────────────▼──────────────────┐
         │   otlp2parquet-writer            │
         │   - write_batch()                │
         │   - Iceberg/Parquet integration  │
         └──────────────────────────────────┘
```

## Component Design

### 1. Shared Error Core

**File:** `crates/otlp2parquet-handlers/src/error.rs`

```rust
/// Core error classification for OTLP ingestion
#[derive(Debug, Clone)]
pub enum OtlpError {
    // 400-level: Client errors
    InvalidRequest {
        message: String,
        hint: Option<String>
    },
    PayloadTooLarge {
        size: usize,
        limit: usize
    },

    // 500-level: Server errors
    ConversionFailed {
        signal: String,
        message: String
    },
    StorageFailed {
        message: String
    },
    CatalogFailed {
        message: String
    },
    InternalError {
        message: String
    },
}

impl OtlpError {
    /// HTTP status code for this error
    pub fn status_code(&self) -> u16;

    /// Error type string for responses
    pub fn error_type(&self) -> &'static str;

    /// Human-readable message (detailed & actionable)
    pub fn message(&self) -> String;

    /// Optional hint for fixing the error
    pub fn hint(&self) -> Option<String>;
}
```

**Design Decisions:**
- Uses struct variants with named fields (clear intent)
- Detailed, actionable error messages (good DX)
- Status code mapping built-in (consistency)
- Cloneable for platform conversions

### 2. Signal Processing Functions

**File:** `crates/otlp2parquet-handlers/src/processor.rs`

```rust
/// Result of processing a signal request
pub struct ProcessingResult {
    pub paths_written: Vec<String>,
    pub records_processed: usize,
    pub batches_flushed: usize,
}

/// Configuration for signal processing
pub struct ProcessorConfig<'a> {
    pub catalog: Option<&'a dyn Catalog>,
    pub namespace: &'a str,
    pub snapshot_timestamp_ms: Option<i64>, // For WASM platforms
}

/// Process OTLP logs request
pub async fn process_logs(
    body: &[u8],
    format: InputFormat,
    config: ProcessorConfig<'_>,
) -> Result<ProcessingResult, OtlpError>;

/// Process OTLP traces request
pub async fn process_traces(
    body: &[u8],
    format: InputFormat,
    config: ProcessorConfig<'_>,
) -> Result<ProcessingResult, OtlpError>;

/// Process OTLP metrics request
pub async fn process_metrics(
    body: &[u8],
    format: InputFormat,
    config: ProcessorConfig<'_>,
) -> Result<ProcessingResult, OtlpError>;
```

**Processing Pattern (all signals):**
1. Parse OTLP request → `OtlpError::InvalidRequest` on failure
2. Split by service name (for partitioning)
3. Convert to Arrow via PassthroughBatcher → `OtlpError::ConversionFailed` on failure
4. Write batches via `write_batch()` → `OtlpError::StorageFailed` on failure
5. Return `ProcessingResult` with paths and counts

**What's NOT in shared processor:**
- Time-based batching (server-only feature)
- Metrics collection (server-only feature)
- Platform-specific logging (console_error!, tracing::error!, etc.)

### 3. Platform Integration

Each platform implements a thin adapter:

**Cloudflare Workers:**
```rust
pub async fn handle_logs_request(
    body_bytes: &[u8],
    format: InputFormat,
    catalog: Option<&Arc<dyn Catalog>>,
    namespace: Option<&str>,
    request_id: &str,
) -> Result<Response> {
    let current_time_ms = worker::Date::now().as_millis() as i64;

    let result = process_logs(
        body_bytes,
        format,
        ProcessorConfig {
            catalog: catalog.map(|c| c.as_ref()),
            namespace: namespace.unwrap_or("default"),
            snapshot_timestamp_ms: Some(current_time_ms),
        },
    )
    .await
    .map_err(|e| convert_to_worker_error(e, request_id))?;

    Response::from_json(&json!({
        "status": "ok",
        "records_processed": result.records_processed,
        "flush_count": result.batches_flushed,
        "partitions": result.paths_written,
    }))
}

fn convert_to_worker_error(err: OtlpError, request_id: &str) -> worker::Error {
    // Convert OtlpError → worker::Error with existing ErrorResponse
}
```

**Lambda:**
```rust
async fn process_logs(
    body: &[u8],
    format: InputFormat,
    state: &LambdaState,
) -> HttpResponseData {
    match otlp2parquet_handlers::process_logs(
        body,
        format,
        ProcessorConfig {
            catalog: state.catalog.as_deref(),
            namespace: &state.namespace,
            snapshot_timestamp_ms: None, // Use system time
        },
    ).await {
        Ok(res) => HttpResponseData::json(200, /* success JSON */),
        Err(e) => HttpResponseData::json(e.status_code(), /* error JSON */),
    }
}
```

**Server:**
Server keeps its existing batching and metrics logic but can optionally use shared errors for consistency. Migration is lowest priority.

## Crate Structure

**New Crate:**
```
crates/otlp2parquet-handlers/
├── Cargo.toml
└── src/
    ├── lib.rs              # Re-exports
    ├── error.rs            # OtlpError
    └── processor.rs        # process_logs/traces/metrics
```

**Dependencies:**
```toml
[dependencies]
otlp2parquet-core = { path = "../otlp2parquet-core", default-features = false }
otlp2parquet-batch = { path = "../otlp2parquet-batch", default-features = false }
otlp2parquet-writer = { path = "../otlp2parquet-writer", default-features = false }
arrow = { version = "54", default-features = false }
```

**Reverse Dependencies:**
- `otlp2parquet-cloudflare` adds `otlp2parquet-handlers`
- `otlp2parquet-lambda` adds `otlp2parquet-handlers`
- `otlp2parquet-server` optionally adds it (lowest priority)

## Migration Strategy

**Phase 1: Create Shared Crate**
1. Create `crates/otlp2parquet-handlers`
2. Implement `OtlpError` with all variants
3. Implement `process_logs()` function
4. Add unit tests for error handling
5. Verify compilation: `cargo check -p otlp2parquet-handlers`

**Phase 2: Migrate Cloudflare (Simplest)**
1. Add `otlp2parquet-handlers` dependency
2. Replace `handle_logs_request` logic with shared processor
3. Keep existing `ErrorResponse` struct, convert from `OtlpError`
4. Run tests: `cargo test -p otlp2parquet-cloudflare`
5. Build WASM: `make wasm-compress`
6. **CRITICAL:** Verify size: `make wasm-size` (must be <3MB)
7. If size OK, continue to traces/metrics

**Phase 3: Migrate Lambda**
1. Add `otlp2parquet-handlers` dependency
2. Replace `process_logs()` logic with shared processor
3. Simplify error handling (remove ad-hoc strings)
4. Run tests: `cargo test -p otlp2parquet-lambda`
5. Continue to traces/metrics

**Phase 4: Migrate Server (Optional)**
1. Server can use shared errors for consistency
2. Keep existing batching and metrics logic
3. Lower priority - existing code works well

**Rollback Plan:**
If WASM size exceeds budget after Phase 2:
1. Add feature flags: `detailed-errors`, `rich-hints`
2. Cloudflare disables these features
3. If still too large, revert and keep duplication

## Testing Strategy

**Unit Tests (otlp2parquet-handlers):**
- Error type behavior (status codes, messages, hints)
- Processing logic for each signal type
- Error conversion scenarios

**Integration Tests (each platform):**
- Existing smoke tests continue to work
- Error responses match expected format
- Success responses have correct fields

**Size Verification:**
- Run `make wasm-size` after each migration step
- Alert if compressed size > 2.5 MB (buffer before 3MB limit)

## Success Criteria

✅ **Consistency:**
- All platforms use same error categories
- All platforms return same success response structure
- Error messages are detailed and actionable

✅ **Reduced Duplication:**
- ~300 LOC eliminated across platform handlers
- Single source of truth for signal processing
- Bug fixes only need to be applied once

✅ **Binary Size:**
- Cloudflare WASM stays under 3MB compressed
- Ideally stays under 2.5MB (buffer for future growth)

✅ **No Regressions:**
- All existing tests pass
- Smoke tests work for all platforms
- Performance unchanged

## Future Enhancements

**Not in this design (defer for later):**
1. Standardize observability (metrics/logging)
2. Unify response structure fields
3. Extract server batching logic to shared crate
4. Add integration tests across all platforms

## Open Questions

None - design approved and ready for implementation.
