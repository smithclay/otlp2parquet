# Architecture Review Report: otlp2parquet MVP Readiness

**Date:** 2025-11-22
**Branch:** `write-path-simplification`
**Reviewer:** Brooks Architecture Principles Analysis

---

## Executive Summary

**Status: READY FOR MVP** with 2 critical fixes needed first.

The `write-path-simplification` branch shows **strong conceptual integrity** through its consolidation of storage/iceberg logic into a unified writer crate. The architecture demonstrates **admirable restraint** against second-system effect, leveraging mature external libraries (icepick, OpenDAL) rather than building abstractions prematurely.

**WASM binary: 8.8 MB uncompressed, 2.01 MB gzipped** - **WITHIN BUDGET** (target: <3MB compressed) ✅

However, there are **2 blocking issues** and several important improvements needed before shipping.

---

## 🔴 CRITICAL ISSUES (Must Fix for MVP)

### 1. **`.unwrap()` in Handler Error Paths**
**Priority: BLOCKER**
**Files:** `crates/otlp2parquet-cloudflare/src/handlers.rs:42, 66, 100`

**Problem:**
```rust
worker::Error::RustError(format!("{}:{}", status_code,
    serde_json::to_string(&error_response).unwrap_or_default()))
```

Using `.unwrap_or_default()` after `serde_json::to_string` on an error handling path. If serialization fails during error handling, you return an **empty string** instead of a meaningful error.

**Brooks Principle Violated:** *Conceptual Integrity* - Error handling should be consistent and predictable. Silent failures in error paths create debugging nightmares.

**Recommendation:**
```rust
// Replace .unwrap_or_default() with a static fallback
let error_json = serde_json::to_string(&error_response)
    .unwrap_or_else(|_| r#"{"error":"internal error","code":"SERIALIZATION_FAILED"}"#.to_string());
worker::Error::RustError(format!("{}:{}", status_code, error_json))
```

**Impact:** Silent failures in error responses could mask bugs in production.

---

### 2. **Missing Error Context in `write_batch` Direct Parquet Path**
**Priority: BLOCKER for plain Parquet mode users**
**File:** `crates/otlp2parquet-writer/src/write.rs:180`

**Problem:**
The plain Parquet write path (when catalog is None) has poor error messages:

```rust
let operator = crate::storage::get_operator()
    .ok_or_else(|| WriterError::WriteFailure(
        "Storage operator not initialized".to_string()
    ))?;
```

This error doesn't tell users **what they need to do** - it's a symptom, not a diagnosis.

**Brooks Principle Violated:** *User-Oriented Design* - Errors should guide users toward solutions, not leave them stranded.

**Recommendation:**
```rust
let operator = crate::storage::get_operator().ok_or_else(|| {
    WriterError::WriteFailure(
        "Storage operator not initialized. Call initialize_storage() with RuntimeConfig before writing. \
         For catalog mode, ensure catalog is provided in WriteBatchRequest.".to_string()
    )
})?;
```

**Impact:** Users will struggle to debug configuration issues in plain Parquet mode.

---

## 🟡 IMPORTANT ISSUES (Should Fix Pre-MVP)

### 3. **Duplication: Signal Processing Logic Across 3 Platforms**
**Priority: HIGH - Will cause maintenance burden**
**Files:**
- `crates/otlp2parquet-lambda/src/handlers.rs:81-160`
- `crates/otlp2parquet-cloudflare/src/handlers.rs:12-110`
- `crates/otlp2parquet-server/src/handlers.rs:30-90`

**Problem:**
Each platform crate reimplements nearly identical logic for:
- Parsing OTLP request → Arrow batches
- Splitting by service
- Calling `write_batch`
- Error handling patterns

**Brooks Principle Violated:** *Surgical Team / Plan to Throw One Away* - Duplication is technical debt that will bite you when you need to fix bugs or add features (e.g., new metric types, schema changes).

**Evidence:**
```rust
// Lambda version (handlers.rs:105)
let per_service_requests = otlp::logs::split_request_by_service(request);
for subset in per_service_requests {
    let batch = state.passthrough.ingest(&subset)?;
    // ... write logic
}

// Cloudflare version (handlers.rs:45) - IDENTICAL
let per_service_requests = otlp::logs::split_request_by_service(request);
for subset in per_service_requests {
    let batch = passthrough.ingest(&subset).map_err(...)?;
    // ... write logic (slightly different error handling)
}
```

**Recommendation:**
Create a **thin shared handler** in `otlp2parquet-core` or `otlp2parquet-batch`:

```rust
// NEW: crates/otlp2parquet-core/src/signal_processor.rs
pub async fn process_signal_request(
    body: &[u8],
    format: InputFormat,
    signal_type: SignalType,
    catalog: Option<&dyn Catalog>,
    namespace: &str,
    timestamp_ms_provider: impl Fn() -> i64,
) -> Result<Vec<String>> {
    // Common logic extracted here
    // Returns paths written
}
```

Then platforms just call this with platform-specific error translation. **This is not premature abstraction** - you have 3 concrete implementations proving the pattern.

**LOC Impact:** Eliminate ~300 lines of duplication, consolidate to ~150 lines of shared code.

---

### 4. **`println!` in Production Code**
**Priority: MEDIUM - Observability gap**
**File:** `crates/otlp2parquet-writer/src/write.rs:183, 207`

**Problem:**
```rust
println!("WARNING: Writing plain Parquet file without catalog integration");
```

This violates your own coding standards (AGENTS.md: "Use tracing macros exclusively, NEVER use println!/eprintln! in production code").

**Brooks Principle Violated:** *Conceptual Integrity* - Logging inconsistency makes troubleshooting harder. CloudWatch/structured logs won't capture `println!`.

**Recommendation:**
```rust
tracing::warn!(
    table = %table_name,
    path = %parquet_path,
    "Writing plain Parquet file without catalog integration"
);
```

**Impact:** Missing observability in Lambda CloudWatch logs and production debugging.

---

### 5. **Incomplete Makefile Targets**
**Priority: MEDIUM - Developer experience**

**Problem:**
- `make wasm-size` doesn't exist (referenced in AGENTS.md)
- No target to check compressed WASM size against budget

**Brooks Principle Violated:** *Conceptual Integrity* - Documentation promises tools that don't exist.

**Recommendation:**
Add to Makefile:
```makefile
.PHONY: wasm-size
wasm-size: build-cloudflare ## Show uncompressed and gzipped WASM binary sizes
	@echo "==> WASM Binary Size Report"
	@ls -lh target/wasm32-unknown-unknown/release/*.wasm | awk '{print "Uncompressed:", $$5, $$9}'
	@gzip -c target/wasm32-unknown-unknown/release/otlp2parquet_cloudflare.wasm | wc -c | awk '{printf "Compressed:   %.2f MB (budget: 3.00 MB)\n", $$1/1024/1024}'
```

**Impact:** Developers can't easily verify they're staying within WASM budget constraints.

---

## 🟢 NICE-TO-HAVE (Post-MVP)

### 6. **Large Function Smell: `write_batch()` at 528 LOC**
**Priority: LOW - Works but hard to test**

The `write.rs:write_batch()` function is doing too much:
- Table name resolution
- Catalog vs non-catalog branching
- Direct Parquet writing (plain mode)
- Three different AppendResult cases
- Error translation

**Not a blocker** because it's well-commented and working, but consider splitting:
```rust
async fn write_with_catalog(...) -> Result<String>
async fn write_plain_parquet(...) -> Result<String>
```

This would make unit testing easier (mock catalog vs mock storage separately).

**Deferral Rationale:** Function complexity is high but well-contained. Can refactor when adding new features.

---

### 7. **Clone-Heavy Pattern in Handlers**
**Priority: LOW - Minor performance concern**

**Pattern seen:** `req.batch.clone()` in write paths (cloudflare/handlers.rs:118, lambda/handlers.rs:145)

RecordBatch uses Arc internally, so `.clone()` is cheap, but it's still unnecessary in non-catalog paths. Consider passing references where possible.

**Not urgent** - this is idiomatic Rust for shared data, and Arrow's Arc makes it cheap.

---

## ✅ STRENGTHS (What's Working Well)

### 1. **Excellent Use of External Abstractions**
You're using `icepick` for Iceberg/Parquet operations and `opendal` for storage. This is **textbook Brooks** - "plan to throw one away" means **don't write what already exists**. This saved you ~2000 LOC of Parquet/catalog code.

### 2. **Strong Workspace Separation**
Crate boundaries are clean:
- `otlp2parquet-core`: Signal schemas (no platform deps)
- `otlp2parquet-writer`: Write path abstraction (platform-agnostic)
- `otlp2parquet-{lambda,cloudflare,server}`: Platform adapters only

This is **conceptual integrity** in action - each crate has a single, clear purpose.

### 3. **WASM Budget Met (2.01 MB compressed)**
You hit your critical <3MB constraint with room to spare. The `opt-level = "z"` and feature stripping worked.

**Breakdown:**
- Uncompressed: 8.8 MB
- Gzipped: 2.01 MB (33% margin under 3MB budget)

### 4. **No Premature Optimization**
No speculative generality detected. Every abstraction serves 2+ concrete use cases (e.g., catalog modes, signal types).

### 5. **Error Handling Philosophy**
The "warn-and-succeed" pattern for catalog failures (write Parquet first, catalog is best-effort) is the **right tradeoff** for an ingestion pipeline. Data durability > metadata consistency.

### 6. **No Production Panics**
Zero instances of `.unwrap()` or `.expect()` in production hot paths (except the error serialization issue #1, which is fixable). Error propagation uses proper `Result` types throughout.

### 7. **Platform Detection Done Right**
Runtime platform detection via environment variables (`CF_WORKER`, `AWS_LAMBDA_FUNCTION_NAME`) is clean and avoids compile-time feature flag complexity.

---

## Detailed Metrics

### Code Quality
- **Zero clippy warnings** (enforced via CI)
- **No TODO/FIXME/HACK comments** in production code
- **Consistent error handling** with custom error types
- **Proper tracing** throughout (except 2 println violations)

### Dependency Management
- All workspace deps use `default-features = false`
- Minimal Arrow features (just `ipc`)
- No duplicate dependencies in tree
- WASM-incompatible deps properly gated with `cfg(not(target_arch = "wasm32"))`

### Architecture
- 8 focused crates with clear boundaries
- 1,010 LOC in writer crate (reasonable)
- Single source of truth for schemas (otlp2parquet-core)
- Platform adapters are thin (~200-300 LOC each)

---

## Summary of Blocking Issues

| # | Issue | Severity | Estimated Fix Time | Files |
|---|-------|----------|-------------------|-------|
| 1 | `.unwrap()` in error handlers | BLOCKER | 5 min | cloudflare/handlers.rs:42,66,100 |
| 2 | Poor error messages in write path | BLOCKER | 10 min | writer/write.rs:180 |
| 3 | Duplicated signal processing | HIGH | 2-3 hours | All 3 platform handlers |
| 4 | `println!` in production | MEDIUM | 5 min | writer/write.rs:183,207 |
| 5 | Missing Makefile targets | MEDIUM | 15 min | Makefile |

**Total critical path:** ~25 minutes to unblock MVP
**Total for clean MVP:** ~3-4 hours including duplication removal

---

## Action Plan

### Phase 1: MVP Blockers (Required)
1. ✅ Fix `.unwrap_or_default()` in cloudflare error handlers
2. ✅ Add context to "Storage operator not initialized" error
3. ✅ Replace `println!` with `tracing::warn!`

**Time:** ~20 minutes
**Risk:** None - these are isolated changes

### Phase 2: Quality Improvements (Recommended)
4. ⚠️ Extract shared signal processing logic to eliminate duplication
5. ✅ Add `make wasm-size` target

**Time:** ~3 hours
**Risk:** Low - refactoring well-tested code paths

### Phase 3: Post-MVP (Optional)
6. Consider splitting `write_batch()` into smaller functions
7. Profile and optimize clone-heavy patterns if performance issues arise

---

## Final Recommendation

**Ship this architecture.** The consolidation into `otlp2parquet-writer` was the right move - you now have one place to fix bugs instead of three. Fix the two blockers (#1, #2) immediately, then decide if you want to tackle duplication (#3) pre-MVP or in the first maintenance cycle.

The code is **idiomatic Rust**, the abstractions are **appropriate to the problem**, and you've **resisted second-system effect**. This is a solid foundation for an MVP.

### Brooks Principles Assessment

| Principle | Grade | Notes |
|-----------|-------|-------|
| Conceptual Integrity | A- | Clean abstraction layers, minor logging inconsistency |
| Second-System Effect | A | No over-engineering, appropriate use of external libs |
| Plan to Throw One Away | A+ | Leveraging icepick/opendal instead of NIH |
| Surgical Team | B+ | Duplication suggests need for shared components |
| Information Hiding | A | Clean crate boundaries, good encapsulation |
| User-Oriented Design | B | Good overall, but some error messages need work |

**Overall: B+ / Ready for MVP with minor fixes**

---

## Appendix: Recent Changes Analysis

Based on git diff stats vs `main`:

**Major additions:**
- Unified smoke test architecture (+386 LOC in docs)
- R2 Data Catalog integration (+247 LOC)
- Lambda plain Parquet mode (+297 LOC design doc)
- Comprehensive testing documentation (+316 LOC)

**Consolidation:**
- Removed separate storage/iceberg crates (saved ~400 LOC)
- Unified writer interface via icepick
- Removed example code in favor of smoke tests

**Net result:** More features, less code. Good sign of architectural maturity.
