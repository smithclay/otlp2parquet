# Performance Audit - OTLP to Parquet Pipeline

This document outlines the comprehensive benchmarking and profiling infrastructure used for measuring and optimizing the OTLP→Arrow→Parquet conversion pipeline.

## Quick Start

```bash
# Run complete automated audit (runs benchmarks + profiling + analysis automatically)
./scripts/perf_audit.py
# ↑ This does everything: cargo bench, cargo bloat, analysis, generates findings.json

# Or manually run individual tools if you want finer control
make bench                  # Just run benchmarks
make profile-all            # Run all profiling tools
make flamegraph             # CPU profiling only
```

## Infrastructure Overview

### 1. Benchmarking (Criterion)

**Location**: `benches/`
- `e2e_pipeline.rs` - Full OTLP→Arrow→Parquet pipeline (most realistic)
- `decode_otlp.rs` - Protobuf/JSON decoding isolation
- `transform_arrow.rs` - OTLP→Arrow conversion isolation
- `parquet_write.rs` - Arrow→Parquet serialization isolation
- `fixtures/mod.rs` - Synthetic data generators (10k/250k/1M log records)

**Workload Matrix**:
- Sizes: Small (10k), Medium (250k), Large (1M) log records
- Formats: Protobuf, JSON
- Compression: None, Gzip
- Batch sizes: 4k, 16k, 64k rows

**Run benchmarks**:
```bash
# Run all benchmarks
cargo bench

# Run specific benchmark
cargo bench --bench e2e_pipeline

# Save baseline for comparison
cargo bench -- --save-baseline before

# Compare against baseline
cargo bench -- --baseline before
```

**Output**: `target/criterion/report/index.html` (HTML dashboard with charts)

### 2. CPU Profiling (Flamegraph)

**Purpose**: Identify hot functions consuming CPU time

```bash
make flamegraph
# Output: flamegraph.svg
```

Open `flamegraph.svg` in browser to see:
- **Width** = % of total CPU time
- **Self time** = time in function itself (not callees)
- **Click to zoom** into specific call stacks

**Install dependencies**:
```bash
cargo install flamegraph
```

### 3. Binary Size Analysis

#### cargo-bloat
Shows which functions/types contribute to binary size:

```bash
make bloat
# Output: bloat.txt (top 20 symbols)
```

Example output:
```
 File  .text     Size Crate
10.2%  45.0%  500.0KB arrow_array
 5.1%  22.5%  250.0KB parquet
 2.3%  10.2%  113.0KB otlp2parquet_core
```

#### cargo-llvm-lines
Shows LLVM IR line counts (correlates with compile time and code size):

```bash
make llvm-lines
# Output: llvm_lines.txt (top 50 generic instantiations)
```

Useful for finding:
- Over-monomorphization (too many generic instantiations)
- Candidates for `#[inline(never)]` to reduce bloat

### 4. Memory Profiling (dhat)

**Status**: Scaffolded but not yet integrated

To enable:
```bash
cargo run --features dhat-heap --bin otlp2parquet-server
```

Will output allocation profile showing:
- Total bytes allocated
- Peak memory usage
- Allocation hotspots by call stack

### 5. Automated Audit Script

**Location**: `scripts/perf_audit.py`

**Runs complete audit pipeline automatically** (no need to run `make bench` first):
1. ✅ **Runs `cargo bench`** - All benchmarks (5-10 minutes)
2. ✅ Parse Criterion JSON results
3. ✅ **Runs `cargo bloat`** - Binary size analysis
4. ✅ Static code analysis for performance anti-patterns
5. ✅ Generate `findings.json` with metrics + recommendations

```bash
# Run with uv (recommended) - does EVERYTHING automatically
./scripts/perf_audit.py

# Or with Python directly
python3 scripts/perf_audit.py
```

**Output**: `findings.json` - Structured performance report

**Note**: The script runs benchmarks for you - you don't need to run `make bench` separately!

```json
{
  "summary": {...},
  "metrics": {
    "e2e_ms_p50": 45.2,
    "e2e_ms_p95": 67.8,
    "throughput_rows_per_s": 221238
  },
  "hotspots": [...],
  "findings": [...],
  "next_actions": [...]
}
```

## Optimization Patches

Three surgical optimizations targeting identified bottlenecks:

### Patch 1: Inline Annotations
**File**: `patches/001-add-inline-annotations.patch`

Adds `#[inline]` to hot path functions:
- `append_log_record` (called per log record)
- `build_resource_context` (called per resource batch)
- `clamp_nanos` (called 2× per log)

**Expected**: 5-10% latency reduction
**Rationale**: Reduces cross-crate call overhead

### Patch 2: Builder Capacity Hints
**File**: `patches/002-add-builder-capacity-hints.patch`

Pre-allocates Arrow builders with expected capacity:
- Adds `ArrowConverter::with_capacity(usize)`
- All 19 builders get capacity hints
- Batch processor passes `max_rows` as hint

**Expected**: 15-20% allocation reduction
**Rationale**: Avoids reallocation/copy during growth

### Patch 3: Service Name Arc
**File**: `patches/003-optimize-service-name-extraction.patch`

Replaces `String` with `Arc<str>` for service_name:
- Eliminates clone on every resource batch
- Shares service_name across logs in same resource

**Expected**: 10-15% string allocation reduction
**Rationale**: Service name is immutable, Arc shares ownership

## Applying Patches

```bash
# Apply all patches
git apply patches/*.patch

# Apply individually
git apply patches/001-add-inline-annotations.patch

# Check what would be applied (dry run)
git apply --check patches/001-add-inline-annotations.patch
```

## Benchmarking Workflow

### Before Optimization
```bash
# Option A: Automated (recommended)
./scripts/perf_audit.py              # Runs benchmarks + analysis automatically
cp findings.json findings-before.json

# Option B: Manual control
cargo bench                          # Run benchmarks manually
./scripts/perf_audit.py              # Still need this for analysis + findings.json
cp findings.json findings-before.json
```

### Apply Optimizations
```bash
# Apply patches
git apply patches/*.patch

# Verify compilation
cargo check
cargo test
```

### After Optimization
```bash
# 1. Benchmark with comparison
cargo bench -- --baseline before

# 2. Record new metrics
./scripts/perf_audit.py
cp findings.json findings-after.json

# 3. Compare
diff findings-before.json findings-after.json
```

### Validation Criteria

Optimizations must satisfy:
- ✅ Improve p95 latency OR alloc_bytes on ≥2 workloads
- ✅ No regressions on any workload
- ✅ All tests pass
- ✅ Zero new dependencies (dev-deps OK)
- ✅ Changes are surgical (< 50 lines per patch)

## Makefile Targets

```bash
make bench              # Run all benchmarks
make bench-baseline     # Save baseline
make bench-compare      # Compare to baseline
make flamegraph         # CPU profile
make bloat              # Binary size analysis
make llvm-lines         # LLVM IR analysis
make profile-all        # Run all profiling tools
```

## Architecture Notes

### Why Criterion?
- Statistical rigor (outlier detection, variance analysis)
- HTML reports with charts
- Baseline comparison
- Industry standard for Rust benchmarking

### Why Synthetic Data?
- Reproducible (no dependency on external files)
- Configurable sizes for scaling tests
- Realistic distribution of log attributes

### Why Not cargo-bench?
- Criterion provides more rigorous statistics
- Better reporting (HTML + charts)
- Baseline comparison built-in

### Hot Path Locations

1. **OTLP Decode**: `otlp2parquet-proto` + `prost::Message::decode`
2. **Arrow Transform**: `otlp2parquet-core/src/otlp/to_arrow.rs`
   - `append_log_record` (per-record processing)
   - Builder append operations
3. **Parquet Write**: `otlp2parquet-storage/src/parquet_writer.rs`
   - `parquet::arrow::ArrowWriter::write`
   - Compression (ZSTD/Snappy)

## Known Bottlenecks (Pre-Optimization)

From initial exploration:

1. **No inline hints** - Hot functions not marked `#[inline]`
2. **Builder reallocation** - No capacity hints, frequent realloc
3. **String clones** - service_name cloned unnecessarily
4. **JSON serialization** - Complex attributes serialized as JSON strings
5. **No vectorization** - Sequential log processing

## Future Work

- [ ] SIMD for timestamp conversion
- [ ] Arrow native List/Map instead of JSON
- [ ] Parallel batch processing
- [ ] Memory pool for builders
- [ ] PGO (Profile-Guided Optimization)
- [ ] WASM benchmarking with workerd
- [ ] Lambda cold/warm start profiling

## Troubleshooting

### Benchmarks fail to compile
```bash
cargo check --benches
# Fix compilation errors, then retry
```

### Flamegraph requires root
```bash
# On Linux, grant perf permissions
echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid
```

### cargo-bloat not found
```bash
cargo install cargo-bloat
```

### Criterion reports high variance
- Run on dedicated machine (no other load)
- Disable CPU frequency scaling
- Run multiple times and average

## References

- [Criterion.rs Docs](https://bheisler.github.io/criterion.rs/book/)
- [Flamegraph Guide](https://www.brendangregg.com/flamegraphs.html)
- [Arrow Performance Best Practices](https://arrow.apache.org/docs/cpp/performance.html)
- [Parquet Format Spec](https://parquet.apache.org/docs/file-format/)
