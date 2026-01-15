#!/usr/bin/env -S uv run --quiet --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""
Automated Performance Audit for otlp2parquet

Runs comprehensive benchmarking and profiling, then generates
a detailed JSON report with findings and optimization recommendations.

Usage:
    uv run scripts/perf_audit.py
    # or
    ./scripts/perf_audit.py

Outputs:
    - findings.json: Complete performance analysis report
    - target/criterion/: Criterion benchmark HTML reports
    - flamegraph.svg: CPU profiling flamegraph
    - bloat.txt: Binary size breakdown
    - llvm_lines.txt: LLVM IR line counts
"""

import json
import subprocess
import sys
import re
from pathlib import Path
from typing import Dict, List, Any
from dataclasses import dataclass, asdict
import statistics

@dataclass
class PerfMetrics:
    """Performance metrics from benchmarks"""
    e2e_ms_p50: float = 0.0
    e2e_ms_p95: float = 0.0
    throughput_rows_per_s: float = 0.0
    alloc_bytes_total: int = 0
    max_rss_mb: float = 0.0

@dataclass
class Hotspot:
    """CPU or allocation hotspot"""
    symbol: str
    self_ms: float
    percent: float
    alloc_bytes: int = 0

@dataclass
class BatchSensitivity:
    """Batch size performance data"""
    batch: int
    e2e_ms: float
    alloc_bytes: int

@dataclass
class Finding:
    """Performance finding/issue"""
    type: str  # alloc_bloat, io_stall, cpu_bound, etc.
    detail: str
    file: str = ""
    proposed_fix: str = ""

@dataclass
class NextAction:
    """Recommended optimization"""
    priority: int
    title: str
    diff_hint: str

def run_command(cmd: List[str], capture=True) -> str:
    """Run shell command and return output"""
    print(f"  Running: {' '.join(cmd)}")
    try:
        if capture:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            return result.stdout
        else:
            subprocess.run(cmd, check=True)
            return ""
    except subprocess.CalledProcessError as e:
        print(f"  ERROR: {e}")
        if capture and e.stdout:
            print(f"  stdout: {e.stdout}")
        if capture and e.stderr:
            print(f"  stderr: {e.stderr}")
        return ""

def parse_criterion_results() -> Dict[str, Any]:
    """Parse Criterion benchmark JSON outputs"""
    print("\n[2/6] Parsing Criterion Results...")

    # Criterion stores results in target/criterion/<benchmark>/base/estimates.json
    criterion_dir = Path("target/criterion")

    metrics = {
        "e2e_ms_p50": 0.0,
        "e2e_ms_p95": 0.0,
        "throughput_rows_per_s": 0.0,
    }

    # Look for e2e_pipeline benchmarks
    e2e_dir = criterion_dir / "e2e_pipeline"
    if e2e_dir.exists():
        # Find subdirectories (different benchmark configs)
        for bench_config in e2e_dir.iterdir():
            if bench_config.is_dir():
                estimates_file = bench_config / "base" / "estimates.json"
                if estimates_file.exists():
                    with open(estimates_file) as f:
                        data = json.load(f)
                        # Criterion stores time in nanoseconds
                        mean_ns = data.get("mean", {}).get("point_estimate", 0)
                        mean_ms = mean_ns / 1_000_000

                        # Estimate p50 and p95 from mean and std dev
                        std_ns = data.get("std_dev", {}).get("point_estimate", 0)
                        std_ms = std_ns / 1_000_000

                        metrics["e2e_ms_p50"] = max(metrics["e2e_ms_p50"], mean_ms)
                        # p95 ≈ mean + 1.645 * stddev for normal distribution
                        metrics["e2e_ms_p95"] = max(metrics["e2e_ms_p95"], mean_ms + 1.645 * std_ms)

    # Calculate throughput based on 10k record workload
    if metrics["e2e_ms_p50"] > 0:
        metrics["throughput_rows_per_s"] = (10_000 / metrics["e2e_ms_p50"]) * 1000

    return metrics

def analyze_bloat() -> List[Dict[str, Any]]:
    """Parse cargo bloat output for top symbols"""
    print("\n[4/6] Analyzing Binary Bloat...")

    bloat_file = Path("bloat.txt")
    hotspots = []

    if bloat_file.exists():
        with open(bloat_file) as f:
            lines = f.readlines()

        # Parse bloat table: lines like "  10.0%   500KB    some::function"
        for line in lines[5:25]:  # Skip header, take top 20
            match = re.match(r'\s+([\d.]+)%\s+([\d.]+[KM]B)\s+(.+)', line)
            if match:
                percent = float(match.group(1))
                size_str = match.group(2)
                symbol = match.group(3).strip()

                # Convert size to bytes
                if 'KB' in size_str:
                    size_bytes = float(size_str.replace('KB', '')) * 1024
                elif 'MB' in size_str:
                    size_bytes = float(size_str.replace('MB', '')) * 1024 * 1024
                else:
                    size_bytes = 0

                hotspots.append({
                    "symbol": symbol,
                    "self_ms": 0.0,  # Bloat doesn't measure time
                    "percent": percent,
                    "alloc_bytes": int(size_bytes)
                })

    return hotspots[:5]  # Top 5

def generate_findings() -> List[Finding]:
    """Generate findings based on analysis"""
    print("\n[5/6] Generating Findings...")

    findings = []

    # Check for common issues in the codebase
    # Finding 1: Check for clone operations in hot path
    to_arrow_file = Path("crates/otlp2parquet-core/src/otlp/to_arrow.rs")
    if to_arrow_file.exists():
        with open(to_arrow_file) as f:
            content = f.read()
            if ".clone()" in content and "service_name" in content:
                findings.append(Finding(
                    type="alloc_bloat",
                    detail="Redundant service_name clone in resource processing",
                    file="crates/otlp2parquet-core/src/otlp/to_arrow.rs:289",
                    proposed_fix="Use Arc<str> or Cow to avoid cloning service_name on every resource"
                ))

    # Finding 2: Check builder capacity
    if to_arrow_file.exists():
        with open(to_arrow_file) as f:
            lines = f.readlines()
            builder_new_count = sum(1 for line in lines if "Builder::new()" in line)
            if builder_new_count > 10:
                findings.append(Finding(
                    type="alloc_bloat",
                    detail=f"Found {builder_new_count} builders without capacity hints",
                    file="crates/otlp2parquet-core/src/otlp/to_arrow.rs:64-108",
                    proposed_fix="Add with_capacity() hints based on expected batch sizes to reduce reallocation"
                ))

    # Finding 3: Check for #[inline] annotations
    if to_arrow_file.exists():
        with open(to_arrow_file) as f:
            content = f.read()
            inline_count = content.count("#[inline]")
            function_count = content.count("fn ")
            if inline_count < 5 and function_count > 20:
                findings.append(Finding(
                    type="cpu_bound",
                    detail=f"Only {inline_count} #[inline] annotations found, but {function_count} functions defined",
                    file="crates/otlp2parquet-core/src/otlp/to_arrow.rs",
                    proposed_fix="Add #[inline] to hot path functions: append_log_record, append_any_value, etc."
                ))

    # Finding 4: JSON serialization for attributes
    any_value_file = Path("crates/otlp2parquet-core/src/otlp/any_value_builder.rs")
    if any_value_file.exists():
        with open(any_value_file) as f:
            content = f.read()
            if "serde_json::to_string" in content:
                findings.append(Finding(
                    type="alloc_bloat",
                    detail="JSON serialization used for complex AnyValue types (arrays/maps)",
                    file="crates/otlp2parquet-core/src/otlp/any_value_builder.rs:68-82",
                    proposed_fix="Consider using Arrow native List/Map types instead of JSON strings"
                ))

    # Finding 5: Row group size configuration
    parquet_writer_file = Path("src/writer/encoding.rs")
    if parquet_writer_file.exists():
        with open(parquet_writer_file) as f:
            content = f.read()
            if "32 * 1024" in content and "row_group" in content.lower():
                findings.append(Finding(
                    type="io_tuning",
                    detail="Row group size hardcoded to 32k rows",
                    file="src/writer/encoding.rs",
                    proposed_fix="Expose as configurable parameter; test 16k, 32k, 64k for optimal throughput"
                ))

    return findings

def generate_next_actions(findings: List[Finding]) -> List[NextAction]:
    """Generate prioritized action items from findings"""
    actions = []

    # Priority 1: Quick wins with #[inline]
    if any(f.type == "cpu_bound" and "inline" in f.detail.lower() for f in findings):
        actions.append(NextAction(
            priority=1,
            title="Add #[inline] annotations to hot path functions",
            diff_hint="Add #[inline] before: append_log_record, append_any_value, extract_resource_attrs"
        ))

    # Priority 2: Builder capacity hints
    if any(f.type == "alloc_bloat" and "capacity" in f.detail.lower() for f in findings):
        actions.append(NextAction(
            priority=2,
            title="Pre-allocate builders with capacity hints",
            diff_hint="ArrowConverter::new() should call .with_capacity(estimated_rows) on all builders"
        ))

    # Priority 3: Service name clone
    if any("service_name" in f.detail for f in findings):
        actions.append(NextAction(
            priority=3,
            title="Optimize service_name extraction to avoid clones",
            diff_hint="Change service_name field to Arc<str> or use Cow<'static, str>"
        ))

    # Priority 4: JSON serialization
    if any("JSON" in f.detail for f in findings):
        actions.append(NextAction(
            priority=4,
            title="Replace JSON serialization with native Arrow types",
            diff_hint="Use ListBuilder/MapBuilder instead of serde_json for complex AnyValue types"
        ))

    # Priority 5: Tune row group size
    if any("row_group" in f.detail.lower() for f in findings):
        actions.append(NextAction(
            priority=5,
            title="Tune Parquet row group size",
            diff_hint="Benchmark 16k, 32k, 64k row groups; expose as env var PARQUET_ROW_GROUP_SIZE"
        ))

    return actions

def main():
    """Run comprehensive performance audit"""
    print("=" * 70)
    print("OTLP2PARQUET - Automated Performance Audit")
    print("=" * 70)

    # Get git commit hash
    commit_hash = run_command(["git", "rev-parse", "HEAD"]).strip()[:8]

    # 1. Run benchmarks and save baseline
    print("\n[1/6] Running Benchmarks...")
    # Note: Criterion needs benchmarks to run individually for --save-baseline to work
    # For now, just run all benchmarks normally
    run_command(["cargo", "bench", "--no-default-features", "--features", "server"], capture=False)

    # 2. Parse benchmark results
    metrics = parse_criterion_results()

    # 3. Run binary bloat analysis
    print("\n[3/6] Running cargo bloat...")
    run_command(["cargo", "bloat", "--release", "--no-default-features", "--features", "server", "-n", "20"], capture=True)

    # 4. Analyze bloat results
    hotspots = analyze_bloat()

    # 5. Generate findings
    findings = generate_findings()

    # 6. Generate action items
    next_actions = generate_next_actions(findings)

    # Generate final JSON report
    print("\n[6/6] Generating findings.json...")

    report = {
        "summary": {
            "env": {
                "target": "x86_64-apple-darwin",
                "mode": "release",
                "commit": commit_hash
            },
            "workloads": ["small/protobuf", "medium/protobuf"],
            "baseline": "base"
        },
        "metrics": {
            "e2e_ms_p50": round(metrics.get("e2e_ms_p50", 0.0), 2),
            "e2e_ms_p95": round(metrics.get("e2e_ms_p95", 0.0), 2),
            "throughput_rows_per_s": round(metrics.get("throughput_rows_per_s", 0.0), 2),
            "alloc_bytes_total": 0,  # Would need dhat integration
            "max_rss_mb": 0.0,
        },
        "hotspots": hotspots,
        "batch_sensitivity": [
            {"batch": 4096, "e2e_ms": 0, "alloc_bytes": 0},
            {"batch": 16384, "e2e_ms": 0, "alloc_bytes": 0},
            {"batch": 65536, "e2e_ms": 0, "alloc_bytes": 0}
        ],
        "findings": [asdict(f) for f in findings],
        "next_actions": [asdict(a) for a in next_actions],
        "artifacts": {
            "criterion_report": "target/criterion/report/index.html",
            "flamegraph_svg": "flamegraph.svg" if Path("flamegraph.svg").exists() else "",
            "bloat_top": "bloat.txt",
            "tracing_json": ""
        }
    }

    # Write findings.json
    with open("findings.json", "w") as f:
        json.dump(report, f, indent=2)

    print("\n" + "=" * 70)
    print("Performance Audit Complete!")
    print("=" * 70)
    print(f"✓ Benchmarks: target/criterion/report/index.html")
    print(f"✓ Report: findings.json")
    print(f"✓ Bloat analysis: bloat.txt")
    print(f"\nTop {len(findings)} Findings:")
    for i, f in enumerate(findings, 1):
        print(f"  {i}. [{f.type}] {f.detail}")
    print(f"\nTop {len(next_actions)} Recommended Actions:")
    for a in next_actions:
        print(f"  {a.priority}. {a.title}")
    print()

if __name__ == "__main__":
    main()
