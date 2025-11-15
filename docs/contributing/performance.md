# Performance and Profiling

This guide covers the tools and workflows for measuring, analyzing, and optimizing the performance of the `otlp2parquet` pipeline.

## Measuring Performance

The first step in optimization is measurement. This project uses the [Criterion](https://bheisler.github.io/criterion.rs/book/) benchmarking framework and provides `make` targets to simplify the process.

!!! tip "Start with the Automated Audit"
    The easiest way to get a comprehensive performance overview is to run the automated audit script.
    ```bash
    ./scripts/perf_audit.py
    ```
    This script runs all benchmarks and profilers and generates a `findings.json` report with key metrics and hotspots.

For more granular analysis, you can run the benchmark suites directly.
```bash
# Run all benchmarks
make bench

# Save a baseline for later comparison
make bench-baseline

# Compare the current version against the baseline
make bench-compare
```
The benchmark output is an HTML dashboard located at `target/criterion/report/index.html`.

## Finding Bottlenecks

Once you have measurements, the next step is to identify slow or inefficient code.

*   **CPU Profiling (Flamegraph)**: To find functions that consume the most CPU time, generate a flamegraph.
    ```bash
    make flamegraph
    ```
*   **Binary Size Analysis**: For serverless functions, binary size is a key metric. Use `cargo-bloat` to see which functions and dependencies contribute most to the binary size.
    ```bash
    make bloat
    ```
*   **Memory Profiling**: To find memory leaks or excessive allocations, build with the `dhat-heap` feature.
    ```bash
    cargo run --features dhat-heap --bin otlp2parquet-server
    ```

## Verifying Optimizations

When applying an optimization, follow this workflow to validate its impact:
1.  **Establish a Baseline**: `make bench-baseline`
2.  **Apply Your Change**.
3.  **Verify and Compare**: `make bench-compare`
4.  **Validate**: Ensure the change improves a key metric (like latency or allocations) without causing regressions. All tests must continue to pass.

The [example optimization patches](https://github.com/smithclay/otlp2parquet/tree/main/patches) in the repository serve as good references.

## Troubleshooting
*   **Benchmarks fail to compile**: Run `cargo check --benches` to isolate errors.
*   **Flamegraph requires root**: On some Linux systems, you may need to grant perf permissions: `echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid`.
*   **High Variance in Criterion**: Ensure your machine has no other significant load. For best results, disable CPU frequency scaling.
