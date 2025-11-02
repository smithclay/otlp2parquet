# Performance, Benchmarking, and Profiling

This guide covers the tools and workflows for measuring, analyzing, and optimizing the performance of the `otlp2parquet` pipeline.

## Measuring Performance

The first step in optimization is measurement. This project uses the [Criterion](https://bheisler.github.io/criterion.rs/book/) benchmarking framework.

### Automated Audit (Recommended)

The easiest way to get a comprehensive performance overview is to run the automated audit script. This script runs all benchmarks and profilers and generates a consolidated report.

```bash
# Run the complete automated audit
./scripts/perf_audit.py
```

This produces a `findings.json` file with key metrics, hotspots, and potential areas for improvement.

### Manual Benchmarking

For more granular analysis, you can run the benchmark suites directly. This is useful for testing a specific part of the pipeline.

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

Once you have performance measurements, the next step is to identify the specific parts of the code that are slow or inefficient.

### CPU Profiling with Flamegraph

To find functions that consume the most CPU time, generate a flamegraph.

```bash
make flamegraph
```

This command generates an `flamegraph.svg` file. Open it in a browser to explore the call stacks. The wider a function appears, the more CPU time it consumed.

### Binary Size Analysis

For a serverless function, binary size is a key performance metric. Use `cargo-bloat` to see which functions and dependencies contribute most to the binary size.

```bash
make bloat
```

### Memory Profiling

To profile memory allocations, you can build the project with the `dhat-heap` feature. This is useful for finding memory leaks or excessive allocations.

```bash
cargo run --features dhat-heap --bin otlp2parquet-server
```

## Applying and Verifying Optimizations

This project contains several [example optimization patches](https://github.com/smithclay/otlp2parquet/tree/main/patches) that demonstrate how bottlenecks were previously identified and fixed. They serve as a good reference.

### Optimization Workflow

When applying a new optimization, follow this workflow to validate its impact:

1.  **Establish a Baseline**: Run the benchmarks before applying your change.

    ```bash
    make bench-baseline
    ```

2.  **Apply Your Change**: Apply your optimization patch or make your code changes.

3.  **Verify and Compare**: Run the benchmarks again and compare against the baseline you saved.

    ```bash
    make bench-compare
    ```

4.  **Validate**: An optimization should improve a key metric (like latency or allocations) without causing regressions in other areas. All tests must continue to pass.

## Future Work

Potential future performance improvements include:

*   Using SIMD for timestamp conversion.
*   Using Arrow-native List/Map types instead of JSON strings for attributes.
*   Implementing parallel batch processing.
*   Adding Profile-Guided Optimization (PGO) to the build process.

## Troubleshooting

*   **Benchmarks fail to compile**: Run `cargo check --benches` to isolate compilation errors.
*   **Flamegraph requires root**: On some Linux systems, you may need to grant perf permissions: `echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid`.
*   **High Variance in Criterion**: Ensure your machine has no other significant load. For best results, disable CPU frequency scaling.
