// Arrow transformation benchmark - measure OTLP → RecordBatch conversion
//
// Isolates the Arrow builder logic from decoding and Parquet serialization.
// This is the core transformation hot path.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use otlp2records::{apply_log_transform, decode_logs, logs_schema, values_to_arrow, InputFormat};

mod fixtures;
use fixtures::{generate_otlp_logs, to_protobuf, WorkloadSize};

/// Benchmark Arrow conversion from pre-parsed OTLP request
fn bench_otlp_to_arrow(c: &mut Criterion) {
    let mut group = c.benchmark_group("otlp_to_arrow");

    for size in [WorkloadSize::Small, WorkloadSize::Medium] {
        let request = generate_otlp_logs(size);
        let bytes = to_protobuf(&request);
        let values = decode_logs(&bytes, InputFormat::Protobuf).unwrap();
        let transformed = apply_log_transform(values).unwrap();
        let record_count = size.record_count();

        group.throughput(Throughput::Elements(record_count as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{:?}", size)),
            &transformed,
            |b, transformed| {
                b.iter(|| {
                    let batch = values_to_arrow(transformed, &logs_schema()).unwrap();
                    black_box(batch);
                });
            },
        );
    }

    group.finish();
}

/// Benchmark incremental batch building
/// Tests memory allocation patterns for different batch sizes
fn bench_incremental_batching(c: &mut Criterion) {
    let mut group = c.benchmark_group("incremental_batching");

    // Generate a large dataset and process in different batch sizes
    let full_request = generate_otlp_logs(WorkloadSize::Medium);
    let bytes = to_protobuf(&full_request);
    let values = decode_logs(&bytes, InputFormat::Protobuf).unwrap();
    let transformed = apply_log_transform(values).unwrap();

    for &batch_size in &[1_000usize, 10_000, 100_000] {
        group.throughput(Throughput::Elements(batch_size as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}rows", batch_size)),
            &transformed,
            |b, transformed| {
                b.iter(|| {
                    let end = batch_size.min(transformed.len());
                    let batch = values_to_arrow(&transformed[..end], &logs_schema()).unwrap();
                    black_box(batch);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_otlp_to_arrow, bench_incremental_batching);
criterion_main!(benches);
