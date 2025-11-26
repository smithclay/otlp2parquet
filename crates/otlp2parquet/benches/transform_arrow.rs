// Arrow transformation benchmark - measure OTLP → RecordBatch conversion
//
// Isolates the Arrow builder logic from decoding and Parquet serialization.
// This is the core transformation hot path.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use otlp2parquet_core::convert_request_to_arrow;

mod fixtures;
use fixtures::{generate_otlp_logs, WorkloadSize};

/// Benchmark Arrow conversion from pre-parsed OTLP request
fn bench_otlp_to_arrow(c: &mut Criterion) {
    let mut group = c.benchmark_group("otlp_to_arrow");

    for size in [WorkloadSize::Small, WorkloadSize::Medium] {
        let request = generate_otlp_logs(size);
        let record_count = size.record_count();

        group.throughput(Throughput::Elements(record_count as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{:?}", size)),
            &request,
            |b, request| {
                b.iter(|| {
                    let (batch, metadata) = convert_request_to_arrow(request).unwrap();
                    black_box((batch, metadata));
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

    for &batch_size in &[1_000usize, 10_000, 100_000] {
        group.throughput(Throughput::Elements(batch_size as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}rows", batch_size)),
            &full_request,
            |b, request| {
                b.iter(|| {
                    // Simulate incremental processing by taking first N log records
                    let limited_request = otlp2parquet_proto::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest {
                        resource_logs: request.resource_logs.iter().take(batch_size / 1000).cloned().collect(),
                    };

                    let (batch, metadata) =
                        convert_request_to_arrow(&limited_request).unwrap();
                    black_box((batch, metadata));
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_otlp_to_arrow, bench_incremental_batching);
criterion_main!(benches);
