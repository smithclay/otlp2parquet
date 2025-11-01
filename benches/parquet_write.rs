// Parquet serialization benchmark - measure RecordBatch → Parquet overhead
//
// Isolates the Parquet writer performance from OTLP decoding and Arrow conversion.
// Tests different compression settings and row group sizes.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use otlp2parquet_core::convert_request_to_arrow;
use otlp2parquet_storage::parquet_writer;

mod fixtures;
use fixtures::{generate_otlp_logs, WorkloadSize};

/// Benchmark Parquet writing from pre-built RecordBatch
fn bench_write_parquet(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_parquet");

    for size in [WorkloadSize::Small, WorkloadSize::Medium] {
        let request = generate_otlp_logs(size);
        let (batch, _metadata) = convert_request_to_arrow(&request).unwrap();
        let record_count = size.record_count();

        group.throughput(Throughput::Elements(record_count as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{:?}", size)),
            &batch,
            |b, batch| {
                b.iter(|| {
                    let mut buffer = Vec::new();
                    parquet_writer::write_batches_to_parquet(&mut buffer, vec![batch.clone()])
                        .unwrap();
                    black_box(buffer);
                });
            },
        );
    }

    group.finish();
}

/// Benchmark Parquet writing with hash computation (production path)
fn bench_write_parquet_with_hash(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_parquet_with_hash");

    for size in [WorkloadSize::Small, WorkloadSize::Medium] {
        let request = generate_otlp_logs(size);
        let (batch, _metadata) = convert_request_to_arrow(&request).unwrap();
        let record_count = size.record_count();

        group.throughput(Throughput::Elements(record_count as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{:?}", size)),
            &batch,
            |b, batch| {
                b.iter(|| {
                    let (buffer, hash) =
                        parquet_writer::write_batches_with_hash(vec![batch.clone()]).unwrap();
                    black_box((buffer, hash));
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_write_parquet, bench_write_parquet_with_hash);
criterion_main!(benches);
