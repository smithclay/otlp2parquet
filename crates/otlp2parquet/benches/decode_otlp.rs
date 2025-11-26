// OTLP decode benchmark - measure only protobuf/JSON parsing overhead
//
// Isolates the deserialization step from Arrow conversion.
// This helps identify if parsing is a bottleneck vs transformation logic.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use otlp2parquet_proto::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;
use prost::Message;

mod fixtures;
use fixtures::{generate_otlp_logs, WorkloadSize};

/// Benchmark protobuf decoding
fn bench_decode_protobuf(c: &mut Criterion) {
    let mut group = c.benchmark_group("decode_protobuf");

    for size in [WorkloadSize::Small, WorkloadSize::Medium] {
        let request = generate_otlp_logs(size);
        let data = fixtures::to_protobuf(&request);
        let record_count = size.record_count();

        group.throughput(Throughput::Elements(record_count as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{:?}", size)),
            &data,
            |b, data| {
                b.iter(|| {
                    let decoded = ExportLogsServiceRequest::decode(&data[..]).unwrap();
                    black_box(decoded);
                });
            },
        );
    }

    group.finish();
}

/// Benchmark JSON decoding
fn bench_decode_json(c: &mut Criterion) {
    let mut group = c.benchmark_group("decode_json");

    for size in [WorkloadSize::Small, WorkloadSize::Medium] {
        let request = generate_otlp_logs(size);
        let data = fixtures::to_json(&request);
        let record_count = size.record_count();

        group.throughput(Throughput::Elements(record_count as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{:?}", size)),
            &data,
            |b, data| {
                b.iter(|| {
                    let decoded: ExportLogsServiceRequest = serde_json::from_slice(data).unwrap();
                    black_box(decoded);
                });
            },
        );
    }

    group.finish();
}

/// Benchmark gzip decompression + protobuf decode
fn bench_decode_gzipped(c: &mut Criterion) {
    let mut group = c.benchmark_group("decode_gzipped");

    for size in [WorkloadSize::Small, WorkloadSize::Medium] {
        let request = generate_otlp_logs(size);
        let data = fixtures::to_protobuf(&request);
        let compressed = fixtures::compress_gzip(&data);
        let record_count = size.record_count();

        group.throughput(Throughput::Elements(record_count as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{:?}", size)),
            &compressed,
            |b, compressed_data| {
                b.iter(|| {
                    use flate2::read::GzDecoder;
                    use std::io::Read;

                    let mut decoder = GzDecoder::new(&compressed_data[..]);
                    let mut decompressed = Vec::new();
                    decoder.read_to_end(&mut decompressed).unwrap();
                    let decoded = ExportLogsServiceRequest::decode(&decompressed[..]).unwrap();
                    black_box(decoded);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_decode_protobuf,
    bench_decode_json,
    bench_decode_gzipped
);
criterion_main!(benches);
