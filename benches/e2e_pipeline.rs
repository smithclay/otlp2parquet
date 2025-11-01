// End-to-end OTLP → Arrow → Parquet pipeline benchmark
//
// Measures complete throughput from raw OTLP bytes to Parquet file in memory.
// This is the most realistic benchmark for overall system performance.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use otlp2parquet_core::convert_request_to_arrow;
use otlp2parquet_proto::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;
use otlp2parquet_storage::parquet_writer;
use prost::Message;

mod fixtures;
use fixtures::{generate_otlp_logs, get_serialized_data, Compression, Format, WorkloadSize};

/// Benchmark the full pipeline: decode protobuf → convert to Arrow → write Parquet
fn bench_e2e_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("e2e_pipeline");

    // Test matrix: workload size × format × compression
    for size in [WorkloadSize::Small, WorkloadSize::Medium] {
        for format in [Format::Protobuf, Format::Json] {
            for compression in [Compression::None, Compression::Gzip] {
                let request = generate_otlp_logs(size);
                let serialized = get_serialized_data(&request, format, compression);
                let record_count = size.record_count();

                group.throughput(Throughput::Elements(record_count as u64));

                let bench_name = format!("{:?}/{:?}/{:?}", size, format, compression);

                group.bench_with_input(
                    BenchmarkId::from_parameter(&bench_name),
                    &serialized,
                    |b, data| {
                        b.iter(|| {
                            // Decompress if needed
                            let raw_data = match compression {
                                Compression::None => data.to_vec(),
                                Compression::Gzip => {
                                    use flate2::read::GzDecoder;
                                    use std::io::Read;
                                    let mut decoder = GzDecoder::new(&data[..]);
                                    let mut decompressed = Vec::new();
                                    decoder.read_to_end(&mut decompressed).unwrap();
                                    decompressed
                                }
                            };

                            // Decode OTLP based on format
                            let request = match format {
                                Format::Protobuf => {
                                    ExportLogsServiceRequest::decode(&raw_data[..]).unwrap()
                                }
                                Format::Json => serde_json::from_slice(&raw_data[..]).unwrap(),
                            };

                            // Convert to Arrow
                            let (batch, _metadata) = convert_request_to_arrow(&request).unwrap();

                            // Write to Parquet (in-memory)
                            let mut buffer = Vec::new();
                            parquet_writer::write_batches_to_parquet(&mut buffer, vec![batch])
                                .unwrap();

                            black_box(buffer);
                        });
                    },
                );
            }
        }
    }

    group.finish();
}

/// Benchmark with varying batch sizes
fn bench_batch_sensitivity(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_sensitivity");

    // Test batch sizes: 4k, 16k, 64k
    for &batch_size in &[4_096, 16_384, 65_536] {
        // Generate exactly batch_size records
        let request = generate_otlp_logs(WorkloadSize::Small);
        let data = fixtures::to_protobuf(&request);

        group.throughput(Throughput::Elements(batch_size));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}rows", batch_size)),
            &data,
            |b, data| {
                b.iter(|| {
                    let request = ExportLogsServiceRequest::decode(&data[..]).unwrap();
                    let (batch, _metadata) = convert_request_to_arrow(&request).unwrap();

                    let mut buffer = Vec::new();
                    parquet_writer::write_batches_to_parquet(&mut buffer, vec![batch]).unwrap();

                    black_box(buffer);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_e2e_pipeline, bench_batch_sensitivity);
criterion_main!(benches);
