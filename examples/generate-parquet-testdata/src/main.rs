//! Generate sample Parquet files from existing OTLP test data
//!
//! Reads protobuf files from testdata/ and converts them to Parquet format
//! Run with: cd examples/generate-parquet-testdata && cargo run

use otlp2parquet_core::otlp::logs;
use otlp2parquet_core::otlp::metrics;
use otlp2parquet_core::otlp::traces;
use otlp2parquet_proto::opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceRequest;
use otlp2parquet_proto::opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use prost::Message;
use std::fs::File;
use std::io::Read;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Generating Parquet files from testdata...");

    // Create testdata/parquet directory if it doesn't exist
    std::fs::create_dir_all("../../testdata/parquet")?;

    // Generate logs
    convert_logs(
        "../../testdata/logs.pb",
        "../../testdata/parquet/logs.parquet",
    )?;

    // Generate metrics (all 5 types)
    convert_metrics(
        "../../testdata/metrics_gauge.pb",
        "../../testdata/parquet/metrics_gauge.parquet",
    )?;
    convert_metrics(
        "../../testdata/metrics_sum.pb",
        "../../testdata/parquet/metrics_sum.parquet",
    )?;
    convert_metrics(
        "../../testdata/metrics_histogram.pb",
        "../../testdata/parquet/metrics_histogram.parquet",
    )?;
    convert_metrics(
        "../../testdata/metrics_exponential_histogram.pb",
        "../../testdata/parquet/metrics_exponential_histogram.parquet",
    )?;
    convert_metrics(
        "../../testdata/metrics_summary.pb",
        "../../testdata/parquet/metrics_summary.parquet",
    )?;

    // Generate traces
    convert_traces(
        "../../testdata/traces.pb",
        "../../testdata/parquet/traces.parquet",
    )?;

    println!("✓ All Parquet files generated successfully in testdata/parquet/");
    Ok(())
}

fn convert_logs(input_path: &str, output_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Read protobuf file
    let mut file = File::open(input_path)?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;

    // Convert to Arrow RecordBatch
    let mut converter = logs::to_arrow::ArrowConverter::new();
    converter.add_from_proto_bytes(&buffer)?;
    let (record_batch, _metadata) = converter.finish()?;

    // Write to Parquet
    let output_file = File::create(output_path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(output_file, record_batch.schema(), Some(props))?;
    writer.write(&record_batch)?;
    writer.close()?;

    println!("  ✓ {} ({} rows)", output_path, record_batch.num_rows());
    Ok(())
}

fn convert_metrics(input_path: &str, output_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Read protobuf file
    let mut file = File::open(input_path)?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;

    // Parse protobuf request
    let request = ExportMetricsServiceRequest::decode(&buffer[..])?;

    // Convert to Arrow RecordBatches (metrics can produce multiple batches for different types)
    let converter = metrics::to_arrow::ArrowConverter::new();
    let (batches, _metadata) = converter.convert(request)?;

    if batches.is_empty() {
        println!("  ⚠ {} - no data", output_path);
        return Ok(());
    }

    // Write first batch to Parquet (each file corresponds to one metric type)
    let (_metric_type, record_batch) = &batches[0];
    let output_file = File::create(output_path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(output_file, record_batch.schema(), Some(props))?;
    writer.write(record_batch)?;
    writer.close()?;

    println!("  ✓ {} ({} rows)", output_path, record_batch.num_rows());
    Ok(())
}

fn convert_traces(input_path: &str, output_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Read protobuf file
    let mut file = File::open(input_path)?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;

    // Parse protobuf request
    let request = ExportTraceServiceRequest::decode(&buffer[..])?;
    let trace_request = traces::TraceRequest {
        resource_spans: request.resource_spans,
    };

    // Convert to Arrow RecordBatches
    let (record_batches, _metadata) = traces::TraceArrowConverter::convert(&trace_request)?;

    if record_batches.is_empty() {
        println!("  ⚠ {} - no data", output_path);
        return Ok(());
    }

    // Write to Parquet
    let record_batch = &record_batches[0];
    let output_file = File::create(output_path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(output_file, record_batch.schema(), Some(props))?;

    let mut total_rows = 0;
    for batch in &record_batches {
        writer.write(batch)?;
        total_rows += batch.num_rows();
    }
    writer.close()?;

    println!("  ✓ {} ({} rows)", output_path, total_rows);
    Ok(())
}
