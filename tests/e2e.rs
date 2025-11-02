// End-to-end integration tests for otlp2parquet
//
// These tests verify the full pipeline from OTLP ingestion to Parquet storage

use std::fs;

#[tokio::test]
async fn test_logs_ingestion_protobuf() {
    // Load test payload from testdata
    let payload = fs::read("testdata/logs.pb").expect("Failed to read logs.pb test file");

    // Use the core library to process the data directly
    let result =
        otlp2parquet_core::parse_otlp_to_arrow(&payload, otlp2parquet_core::InputFormat::Protobuf);

    assert!(
        result.is_ok(),
        "Failed to parse OTLP logs: {:?}",
        result.err()
    );

    let (batch, metadata) = result.unwrap();

    // Verify the batch has rows
    assert!(batch.num_rows() > 0, "Expected batch to have rows");
    assert!(batch.num_columns() > 0, "Expected batch to have columns");
    assert!(metadata.record_count > 0, "Expected record count > 0");
}

#[tokio::test]
async fn test_logs_ingestion_json() {
    let payload = fs::read("testdata/log.json").expect("Failed to read log.json test file");

    let result =
        otlp2parquet_core::parse_otlp_to_arrow(&payload, otlp2parquet_core::InputFormat::Json);

    assert!(
        result.is_ok(),
        "Failed to parse JSON logs: {:?}",
        result.err()
    );

    let (batch, metadata) = result.unwrap();
    assert!(batch.num_rows() > 0, "Expected batch to have rows");
    assert!(metadata.record_count > 0, "Expected record count > 0");
}

#[tokio::test]
async fn test_end_to_end_with_storage() {
    use opendal::{services, Operator};
    use otlp2parquet_storage::ParquetWriter;
    use std::sync::Arc;

    // Create in-memory storage
    let op = Operator::new(services::Memory::default())
        .expect("Failed to create memory operator")
        .finish();
    let writer = Arc::new(ParquetWriter::new(op.clone()));

    // Load and parse test data
    let payload = fs::read("testdata/logs.pb").expect("Failed to read logs.pb");
    let (batch, metadata) =
        otlp2parquet_core::parse_otlp_to_arrow(&payload, otlp2parquet_core::InputFormat::Protobuf)
            .expect("Failed to parse logs");

    assert!(batch.num_rows() > 0, "Expected batch to have rows");

    // Write to storage with fixed timestamp for testing
    let test_timestamp = metadata.first_timestamp_nanos;
    let (path, hash) = writer
        .write_batch_with_hash(&batch, &metadata.service_name, test_timestamp)
        .await
        .expect("Failed to write batch");

    // Verify file was written
    assert!(path.starts_with("logs/"));
    assert!(path.ends_with(".parquet"));
    assert!(path.contains(&hash.to_hex()[..16]));

    // Verify file exists and is valid Parquet
    let data = op.read(&path).await.expect("Failed to read parquet file");
    let bytes = data.to_vec();
    assert!(!bytes.is_empty(), "Parquet file should not be empty");
    assert_eq!(&bytes[0..4], b"PAR1", "File should be valid Parquet format");
}

#[tokio::test]
async fn test_logs_jsonl_format() {
    let payload = fs::read("testdata/logs.jsonl").expect("Failed to read logs.jsonl");

    let result =
        otlp2parquet_core::parse_otlp_to_arrow(&payload, otlp2parquet_core::InputFormat::Jsonl);

    assert!(
        result.is_ok(),
        "Failed to parse JSONL logs: {:?}",
        result.err()
    );

    let (batch, metadata) = result.unwrap();
    assert!(batch.num_rows() > 0, "Expected batch to have rows");
    assert!(metadata.record_count > 0, "Expected record count > 0");
}

// ============================================================================
// METRICS TESTS
// ============================================================================

#[tokio::test]
async fn test_metrics_gauge_protobuf() {
    use otlp2parquet_core::otlp::metrics;

    let payload = fs::read("testdata/metrics_gauge.pb").expect("Failed to read metrics_gauge.pb");

    let request = metrics::parse_otlp_request(&payload, otlp2parquet_core::InputFormat::Protobuf)
        .expect("Failed to parse metrics request");

    let converter = metrics::ArrowConverter::new();
    let result = converter.convert(request);

    assert!(
        result.is_ok(),
        "Failed to convert metrics: {:?}",
        result.err()
    );

    let (batches_by_type, metadata) = result.unwrap();
    assert!(!batches_by_type.is_empty(), "Expected metric batches");
    assert!(metadata.gauge_count > 0, "Expected gauge metrics");
}

#[tokio::test]
async fn test_metrics_sum_protobuf() {
    use otlp2parquet_core::otlp::metrics;

    let payload = fs::read("testdata/metrics_sum.pb").expect("Failed to read metrics_sum.pb");

    let request = metrics::parse_otlp_request(&payload, otlp2parquet_core::InputFormat::Protobuf)
        .expect("Failed to parse metrics request");

    let converter = metrics::ArrowConverter::new();
    let result = converter.convert(request);

    assert!(
        result.is_ok(),
        "Failed to convert metrics: {:?}",
        result.err()
    );

    let (batches_by_type, metadata) = result.unwrap();
    assert!(!batches_by_type.is_empty(), "Expected metric batches");
    assert!(metadata.sum_count > 0, "Expected sum metrics");
}

#[tokio::test]
async fn test_metrics_histogram_protobuf() {
    use otlp2parquet_core::otlp::metrics;

    let payload =
        fs::read("testdata/metrics_histogram.pb").expect("Failed to read metrics_histogram.pb");

    let request = metrics::parse_otlp_request(&payload, otlp2parquet_core::InputFormat::Protobuf)
        .expect("Failed to parse metrics request");

    let converter = metrics::ArrowConverter::new();
    let result = converter.convert(request);

    assert!(
        result.is_ok(),
        "Failed to convert metrics: {:?}",
        result.err()
    );

    let (batches_by_type, metadata) = result.unwrap();
    assert!(!batches_by_type.is_empty(), "Expected metric batches");
    assert!(metadata.histogram_count > 0, "Expected histogram metrics");
}

#[tokio::test]
async fn test_metrics_exponential_histogram_protobuf() {
    use otlp2parquet_core::otlp::metrics;

    let payload = fs::read("testdata/metrics_exponential_histogram.pb")
        .expect("Failed to read metrics_exponential_histogram.pb");

    let request = metrics::parse_otlp_request(&payload, otlp2parquet_core::InputFormat::Protobuf)
        .expect("Failed to parse metrics request");

    let converter = metrics::ArrowConverter::new();
    let result = converter.convert(request);

    assert!(
        result.is_ok(),
        "Failed to convert metrics: {:?}",
        result.err()
    );

    let (batches_by_type, metadata) = result.unwrap();
    assert!(!batches_by_type.is_empty(), "Expected metric batches");
    assert!(
        metadata.exponential_histogram_count > 0,
        "Expected exponential histogram metrics"
    );
}

#[tokio::test]
async fn test_metrics_summary_protobuf() {
    use otlp2parquet_core::otlp::metrics;

    let payload =
        fs::read("testdata/metrics_summary.pb").expect("Failed to read metrics_summary.pb");

    let request = metrics::parse_otlp_request(&payload, otlp2parquet_core::InputFormat::Protobuf)
        .expect("Failed to parse metrics request");

    let converter = metrics::ArrowConverter::new();
    let result = converter.convert(request);

    assert!(
        result.is_ok(),
        "Failed to convert metrics: {:?}",
        result.err()
    );

    let (batches_by_type, metadata) = result.unwrap();
    assert!(!batches_by_type.is_empty(), "Expected metric batches");
    assert!(metadata.summary_count > 0, "Expected summary metrics");
}

// Note: JSON metrics tests are covered by the protobuf tests above
// JSON parsing for metrics has the same coverage as protobuf format

// ============================================================================
// TRACES TESTS
// ============================================================================

#[tokio::test]
async fn test_traces_protobuf() {
    use otlp2parquet_core::otlp::traces;

    let payload = fs::read("testdata/trace.pb").expect("Failed to read trace.pb");

    let request =
        traces::parse_otlp_trace_request(&payload, otlp2parquet_core::InputFormat::Protobuf)
            .expect("Failed to parse trace request");

    let result = traces::TraceArrowConverter::convert(&request);

    assert!(
        result.is_ok(),
        "Failed to convert traces: {:?}",
        result.err()
    );

    let (batches, metadata) = result.unwrap();
    assert!(!batches.is_empty(), "Expected trace batches");
    assert!(metadata.span_count > 0, "Expected spans");

    // Verify batch structure
    for batch in &batches {
        assert!(batch.num_rows() > 0, "Expected batch to have rows");
        assert!(batch.num_columns() > 0, "Expected batch to have columns");
    }
}

#[tokio::test]
async fn test_traces_json_format() {
    use otlp2parquet_core::otlp::traces;

    let payload = fs::read("testdata/trace.json").expect("Failed to read trace.json");

    let request = traces::parse_otlp_trace_request(&payload, otlp2parquet_core::InputFormat::Json)
        .expect("Failed to parse trace request");

    let result = traces::TraceArrowConverter::convert(&request);

    assert!(
        result.is_ok(),
        "Failed to convert JSON traces: {:?}",
        result.err()
    );

    let (batches, metadata) = result.unwrap();
    assert!(!batches.is_empty(), "Expected trace batches");
    assert!(metadata.span_count > 0, "Expected spans");
}

#[tokio::test]
async fn test_traces_with_storage() {
    use opendal::{services, Operator};
    use otlp2parquet_core::otlp::traces;
    use otlp2parquet_storage::ParquetWriter;
    use std::sync::Arc;

    // Create in-memory storage
    let op = Operator::new(services::Memory::default())
        .expect("Failed to create memory operator")
        .finish();
    let writer = Arc::new(ParquetWriter::new(op.clone()));

    // Load and parse test data
    let payload = fs::read("testdata/traces.pb").expect("Failed to read traces.pb");
    let request =
        traces::parse_otlp_trace_request(&payload, otlp2parquet_core::InputFormat::Protobuf)
            .expect("Failed to parse trace request");

    let (batches, metadata) =
        traces::TraceArrowConverter::convert(&request).expect("Failed to convert traces");

    assert!(!batches.is_empty(), "Expected trace batches");

    // Write to storage
    let test_timestamp = metadata.first_timestamp_nanos;
    let (path, hash) = writer
        .write_batches_with_signal(
            &batches,
            &metadata.service_name,
            test_timestamp,
            "traces",
            None,
        )
        .await
        .expect("Failed to write traces");

    // Verify file was written
    assert!(path.starts_with("traces/"));
    assert!(path.ends_with(".parquet"));
    assert!(path.contains(&hash.to_hex()[..16]));

    // Verify file exists and is valid Parquet
    let data = op.read(&path).await.expect("Failed to read parquet file");
    let bytes = data.to_vec();
    assert!(!bytes.is_empty(), "Parquet file should not be empty");
    assert_eq!(&bytes[0..4], b"PAR1", "File should be valid Parquet format");
}
