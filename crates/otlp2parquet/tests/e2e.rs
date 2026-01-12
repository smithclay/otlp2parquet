// End-to-end integration tests for otlp2parquet
//
// These tests verify the full pipeline from OTLP ingestion to Arrow batches.

use std::fs;
use std::path::PathBuf;

use otlp2parquet_common::normalize_json_bytes;
use otlp2parquet_handlers::{
    decode_logs_partitioned, decode_metrics_partitioned, decode_traces_partitioned,
};
use otlp2records::{
    decode_metrics, transform_logs, transform_metrics, transform_traces, InputFormat,
};

/// Get path to workspace root testdata directory
fn testdata_path(file: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("testdata")
        .join(file)
}

#[tokio::test]
async fn test_logs_ingestion_protobuf() {
    let payload = fs::read(testdata_path("logs.pb")).expect("Failed to read logs.pb test file");

    let batch =
        transform_logs(&payload, InputFormat::Protobuf).expect("Failed to transform OTLP logs");

    assert!(batch.num_rows() > 0, "Expected batch to have rows");
    assert!(batch.num_columns() > 0, "Expected batch to have columns");
}

#[tokio::test]
async fn test_logs_ingestion_json() {
    let payload = fs::read(testdata_path("log.json")).expect("Failed to read log.json test file");
    let payload = normalize_json_bytes(&payload).expect("Failed to normalize OTLP JSON logs");

    let batch =
        transform_logs(&payload, InputFormat::Json).expect("Failed to transform OTLP JSON logs");

    assert!(batch.num_rows() > 0, "Expected batch to have rows");
}

#[tokio::test]
async fn test_logs_jsonl_format() {
    let payload = fs::read(testdata_path("logs.jsonl")).expect("Failed to read logs.jsonl");

    // Use the handlers codec which handles JSONL internally
    let grouped = decode_logs_partitioned(&payload, otlp2parquet_common::InputFormat::Jsonl)
        .expect("Failed to decode JSONL logs");

    assert!(grouped.total_records > 0, "Expected batch to have rows");
}

// ============================================================================
// METRICS TESTS
// ============================================================================

#[tokio::test]
async fn test_metrics_gauge_protobuf() {
    let payload =
        fs::read(testdata_path("metrics_gauge.pb")).expect("Failed to read metrics_gauge.pb");

    let batches = transform_metrics(&payload, InputFormat::Protobuf)
        .expect("Failed to transform gauge metrics");

    assert!(batches.gauge.is_some(), "Expected gauge batch");
    assert!(batches.gauge.unwrap().num_rows() > 0, "Expected gauge rows");
}

#[tokio::test]
async fn test_metrics_sum_protobuf() {
    let payload = fs::read(testdata_path("metrics_sum.pb")).expect("Failed to read metrics_sum.pb");

    let batches = transform_metrics(&payload, InputFormat::Protobuf)
        .expect("Failed to transform sum metrics");

    assert!(batches.sum.is_some(), "Expected sum batch");
    assert!(batches.sum.unwrap().num_rows() > 0, "Expected sum rows");
}

#[tokio::test]
async fn test_metrics_histogram_protobuf_skipped() {
    let payload = fs::read(testdata_path("metrics_histogram.pb"))
        .expect("Failed to read metrics_histogram.pb");

    let result = decode_metrics(&payload, InputFormat::Protobuf)
        .expect("Failed to decode histogram metrics");

    assert!(
        result.skipped.histograms > 0,
        "Expected histogram data to be skipped"
    );
}

#[tokio::test]
async fn test_metrics_exponential_histogram_protobuf_skipped() {
    let payload = fs::read(testdata_path("metrics_exponential_histogram.pb"))
        .expect("Failed to read metrics_exponential_histogram.pb");

    let result = decode_metrics(&payload, InputFormat::Protobuf)
        .expect("Failed to decode exponential histogram metrics");

    assert!(
        result.skipped.exponential_histograms > 0,
        "Expected exponential histogram data to be skipped"
    );
}

#[tokio::test]
async fn test_metrics_summary_protobuf_skipped() {
    let payload =
        fs::read(testdata_path("metrics_summary.pb")).expect("Failed to read metrics_summary.pb");

    let result =
        decode_metrics(&payload, InputFormat::Protobuf).expect("Failed to decode summary metrics");

    assert!(
        result.skipped.summaries > 0,
        "Expected summary data to be skipped"
    );
}

// ============================================================================
// METRICS JSON/JSONL FORMAT TESTS
// ============================================================================

#[tokio::test]
async fn test_metrics_gauge_json() {
    let payload =
        fs::read(testdata_path("metrics_gauge.json")).expect("Failed to read metrics_gauge.json");
    let payload = normalize_json_bytes(&payload).expect("Failed to normalize gauge metrics JSON");

    let batches =
        transform_metrics(&payload, InputFormat::Json).expect("Failed to transform gauge metrics");

    assert!(batches.gauge.is_some(), "Expected gauge batch");
}

#[tokio::test]
async fn test_metrics_sum_json() {
    let payload =
        fs::read(testdata_path("metrics_sum.json")).expect("Failed to read metrics_sum.json");
    let payload = normalize_json_bytes(&payload).expect("Failed to normalize sum metrics JSON");

    let batches =
        transform_metrics(&payload, InputFormat::Json).expect("Failed to transform sum metrics");

    assert!(batches.sum.is_some(), "Expected sum batch");
}

#[tokio::test]
async fn test_metrics_gauge_jsonl() {
    let payload =
        fs::read(testdata_path("metrics_gauge.jsonl")).expect("Failed to read metrics_gauge.jsonl");

    // Use the handlers codec which handles JSONL internally
    let partitioned = decode_metrics_partitioned(&payload, otlp2parquet_common::InputFormat::Jsonl)
        .expect("Failed to decode metrics JSONL");

    assert!(
        !partitioned.gauge.is_empty(),
        "Expected gauge metrics, got empty"
    );
}

#[tokio::test]
async fn test_metrics_sum_jsonl() {
    let payload =
        fs::read(testdata_path("metrics_sum.jsonl")).expect("Failed to read metrics_sum.jsonl");

    // Use the handlers codec which handles JSONL internally
    let partitioned = decode_metrics_partitioned(&payload, otlp2parquet_common::InputFormat::Jsonl)
        .expect("Failed to decode metrics JSONL");

    assert!(!partitioned.sum.is_empty(), "Expected sum metrics");
}

#[tokio::test]
async fn test_metrics_histogram_jsonl_skipped() {
    let payload = fs::read(testdata_path("metrics_histogram.jsonl"))
        .expect("Failed to read metrics_histogram.jsonl");

    // Use the handlers codec which handles JSONL internally
    let partitioned = decode_metrics_partitioned(&payload, otlp2parquet_common::InputFormat::Jsonl)
        .expect("Failed to decode metrics JSONL");

    assert!(
        partitioned.skipped.histograms > 0,
        "Expected histogram data to be skipped"
    );
}

// ============================================================================
// TRACES TESTS
// ============================================================================

#[tokio::test]
async fn test_traces_protobuf() {
    let payload = fs::read(testdata_path("trace.pb")).expect("Failed to read trace.pb");

    let batch =
        transform_traces(&payload, InputFormat::Protobuf).expect("Failed to transform traces");

    assert!(batch.num_rows() > 0, "Expected spans");
}

#[tokio::test]
async fn test_traces_json_format() {
    let payload = fs::read(testdata_path("trace.json")).expect("Failed to read trace.json");
    let payload = normalize_json_bytes(&payload).expect("Failed to normalize OTLP JSON traces");

    let batch =
        transform_traces(&payload, InputFormat::Json).expect("Failed to transform JSON traces");

    assert!(batch.num_rows() > 0, "Expected spans");
}

#[tokio::test]
async fn test_traces_jsonl_format() {
    let payload = fs::read(testdata_path("traces.jsonl")).expect("Failed to read traces.jsonl");

    // Use the handlers codec which handles JSONL internally
    let grouped = decode_traces_partitioned(&payload, otlp2parquet_common::InputFormat::Jsonl)
        .expect("Failed to decode JSONL traces");

    assert!(grouped.total_records > 0, "Expected spans in JSONL traces");
}

// ============================================================================
// NEGATIVE TESTS - Invalid Data
// ============================================================================

#[tokio::test]
async fn test_invalid_severity_number() {
    let payload = fs::read(testdata_path("invalid/log_invalid_severity.json"))
        .expect("Failed to read invalid test file");
    let payload =
        normalize_json_bytes(&payload).expect("Failed to normalize invalid severity payload");

    let result = transform_logs(&payload, InputFormat::Json);
    assert!(
        result.is_err(),
        "Expected error for invalid severity number"
    );
}

#[tokio::test]
async fn test_invalid_base64_trace_id() {
    let payload = fs::read(testdata_path("invalid/trace_invalid_base64.json"))
        .expect("Failed to read invalid test file");
    let result = normalize_json_bytes(&payload).and_then(|payload| {
        transform_traces(&payload, InputFormat::Json).map_err(anyhow::Error::from)
    });
    assert!(
        result.is_err(),
        "Expected error for invalid base64 trace ID"
    );
}

#[tokio::test]
async fn test_invalid_aggregation_temporality() {
    let payload = fs::read(testdata_path("invalid/metrics_invalid_temporality.json"))
        .expect("Failed to read invalid test file");
    let payload =
        normalize_json_bytes(&payload).expect("Failed to normalize invalid temporality payload");

    let result = transform_metrics(&payload, InputFormat::Json);
    assert!(
        result.is_err(),
        "Expected error for invalid aggregation temporality"
    );
}

#[tokio::test]
async fn test_malformed_json() {
    let payload = fs::read(testdata_path("invalid/malformed.json"))
        .expect("Failed to read invalid test file");

    let result = normalize_json_bytes(&payload).and_then(|payload| {
        transform_logs(&payload, InputFormat::Json).map_err(anyhow::Error::from)
    });
    assert!(result.is_err(), "Expected error for malformed JSON");
}

#[tokio::test]
async fn test_invalid_span_kind() {
    let payload = fs::read(testdata_path("invalid/trace_invalid_kind.json"))
        .expect("Failed to read invalid test file");
    let payload =
        normalize_json_bytes(&payload).expect("Failed to normalize invalid span kind payload");

    let result = transform_traces(&payload, InputFormat::Json);
    assert!(result.is_err(), "Expected error for invalid span kind");
}

#[tokio::test]
async fn test_invalid_trace_id_encoding() {
    let payload = fs::read(testdata_path("invalid/trace_mixed_encoding.json"))
        .expect("Failed to read invalid test file");
    let result = normalize_json_bytes(&payload).and_then(|payload| {
        transform_traces(&payload, InputFormat::Json).map_err(anyhow::Error::from)
    });
    assert!(
        result.is_err(),
        "Expected error for invalid trace ID encoding"
    );
}
