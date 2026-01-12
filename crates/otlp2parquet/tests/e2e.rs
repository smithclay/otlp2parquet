// End-to-end integration tests for otlp2parquet
//
// These tests verify the full pipeline from OTLP ingestion to Arrow batches.

use std::fs;
use std::path::PathBuf;

use otlp2parquet_common::{
    count_skipped_metric_data_points, normalise_json_value, normalize_json_bytes,
};
use otlp2records::{
    apply_log_transform, apply_metric_transform, decode_logs, decode_metrics, logs_schema,
    transform_logs, transform_metrics, transform_traces, values_to_arrow, InputFormat,
};
use vrl::value::Value;

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

    let values = decode_jsonl_values(&payload, |line| decode_logs(line, InputFormat::Json))
        .expect("Failed to decode JSONL logs");
    let transformed = apply_log_transform(values).expect("Failed to transform logs");
    let batch = values_to_arrow(&transformed, &logs_schema()).expect("Failed to build Arrow batch");

    assert!(batch.num_rows() > 0, "Expected batch to have rows");
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

    let decode_result = decode_jsonl_metrics(&payload).expect("Failed to decode metrics JSONL");
    let metric_values =
        apply_metric_transform(decode_result.values).expect("Failed to transform metrics");

    assert!(!metric_values.gauge.is_empty(), "Expected gauge metrics");
}

#[tokio::test]
async fn test_metrics_sum_jsonl() {
    let payload =
        fs::read(testdata_path("metrics_sum.jsonl")).expect("Failed to read metrics_sum.jsonl");

    let decode_result = decode_jsonl_metrics(&payload).expect("Failed to decode metrics JSONL");
    let metric_values =
        apply_metric_transform(decode_result.values).expect("Failed to transform metrics");

    assert!(!metric_values.sum.is_empty(), "Expected sum metrics");
}

#[tokio::test]
async fn test_metrics_histogram_jsonl_skipped() {
    let payload = fs::read(testdata_path("metrics_histogram.jsonl"))
        .expect("Failed to read metrics_histogram.jsonl");

    let decode_result = decode_jsonl_metrics(&payload).expect("Failed to decode metrics JSONL");

    assert!(
        decode_result.skipped.histograms > 0,
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

fn decode_jsonl_values<F>(body: &[u8], mut decode: F) -> Result<Vec<Value>, String>
where
    F: FnMut(&[u8]) -> Result<Vec<Value>, otlp2records::decode::DecodeError>,
{
    let text = std::str::from_utf8(body).map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    let mut saw_line = false;

    for (line_num, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        saw_line = true;
        let normalized = normalize_json_bytes(trimmed.as_bytes()).map_err(|e| e.to_string())?;
        let values = decode(&normalized).map_err(|e| format!("line {}: {}", line_num + 1, e))?;
        out.extend(values);
    }

    if !saw_line {
        return Err("jsonl payload contained no records".to_string());
    }

    Ok(out)
}

fn decode_jsonl_metrics(body: &[u8]) -> Result<otlp2records::DecodeMetricsResult, String> {
    let text = std::str::from_utf8(body).map_err(|e| e.to_string())?;
    let mut values = Vec::new();
    let mut skipped = otlp2records::SkippedMetrics::default();
    let mut saw_line = false;

    for (line_num, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        saw_line = true;
        let mut value: serde_json::Value =
            serde_json::from_slice(trimmed.as_bytes()).map_err(|e| e.to_string())?;
        let counts = count_skipped_metric_data_points(&value);
        normalise_json_value(&mut value, None).map_err(|e| e.to_string())?;
        let normalized = serde_json::to_vec(&value).map_err(|e| e.to_string())?;
        let mut result = decode_metrics(&normalized, InputFormat::Json)
            .map_err(|e| format!("line {}: {}", line_num + 1, e))?;
        result.skipped.histograms += counts.histograms;
        result.skipped.exponential_histograms += counts.exponential_histograms;
        result.skipped.summaries += counts.summaries;
        values.extend(result.values);
        merge_skipped(&mut skipped, &result.skipped);
    }

    if !saw_line {
        return Err("jsonl payload contained no records".to_string());
    }

    Ok(otlp2records::DecodeMetricsResult { values, skipped })
}

fn merge_skipped(target: &mut otlp2records::SkippedMetrics, other: &otlp2records::SkippedMetrics) {
    target.histograms += other.histograms;
    target.exponential_histograms += other.exponential_histograms;
    target.summaries += other.summaries;
    target.nan_values += other.nan_values;
    target.infinity_values += other.infinity_values;
    target.missing_values += other.missing_values;
}
