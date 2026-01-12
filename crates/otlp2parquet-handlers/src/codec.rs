//! Shared codec utilities for OTLP decoding and value extraction.
//!
//! This module provides pure functions for decoding OTLP payloads that can be
//! used across all platform handlers (server, lambda, cloudflare).

use arrow::compute::concat_batches;
use otlp2parquet_common::{
    count_skipped_metric_data_points, normalise_json_value, normalize_json_bytes, InputFormat,
};
use otlp2records::{
    group_batch_by_service, transform_logs, transform_metrics, transform_traces,
    InputFormat as RecordsFormat,
};

// Re-export types that consumers need
pub use otlp2records::{
    PartitionedBatch, PartitionedMetrics, ServiceGroupedBatches, SkippedMetrics,
};

/// Convert InputFormat to otlp2records RecordsFormat
pub fn to_records_format(format: InputFormat) -> RecordsFormat {
    match format {
        InputFormat::Protobuf => RecordsFormat::Protobuf,
        InputFormat::Json => RecordsFormat::Json,
        InputFormat::Jsonl => RecordsFormat::Json,
    }
}

/// Merge skip counts from one SkippedMetrics into another
pub fn merge_skipped(target: &mut SkippedMetrics, other: &SkippedMetrics) {
    target.histograms += other.histograms;
    target.exponential_histograms += other.exponential_histograms;
    target.summaries += other.summaries;
    target.nan_values += other.nan_values;
    target.infinity_values += other.infinity_values;
    target.missing_values += other.missing_values;
}

/// Report skipped metrics via tracing.
/// Uses warn level to ensure visibility in production logs.
pub fn report_skipped_metrics(skipped: &SkippedMetrics) {
    if skipped.has_skipped() {
        tracing::warn!(
            histograms = skipped.histograms,
            exponential_histograms = skipped.exponential_histograms,
            summaries = skipped.summaries,
            nan_values = skipped.nan_values,
            infinity_values = skipped.infinity_values,
            missing_values = skipped.missing_values,
            total = skipped.total(),
            "Skipped unsupported or invalid metric data points"
        );
    }
}

// =============================================================================
// Decode functions - return partitioned Arrow batches
// =============================================================================

/// Decode and transform logs, returning batches grouped by service.
/// Returns String errors for easy wrapping by platform-specific error types.
pub fn decode_logs_partitioned(
    body: &[u8],
    format: InputFormat,
) -> Result<ServiceGroupedBatches, String> {
    match format {
        InputFormat::Jsonl => decode_logs_jsonl_partitioned(body),
        InputFormat::Json => {
            let normalized = normalize_json_bytes(body).map_err(|e| e.to_string())?;
            let batch =
                transform_logs(&normalized, RecordsFormat::Json).map_err(|e| e.to_string())?;
            Ok(group_batch_by_service(batch))
        }
        InputFormat::Protobuf => {
            let batch = transform_logs(body, RecordsFormat::Protobuf).map_err(|e| e.to_string())?;
            Ok(group_batch_by_service(batch))
        }
    }
}

/// Decode and transform traces, returning batches grouped by service.
/// Returns String errors for easy wrapping by platform-specific error types.
pub fn decode_traces_partitioned(
    body: &[u8],
    format: InputFormat,
) -> Result<ServiceGroupedBatches, String> {
    match format {
        InputFormat::Jsonl => decode_traces_jsonl_partitioned(body),
        InputFormat::Json => {
            let normalized = normalize_json_bytes(body).map_err(|e| e.to_string())?;
            let batch =
                transform_traces(&normalized, RecordsFormat::Json).map_err(|e| e.to_string())?;
            Ok(group_batch_by_service(batch))
        }
        InputFormat::Protobuf => {
            let batch =
                transform_traces(body, RecordsFormat::Protobuf).map_err(|e| e.to_string())?;
            Ok(group_batch_by_service(batch))
        }
    }
}

/// Decode and transform metrics, returning partitioned batches by type and service.
/// Returns String errors for easy wrapping by platform-specific error types.
pub fn decode_metrics_partitioned(
    body: &[u8],
    format: InputFormat,
) -> Result<PartitionedMetrics, String> {
    match format {
        InputFormat::Jsonl => decode_metrics_jsonl_partitioned(body),
        InputFormat::Json => decode_metrics_json_partitioned(body),
        InputFormat::Protobuf => {
            let batches =
                transform_metrics(body, RecordsFormat::Protobuf).map_err(|e| e.to_string())?;
            Ok(PartitionedMetrics {
                gauge: batches
                    .gauge
                    .map(group_batch_by_service)
                    .unwrap_or_default(),
                sum: batches.sum.map(group_batch_by_service).unwrap_or_default(),
                skipped: batches.skipped,
            })
        }
    }
}

// =============================================================================
// JSONL handling - process line by line and concatenate results
// =============================================================================

use arrow::array::RecordBatch;
use otlp2records::Result as RecordsResult;

/// Generic JSONL decoder that processes each line and concatenates results.
fn decode_jsonl_partitioned<F>(body: &[u8], transform: F) -> Result<ServiceGroupedBatches, String>
where
    F: Fn(&[u8]) -> RecordsResult<RecordBatch>,
{
    let text = std::str::from_utf8(body).map_err(|e| e.to_string())?;
    let mut batches = Vec::new();
    let mut saw_line = false;

    for (line_num, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        saw_line = true;
        let normalized = normalize_json_bytes(trimmed.as_bytes())
            .map_err(|e| format!("line {}: {}", line_num + 1, e))?;
        let batch = transform(&normalized).map_err(|e| format!("line {}: {}", line_num + 1, e))?;
        if batch.num_rows() > 0 {
            batches.push(batch);
        }
    }

    if !saw_line {
        return Err("jsonl payload contained no records".to_string());
    }

    if batches.is_empty() {
        return Ok(ServiceGroupedBatches::default());
    }

    let schema = batches[0].schema();
    let combined =
        concat_batches(&schema, &batches).map_err(|e| format!("concat batches: {}", e))?;
    Ok(group_batch_by_service(combined))
}

/// Decode JSONL logs payload, processing line by line.
fn decode_logs_jsonl_partitioned(body: &[u8]) -> Result<ServiceGroupedBatches, String> {
    decode_jsonl_partitioned(body, |data| transform_logs(data, RecordsFormat::Json))
}

/// Decode JSONL traces payload, processing line by line.
fn decode_traces_jsonl_partitioned(body: &[u8]) -> Result<ServiceGroupedBatches, String> {
    decode_jsonl_partitioned(body, |data| transform_traces(data, RecordsFormat::Json))
}

/// Decode JSONL metrics payload, processing line by line.
fn decode_metrics_jsonl_partitioned(body: &[u8]) -> Result<PartitionedMetrics, String> {
    let text = std::str::from_utf8(body).map_err(|e| e.to_string())?;
    let mut gauge_batches = Vec::new();
    let mut sum_batches = Vec::new();
    let mut skipped = SkippedMetrics::default();
    let mut saw_line = false;

    for (line_num, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        saw_line = true;
        let mut value: serde_json::Value = serde_json::from_slice(trimmed.as_bytes())
            .map_err(|e| format!("line {}: {}", line_num + 1, e))?;
        let counts = count_skipped_metric_data_points(&value);
        normalise_json_value(&mut value, None)
            .map_err(|e| format!("line {}: {}", line_num + 1, e))?;
        let normalized =
            serde_json::to_vec(&value).map_err(|e| format!("line {}: {}", line_num + 1, e))?;

        let batches = transform_metrics(&normalized, RecordsFormat::Json)
            .map_err(|e| format!("line {}: {}", line_num + 1, e))?;

        if let Some(batch) = batches.gauge {
            if batch.num_rows() > 0 {
                gauge_batches.push(batch);
            }
        }
        if let Some(batch) = batches.sum {
            if batch.num_rows() > 0 {
                sum_batches.push(batch);
            }
        }

        // Merge skipped counts
        skipped.histograms += counts.histograms + batches.skipped.histograms;
        skipped.exponential_histograms +=
            counts.exponential_histograms + batches.skipped.exponential_histograms;
        skipped.summaries += counts.summaries + batches.skipped.summaries;
        merge_skipped(&mut skipped, &batches.skipped);
    }

    if !saw_line {
        return Err("jsonl payload contained no records".to_string());
    }

    // Concatenate gauge batches
    let gauge = if gauge_batches.is_empty() {
        ServiceGroupedBatches::default()
    } else {
        let schema = gauge_batches[0].schema();
        let combined =
            concat_batches(&schema, &gauge_batches).map_err(|e| format!("concat gauge: {}", e))?;
        group_batch_by_service(combined)
    };

    // Concatenate sum batches
    let sum = if sum_batches.is_empty() {
        ServiceGroupedBatches::default()
    } else {
        let schema = sum_batches[0].schema();
        let combined =
            concat_batches(&schema, &sum_batches).map_err(|e| format!("concat sum: {}", e))?;
        group_batch_by_service(combined)
    };

    Ok(PartitionedMetrics {
        gauge,
        sum,
        skipped,
    })
}

/// Decode JSON metrics payload, tracking skipped metric types.
fn decode_metrics_json_partitioned(body: &[u8]) -> Result<PartitionedMetrics, String> {
    let mut value: serde_json::Value = serde_json::from_slice(body).map_err(|e| e.to_string())?;
    let counts = count_skipped_metric_data_points(&value);
    normalise_json_value(&mut value, None).map_err(|e| e.to_string())?;
    let normalized = serde_json::to_vec(&value).map_err(|e| e.to_string())?;

    let batches = transform_metrics(&normalized, RecordsFormat::Json).map_err(|e| e.to_string())?;

    let mut skipped = batches.skipped;
    skipped.histograms += counts.histograms;
    skipped.exponential_histograms += counts.exponential_histograms;
    skipped.summaries += counts.summaries;

    Ok(PartitionedMetrics {
        gauge: batches
            .gauge
            .map(group_batch_by_service)
            .unwrap_or_default(),
        sum: batches.sum.map(group_batch_by_service).unwrap_or_default(),
        skipped,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_records_format() {
        assert!(matches!(
            to_records_format(InputFormat::Protobuf),
            RecordsFormat::Protobuf
        ));
        assert!(matches!(
            to_records_format(InputFormat::Json),
            RecordsFormat::Json
        ));
        assert!(matches!(
            to_records_format(InputFormat::Jsonl),
            RecordsFormat::Json
        ));
    }

    #[test]
    fn test_merge_skipped() {
        let mut target = SkippedMetrics::default();
        let other = SkippedMetrics {
            histograms: 1,
            exponential_histograms: 2,
            summaries: 3,
            nan_values: 4,
            infinity_values: 5,
            missing_values: 6,
        };

        merge_skipped(&mut target, &other);

        assert_eq!(target.histograms, 1);
        assert_eq!(target.exponential_histograms, 2);
        assert_eq!(target.summaries, 3);
        assert_eq!(target.nan_values, 4);
        assert_eq!(target.infinity_values, 5);
        assert_eq!(target.missing_values, 6);
    }

    #[test]
    fn test_decode_logs_partitioned_empty_jsonl() {
        let result = decode_logs_partitioned(b"", InputFormat::Jsonl);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("no records"));
    }

    #[test]
    fn test_decode_traces_partitioned_empty_jsonl() {
        let result = decode_traces_partitioned(b"", InputFormat::Jsonl);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("no records"));
    }

    #[test]
    fn test_decode_metrics_partitioned_empty_jsonl() {
        let result = decode_metrics_partitioned(b"", InputFormat::Jsonl);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("no records"));
    }
}
