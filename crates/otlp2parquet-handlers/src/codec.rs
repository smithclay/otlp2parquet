//! Shared codec utilities for OTLP decoding and value extraction.
//!
//! This module provides pure functions for decoding OTLP payloads that can be
//! used across all platform handlers (server, lambda, cloudflare).

use otlp2records::{
    group_batch_by_service, transform_logs, transform_metrics, transform_traces, InputFormat,
};

// Re-export types that consumers need
pub use otlp2records::{
    InputFormat as RecordsInputFormat, PartitionedBatch, PartitionedMetrics, ServiceGroupedBatches,
    SkippedMetrics,
};

// Re-export InputFormat from common for backward compatibility
pub use otlp2parquet_common::InputFormat as CommonInputFormat;

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
    let batch = transform_logs(body, format).map_err(|e| e.to_string())?;
    Ok(group_batch_by_service(batch))
}

/// Decode and transform traces, returning batches grouped by service.
/// Returns String errors for easy wrapping by platform-specific error types.
pub fn decode_traces_partitioned(
    body: &[u8],
    format: InputFormat,
) -> Result<ServiceGroupedBatches, String> {
    let batch = transform_traces(body, format).map_err(|e| e.to_string())?;
    Ok(group_batch_by_service(batch))
}

/// Decode and transform metrics, returning partitioned batches by type and service.
/// Returns String errors for easy wrapping by platform-specific error types.
pub fn decode_metrics_partitioned(
    body: &[u8],
    format: InputFormat,
) -> Result<PartitionedMetrics, String> {
    let batches = transform_metrics(body, format).map_err(|e| e.to_string())?;
    Ok(PartitionedMetrics {
        gauge: batches
            .gauge
            .map(group_batch_by_service)
            .unwrap_or_default(),
        sum: batches.sum.map(group_batch_by_service).unwrap_or_default(),
        skipped: batches.skipped,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
    }

    #[test]
    fn test_decode_traces_partitioned_empty_jsonl() {
        let result = decode_traces_partitioned(b"", InputFormat::Jsonl);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_metrics_partitioned_empty_jsonl() {
        let result = decode_metrics_partitioned(b"", InputFormat::Jsonl);
        assert!(result.is_err());
    }
}
