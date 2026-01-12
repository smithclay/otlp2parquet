//! Signal processing for OTLP ingestion.
//!
//! This module provides the main processing functions for logs, traces, and metrics.

use crate::codec::{
    decode_logs_partitioned, decode_metrics_partitioned, decode_traces_partitioned,
    report_skipped_metrics, ServiceGroupedBatches, SkippedMetrics,
};
use crate::error::OtlpError;
use otlp2parquet_common::{InputFormat, MetricType, SignalType};
use otlp2parquet_writer::WriteBatchRequest;

/// Result of processing a signal request
#[derive(Debug, Clone)]
pub struct ProcessingResult {
    pub paths_written: Vec<String>,
    pub records_processed: usize,
    pub batches_flushed: usize,
    /// Metrics that were skipped (unsupported types, invalid values)
    pub skipped: Option<SkippedMetrics>,
}

/// Process OTLP logs request
pub async fn process_logs(body: &[u8], format: InputFormat) -> Result<ProcessingResult, OtlpError> {
    let grouped = decode_logs_partitioned(body, format).map_err(|e| invalid_request("logs", e))?;

    let mut paths = Vec::new();
    let mut total_records = 0;

    for pb in grouped.batches {
        if pb.batch.num_rows() == 0 {
            continue;
        }

        total_records += pb.record_count;

        let path = otlp2parquet_writer::write_batch(WriteBatchRequest {
            batch: &pb.batch,
            signal_type: SignalType::Logs,
            metric_type: None,
            service_name: &pb.service_name,
            timestamp_micros: pb.min_timestamp_micros,
        })
        .await
        .map_err(|e| OtlpError::StorageFailed {
            message: e.to_string(),
        })?;

        paths.push(path);
    }

    let batch_count = paths.len();
    Ok(ProcessingResult {
        paths_written: paths,
        records_processed: total_records,
        batches_flushed: batch_count,
        skipped: None,
    })
}

/// Process OTLP traces request
pub async fn process_traces(
    body: &[u8],
    format: InputFormat,
) -> Result<ProcessingResult, OtlpError> {
    let grouped =
        decode_traces_partitioned(body, format).map_err(|e| invalid_request("traces", e))?;

    let mut paths = Vec::new();
    let mut total_spans = 0;

    for pb in grouped.batches {
        if pb.batch.num_rows() == 0 {
            continue;
        }

        total_spans += pb.record_count;

        let path = otlp2parquet_writer::write_batch(WriteBatchRequest {
            batch: &pb.batch,
            signal_type: SignalType::Traces,
            metric_type: None,
            service_name: &pb.service_name,
            timestamp_micros: pb.min_timestamp_micros,
        })
        .await
        .map_err(|e| OtlpError::StorageFailed {
            message: e.to_string(),
        })?;

        paths.push(path);
    }

    let batch_count = paths.len();
    Ok(ProcessingResult {
        paths_written: paths,
        records_processed: total_spans,
        batches_flushed: batch_count,
        skipped: None,
    })
}

/// Process OTLP metrics request
pub async fn process_metrics(
    body: &[u8],
    format: InputFormat,
) -> Result<ProcessingResult, OtlpError> {
    let partitioned =
        decode_metrics_partitioned(body, format).map_err(|e| invalid_request("metrics", e))?;

    report_skipped_metrics(&partitioned.skipped);

    let mut paths = Vec::new();
    let mut total_data_points = 0;

    // Write gauge metrics
    total_data_points +=
        write_metric_batches(MetricType::Gauge, partitioned.gauge, &mut paths).await?;

    // Write sum metrics
    total_data_points += write_metric_batches(MetricType::Sum, partitioned.sum, &mut paths).await?;

    let batch_count = paths.len();
    let skipped = if partitioned.skipped.has_skipped() {
        Some(partitioned.skipped)
    } else {
        None
    };
    Ok(ProcessingResult {
        paths_written: paths,
        records_processed: total_data_points,
        batches_flushed: batch_count,
        skipped,
    })
}

async fn write_metric_batches(
    metric_type: MetricType,
    grouped: ServiceGroupedBatches,
    paths: &mut Vec<String>,
) -> Result<usize, OtlpError> {
    if grouped.is_empty() {
        return Ok(0);
    }

    // Validate supported metric types
    match metric_type {
        MetricType::Gauge | MetricType::Sum => {}
        _ => {
            tracing::warn!(
                metric_type = ?metric_type,
                count = grouped.total_records,
                "Unsupported metric type - data not persisted"
            );
            return Ok(0);
        }
    };

    let mut total_data_points = 0;

    for pb in grouped.batches {
        if pb.batch.num_rows() == 0 {
            continue;
        }

        total_data_points += pb.record_count;

        let path = otlp2parquet_writer::write_batch(WriteBatchRequest {
            batch: &pb.batch,
            signal_type: SignalType::Metrics,
            metric_type: Some(metric_type.as_str()),
            service_name: &pb.service_name,
            timestamp_micros: pb.min_timestamp_micros,
        })
        .await
        .map_err(|e| OtlpError::StorageFailed {
            message: e.to_string(),
        })?;

        paths.push(path);
    }

    Ok(total_data_points)
}

fn invalid_request(signal: &str, message: impl Into<String>) -> OtlpError {
    OtlpError::InvalidRequest {
        message: format!(
            "Failed to parse OTLP {} request: {}",
            signal,
            message.into()
        ),
        hint: Some(
            "Ensure the request body contains valid OTLP protobuf, JSON, or JSONL format.".into(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_processing_result_creation() {
        let result = ProcessingResult {
            paths_written: vec!["path1".to_string(), "path2".to_string()],
            records_processed: 100,
            batches_flushed: 2,
            skipped: None,
        };

        assert_eq!(result.paths_written.len(), 2);
        assert_eq!(result.records_processed, 100);
        assert_eq!(result.batches_flushed, 2);
    }

    #[tokio::test]
    async fn test_process_logs_invalid_request() {
        let invalid_data = b"not valid otlp data";

        let result = process_logs(invalid_data, InputFormat::Protobuf).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.error_type(), "InvalidRequest");
        assert!(err.message().contains("Failed to parse OTLP logs request"));
    }
}
