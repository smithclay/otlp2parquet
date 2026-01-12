//! Signal processing for OTLP ingestion.
//!
//! This module provides the main processing functions for logs, traces, and metrics.

use crate::codec;
use crate::error::OtlpError;
use otlp2parquet_batch::{LogSignalProcessor, PassthroughBatcher};
use otlp2parquet_common::{InputFormat, MetricType, SignalType};
use otlp2parquet_writer::WriteBatchRequest;
use otlp2records::{
    apply_log_transform, apply_metric_transform, apply_trace_transform, gauge_schema, sum_schema,
    traces_schema, DecodeMetricsResult,
};
use vrl::value::Value;

/// Result of processing a signal request
#[derive(Debug, Clone, PartialEq)]
pub struct ProcessingResult {
    pub paths_written: Vec<String>,
    pub records_processed: usize,
    pub batches_flushed: usize,
}

/// Process OTLP logs request
pub async fn process_logs(body: &[u8], format: InputFormat) -> Result<ProcessingResult, OtlpError> {
    let values = decode_logs_values(body, format)?;
    let transformed = apply_log_transform(values).map_err(|e| OtlpError::ConversionFailed {
        signal: "logs".into(),
        message: e.to_string(),
    })?;

    let per_service_values = codec::group_values_by_service(transformed);
    let passthrough = PassthroughBatcher::<LogSignalProcessor>::default();
    let mut batches = Vec::new();
    let mut total_records = 0;

    for (_, subset) in per_service_values {
        let batch = passthrough
            .ingest(&subset)
            .map_err(|e| OtlpError::ConversionFailed {
                signal: "logs".into(),
                message: e.to_string(),
            })?;
        total_records += batch.metadata.record_count;
        batches.push(batch);
    }

    let mut paths = Vec::new();
    let batch_count = batches.len();
    for batch in batches {
        for record_batch in &batch.batches {
            let path = otlp2parquet_writer::write_batch(WriteBatchRequest {
                batch: record_batch,
                signal_type: SignalType::Logs,
                metric_type: None,
                service_name: &batch.metadata.service_name,
                timestamp_micros: batch.metadata.first_timestamp_micros,
            })
            .await
            .map_err(|e| OtlpError::StorageFailed {
                message: e.to_string(),
            })?;

            paths.push(path);
        }
    }

    Ok(ProcessingResult {
        paths_written: paths,
        records_processed: total_records,
        batches_flushed: batch_count,
    })
}

/// Process OTLP traces request
pub async fn process_traces(
    body: &[u8],
    format: InputFormat,
) -> Result<ProcessingResult, OtlpError> {
    let values = decode_traces_values(body, format)?;
    let transformed = apply_trace_transform(values).map_err(|e| OtlpError::ConversionFailed {
        signal: "traces".into(),
        message: e.to_string(),
    })?;

    let per_service_values = codec::group_values_by_service(transformed);
    let mut paths = Vec::new();
    let mut spans_processed = 0;

    for (service_name, subset) in per_service_values {
        let batch = otlp2records::values_to_arrow(&subset, &traces_schema()).map_err(|e| {
            OtlpError::ConversionFailed {
                signal: "traces".into(),
                message: e.to_string(),
            }
        })?;

        if batch.num_rows() == 0 {
            continue;
        }

        let ts_micros = codec::first_timestamp_micros(&subset);
        spans_processed += subset.len();

        let path = otlp2parquet_writer::write_batch(WriteBatchRequest {
            batch: &batch,
            signal_type: SignalType::Traces,
            metric_type: None,
            service_name: &service_name,
            timestamp_micros: ts_micros,
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
        records_processed: spans_processed,
        batches_flushed: batch_count,
    })
}

/// Process OTLP metrics request
pub async fn process_metrics(
    body: &[u8],
    format: InputFormat,
) -> Result<ProcessingResult, OtlpError> {
    let decode_result = decode_metrics_values(body, format)?;
    codec::report_skipped_metrics(&decode_result.skipped);

    let metric_values =
        apply_metric_transform(decode_result.values).map_err(|e| OtlpError::ConversionFailed {
            signal: "metrics".into(),
            message: e.to_string(),
        })?;

    let mut paths = Vec::new();
    let mut total_data_points = 0;

    total_data_points +=
        write_metric_batches(MetricType::Gauge, metric_values.gauge, &mut paths).await?;

    total_data_points +=
        write_metric_batches(MetricType::Sum, metric_values.sum, &mut paths).await?;

    let batch_count = paths.len();
    Ok(ProcessingResult {
        paths_written: paths,
        records_processed: total_data_points,
        batches_flushed: batch_count,
    })
}

async fn write_metric_batches(
    metric_type: MetricType,
    values: Vec<Value>,
    paths: &mut Vec<String>,
) -> Result<usize, OtlpError> {
    if values.is_empty() {
        return Ok(0);
    }

    let schema = match metric_type {
        MetricType::Gauge => gauge_schema(),
        MetricType::Sum => sum_schema(),
        _ => {
            tracing::warn!(
                metric_type = ?metric_type,
                count = values.len(),
                "Unsupported metric type - data not persisted"
            );
            return Ok(0);
        }
    };

    let mut total_data_points = 0;

    for (service_name, subset) in codec::group_values_by_service(values) {
        let batch = otlp2records::values_to_arrow(&subset, &schema).map_err(|e| {
            OtlpError::ConversionFailed {
                signal: "metrics".into(),
                message: e.to_string(),
            }
        })?;

        if batch.num_rows() == 0 {
            continue;
        }

        let ts_micros = codec::first_timestamp_micros(&subset);
        total_data_points += subset.len();

        let path = otlp2parquet_writer::write_batch(WriteBatchRequest {
            batch: &batch,
            signal_type: SignalType::Metrics,
            metric_type: Some(metric_type.as_str()),
            service_name: &service_name,
            timestamp_micros: ts_micros,
        })
        .await
        .map_err(|e| OtlpError::StorageFailed {
            message: e.to_string(),
        })?;

        paths.push(path);
    }

    Ok(total_data_points)
}

// =============================================================================
// Thin wrappers that convert String errors to OtlpError
// =============================================================================

fn decode_logs_values(body: &[u8], format: InputFormat) -> Result<Vec<Value>, OtlpError> {
    codec::decode_logs_values(body, format).map_err(|e| invalid_request("logs", e))
}

fn decode_traces_values(body: &[u8], format: InputFormat) -> Result<Vec<Value>, OtlpError> {
    codec::decode_traces_values(body, format).map_err(|e| invalid_request("traces", e))
}

fn decode_metrics_values(
    body: &[u8],
    format: InputFormat,
) -> Result<DecodeMetricsResult, OtlpError> {
    codec::decode_metrics_values(body, format).map_err(|e| invalid_request("metrics", e))
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
