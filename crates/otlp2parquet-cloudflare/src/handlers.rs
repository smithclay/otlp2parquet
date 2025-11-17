// Request handlers for OTLP signals (logs, traces, metrics)

use arrow::array::{Array, RecordBatch, StringArray, TimestampNanosecondArray};
use otlp2parquet_batch::{LogSignalProcessor, PassthroughBatcher};
use otlp2parquet_core::{otlp, SignalType};
use otlp2parquet_writer::{IcepickWriter, OtlpWriter};
use serde_json::json;
use std::sync::Arc;
use worker::{console_error, Response, Result};

/// Handle logs request
pub async fn handle_logs_request(
    body_bytes: &[u8],
    format: otlp2parquet_core::InputFormat,
    content_type: Option<&str>,
    writer: &Arc<IcepickWriter>,
) -> Result<Response> {
    // Cloudflare Workers: Always use passthrough (no batching)
    // Batching requires time-based operations which aren't supported in WASM
    let passthrough: PassthroughBatcher<LogSignalProcessor> = PassthroughBatcher::default();

    // Process logs
    let request = otlp::parse_otlp_request(body_bytes, format).map_err(|e| {
        console_error!(
            "Failed to parse OTLP logs (format: {:?}, content-type: {:?}): {:?}",
            format,
            content_type,
            e
        );
        worker::Error::RustError(format!("Processing error: {}", e))
    })?;

    let per_service_requests = otlp::logs::split_request_by_service(request);
    let mut uploads = Vec::new();
    let mut total_records = 0usize;

    for subset in per_service_requests {
        let batch = passthrough.ingest(&subset).map_err(|err| {
            console_error!("Failed to convert OTLP to Arrow: {:?}", err);
            worker::Error::RustError(format!("Encoding error: {}", err))
        })?;
        total_records += batch.metadata.record_count;
        uploads.push(batch);
    }

    let mut uploaded_paths = Vec::new();
    for batch in uploads {
        // Write using OtlpWriter trait
        for record_batch in &batch.batches {
            let result = writer
                .write_batch(
                    record_batch,
                    SignalType::Logs,
                    None,
                    &batch.metadata.service_name,
                    batch.metadata.first_timestamp_nanos,
                )
                .await
                .map_err(|e| {
                    console_error!("Failed to write logs: {:?}", e);
                    worker::Error::RustError(format!("Write error: {}", e))
                })?;

            uploaded_paths.push(result.path);
        }
    }

    let response_body = json!({
        "status": "ok",
        "records_processed": total_records,
        "flush_count": uploaded_paths.len(),
        "partitions": uploaded_paths,
    });

    Response::from_json(&response_body)
}

/// Handle traces request
pub async fn handle_traces_request(
    body_bytes: &[u8],
    format: otlp2parquet_core::InputFormat,
    content_type: Option<&str>,
    writer: &Arc<IcepickWriter>,
) -> Result<Response> {
    // Parse OTLP traces request
    let request = otlp::traces::parse_otlp_trace_request(body_bytes, format).map_err(|e| {
        console_error!(
            "Failed to parse OTLP traces (format: {:?}, content-type: {:?}): {:?}",
            format,
            content_type,
            e
        );
        worker::Error::RustError(format!("Processing error: {}", e))
    })?;

    let per_service_requests = otlp::traces::split_request_by_service(request);
    let mut uploaded_paths = Vec::new();
    let mut spans_processed = 0usize;

    for subset in per_service_requests {
        let (batches, metadata) =
            otlp::traces::TraceArrowConverter::convert(&subset).map_err(|e| {
                console_error!("Failed to convert OTLP traces to Arrow: {:?}", e);
                worker::Error::RustError(format!("Encoding error: {}", e))
            })?;

        if batches.is_empty() || metadata.span_count == 0 {
            continue;
        }

        spans_processed += metadata.span_count;

        // Write using OtlpWriter trait
        for batch in &batches {
            let result = writer
                .write_batch(
                    batch,
                    SignalType::Traces,
                    None,
                    metadata.service_name.as_ref(),
                    metadata.first_timestamp_nanos,
                )
                .await
                .map_err(|e| {
                    console_error!("Failed to write traces: {:?}", e);
                    worker::Error::RustError(format!("Write error: {}", e))
                })?;

            uploaded_paths.push(result.path);
        }
    }

    if spans_processed == 0 {
        let response = Response::from_json(&json!({
            "status": "ok",
            "message": "No trace spans to process",
        }))?;
        return Ok(response);
    }

    let response_body = json!({
        "status": "ok",
        "spans_processed": spans_processed,
        "partitions": uploaded_paths,
    });

    Response::from_json(&response_body)
}

/// Handle metrics request (separate from logs due to multiple batches per type)
pub async fn handle_metrics_request(
    body_bytes: &[u8],
    format: otlp2parquet_core::InputFormat,
    content_type: Option<&str>,
    writer: &Arc<IcepickWriter>,
) -> Result<Response> {
    // Parse OTLP metrics request
    let request = otlp::metrics::parse_otlp_request(body_bytes, format).map_err(|e| {
        console_error!(
            "Failed to parse OTLP metrics (format: {:?}, content-type: {:?}): {:?}",
            format,
            content_type,
            e
        );
        worker::Error::RustError(format!("Processing error: {}", e))
    })?;

    let per_service_requests = otlp::metrics::split_request_by_service(request);
    let converter = otlp::metrics::ArrowConverter::new();
    let mut aggregated = otlp::metrics::MetricsMetadata::default();
    let mut uploaded_paths = Vec::new();

    for subset in per_service_requests {
        let (batches_by_type, subset_metadata) = converter.convert(subset).map_err(|e| {
            console_error!("Failed to convert OTLP metrics to Arrow: {:?}", e);
            worker::Error::RustError(format!("Encoding error: {}", e))
        })?;

        aggregated.resource_metrics_count += subset_metadata.resource_metrics_count;
        aggregated.scope_metrics_count += subset_metadata.scope_metrics_count;
        aggregated.gauge_count += subset_metadata.gauge_count;
        aggregated.sum_count += subset_metadata.sum_count;
        aggregated.histogram_count += subset_metadata.histogram_count;
        aggregated.exponential_histogram_count += subset_metadata.exponential_histogram_count;
        aggregated.summary_count += subset_metadata.summary_count;

        for (metric_type, batch) in batches_by_type {
            let service_name = extract_service_name(&batch);
            let timestamp_nanos = extract_first_timestamp(&batch);

            // Write using OtlpWriter trait
            let result = writer
                .write_batch(
                    &batch,
                    SignalType::Metrics,
                    Some(&metric_type),
                    &service_name,
                    timestamp_nanos,
                )
                .await
                .map_err(|e| {
                    console_error!("Failed to write {} metrics: {:?}", metric_type, e);
                    worker::Error::RustError(format!("Write error: {}", e))
                })?;

            uploaded_paths.push(result.path);
        }
    }

    if uploaded_paths.is_empty() {
        let response = Response::from_json(&json!({
            "status": "ok",
            "message": "No metrics data points to process",
        }))?;
        return Ok(response);
    }

    let total_data_points = aggregated.gauge_count
        + aggregated.sum_count
        + aggregated.histogram_count
        + aggregated.exponential_histogram_count
        + aggregated.summary_count;

    let response_body = json!({
        "status": "ok",
        "data_points_processed": total_data_points,
        "gauge_count": aggregated.gauge_count,
        "sum_count": aggregated.sum_count,
        "histogram_count": aggregated.histogram_count,
        "exponential_histogram_count": aggregated.exponential_histogram_count,
        "summary_count": aggregated.summary_count,
        "partitions": uploaded_paths,
    });

    Response::from_json(&response_body)
}

fn extract_service_name(batch: &RecordBatch) -> String {
    let fallback = otlp::common::UNKNOWN_SERVICE_NAME;

    if let Some(array) = batch.column(1).as_any().downcast_ref::<StringArray>() {
        for idx in 0..array.len() {
            if array.is_valid(idx) {
                let value = array.value(idx);
                if !value.is_empty() {
                    return value.to_string();
                }
            }
        }
    }

    fallback.to_string()
}

fn extract_first_timestamp(batch: &RecordBatch) -> i64 {
    if let Some(array) = batch
        .column(0)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
    {
        let mut min_value = i64::MAX;
        for idx in 0..array.len() {
            if array.is_valid(idx) {
                let value = array.value(idx);
                if value < min_value {
                    min_value = value;
                }
            }
        }

        if min_value != i64::MAX {
            return min_value;
        }
    }

    0
}
