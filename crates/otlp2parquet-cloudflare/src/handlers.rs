// Request handlers for OTLP signals (logs, traces, metrics)

use arrow::array::{Array, RecordBatch, StringArray, TimestampNanosecondArray};
use otlp2parquet_batch::{LogSignalProcessor, PassthroughBatcher};
use otlp2parquet_core::{otlp, SignalType};
use serde_json::json;
use std::sync::Arc;
use worker::{console_error, Response, Result};

use crate::errors;

/// Handle logs request
pub async fn handle_logs_request(
    body_bytes: &[u8],
    format: otlp2parquet_core::InputFormat,
    content_type: Option<&str>,
    catalog: Option<&Arc<dyn otlp2parquet_writer::icepick::catalog::Catalog>>,
    namespace: Option<&str>,
    catalog_enabled: bool,
    request_id: &str,
) -> Result<Response> {
    // Cloudflare Workers: Always use passthrough (no batching)
    // Batching requires time-based operations which aren't supported in WASM
    let passthrough: PassthroughBatcher<LogSignalProcessor> = PassthroughBatcher::default();

    // Process logs
    let request = otlp::parse_otlp_request(body_bytes, format).map_err(|e| {
        console_error!(
            "[{}] Failed to parse OTLP logs (format: {:?}, content-type: {:?}): {:?}",
            request_id,
            format,
            content_type,
            e
        );
        let error = errors::OtlpErrorKind::InvalidRequest(format!(
            "Failed to parse OTLP logs request: {}. Ensure the request body contains valid OTLP protobuf, JSON, or JSONL format.",
            e
        ));
        let status_code = error.status_code();
        let error_response = errors::ErrorResponse::from_error(error, Some(request_id.to_string()));
        // Convert to worker error with proper status code
        let error_json = serde_json::to_string(&error_response)
            .unwrap_or_else(|_| r#"{"error":"internal error","code":"SERIALIZATION_FAILED"}"#.to_string());
        worker::Error::RustError(format!("{}:{}", status_code, error_json))
    })?;

    let per_service_requests = otlp::logs::split_request_by_service(request);
    let mut uploads = Vec::new();
    let mut total_records = 0usize;

    for subset in per_service_requests {
        let batch = passthrough.ingest(&subset).map_err(|err| {
            console_error!(
                "[{}] Failed to convert OTLP to Arrow: {:?}",
                request_id,
                err
            );
            let error = errors::OtlpErrorKind::InternalError(format!(
                "Failed to convert OTLP logs to Arrow format: {}",
                err
            ));
            let status_code = error.status_code();
            let error_response =
                errors::ErrorResponse::from_error(error, Some(request_id.to_string()));
            let error_json = serde_json::to_string(&error_response).unwrap_or_else(|_| {
                r#"{"error":"internal error","code":"SERIALIZATION_FAILED"}"#.to_string()
            });
            worker::Error::RustError(format!("{}:{}", status_code, error_json))
        })?;
        total_records += batch.metadata.record_count;
        uploads.push(batch);
    }

    let mut uploaded_paths = Vec::new();
    for batch in uploads {
        // Write via write_batch function
        // Get current time in milliseconds for Iceberg snapshot timestamp
        let current_time_ms = worker::Date::now().as_millis() as i64;

        for record_batch in &batch.batches {
            let path = otlp2parquet_writer::write_batch(
                otlp2parquet_writer::WriteBatchRequest {
                    catalog: catalog.map(|arc| arc.as_ref()),
                    namespace: namespace.unwrap_or("default"),
                    batch: record_batch,
                    signal_type: SignalType::Logs,
                    metric_type: None,
                    service_name: &batch.metadata.service_name,
                    timestamp_micros: batch.metadata.first_timestamp_nanos,
                    snapshot_timestamp_ms: Some(current_time_ms),
                },
            )
            .await
            .map_err(|e| {
                console_error!("[{}] Failed to write logs: {:?}", request_id, e);
                let error = errors::OtlpErrorKind::StorageError(format!(
                    "Failed to write logs to R2 storage: {}. Check R2 credentials and bucket permissions.",
                    e
                ));
                let status_code = error.status_code();
                let error_response = errors::ErrorResponse::from_error(error, Some(request_id.to_string()));
                let error_json = serde_json::to_string(&error_response)
                    .unwrap_or_else(|_| r#"{"error":"internal error","code":"SERIALIZATION_FAILED"}"#.to_string());
                worker::Error::RustError(format!("{}:{}", status_code, error_json))
            })?;

            uploaded_paths.push(path);
        }
    }

    let response_body = json!({
        "status": "ok",
        "records_processed": total_records,
        "flush_count": uploaded_paths.len(),
        "partitions": uploaded_paths,
        "catalog_enabled": catalog_enabled,
    });

    Response::from_json(&response_body)
}

/// Handle traces request
pub async fn handle_traces_request(
    body_bytes: &[u8],
    format: otlp2parquet_core::InputFormat,
    content_type: Option<&str>,
    catalog: Option<&Arc<dyn otlp2parquet_writer::icepick::catalog::Catalog>>,
    namespace: Option<&str>,
    catalog_enabled: bool,
    request_id: &str,
) -> Result<Response> {
    // Parse OTLP traces request
    let request = otlp::traces::parse_otlp_trace_request(body_bytes, format).map_err(|e| {
        console_error!(
            "[{}] Failed to parse OTLP traces (format: {:?}, content-type: {:?}): {:?}",
            request_id,
            format,
            content_type,
            e
        );
        let error = errors::OtlpErrorKind::InvalidRequest(format!(
            "Failed to parse OTLP traces request: {}. Ensure the request body contains valid OTLP protobuf, JSON, or JSONL format.",
            e
        ));
        let status_code = error.status_code();
        let error_response = errors::ErrorResponse::from_error(error, Some(request_id.to_string()));
        let error_json = serde_json::to_string(&error_response)
            .unwrap_or_else(|_| r#"{"error":"internal error","code":"SERIALIZATION_FAILED"}"#.to_string());
        worker::Error::RustError(format!("{}:{}", status_code, error_json))
    })?;

    let per_service_requests = otlp::traces::split_request_by_service(request);
    let mut uploaded_paths = Vec::new();
    let mut spans_processed = 0usize;

    for subset in per_service_requests {
        let (batches, metadata) =
            otlp::traces::TraceArrowConverter::convert(&subset).map_err(|e| {
                console_error!(
                    "[{}] Failed to convert OTLP traces to Arrow: {:?}",
                    request_id,
                    e
                );
                let error = errors::OtlpErrorKind::InternalError(format!(
                    "Failed to convert OTLP traces to Arrow format: {}",
                    e
                ));
                let status_code = error.status_code();
                let error_response =
                    errors::ErrorResponse::from_error(error, Some(request_id.to_string()));
                let error_json = serde_json::to_string(&error_response).unwrap_or_else(|_| {
                    r#"{"error":"internal error","code":"SERIALIZATION_FAILED"}"#.to_string()
                });
                worker::Error::RustError(format!("{}:{}", status_code, error_json))
            })?;

        if batches.is_empty() || metadata.span_count == 0 {
            continue;
        }

        spans_processed += metadata.span_count;

        // Write via write_batch function
        // Get current time in milliseconds for Iceberg snapshot timestamp
        let current_time_ms = worker::Date::now().as_millis() as i64;

        for batch in &batches {
            let path = otlp2parquet_writer::write_batch(
                otlp2parquet_writer::WriteBatchRequest {
                    catalog: catalog.map(|arc| arc.as_ref()),
                    namespace: namespace.unwrap_or("default"),
                    batch,
                    signal_type: SignalType::Traces,
                    metric_type: None,
                    service_name: metadata.service_name.as_ref(),
                    timestamp_micros: metadata.first_timestamp_nanos,
                    snapshot_timestamp_ms: Some(current_time_ms),
                },
            )
            .await
            .map_err(|e| {
                console_error!("[{}] Failed to write traces: {:?}", request_id, e);
                let error = errors::OtlpErrorKind::StorageError(format!(
                    "Failed to write traces to R2 storage: {}. Check R2 credentials and bucket permissions.",
                    e
                ));
                let status_code = error.status_code();
                let error_response = errors::ErrorResponse::from_error(error, Some(request_id.to_string()));
                let error_json = serde_json::to_string(&error_response)
                    .unwrap_or_else(|_| r#"{"error":"internal error","code":"SERIALIZATION_FAILED"}"#.to_string());
                worker::Error::RustError(format!("{}:{}", status_code, error_json))
            })?;

            uploaded_paths.push(path);
        }
    }

    if spans_processed == 0 {
        let response = Response::from_json(&json!({
            "status": "ok",
            "message": "No trace spans to process",
            "catalog_enabled": catalog_enabled,
        }))?;
        return Ok(response);
    }

    let response_body = json!({
        "status": "ok",
        "spans_processed": spans_processed,
        "partitions": uploaded_paths,
        "catalog_enabled": catalog_enabled,
    });

    Response::from_json(&response_body)
}

/// Handle metrics request (separate from logs due to multiple batches per type)
pub async fn handle_metrics_request(
    body_bytes: &[u8],
    format: otlp2parquet_core::InputFormat,
    content_type: Option<&str>,
    catalog: Option<&Arc<dyn otlp2parquet_writer::icepick::catalog::Catalog>>,
    namespace: Option<&str>,
    catalog_enabled: bool,
    request_id: &str,
) -> Result<Response> {
    // Parse OTLP metrics request
    let request = otlp::metrics::parse_otlp_request(body_bytes, format).map_err(|e| {
        console_error!(
            "[{}] Failed to parse OTLP metrics (format: {:?}, content-type: {:?}): {:?}",
            request_id,
            format,
            content_type,
            e
        );
        let error = errors::OtlpErrorKind::InvalidRequest(format!(
            "Failed to parse OTLP metrics request: {}. Ensure the request body contains valid OTLP protobuf, JSON, or JSONL format.",
            e
        ));
        let status_code = error.status_code();
        let error_response = errors::ErrorResponse::from_error(error, Some(request_id.to_string()));
        let error_json = serde_json::to_string(&error_response)
            .unwrap_or_else(|_| r#"{"error":"internal error","code":"SERIALIZATION_FAILED"}"#.to_string());
        worker::Error::RustError(format!("{}:{}", status_code, error_json))
    })?;

    let per_service_requests = otlp::metrics::split_request_by_service(request);
    let converter = otlp::metrics::ArrowConverter::new();
    let mut aggregated = otlp::metrics::MetricsMetadata::default();
    let mut uploaded_paths = Vec::new();

    for subset in per_service_requests {
        let (batches_by_type, subset_metadata) = converter.convert(subset).map_err(|e| {
            console_error!(
                "[{}] Failed to convert OTLP metrics to Arrow: {:?}",
                request_id,
                e
            );
            let error = errors::OtlpErrorKind::InternalError(format!(
                "Failed to convert OTLP metrics to Arrow format: {}",
                e
            ));
            let status_code = error.status_code();
            let error_response =
                errors::ErrorResponse::from_error(error, Some(request_id.to_string()));
            let error_json = serde_json::to_string(&error_response).unwrap_or_else(|_| {
                r#"{"error":"internal error","code":"SERIALIZATION_FAILED"}"#.to_string()
            });
            worker::Error::RustError(format!("{}:{}", status_code, error_json))
        })?;

        aggregated.resource_metrics_count += subset_metadata.resource_metrics_count;
        aggregated.scope_metrics_count += subset_metadata.scope_metrics_count;
        aggregated.gauge_count += subset_metadata.gauge_count;
        aggregated.sum_count += subset_metadata.sum_count;
        aggregated.histogram_count += subset_metadata.histogram_count;
        aggregated.exponential_histogram_count += subset_metadata.exponential_histogram_count;
        aggregated.summary_count += subset_metadata.summary_count;

        // Get current time in milliseconds for Iceberg snapshot timestamp
        let current_time_ms = worker::Date::now().as_millis() as i64;

        for (metric_type, batch) in batches_by_type {
            let service_name = extract_service_name(&batch);
            let timestamp_nanos = extract_first_timestamp(&batch);

            // Write via write_batch function
            let path = otlp2parquet_writer::write_batch(
                otlp2parquet_writer::WriteBatchRequest {
                    catalog: catalog.map(|arc| arc.as_ref()),
                    namespace: namespace.unwrap_or("default"),
                    batch: &batch,
                    signal_type: SignalType::Metrics,
                    metric_type: Some(&metric_type),
                    service_name: &service_name,
                    timestamp_micros: timestamp_nanos,
                    snapshot_timestamp_ms: Some(current_time_ms),
                },
            )
            .await
            .map_err(|e| {
                console_error!("[{}] Failed to write {} metrics: {:?}", request_id, metric_type, e);
                let error = errors::OtlpErrorKind::StorageError(format!(
                    "Failed to write {} metrics to R2 storage: {}. Check R2 credentials and bucket permissions.",
                    metric_type, e
                ));
                let status_code = error.status_code();
                let error_response = errors::ErrorResponse::from_error(error, Some(request_id.to_string()));
                let error_json = serde_json::to_string(&error_response)
                    .unwrap_or_else(|_| r#"{"error":"internal error","code":"SERIALIZATION_FAILED"}"#.to_string());
                worker::Error::RustError(format!("{}:{}", status_code, error_json))
            })?;

            uploaded_paths.push(path);
        }
    }

    if uploaded_paths.is_empty() {
        let response = Response::from_json(&json!({
            "status": "ok",
            "message": "No metrics data points to process",
            "catalog_enabled": catalog_enabled,
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
        "catalog_enabled": catalog_enabled,
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
