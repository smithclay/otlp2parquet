// Request handlers for OTLP signals (logs, traces, metrics)

use arrow::array::{Array, RecordBatch, StringArray, TimestampNanosecondArray};
use otlp2parquet_core::{otlp, SignalType};
use otlp2parquet_handlers::{
    process_logs as process_logs_handler, process_traces, OtlpError, ProcessorConfig,
};
use serde_json::json;
use std::sync::Arc;
use worker::{console_error, Response, Result};

use crate::errors;

/// Convert OtlpError to worker::Error
fn convert_to_worker_error(err: OtlpError, request_id: &str) -> worker::Error {
    let status_code = err.status_code();
    let error_response = errors::ErrorResponse {
        error: err.error_type().to_string(),
        message: err.message(),
        details: err.hint(),
        request_id: Some(request_id.to_string()),
    };

    let error_json = serde_json::to_string(&error_response).unwrap_or_else(|_| {
        r#"{"error":"internal error","code":"SERIALIZATION_FAILED"}"#.to_string()
    });
    worker::Error::RustError(format!("{}:{}", status_code, error_json))
}

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
    let current_time_ms = worker::Date::now().as_millis() as i64;

    let result = process_logs_handler(
        body_bytes,
        format,
        ProcessorConfig {
            catalog: catalog.map(|c| c.as_ref()),
            namespace: namespace.unwrap_or("default"),
            snapshot_timestamp_ms: Some(current_time_ms),
        },
    )
    .await
    .map_err(|e| {
        console_error!(
            "[{}] Failed to process logs (format: {:?}, content-type: {:?}): {:?}",
            request_id,
            format,
            content_type,
            e.message()
        );
        convert_to_worker_error(e, request_id)
    })?;

    let response_body = json!({
        "status": "ok",
        "records_processed": result.records_processed,
        "flush_count": result.batches_flushed,
        "partitions": result.paths_written,
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
    let current_time_ms = worker::Date::now().as_millis() as i64;

    let result = process_traces(
        body_bytes,
        format,
        otlp2parquet_handlers::ProcessorConfig {
            catalog: catalog.map(|c| c.as_ref()),
            namespace: namespace.unwrap_or("default"),
            snapshot_timestamp_ms: Some(current_time_ms),
        },
    )
    .await
    .map_err(|e| {
        console_error!(
            "[{}] Failed to process traces (format: {:?}, content-type: {:?}): {:?}",
            request_id,
            format,
            content_type,
            e.message()
        );
        convert_to_worker_error(e, request_id)
    })?;

    let response_body = json!({
        "status": "ok",
        "spans_processed": result.records_processed,
        "partitions": result.paths_written,
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
