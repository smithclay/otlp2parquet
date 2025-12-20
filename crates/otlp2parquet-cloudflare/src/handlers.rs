// Request handlers for OTLP signals (logs, traces, metrics)

use otlp2parquet_handlers::{
    process_logs as process_logs_handler, process_metrics, process_traces, OtlpError,
    ProcessorConfig,
};
use otlp2parquet_writer::icepick::catalog::Catalog;
use serde_json::json;
use worker::{Response, Result};

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
    namespace: &str,
    request_id: &str,
    catalog: Option<&dyn Catalog>,
) -> Result<Response> {
    let current_time_ms = worker::Date::now().as_millis() as i64;

    let result = process_logs_handler(
        body_bytes,
        format,
        ProcessorConfig {
            catalog,
            namespace,
            snapshot_timestamp_ms: Some(current_time_ms),
            retry_policy: otlp2parquet_writer::RetryPolicy::default(),
        },
    )
    .await
    .map_err(|e| {
        tracing::error!(
            request_id = %request_id,
            format = ?format,
            content_type = ?content_type,
            error = %e.message(),
            "Failed to process logs"
        );
        convert_to_worker_error(e, request_id)
    })?;

    let response_body = json!({
        "status": "ok",
        "records_processed": result.records_processed,
        "flush_count": result.batches_flushed,
        "partitions": result.paths_written,
    });

    Response::from_json(&response_body)
}

/// Handle traces request
pub async fn handle_traces_request(
    body_bytes: &[u8],
    format: otlp2parquet_core::InputFormat,
    content_type: Option<&str>,
    namespace: &str,
    request_id: &str,
    catalog: Option<&dyn Catalog>,
) -> Result<Response> {
    let current_time_ms = worker::Date::now().as_millis() as i64;

    let result = process_traces(
        body_bytes,
        format,
        otlp2parquet_handlers::ProcessorConfig {
            catalog,
            namespace,
            snapshot_timestamp_ms: Some(current_time_ms),
            retry_policy: otlp2parquet_writer::RetryPolicy::default(),
        },
    )
    .await
    .map_err(|e| {
        tracing::error!(
            request_id = %request_id,
            format = ?format,
            content_type = ?content_type,
            error = %e.message(),
            "Failed to process traces"
        );
        convert_to_worker_error(e, request_id)
    })?;

    let response_body = json!({
        "status": "ok",
        "spans_processed": result.records_processed,
        "partitions": result.paths_written,
    });

    Response::from_json(&response_body)
}

/// Handle metrics request (separate from logs due to multiple batches per type)
pub async fn handle_metrics_request(
    body_bytes: &[u8],
    format: otlp2parquet_core::InputFormat,
    content_type: Option<&str>,
    namespace: &str,
    request_id: &str,
    catalog: Option<&dyn Catalog>,
) -> Result<Response> {
    let current_time_ms = worker::Date::now().as_millis() as i64;

    let result = process_metrics(
        body_bytes,
        format,
        ProcessorConfig {
            catalog,
            namespace,
            snapshot_timestamp_ms: Some(current_time_ms),
            retry_policy: otlp2parquet_writer::RetryPolicy::default(),
        },
    )
    .await
    .map_err(|e| {
        tracing::error!(
            request_id = %request_id,
            format = ?format,
            content_type = ?content_type,
            error = %e.message(),
            "Failed to process metrics"
        );
        convert_to_worker_error(e, request_id)
    })?;

    let response_body = json!({
        "status": "ok",
        "data_points_processed": result.records_processed,
        "partitions": result.paths_written,
    });

    Response::from_json(&response_body)
}
