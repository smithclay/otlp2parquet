// HTTP request handlers for Lambda
//
// Handles the core logic of processing OTLP requests and generating responses

use anyhow::Result;
use base64::Engine;
use otlp2parquet_handlers::{
    process_logs as process_logs_handler, process_metrics as process_metrics_handler,
    process_traces as process_traces_handler, OtlpError, ProcessorConfig,
};
use serde_json::json;
use std::borrow::Cow;

use crate::{HttpResponseData, LambdaState};

const HEALTHY_TEXT: &str = "Healthy";

/// Convert OtlpError to HttpResponseData
fn convert_to_http_response(err: OtlpError) -> HttpResponseData {
    let status_code = err.status_code();
    let response_body = json!({
        "error": err.error_type(),
        "message": err.message(),
        "hint": err.hint(),
    });
    HttpResponseData::json(status_code, response_body.to_string())
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum SignalKind {
    Logs,
    Traces,
    Metrics,
}

/// Handle incoming HTTP request based on method and path
pub(crate) async fn handle_http_request(
    method: &str,
    path: &str,
    body: Option<&str>,
    is_base64_encoded: bool,
    content_type: Option<&str>,
    state: &LambdaState,
) -> HttpResponseData {
    let method = method.trim().to_ascii_uppercase();
    match method.as_str() {
        "POST" => handle_post(path, body, is_base64_encoded, content_type, state).await,
        "GET" => handle_get(path),
        _ => HttpResponseData::json(405, json!({ "error": "method not allowed" }).to_string()),
    }
}

/// Handle POST requests - OTLP log ingestion
async fn handle_post(
    path: &str,
    body: Option<&str>,
    is_base64_encoded: bool,
    content_type: Option<&str>,
    state: &LambdaState,
) -> HttpResponseData {
    let signal = match path {
        "/v1/logs" => SignalKind::Logs,
        "/v1/traces" => SignalKind::Traces,
        "/v1/metrics" => SignalKind::Metrics,
        _ => return HttpResponseData::json(404, json!({ "error": "not found" }).to_string()),
    };

    let body = match decode_body(body, is_base64_encoded) {
        Ok(bytes) => bytes,
        Err(response) => return response,
    };

    if body.len() > state.max_payload_bytes {
        return HttpResponseData::json(
            413,
            json!({
                "error": "payload too large",
                "limit_bytes": state.max_payload_bytes,
            })
            .to_string(),
        );
    }

    // Detect input format from Content-Type header
    let format = otlp2parquet_core::InputFormat::from_content_type(content_type);

    match signal {
        SignalKind::Logs => process_logs(body.as_ref(), format, content_type, state).await,
        SignalKind::Metrics => process_metrics(body.as_ref(), format, content_type, state).await,
        SignalKind::Traces => process_traces(body.as_ref(), format, content_type, state).await,
    }
}

async fn process_logs(
    body: &[u8],
    format: otlp2parquet_core::InputFormat,
    content_type: Option<&str>,
    state: &LambdaState,
) -> HttpResponseData {
    let result = process_logs_handler(
        body,
        format,
        ProcessorConfig {
            catalog: state.catalog.as_deref(),
            namespace: &state.namespace,
            snapshot_timestamp_ms: None,
        },
    )
    .await
    .map_err(|e| {
        tracing::error!(
            "Failed to process logs (format: {:?}, content-type: {:?}): {:?}",
            format,
            content_type,
            e.message()
        );
        convert_to_http_response(e)
    });

    match result {
        Ok(processing_result) => HttpResponseData::json(
            200,
            json!({
                "status": "ok",
                "records_processed": processing_result.records_processed,
                "flush_count": processing_result.batches_flushed,
                "partitions": processing_result.paths_written,
            })
            .to_string(),
        ),
        Err(response) => response,
    }
}

async fn process_metrics(
    body: &[u8],
    format: otlp2parquet_core::InputFormat,
    content_type: Option<&str>,
    state: &LambdaState,
) -> HttpResponseData {
    let result = process_metrics_handler(
        body,
        format,
        ProcessorConfig {
            catalog: state.catalog.as_deref(),
            namespace: &state.namespace,
            snapshot_timestamp_ms: None,
        },
    )
    .await
    .map_err(|e| {
        tracing::error!(
            "Failed to process metrics (format: {:?}, content-type: {:?}): {:?}",
            format,
            content_type,
            e.message()
        );
        convert_to_http_response(e)
    });

    match result {
        Ok(processing_result) => HttpResponseData::json(
            200,
            json!({
                "status": "ok",
                "data_points_processed": processing_result.records_processed,
                "partitions": processing_result.paths_written,
            })
            .to_string(),
        ),
        Err(response) => response,
    }
}

async fn process_traces(
    body: &[u8],
    format: otlp2parquet_core::InputFormat,
    content_type: Option<&str>,
    state: &LambdaState,
) -> HttpResponseData {
    let result = process_traces_handler(
        body,
        format,
        ProcessorConfig {
            catalog: state.catalog.as_deref(),
            namespace: &state.namespace,
            snapshot_timestamp_ms: None,
        },
    )
    .await
    .map_err(|e| {
        tracing::error!(
            "Failed to process traces (format: {:?}, content-type: {:?}): {:?}",
            format,
            content_type,
            e.message()
        );
        convert_to_http_response(e)
    });

    match result {
        Ok(processing_result) => HttpResponseData::json(
            200,
            json!({
                "status": "ok",
                "spans_processed": processing_result.records_processed,
                "flush_count": processing_result.batches_flushed,
                "partitions": processing_result.paths_written,
            })
            .to_string(),
        ),
        Err(response) => response,
    }
}

/// Handle GET requests - health checks
fn handle_get(path: &str) -> HttpResponseData {
    match path {
        "/health" => HttpResponseData::text(200, HEALTHY_TEXT.to_string()),
        _ => HttpResponseData::json(404, json!({ "error": "not found" }).to_string()),
    }
}

/// Decode request body, handling base64 encoding
fn decode_body<'a>(
    body: Option<&'a str>,
    is_base64_encoded: bool,
) -> Result<Cow<'a, [u8]>, HttpResponseData> {
    let body = body.ok_or_else(|| {
        HttpResponseData::json(400, json!({ "error": "missing request body" }).to_string())
    })?;

    if is_base64_encoded {
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(body.as_bytes())
            .map_err(|_| {
                HttpResponseData::json(400, json!({ "error": "invalid base64 body" }).to_string())
            })?;
        Ok(Cow::Owned(decoded))
    } else {
        Ok(Cow::Borrowed(body.as_bytes()))
    }
}

/// Extract canonical path from request (strip query string)
pub(crate) fn canonical_path(path: Option<&str>) -> String {
    let raw = path.unwrap_or("/");
    raw.split('?').next().unwrap_or("/").to_string()
}
