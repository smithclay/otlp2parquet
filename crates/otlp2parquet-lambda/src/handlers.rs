// HTTP request handlers for Lambda
//
// Handles the core logic of processing OTLP requests and generating responses

use anyhow::Result;
use arrow::array::{Array, RecordBatch, StringArray, TimestampNanosecondArray};
use base64::Engine;
use otlp2parquet_core::otlp;
use serde_json::json;
use std::borrow::Cow;

use crate::{HttpResponseData, LambdaState};

const HEALTHY_TEXT: &str = "Healthy";

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
    let request = match otlp::parse_otlp_request(body, format) {
        Ok(req) => req,
        Err(err) => {
            tracing::error!(
                "Failed to parse OTLP logs (format: {:?}, content-type: {:?}): {}",
                format,
                content_type,
                err
            );
            return HttpResponseData::json(
                400,
                json!({ "error": "invalid OTLP payload" }).to_string(),
            );
        }
    };

    let per_service_requests = otlp::logs::split_request_by_service(request);
    let mut uploads = Vec::new();
    let mut total_records = 0usize;

    for subset in per_service_requests {
        match state.passthrough.ingest(&subset) {
            Ok(batch) => {
                total_records += batch.metadata.record_count;
                uploads.push(batch);
            }
            Err(err) => {
                tracing::error!("Failed to convert OTLP to Arrow: {}", err);
                return HttpResponseData::json(
                    500,
                    json!({ "error": "internal encoding failure" }).to_string(),
                );
            }
        }
    }

    let mut uploaded_paths = Vec::new();
    for batch in uploads {
        // Write logs via new OtlpWriter trait
        for record_batch in &batch.batches {
            match state
                .writer
                .write_batch(
                    record_batch,
                    otlp2parquet_core::SignalType::Logs,
                    None, // No metric type for logs
                    &batch.metadata.service_name,
                    batch.metadata.first_timestamp_nanos,
                )
                .await
            {
                Ok(result) => {
                    uploaded_paths.push(result.path);
                }
                Err(err) => {
                    tracing::error!("Failed to write logs: {}", err);
                    return HttpResponseData::json(
                        500,
                        json!({ "error": "internal storage failure" }).to_string(),
                    );
                }
            }
        }
    }

    HttpResponseData::json(
        200,
        json!({
            "status": "ok",
            "records_processed": total_records,
            "flush_count": uploaded_paths.len(),
            "partitions": uploaded_paths,
        })
        .to_string(),
    )
}

async fn process_metrics(
    body: &[u8],
    format: otlp2parquet_core::InputFormat,
    content_type: Option<&str>,
    state: &LambdaState,
) -> HttpResponseData {
    // Parse OTLP metrics request
    let request = match otlp::metrics::parse_otlp_request(body, format) {
        Ok(req) => req,
        Err(err) => {
            tracing::error!(
                "Failed to parse OTLP metrics (format: {:?}, content-type: {:?}): {}",
                format,
                content_type,
                err
            );
            return HttpResponseData::json(
                400,
                json!({ "error": "invalid OTLP metrics payload" }).to_string(),
            );
        }
    };

    let per_service_requests = otlp::metrics::split_request_by_service(request);
    let converter = otlp::metrics::ArrowConverter::new();
    let mut aggregated = otlp::metrics::MetricsMetadata::default();
    let mut uploaded_paths = Vec::new();

    for subset in per_service_requests {
        let (batches_by_type, subset_metadata) = match converter.convert(subset) {
            Ok(result) => result,
            Err(err) => {
                tracing::error!("Failed to convert OTLP metrics to Arrow: {}", err);
                return HttpResponseData::json(
                    500,
                    json!({ "error": "internal encoding failure" }).to_string(),
                );
            }
        };

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

            match state
                .writer
                .write_batch(
                    &batch,
                    otlp2parquet_core::SignalType::Metrics,
                    Some(&metric_type),
                    &service_name,
                    timestamp_nanos,
                )
                .await
            {
                Ok(result) => {
                    uploaded_paths.push(result.path);
                }
                Err(err) => {
                    tracing::error!("Failed to write {} metrics: {}", metric_type, err);
                    return HttpResponseData::json(
                        500,
                        json!({ "error": "internal storage failure" }).to_string(),
                    );
                }
            }
        }
    }

    if uploaded_paths.is_empty() {
        return HttpResponseData::json(
            200,
            json!({
                "status": "ok",
                "message": "No metrics data points to process",
            })
            .to_string(),
        );
    }

    let total_data_points = aggregated.gauge_count
        + aggregated.sum_count
        + aggregated.histogram_count
        + aggregated.exponential_histogram_count
        + aggregated.summary_count;

    HttpResponseData::json(
        200,
        json!({
            "status": "ok",
            "data_points_processed": total_data_points,
            "gauge_count": aggregated.gauge_count,
            "sum_count": aggregated.sum_count,
            "histogram_count": aggregated.histogram_count,
            "exponential_histogram_count": aggregated.exponential_histogram_count,
            "summary_count": aggregated.summary_count,
            "partitions": uploaded_paths,
        })
        .to_string(),
    )
}

async fn process_traces(
    body: &[u8],
    format: otlp2parquet_core::InputFormat,
    content_type: Option<&str>,
    state: &LambdaState,
) -> HttpResponseData {
    let request = match otlp::traces::parse_otlp_trace_request(body, format) {
        Ok(req) => req,
        Err(err) => {
            tracing::error!(
                "Failed to parse OTLP traces (format: {:?}, content-type: {:?}): {}",
                format,
                content_type,
                err
            );
            return HttpResponseData::json(
                400,
                json!({ "error": "invalid OTLP traces payload" }).to_string(),
            );
        }
    };

    let per_service_requests = otlp::traces::split_request_by_service(request);
    let mut uploads = Vec::new();
    let mut total_spans = 0usize;

    for subset in per_service_requests {
        match otlp::traces::TraceArrowConverter::convert(&subset) {
            Ok((batches, metadata)) => {
                total_spans += metadata.span_count;
                uploads.push((batches, metadata));
            }
            Err(err) => {
                tracing::error!("Failed to convert OTLP traces to Arrow: {}", err);
                return HttpResponseData::json(
                    500,
                    json!({ "error": "internal encoding failure" }).to_string(),
                );
            }
        }
    }

    let mut uploaded_paths = Vec::new();
    for (batches, _metadata) in uploads {
        // Write traces via Writer (handles both Iceberg and PlainS3 modes)
        for record_batch in &batches {
            let service_name = extract_service_name(record_batch);
            let timestamp_nanos = extract_first_timestamp(record_batch);

            match state
                .writer
                .write_batch(
                    record_batch,
                    otlp2parquet_core::SignalType::Traces,
                    None, // No metric type for traces
                    &service_name,
                    timestamp_nanos,
                )
                .await
            {
                Ok(result) => {
                    uploaded_paths.push(result.path);
                }
                Err(err) => {
                    tracing::error!("Failed to write traces: {}", err);
                    return HttpResponseData::json(
                        500,
                        json!({ "error": "internal storage failure" }).to_string(),
                    );
                }
            }
        }
    }

    HttpResponseData::json(
        200,
        json!({
            "status": "ok",
            "spans_processed": total_spans,
            "flush_count": uploaded_paths.len(),
            "partitions": uploaded_paths,
        })
        .to_string(),
    )
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
