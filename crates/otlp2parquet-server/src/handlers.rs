// HTTP request handlers for server mode
//
// Implements OTLP ingestion and health check endpoints

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use metrics::{counter, histogram};
use otlp2parquet_batch::CompletedBatch;
use otlp2parquet_core::{otlp, InputFormat};
use serde_json::json;
use std::time::Instant;
use tracing::{debug, info, warn};

use crate::{AppError, AppState};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum SignalKind {
    Logs,
    Traces,
    Metrics,
}

impl SignalKind {
    fn as_str(&self) -> &'static str {
        match self {
            SignalKind::Logs => "logs",
            SignalKind::Traces => "traces",
            SignalKind::Metrics => "metrics",
        }
    }
}

/// POST /v1/logs - OTLP log ingestion endpoint
pub(crate) async fn handle_logs(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, AppError> {
    handle_signal(SignalKind::Logs, &state, headers, body).await
}

/// POST /v1/traces - OTLP trace ingestion endpoint (stub)
pub(crate) async fn handle_traces(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, AppError> {
    handle_signal(SignalKind::Traces, &state, headers, body).await
}

/// POST /v1/metrics - OTLP metrics ingestion endpoint
pub(crate) async fn handle_metrics(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, AppError> {
    handle_signal(SignalKind::Metrics, &state, headers, body).await
}

/// GET /health - Basic health check
pub(crate) async fn health_check() -> impl IntoResponse {
    (StatusCode::OK, Json(json!({"status": "healthy"})))
}

/// GET /ready - Readiness check (includes storage connectivity)
pub(crate) async fn ready_check(State(state): State<AppState>) -> impl IntoResponse {
    // Test storage connectivity by listing (basic check)
    match state.storage.list("").await {
        Ok(_) => (
            StatusCode::OK,
            Json(json!({"status": "ready", "storage": "connected"})),
        ),
        Err(e) => {
            warn!("Storage readiness check failed: {}", e);
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(
                    json!({"status": "not ready", "storage": "disconnected", "error": e.to_string()}),
                ),
            )
        }
    }
}

async fn handle_signal(
    signal: SignalKind,
    state: &AppState,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, AppError> {
    let content_type = headers.get("content-type").and_then(|v| v.to_str().ok());
    let format = InputFormat::from_content_type(content_type);

    debug!(
        "Received OTLP {} request ({} bytes, format: {:?}, content-type: {:?})",
        signal.as_str(),
        body.len(),
        format,
        content_type
    );

    let max_payload = state.max_payload_bytes;
    if body.len() > max_payload {
        counter!("otlp.ingest.rejected", 1);
        return Err(AppError::with_status(
            StatusCode::PAYLOAD_TOO_LARGE,
            anyhow::anyhow!("payload {} exceeds limit {}", body.len(), max_payload),
        ));
    }

    match signal {
        SignalKind::Logs => process_logs(state, format, body).await,
        SignalKind::Traces => Ok(build_not_implemented_response("traces")),
        SignalKind::Metrics => process_metrics(state, format, body).await,
    }
}

async fn process_logs(
    state: &AppState,
    format: InputFormat,
    body: axum::body::Bytes,
) -> Result<Response, AppError> {
    let start = Instant::now();
    counter!("otlp.ingest.requests", 1);
    histogram!("otlp.ingest.bytes", body.len() as f64);

    let request = otlp::parse_otlp_request(&body, format)
        .map_err(|e| e.context("Failed to parse OTLP request payload"))?;

    let mut uploads: Vec<CompletedBatch<otlp::LogMetadata>> = Vec::new();
    let metadata: otlp::LogMetadata;

    if let Some(batcher) = &state.batcher {
        let mut expired = batcher
            .drain_expired()
            .map_err(|e| e.context("Failed to flush expired batches"))?;
        uploads.append(&mut expired);

        let (mut ready, meta) = batcher
            .ingest(&request, body.len())
            .map_err(|e| e.context("Failed to enqueue batch"))?;
        uploads.append(&mut ready);
        metadata = meta;
    } else {
        let batch = state
            .passthrough
            .ingest(&request)
            .map_err(|e| e.context("Failed to convert OTLP to Arrow"))?;
        metadata = batch.metadata.clone();
        uploads.push(batch);
    }

    counter!("otlp.ingest.records", metadata.record_count as u64);

    let mut uploaded_paths = Vec::new();
    for completed in uploads {
        let (partition_path, _hash) = state
            .parquet_writer
            .write_batches_with_hash(
                &completed.batches,
                &completed.metadata.service_name,
                completed.metadata.first_timestamp_nanos,
            )
            .await
            .map_err(|e| e.context("Failed to write Parquet to storage"))?;

        counter!("otlp.batch.flushes", 1);
        histogram!("otlp.batch.rows", completed.metadata.record_count as f64);
        info!(
            "Committed batch path={} service={} rows={}",
            partition_path, completed.metadata.service_name, completed.metadata.record_count
        );
        uploaded_paths.push(partition_path);
    }

    histogram!(
        "otlp.ingest.latency_ms",
        start.elapsed().as_secs_f64() * 1000.0
    );

    let response = Json(json!({
        "status": "ok",
        "records_processed": metadata.record_count,
        "flush_count": uploaded_paths.len(),
        "partitions": uploaded_paths,
    }));

    Ok((StatusCode::OK, response).into_response())
}

async fn process_metrics(
    state: &AppState,
    format: InputFormat,
    body: axum::body::Bytes,
) -> Result<Response, AppError> {
    let start = Instant::now();
    counter!("otlp.ingest.requests", 1, "signal" => "metrics");
    histogram!("otlp.ingest.bytes", body.len() as f64, "signal" => "metrics");

    // Parse OTLP metrics request
    let request = otlp::metrics::parse_otlp_request(&body, format)
        .map_err(|e| e.context("Failed to parse OTLP metrics request payload"))?;

    // Convert to Arrow (returns multiple batches, one per metric type)
    let converter = otlp::metrics::ArrowConverter::new();
    let (batches_by_type, metadata) = converter
        .convert(request)
        .map_err(|e| e.context("Failed to convert OTLP metrics to Arrow"))?;

    if batches_by_type.is_empty() {
        return Ok((
            StatusCode::OK,
            Json(json!({
                "status": "ok",
                "message": "No metrics data points to process",
            })),
        )
            .into_response());
    }

    // Count total data points across all metric types
    let total_data_points = metadata.gauge_count
        + metadata.sum_count
        + metadata.histogram_count
        + metadata.exponential_histogram_count
        + metadata.summary_count;

    counter!("otlp.ingest.records", total_data_points as u64, "signal" => "metrics");

    // Write each metric type batch to its own Parquet file
    let mut uploaded_paths = Vec::new();
    for (metric_type, batch) in batches_by_type {
        let service_name = "default"; // TODO: Extract from metadata
        let timestamp_nanos = 0; // TODO: Extract first timestamp from batch

        let (partition_path, _hash) = state
            .parquet_writer
            .write_batches_with_signal(
                &[batch],
                service_name,
                timestamp_nanos,
                "metrics",
                Some(&metric_type), // Use metric type as subdirectory
            )
            .await
            .map_err(|e| e.context(format!("Failed to write {} metrics Parquet", metric_type)))?;

        counter!("otlp.metrics.flushes", 1, "metric_type" => metric_type.clone());
        info!(
            "Committed metrics batch path={} metric_type={} service={}",
            partition_path, metric_type, service_name
        );
        uploaded_paths.push(partition_path);
    }

    histogram!(
        "otlp.ingest.latency_ms",
        start.elapsed().as_secs_f64() * 1000.0,
        "signal" => "metrics"
    );

    let response = Json(json!({
        "status": "ok",
        "data_points_processed": total_data_points,
        "gauge_count": metadata.gauge_count,
        "sum_count": metadata.sum_count,
        "histogram_count": metadata.histogram_count,
        "exponential_histogram_count": metadata.exponential_histogram_count,
        "summary_count": metadata.summary_count,
        "partitions": uploaded_paths,
    }));

    Ok((StatusCode::OK, response).into_response())
}

fn build_not_implemented_response(signal_type: &str) -> Response {
    warn!(
        "Received OTLP {} request but ingestion is not implemented yet",
        signal_type
    );
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({
            "error": format!("OTLP {} ingestion not implemented yet", signal_type),
        })),
    )
        .into_response()
}
