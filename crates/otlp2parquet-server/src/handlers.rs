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
}

impl SignalKind {
    fn as_str(&self) -> &'static str {
        match self {
            SignalKind::Logs => "logs",
            SignalKind::Traces => "traces",
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
        SignalKind::Traces => Ok(build_not_implemented_response()),
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

fn build_not_implemented_response() -> Response {
    warn!("Received OTLP traces request but trace ingestion is not implemented yet");
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({
            "error": "OTLP trace ingestion not implemented yet",
        })),
    )
        .into_response()
}
