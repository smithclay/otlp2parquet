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
use otlp2parquet_common::{InputFormat, MetricType, SignalType};
use otlp2parquet_handlers::{
    decode_logs_partitioned, decode_metrics_partitioned, decode_traces_partitioned,
    report_skipped_metrics, ServiceGroupedBatches,
};
use serde_json::json;
use std::time::Instant;
use tracing::{debug, info};

use crate::{AppError, AppState};

/// POST /v1/logs - OTLP log ingestion endpoint
pub(crate) async fn handle_logs(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, AppError> {
    handle_signal(SignalType::Logs, &state, headers, body).await
}

/// POST /v1/traces - OTLP trace ingestion endpoint (stub)
pub(crate) async fn handle_traces(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, AppError> {
    handle_signal(SignalType::Traces, &state, headers, body).await
}

/// POST /v1/metrics - OTLP metrics ingestion endpoint
pub(crate) async fn handle_metrics(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, AppError> {
    handle_signal(SignalType::Metrics, &state, headers, body).await
}

/// GET /health - Basic health check
pub(crate) async fn health_check() -> impl IntoResponse {
    (StatusCode::OK, Json(json!({"status": "healthy"})))
}

/// GET /ready - Readiness check
pub(crate) async fn ready_check(State(_state): State<AppState>) -> impl IntoResponse {
    // Writer is always ready after initialization
    // TODO: Add actual health checks if needed (e.g., test write to storage)
    (StatusCode::OK, Json(json!({"status": "ready"})))
}

async fn handle_signal(
    signal: SignalType,
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
        SignalType::Logs => process_logs(state, format, body).await,
        SignalType::Traces => process_traces(state, format, body).await,
        SignalType::Metrics => process_metrics(state, format, body).await,
    }
}

async fn process_logs(
    _state: &AppState,
    format: InputFormat,
    body: axum::body::Bytes,
) -> Result<Response, AppError> {
    let start = Instant::now();
    counter!("otlp.ingest.requests", 1);
    histogram!("otlp.ingest.bytes", body.len() as f64);

    let parse_start = Instant::now();
    let grouped = decode_logs_partitioned(&body, format).map_err(|e| {
        AppError::bad_request(anyhow::anyhow!("Failed to parse OTLP logs request: {}", e))
    })?;
    debug!(
        elapsed_us = parse_start.elapsed().as_micros() as u64,
        signal = "logs",
        records = grouped.total_records,
        "parse"
    );

    let mut uploaded_paths = Vec::new();
    let mut total_records: usize = 0;

    let write_start = Instant::now();
    for pb in grouped.batches {
        if pb.batch.num_rows() == 0 {
            continue;
        }

        total_records += pb.record_count;
        counter!("otlp.ingest.records", pb.record_count as u64);

        let path = otlp2parquet_writer::write_batch(otlp2parquet_writer::WriteBatchRequest {
            batch: &pb.batch,
            signal_type: SignalType::Logs,
            metric_type: None,
            service_name: &pb.service_name,
            timestamp_micros: pb.min_timestamp_micros,
        })
        .await
        .map_err(|e| {
            AppError::internal(anyhow::anyhow!("Failed to write logs to storage: {}", e))
        })?;

        counter!("otlp.batch.flushes", 1);
        histogram!("otlp.batch.rows", pb.record_count as f64);
        info!(
            "Committed batch path={} service={} rows={}",
            path, pb.service_name, pb.record_count
        );
        uploaded_paths.push(path);
    }
    debug!(
        elapsed_us = write_start.elapsed().as_micros() as u64,
        signal = "logs",
        "write"
    );

    histogram!(
        "otlp.ingest.latency_ms",
        start.elapsed().as_secs_f64() * 1000.0
    );

    let response = Json(json!({
        "status": "ok",
        "records_processed": total_records,
        "flush_count": uploaded_paths.len(),
        "partitions": uploaded_paths,
    }));

    Ok((StatusCode::OK, response).into_response())
}

async fn process_traces(
    _state: &AppState,
    format: InputFormat,
    body: axum::body::Bytes,
) -> Result<Response, AppError> {
    let start = Instant::now();
    counter!("otlp.ingest.requests", 1, "signal" => "traces");
    histogram!("otlp.ingest.bytes", body.len() as f64, "signal" => "traces");

    let parse_start = Instant::now();
    let grouped = decode_traces_partitioned(&body, format).map_err(|e| {
        AppError::bad_request(anyhow::anyhow!(
            "Failed to parse OTLP traces request: {}",
            e
        ))
    })?;
    debug!(
        elapsed_us = parse_start.elapsed().as_micros() as u64,
        signal = "traces",
        spans = grouped.total_records,
        "parse"
    );

    let mut uploaded_paths = Vec::new();
    let mut spans_processed: usize = 0;

    let write_start = Instant::now();
    for pb in grouped.batches {
        if pb.batch.num_rows() == 0 {
            continue;
        }

        spans_processed += pb.record_count;
        counter!(
            "otlp.ingest.records",
            pb.record_count as u64,
            "signal" => "traces"
        );

        let path = otlp2parquet_writer::write_batch(otlp2parquet_writer::WriteBatchRequest {
            batch: &pb.batch,
            signal_type: SignalType::Traces,
            metric_type: None,
            service_name: &pb.service_name,
            timestamp_micros: pb.min_timestamp_micros,
        })
        .await
        .map_err(|e| {
            AppError::internal(anyhow::anyhow!("Failed to write traces to storage: {}", e))
        })?;

        counter!("otlp.traces.flushes", 1);
        histogram!("otlp.batch.rows", pb.record_count as f64, "signal" => "traces");
        info!(
            "Committed traces batch path={} service={} spans={}",
            path, pb.service_name, pb.record_count
        );
        uploaded_paths.push(path);
    }
    debug!(
        elapsed_us = write_start.elapsed().as_micros() as u64,
        signal = "traces",
        "write"
    );

    if spans_processed == 0 {
        return Ok((
            StatusCode::OK,
            Json(json!({
                "status": "ok",
                "message": "No trace spans to process",
            })),
        )
            .into_response());
    }

    histogram!(
        "otlp.ingest.latency_ms",
        start.elapsed().as_secs_f64() * 1000.0,
        "signal" => "traces"
    );

    let response = Json(json!({
        "status": "ok",
        "spans_processed": spans_processed,
        "partitions": uploaded_paths,
    }));

    Ok((StatusCode::OK, response).into_response())
}

async fn process_metrics(
    _state: &AppState,
    format: InputFormat,
    body: axum::body::Bytes,
) -> Result<Response, AppError> {
    let start = Instant::now();
    counter!("otlp.ingest.requests", 1, "signal" => "metrics");
    histogram!("otlp.ingest.bytes", body.len() as f64, "signal" => "metrics");

    let parse_start = Instant::now();
    let partitioned = decode_metrics_partitioned(&body, format).map_err(|e| {
        AppError::bad_request(anyhow::anyhow!(
            "Failed to parse OTLP metrics request: {}",
            e
        ))
    })?;
    report_skipped_metrics(&partitioned.skipped);
    debug!(
        elapsed_us = parse_start.elapsed().as_micros() as u64,
        signal = "metrics",
        gauge_batches = partitioned.gauge.len(),
        sum_batches = partitioned.sum.len(),
        "parse"
    );

    let gauge_count = partitioned.gauge.total_records;
    let sum_count = partitioned.sum.total_records;

    let write_start = Instant::now();
    let mut uploaded_paths = Vec::new();

    uploaded_paths.extend(write_metric_batches(MetricType::Gauge, partitioned.gauge).await?);
    uploaded_paths.extend(write_metric_batches(MetricType::Sum, partitioned.sum).await?);

    debug!(
        elapsed_us = write_start.elapsed().as_micros() as u64,
        signal = "metrics",
        "write"
    );

    if uploaded_paths.is_empty() {
        return Ok((
            StatusCode::OK,
            Json(json!({
                "status": "ok",
                "message": "No metrics data points to process",
            })),
        )
            .into_response());
    }

    let total_data_points = gauge_count
        + sum_count
        + partitioned.skipped.histograms
        + partitioned.skipped.exponential_histograms
        + partitioned.skipped.summaries;

    counter!(
        "otlp.ingest.records",
        total_data_points as u64,
        "signal" => "metrics"
    );

    histogram!(
        "otlp.ingest.latency_ms",
        start.elapsed().as_secs_f64() * 1000.0,
        "signal" => "metrics"
    );

    let response = Json(json!({
        "status": "ok",
        "data_points_processed": gauge_count + sum_count,
        "gauge_count": gauge_count,
        "sum_count": sum_count,
        "histogram_count": partitioned.skipped.histograms,
        "exponential_histogram_count": partitioned.skipped.exponential_histograms,
        "summary_count": partitioned.skipped.summaries,
        "partitions": uploaded_paths,
    }));

    Ok((StatusCode::OK, response).into_response())
}

async fn write_metric_batches(
    metric_type: MetricType,
    grouped: ServiceGroupedBatches,
) -> Result<Vec<String>, AppError> {
    if grouped.is_empty() {
        return Ok(Vec::new());
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
            return Ok(Vec::new());
        }
    };

    let mut paths = Vec::new();

    for pb in grouped.batches {
        if pb.batch.num_rows() == 0 {
            continue;
        }

        let path = otlp2parquet_writer::write_batch(otlp2parquet_writer::WriteBatchRequest {
            batch: &pb.batch,
            signal_type: SignalType::Metrics,
            metric_type: Some(metric_type.as_str()),
            service_name: &pb.service_name,
            timestamp_micros: pb.min_timestamp_micros,
        })
        .await
        .map_err(|e| {
            AppError::internal(anyhow::anyhow!(
                "Failed to write {} metrics: {}",
                metric_type,
                e
            ))
        })?;

        counter!("otlp.metrics.flushes", 1, "metric_type" => metric_type.as_str());
        info!(
            "Committed metrics batch path={} metric_type={} service={} points={}",
            path, metric_type, pb.service_name, pb.record_count
        );
        paths.push(path);
    }

    Ok(paths)
}

/// Persist a completed batch from the BatchManager to storage.
/// Used by background flush and shutdown handlers.
pub(crate) async fn persist_log_batch(
    _state: &AppState,
    completed: &CompletedBatch,
) -> Result<Vec<String>, anyhow::Error> {
    let mut paths = Vec::new();

    for batch in &completed.batches {
        if batch.num_rows() == 0 {
            continue;
        }

        let path = otlp2parquet_writer::write_batch(otlp2parquet_writer::WriteBatchRequest {
            batch,
            signal_type: SignalType::Logs,
            metric_type: None,
            service_name: &completed.metadata.service_name,
            timestamp_micros: completed.metadata.first_timestamp_micros,
        })
        .await?;

        counter!("otlp.batch.flushes", 1);
        paths.push(path);
    }

    Ok(paths)
}
