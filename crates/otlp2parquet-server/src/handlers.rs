// HTTP request handlers for server mode
//
// Implements OTLP ingestion and health check endpoints

use anyhow::Context;
use arrow::array::{Array, RecordBatch, TimestampNanosecondArray};
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use metrics::{counter, histogram};
use otlp2parquet_batch::CompletedBatch;
use otlp2parquet_core::{otlp, InputFormat, ParquetWriteResult};
use prost::Message;
use serde_json::json;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info};

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

/// GET /ready - Readiness check
pub(crate) async fn ready_check(State(_state): State<AppState>) -> impl IntoResponse {
    // Writer is always ready after initialization
    // TODO: Add actual health checks if needed (e.g., test write to storage)
    (StatusCode::OK, Json(json!({"status": "ready"})))
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
        SignalKind::Traces => process_traces(state, format, body).await,
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
        .map_err(|e| AppError::bad_request(e.context("Failed to parse OTLP request payload")))?;

    let per_service_requests = otlp::logs::split_request_by_service(request);
    let mut uploads: Vec<CompletedBatch<otlp::LogMetadata>> = Vec::new();
    let mut total_records: usize = 0;

    if let Some(batcher) = &state.batcher {
        let mut expired = batcher
            .drain_expired()
            .map_err(|e| AppError::internal(e.context("Failed to flush expired batches")))?;
        uploads.append(&mut expired);

        for subset in &per_service_requests {
            let approx_bytes = subset.encoded_len();
            let (mut ready, meta) = batcher
                .ingest(subset, approx_bytes)
                .map_err(|e| AppError::internal(e.context("Failed to enqueue batch")))?;
            total_records += meta.record_count;
            uploads.append(&mut ready);
        }
    } else {
        for subset in per_service_requests {
            let batch = state
                .passthrough
                .ingest(&subset)
                .map_err(|e| AppError::bad_request(e.context("Failed to convert OTLP to Arrow")))?;
            total_records += batch.metadata.record_count;
            uploads.push(batch);
        }
    }

    counter!("otlp.ingest.records", total_records as u64);

    let mut uploaded_paths = Vec::new();
    for completed in uploads {
        let write_result = persist_log_batch(state, &completed)
            .await
            .map_err(AppError::internal)?;
        let partition_path = write_result.path.clone();

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
        "records_processed": total_records,
        "flush_count": uploaded_paths.len(),
        "partitions": uploaded_paths,
    }));

    Ok((StatusCode::OK, response).into_response())
}

async fn process_traces(
    state: &AppState,
    format: InputFormat,
    body: axum::body::Bytes,
) -> Result<Response, AppError> {
    let start = Instant::now();
    counter!("otlp.ingest.requests", 1, "signal" => "traces");
    histogram!("otlp.ingest.bytes", body.len() as f64, "signal" => "traces");

    // Parse OTLP traces request
    let request = otlp::traces::parse_otlp_trace_request(&body, format).map_err(|e| {
        AppError::bad_request(e.context("Failed to parse OTLP traces request payload"))
    })?;

    let per_service_requests = otlp::traces::split_request_by_service(request);
    let mut uploaded_paths = Vec::new();
    let mut spans_processed: usize = 0;

    for subset in per_service_requests {
        let (batches, metadata) =
            otlp::traces::TraceArrowConverter::convert(&subset).map_err(|e| {
                AppError::bad_request(e.context("Failed to convert OTLP traces to Arrow"))
            })?;

        if batches.is_empty() || metadata.span_count == 0 {
            continue;
        }

        spans_processed += metadata.span_count;
        counter!(
            "otlp.ingest.records",
            metadata.span_count as u64,
            "signal" => "traces"
        );

        let service_name = metadata.service_name.as_ref();

        // Write traces via write_batch function
        let mut partition_path = String::new();
        for batch in &batches {
            let path = otlp2parquet_writer::write_batch(otlp2parquet_writer::WriteBatchRequest {
                catalog: state.catalog.as_ref().map(|c| c.as_ref()),
                namespace: &state.namespace,
                batch,
                signal_type: otlp2parquet_core::SignalType::Traces,
                metric_type: None,
                service_name,
                timestamp_micros: metadata.first_timestamp_nanos,
                snapshot_timestamp_ms: None,
                retry_policy: otlp2parquet_writer::RetryPolicy::default(),
            })
            .await
            .map_err(|e| {
                AppError::internal(anyhow::anyhow!("Failed to write traces to storage: {}", e))
            })?;

            partition_path = path; // Track last path for logging
        }

        counter!("otlp.traces.flushes", 1);
        histogram!(
            "otlp.batch.rows",
            metadata.span_count as f64,
            "signal" => "traces"
        );
        info!(
            "Committed traces batch path={} service={} spans={}",
            partition_path, service_name, metadata.span_count
        );
        uploaded_paths.push(partition_path);
    }

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
    state: &AppState,
    format: InputFormat,
    body: axum::body::Bytes,
) -> Result<Response, AppError> {
    let start = Instant::now();
    counter!("otlp.ingest.requests", 1, "signal" => "metrics");
    histogram!("otlp.ingest.bytes", body.len() as f64, "signal" => "metrics");

    // Parse OTLP metrics request
    let request = otlp::metrics::parse_otlp_request(&body, format).map_err(|e| {
        AppError::bad_request(e.context("Failed to parse OTLP metrics request payload"))
    })?;

    let per_service_requests = otlp::metrics::split_request_by_service(request);
    let converter = otlp::metrics::ArrowConverter::new();
    let mut aggregated = otlp::metrics::MetricsMetadata::default();
    let mut uploaded_paths = Vec::new();

    for subset in per_service_requests {
        let (batches_by_type, subset_metadata) = converter.convert(subset).map_err(|e| {
            AppError::bad_request(e.context("Failed to convert OTLP metrics to Arrow"))
        })?;

        aggregated.resource_metrics_count += subset_metadata.resource_metrics_count;
        aggregated.scope_metrics_count += subset_metadata.scope_metrics_count;
        aggregated.gauge_count += subset_metadata.gauge_count;
        aggregated.sum_count += subset_metadata.sum_count;
        aggregated.histogram_count += subset_metadata.histogram_count;
        aggregated.exponential_histogram_count += subset_metadata.exponential_histogram_count;
        aggregated.summary_count += subset_metadata.summary_count;

        let service_name = subset_metadata.service_name().to_string();

        for (metric_type, batch) in batches_by_type {
            let timestamp_nanos = subset_metadata
                .first_timestamp_for(&metric_type)
                .unwrap_or_else(|| extract_first_timestamp(&batch));

            let partition_path =
                otlp2parquet_writer::write_batch(otlp2parquet_writer::WriteBatchRequest {
                    catalog: state.catalog.as_ref().map(|c| c.as_ref()),
                    namespace: &state.namespace,
                    batch: &batch,
                    signal_type: otlp2parquet_core::SignalType::Metrics,
                    metric_type: Some(&metric_type),
                    service_name: &service_name,
                    timestamp_micros: timestamp_nanos,
                    snapshot_timestamp_ms: None,
                    retry_policy: otlp2parquet_writer::RetryPolicy::default(),
                })
                .await
                .map_err(|e| {
                    AppError::internal(anyhow::anyhow!(
                        "Failed to write {} metrics: {}",
                        metric_type,
                        e
                    ))
                })?;

            counter!("otlp.metrics.flushes", 1, "metric_type" => metric_type.clone());
            info!(
                "Committed metrics batch path={} metric_type={} service={}",
                partition_path, metric_type, service_name
            );
            uploaded_paths.push(partition_path);
        }
    }

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

    let total_data_points = aggregated.gauge_count
        + aggregated.sum_count
        + aggregated.histogram_count
        + aggregated.exponential_histogram_count
        + aggregated.summary_count;

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
        "data_points_processed": total_data_points,
        "gauge_count": aggregated.gauge_count,
        "sum_count": aggregated.sum_count,
        "histogram_count": aggregated.histogram_count,
        "exponential_histogram_count": aggregated.exponential_histogram_count,
        "summary_count": aggregated.summary_count,
        "partitions": uploaded_paths,
    }));

    Ok((StatusCode::OK, response).into_response())
}

pub(crate) async fn persist_log_batch(
    state: &AppState,
    completed: &CompletedBatch<otlp::LogMetadata>,
) -> anyhow::Result<ParquetWriteResult> {
    let mut uploaded_paths = Vec::new();

    for batch in &completed.batches {
        let path = otlp2parquet_writer::write_batch(otlp2parquet_writer::WriteBatchRequest {
            catalog: state.catalog.as_ref().map(|c| c.as_ref()),
            namespace: &state.namespace,
            batch,
            signal_type: otlp2parquet_core::SignalType::Logs,
            metric_type: None,
            service_name: &completed.metadata.service_name,
            timestamp_micros: completed.metadata.first_timestamp_nanos,
            snapshot_timestamp_ms: None,
            retry_policy: otlp2parquet_writer::RetryPolicy::default(),
        })
        .await
        .context("Failed to write logs to storage")?;

        uploaded_paths.push(path);
    }

    // Return minimal ParquetWriteResult for backward compatibility
    let schema = completed
        .batches
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| Arc::new(arrow::datatypes::Schema::empty()));

    // Create minimal parquet metadata (required fields only)
    use parquet::schema::types::Type;
    let parquet_schema = Type::group_type_builder("schema")
        .build()
        .context("Failed to build parquet schema")?;
    let schema_descriptor = Arc::new(parquet::schema::types::SchemaDescriptor::new(Arc::new(
        parquet_schema,
    )));

    let file_metadata = parquet::file::metadata::FileMetaData::new(
        0,    // version
        0,    // num rows
        None, // created_by
        None, // key_value_metadata
        schema_descriptor,
        None, // column_orders
    );

    let parquet_metadata = Arc::new(parquet::file::metadata::ParquetMetaData::new(
        file_metadata,
        vec![], // row groups
    ));

    Ok(ParquetWriteResult {
        path: uploaded_paths.into_iter().next().unwrap_or_default(),
        hash: otlp2parquet_core::Blake3Hash::new([0u8; 32]),
        file_size: 0,
        row_count: completed.metadata.record_count as i64,
        arrow_schema: schema,
        parquet_metadata,
        completed_at: chrono::Utc::now(),
    })
}

fn extract_first_timestamp(batch: &RecordBatch) -> i64 {
    // Support both nanos and micros; always return microsecond precision.
    let mut min_micros = i64::MAX;

    if let Some(array) = batch
        .column(0)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
    {
        for idx in 0..array.len() {
            if array.is_valid(idx) {
                min_micros = min_micros.min(array.value(idx) / 1_000);
            }
        }
    }

    if min_micros == i64::MAX {
        if let Some(array) = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
        {
            for idx in 0..array.len() {
                if array.is_valid(idx) {
                    min_micros = min_micros.min(array.value(idx));
                }
            }
        }
    }

    if min_micros == i64::MAX {
        0
    } else {
        min_micros
    }
}
