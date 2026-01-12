// HTTP request handlers for server mode
//
// Implements OTLP ingestion and health check endpoints

use anyhow::Context;
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use metrics::{counter, histogram};
use otlp2parquet_batch::{CompletedBatch, LogMetadata};
use otlp2parquet_common::{
    count_skipped_metric_data_points, normalise_json_value, normalize_json_bytes, InputFormat,
    MetricType, SignalType,
};
use otlp2records::{
    apply_log_transform, apply_metric_transform, apply_trace_transform, decode_logs,
    decode_metrics, decode_traces, gauge_schema, sum_schema, traces_schema, DecodeMetricsResult,
    InputFormat as RecordsFormat, SkippedMetrics,
};
use serde_json::json;
use std::collections::HashMap;
use std::time::Instant;
use tracing::{debug, info};
use vrl::value::{KeyString, Value};

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

    let parse_start = Instant::now();
    let values = decode_logs_values(&body, format).map_err(AppError::bad_request)?;
    let transformed = apply_log_transform(values)
        .map_err(|e| AppError::bad_request(anyhow::anyhow!(e.to_string())))?;
    debug!(
        elapsed_us = parse_start.elapsed().as_micros() as u64,
        signal = "logs",
        "parse"
    );

    let per_service_values = group_values_by_service(transformed);
    let total_values: usize = per_service_values.iter().map(|(_, v)| v.len()).sum();
    let mut uploads: Vec<CompletedBatch<LogMetadata>> = Vec::new();
    let mut total_records: usize = 0;

    let convert_start = Instant::now();
    if let Some(batcher) = &state.batcher {
        let mut expired = batcher
            .drain_expired()
            .map_err(|e| AppError::internal(e.context("Failed to flush expired batches")))?;
        uploads.append(&mut expired);

        for (_, subset) in &per_service_values {
            let approx_bytes = estimate_subset_bytes(body.len(), subset.len(), total_values);
            let (mut ready, meta) = batcher
                .ingest(subset, approx_bytes)
                .map_err(|e| AppError::internal(e.context("Failed to enqueue batch")))?;
            total_records += meta.record_count;
            uploads.append(&mut ready);
        }
    } else {
        for (_, subset) in per_service_values {
            let batch = state
                .passthrough
                .ingest(&subset)
                .map_err(|e| AppError::bad_request(e.context("Failed to convert OTLP to Arrow")))?;
            total_records += batch.metadata.record_count;
            uploads.push(batch);
        }
    }
    debug!(
        elapsed_us = convert_start.elapsed().as_micros() as u64,
        signal = "logs",
        "convert"
    );

    counter!("otlp.ingest.records", total_records as u64);

    let mut uploaded_paths = Vec::new();
    let write_start = Instant::now();
    for completed in uploads {
        let partition_paths = persist_log_batch(state, &completed)
            .await
            .map_err(AppError::internal)?;

        counter!("otlp.batch.flushes", 1);
        histogram!("otlp.batch.rows", completed.metadata.record_count as f64);
        info!(
            "Committed batch paths={:?} service={} rows={}",
            partition_paths, completed.metadata.service_name, completed.metadata.record_count
        );
        uploaded_paths.extend(partition_paths);
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
    let values = decode_traces_values(&body, format).map_err(AppError::bad_request)?;
    let transformed = apply_trace_transform(values)
        .map_err(|e| AppError::bad_request(anyhow::anyhow!(e.to_string())))?;
    debug!(
        elapsed_us = parse_start.elapsed().as_micros() as u64,
        signal = "traces",
        "parse"
    );

    let per_service_values = group_values_by_service(transformed);
    let mut uploaded_paths = Vec::new();
    let mut spans_processed: usize = 0;

    let convert_start = Instant::now();
    let mut all_batches = Vec::new();
    for (service_name, subset) in per_service_values {
        let batch = otlp2records::values_to_arrow(&subset, &traces_schema()).map_err(|e| {
            AppError::bad_request(anyhow::anyhow!(
                "Failed to convert OTLP traces to Arrow: {}",
                e
            ))
        })?;

        if batch.num_rows() == 0 {
            continue;
        }

        spans_processed += subset.len();
        counter!(
            "otlp.ingest.records",
            subset.len() as u64,
            "signal" => "traces"
        );

        all_batches.push((service_name, batch, first_timestamp_micros(&subset)));
    }
    debug!(
        elapsed_us = convert_start.elapsed().as_micros() as u64,
        signal = "traces",
        "convert"
    );

    let write_start = Instant::now();
    for (service_name, batch, first_timestamp) in all_batches {
        let path = otlp2parquet_writer::write_batch(otlp2parquet_writer::WriteBatchRequest {
            batch: &batch,
            signal_type: SignalType::Traces,
            metric_type: None,
            service_name: &service_name,
            timestamp_micros: first_timestamp,
        })
        .await
        .map_err(|e| {
            AppError::internal(anyhow::anyhow!("Failed to write traces to storage: {}", e))
        })?;

        counter!("otlp.traces.flushes", 1);
        histogram!("otlp.batch.rows", batch.num_rows() as f64, "signal" => "traces");
        info!(
            "Committed traces batch path={} service={} spans={}",
            path,
            service_name,
            batch.num_rows()
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
    let decode_result = decode_metrics_values(&body, format).map_err(AppError::bad_request)?;
    report_skipped_metrics(&decode_result.skipped);

    let metric_values = apply_metric_transform(decode_result.values).map_err(|e| {
        AppError::bad_request(anyhow::anyhow!(
            "Failed to convert OTLP metrics to Arrow: {}",
            e
        ))
    })?;
    debug!(
        elapsed_us = parse_start.elapsed().as_micros() as u64,
        signal = "metrics",
        "parse"
    );

    let gauge_count = metric_values.gauge.len();
    let sum_count = metric_values.sum.len();

    let convert_start = Instant::now();
    let mut uploaded_paths = Vec::new();

    uploaded_paths.extend(write_metric_batches(MetricType::Gauge, &metric_values.gauge).await?);
    uploaded_paths.extend(write_metric_batches(MetricType::Sum, &metric_values.sum).await?);

    debug!(
        elapsed_us = convert_start.elapsed().as_micros() as u64,
        signal = "metrics",
        "convert"
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
        + decode_result.skipped.histograms
        + decode_result.skipped.exponential_histograms
        + decode_result.skipped.summaries;

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
        "histogram_count": decode_result.skipped.histograms,
        "exponential_histogram_count": decode_result.skipped.exponential_histograms,
        "summary_count": decode_result.skipped.summaries,
        "partitions": uploaded_paths,
    }));

    Ok((StatusCode::OK, response).into_response())
}

pub(crate) async fn persist_log_batch(
    _state: &AppState,
    completed: &CompletedBatch<LogMetadata>,
) -> anyhow::Result<Vec<String>> {
    let mut uploaded_paths = Vec::new();

    for batch in &completed.batches {
        let path = otlp2parquet_writer::write_batch(otlp2parquet_writer::WriteBatchRequest {
            batch,
            signal_type: SignalType::Logs,
            metric_type: None,
            service_name: &completed.metadata.service_name,
            timestamp_micros: completed.metadata.first_timestamp_micros,
        })
        .await
        .context("Failed to write logs to storage")?;

        uploaded_paths.push(path);
    }

    Ok(uploaded_paths)
}

fn decode_logs_values(body: &[u8], format: InputFormat) -> anyhow::Result<Vec<Value>> {
    match format {
        InputFormat::Jsonl => {
            decode_jsonl_values(body, |line| decode_logs(line, RecordsFormat::Json))
                .map_err(|e| anyhow::anyhow!("Failed to parse OTLP logs request: {}", e))
        }
        InputFormat::Json => {
            let normalized = normalize_json_bytes(body)?;
            decode_logs(&normalized, RecordsFormat::Json)
                .map_err(|e| anyhow::anyhow!("Failed to parse OTLP logs request: {}", e))
        }
        InputFormat::Protobuf => decode_logs(body, to_records_format(format))
            .map_err(|e| anyhow::anyhow!("Failed to parse OTLP logs request: {}", e)),
    }
}

fn decode_traces_values(body: &[u8], format: InputFormat) -> anyhow::Result<Vec<Value>> {
    match format {
        InputFormat::Jsonl => {
            decode_jsonl_values(body, |line| decode_traces(line, RecordsFormat::Json))
                .map_err(|e| anyhow::anyhow!("Failed to parse OTLP traces request: {}", e))
        }
        InputFormat::Json => {
            let normalized = normalize_json_bytes(body)?;
            decode_traces(&normalized, RecordsFormat::Json)
                .map_err(|e| anyhow::anyhow!("Failed to parse OTLP traces request: {}", e))
        }
        InputFormat::Protobuf => decode_traces(body, to_records_format(format))
            .map_err(|e| anyhow::anyhow!("Failed to parse OTLP traces request: {}", e)),
    }
}

fn decode_metrics_values(body: &[u8], format: InputFormat) -> anyhow::Result<DecodeMetricsResult> {
    match format {
        InputFormat::Jsonl => decode_jsonl_metrics(body),
        InputFormat::Json => decode_metrics_json(body),
        InputFormat::Protobuf => decode_metrics(body, to_records_format(format))
            .map_err(|e| anyhow::anyhow!("Failed to parse OTLP metrics request: {}", e)),
    }
}

fn decode_jsonl_values<F>(body: &[u8], mut decode: F) -> Result<Vec<Value>, String>
where
    F: FnMut(&[u8]) -> Result<Vec<Value>, otlp2records::decode::DecodeError>,
{
    let text = std::str::from_utf8(body).map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    let mut saw_line = false;

    for (line_num, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        saw_line = true;
        let normalized = normalize_json_bytes(trimmed.as_bytes()).map_err(|e| e.to_string())?;
        let values = decode(&normalized).map_err(|e| format!("line {}: {}", line_num + 1, e))?;
        out.extend(values);
    }

    if !saw_line {
        return Err("jsonl payload contained no records".to_string());
    }

    Ok(out)
}

fn decode_jsonl_metrics(body: &[u8]) -> anyhow::Result<DecodeMetricsResult> {
    let text = std::str::from_utf8(body)
        .map_err(|e| anyhow::anyhow!("Failed to parse OTLP metrics request: {}", e))?;
    let mut values = Vec::new();
    let mut skipped = SkippedMetrics::default();
    let mut saw_line = false;

    for (line_num, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        saw_line = true;
        let mut value: serde_json::Value =
            serde_json::from_slice(trimmed.as_bytes()).map_err(|e| {
                anyhow::anyhow!(
                    "Failed to parse OTLP metrics request (line {}): {}",
                    line_num + 1,
                    e
                )
            })?;
        let counts = count_skipped_metric_data_points(&value);
        normalise_json_value(&mut value, None).map_err(|e| {
            anyhow::anyhow!(
                "Failed to parse OTLP metrics request (line {}): {}",
                line_num + 1,
                e
            )
        })?;
        let normalized = serde_json::to_vec(&value).map_err(|e| {
            anyhow::anyhow!(
                "Failed to parse OTLP metrics request (line {}): {}",
                line_num + 1,
                e
            )
        })?;
        let mut result = decode_metrics(&normalized, RecordsFormat::Json).map_err(|e| {
            anyhow::anyhow!(
                "Failed to parse OTLP metrics request (line {}): {}",
                line_num + 1,
                e
            )
        })?;
        result.skipped.histograms += counts.histograms;
        result.skipped.exponential_histograms += counts.exponential_histograms;
        result.skipped.summaries += counts.summaries;
        values.extend(result.values);
        merge_skipped(&mut skipped, &result.skipped);
    }

    if !saw_line {
        return Err(anyhow::anyhow!(
            "Failed to parse OTLP metrics request: jsonl payload contained no records"
        ));
    }

    Ok(DecodeMetricsResult { values, skipped })
}

fn decode_metrics_json(body: &[u8]) -> anyhow::Result<DecodeMetricsResult> {
    let mut value: serde_json::Value = serde_json::from_slice(body)
        .map_err(|e| anyhow::anyhow!("Failed to parse OTLP metrics request: {}", e))?;
    let counts = count_skipped_metric_data_points(&value);
    normalise_json_value(&mut value, None)
        .map_err(|e| anyhow::anyhow!("Failed to parse OTLP metrics request: {}", e))?;
    let normalized = serde_json::to_vec(&value)
        .map_err(|e| anyhow::anyhow!("Failed to parse OTLP metrics request: {}", e))?;
    let mut result = decode_metrics(&normalized, RecordsFormat::Json)
        .map_err(|e| anyhow::anyhow!("Failed to parse OTLP metrics request: {}", e))?;
    result.skipped.histograms += counts.histograms;
    result.skipped.exponential_histograms += counts.exponential_histograms;
    result.skipped.summaries += counts.summaries;
    Ok(result)
}

async fn write_metric_batches(
    metric_type: MetricType,
    values: &[Value],
) -> Result<Vec<String>, AppError> {
    if values.is_empty() {
        return Ok(Vec::new());
    }

    let schema = match metric_type {
        MetricType::Gauge => gauge_schema(),
        MetricType::Sum => sum_schema(),
        _ => return Ok(Vec::new()),
    };

    let mut paths = Vec::new();

    for (service_name, subset) in group_values_by_service(values.to_vec()) {
        let batch = otlp2records::values_to_arrow(&subset, &schema).map_err(|e| {
            AppError::bad_request(anyhow::anyhow!(
                "Failed to convert OTLP metrics to Arrow: {}",
                e
            ))
        })?;

        if batch.num_rows() == 0 {
            continue;
        }

        let path = otlp2parquet_writer::write_batch(otlp2parquet_writer::WriteBatchRequest {
            batch: &batch,
            signal_type: SignalType::Metrics,
            metric_type: Some(metric_type.as_str()),
            service_name: &service_name,
            timestamp_micros: first_timestamp_micros(&subset),
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
            "Committed metrics batch path={} metric_type={} service={}",
            path, metric_type, service_name
        );
        paths.push(path);
    }

    Ok(paths)
}

fn merge_skipped(target: &mut SkippedMetrics, other: &SkippedMetrics) {
    target.histograms += other.histograms;
    target.exponential_histograms += other.exponential_histograms;
    target.summaries += other.summaries;
    target.nan_values += other.nan_values;
    target.infinity_values += other.infinity_values;
    target.missing_values += other.missing_values;
}

fn report_skipped_metrics(skipped: &SkippedMetrics) {
    if skipped.has_skipped() {
        tracing::debug!(
            histograms = skipped.histograms,
            exponential_histograms = skipped.exponential_histograms,
            summaries = skipped.summaries,
            nan_values = skipped.nan_values,
            infinity_values = skipped.infinity_values,
            missing_values = skipped.missing_values,
            total = skipped.total(),
            "Skipped unsupported or invalid metric data points"
        );
    }
}

fn to_records_format(format: InputFormat) -> RecordsFormat {
    match format {
        InputFormat::Protobuf => RecordsFormat::Protobuf,
        InputFormat::Json => RecordsFormat::Json,
        InputFormat::Jsonl => RecordsFormat::Json,
    }
}

fn group_values_by_service(values: Vec<Value>) -> Vec<(String, Vec<Value>)> {
    let mut order = Vec::new();
    let mut index: HashMap<String, usize> = HashMap::new();
    let mut groups: Vec<Vec<Value>> = Vec::new();

    for value in values {
        let service = extract_service_name(&value);
        let idx = if let Some(idx) = index.get(&service) {
            *idx
        } else {
            let idx = groups.len();
            index.insert(service.clone(), idx);
            order.push(service);
            groups.push(Vec::new());
            idx
        };
        groups[idx].push(value);
    }

    order.into_iter().zip(groups).collect()
}

fn extract_service_name(value: &Value) -> String {
    let key: KeyString = "service_name".into();
    if let Value::Object(map) = value {
        if let Some(Value::Bytes(bytes)) = map.get(&key) {
            if !bytes.is_empty() {
                return String::from_utf8_lossy(bytes).into_owned();
            }
        }
    }
    "unknown".to_string()
}

fn first_timestamp_micros(values: &[Value]) -> i64 {
    let key: KeyString = "timestamp".into();
    let mut min: Option<i64> = None;

    for value in values {
        if let Value::Object(map) = value {
            if let Some(ts) = map.get(&key) {
                if let Some(millis) = value_to_i64(ts) {
                    let micros = millis.saturating_mul(1_000);
                    min = Some(min.map_or(micros, |current| current.min(micros)));
                }
            }
        }
    }

    min.unwrap_or(0)
}

fn value_to_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Integer(i) => Some(*i),
        Value::Float(f) => Some(f.into_inner() as i64),
        _ => None,
    }
}

fn estimate_subset_bytes(total_bytes: usize, subset_len: usize, total_len: usize) -> usize {
    if total_len == 0 {
        return 0;
    }

    let ratio = subset_len as f64 / total_len as f64;
    (total_bytes as f64 * ratio).round() as usize
}
