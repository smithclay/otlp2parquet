//! Batched signal handlers for Durable Object routing.
//!
//! These handlers parse OTLP data, convert to Arrow, and route to Durable Objects
//! for batching before writing to R2.

use crate::{errors, make_do_id, MetricType, SignalKey};
use arrow::record_batch::RecordBatch;
use otlp2parquet_core::batch::ipc::serialize_batch;
use otlp2parquet_core::InputFormat;
use serde::Deserialize;
use uuid::Uuid;
use worker::{Env, Headers, Method, Request, RequestInit, Response, Result};

/// Response from Durable Object ingest endpoint.
/// Must match `IngestResponse` in batcher.rs.
#[derive(Debug, Deserialize)]
struct DoIngestResponse {
    #[allow(dead_code)]
    status: String,
    buffered_records: i64,
    buffered_bytes: i64,
}

/// Context for batched request handling.
pub struct BatchContext<'a> {
    pub env: &'a Env,
    pub request_id: &'a str,
}

/// Convert an error message to a structured InvalidRequest error response.
fn invalid_request_response(message: String, request_id: &str) -> Response {
    let error = errors::OtlpErrorKind::InvalidRequest(message);
    let status_code = error.status_code();
    errors::ErrorResponse::from_error(error, Some(request_id.to_string()))
        .into_response(status_code)
        .unwrap_or_else(|_| Response::error("Invalid request", 400).unwrap())
}

/// Handle batched logs - parse, convert to Arrow, route to DO.
pub async fn handle_batched_logs(
    ctx: &BatchContext<'_>,
    body: &[u8],
    format: InputFormat,
) -> Result<Response> {
    let (batch, metadata) = match otlp2parquet_core::parse_otlp_to_arrow(body, format) {
        Ok(result) => result,
        Err(e) => {
            tracing::error!(
                request_id = %ctx.request_id,
                error = ?e,
                "Failed to parse logs for batching"
            );
            return Ok(invalid_request_response(
                format!("Failed to parse logs: {}", e),
                ctx.request_id,
            ));
        }
    };

    tracing::debug!(
        request_id = %ctx.request_id,
        service = %metadata.service_name,
        records = metadata.record_count,
        "Batching logs"
    );

    route_to_batcher(
        ctx.env,
        SignalKey::Logs,
        &metadata.service_name,
        &[batch],
        metadata.first_timestamp_micros,
    )
    .await
}

/// Handle batched traces - parse, convert to Arrow, route to DO.
pub async fn handle_batched_traces(
    ctx: &BatchContext<'_>,
    body: &[u8],
    format: InputFormat,
) -> Result<Response> {
    use otlp2parquet_core::otlp::traces;

    let request = match traces::parse_otlp_trace_request(body, format) {
        Ok(result) => result,
        Err(e) => {
            tracing::error!(
                request_id = %ctx.request_id,
                error = ?e,
                "Failed to parse traces for batching"
            );
            return Ok(invalid_request_response(
                format!("Failed to parse traces: {}", e),
                ctx.request_id,
            ));
        }
    };

    let (batches, metadata) = match traces::TraceArrowConverter::convert(&request) {
        Ok(result) => result,
        Err(e) => {
            tracing::error!(
                request_id = %ctx.request_id,
                error = ?e,
                "Failed to convert traces to Arrow"
            );
            return Ok(invalid_request_response(
                format!("Failed to convert traces: {}", e),
                ctx.request_id,
            ));
        }
    };

    tracing::debug!(
        request_id = %ctx.request_id,
        service = %metadata.service_name,
        spans = metadata.span_count,
        "Batching traces"
    );

    route_to_batcher(
        ctx.env,
        SignalKey::Traces,
        &metadata.service_name,
        &batches,
        metadata.first_timestamp_micros,
    )
    .await
}

/// Handle batched metrics - parse, convert to Arrow, route to DOs in parallel.
pub async fn handle_batched_metrics(
    ctx: &BatchContext<'_>,
    body: &[u8],
    format: InputFormat,
) -> Result<Response> {
    use futures::future::try_join_all;
    use otlp2parquet_core::otlp::metrics;

    let request = match metrics::parse_otlp_request(body, format) {
        Ok(result) => result,
        Err(e) => {
            tracing::error!(
                request_id = %ctx.request_id,
                error = ?e,
                "Failed to parse metrics for batching"
            );
            return Ok(invalid_request_response(
                format!("Failed to parse metrics: {}", e),
                ctx.request_id,
            ));
        }
    };

    let converter = metrics::ArrowConverter::new();
    let (batches, metadata) = match converter.convert(request) {
        Ok(result) => result,
        Err(e) => {
            tracing::error!(
                request_id = %ctx.request_id,
                error = ?e,
                "Failed to convert metrics to Arrow"
            );
            return Ok(invalid_request_response(
                format!("Failed to convert metrics: {}", e),
                ctx.request_id,
            ));
        }
    };

    tracing::debug!(
        request_id = %ctx.request_id,
        service = %metadata.service_name(),
        gauge = metadata.gauge_count,
        sum = metadata.sum_count,
        histogram = metadata.histogram_count,
        exp_histogram = metadata.exponential_histogram_count,
        summary = metadata.summary_count,
        "Batching metrics"
    );

    if batches.is_empty() {
        return Response::from_json(&serde_json::json!({
            "status": "accepted",
            "mode": "batched",
            "buffered_records": 0,
            "buffered_bytes": 0,
        }));
    }

    // Route each metric type to its own DO in parallel
    let service_name = metadata.service_name();
    let futures: Vec<_> = batches
        .into_iter()
        .filter_map(|(metric_type_str, batch)| {
            let metric_type = parse_metric_type(&metric_type_str, ctx.request_id)?;
            let signal_key = SignalKey::Metrics(metric_type);
            let first_timestamp = metadata
                .first_timestamp_for(&metric_type_str)
                .unwrap_or_default();

            Some(route_single_metric(
                ctx.env,
                ctx.request_id,
                signal_key,
                service_name,
                batch,
                first_timestamp,
            ))
        })
        .collect();

    let results = try_join_all(futures).await?;
    aggregate_do_responses(results, ctx.request_id).await
}

/// Parse metric type string to enum, logging warning on unknown types.
fn parse_metric_type(metric_type_str: &str, request_id: &str) -> Option<MetricType> {
    match metric_type_str {
        "gauge" => Some(MetricType::Gauge),
        "sum" => Some(MetricType::Sum),
        "histogram" => Some(MetricType::Histogram),
        "exponential_histogram" => Some(MetricType::ExponentialHistogram),
        "summary" => Some(MetricType::Summary),
        unknown => {
            tracing::warn!(
                request_id = %request_id,
                metric_type = %unknown,
                "Unknown metric type, skipping"
            );
            None
        }
    }
}

/// Route a single metric batch to its DO.
async fn route_single_metric(
    env: &Env,
    request_id: &str,
    signal_key: SignalKey,
    service_name: &str,
    batch: RecordBatch,
    first_timestamp: i64,
) -> Result<Response> {
    tracing::debug!(
        request_id = %request_id,
        signal_key = %signal_key,
        records = batch.num_rows(),
        "Routing batch to DO"
    );
    route_to_batcher(env, signal_key, service_name, &[batch], first_timestamp).await
}

/// Aggregate buffered stats from multiple DO responses.
async fn aggregate_do_responses(results: Vec<Response>, request_id: &str) -> Result<Response> {
    let num_routed = results.len();
    let mut total_records: i64 = 0;
    let mut total_bytes: i64 = 0;

    for mut response in results {
        let status = response.status_code();
        if status != 200 {
            let body = response.text().await.unwrap_or_default();
            tracing::error!(
                request_id = %request_id,
                status,
                body_preview = %body.chars().take(200).collect::<String>(),
                "DO returned non-success status"
            );
            return Response::error(format!("Durable Object returned status {}", status), status);
        }

        match response.json::<DoIngestResponse>().await {
            Ok(resp) => {
                total_records = total_records.saturating_add(resp.buffered_records);
                total_bytes = total_bytes.saturating_add(resp.buffered_bytes);
            }
            Err(e) => {
                tracing::error!(request_id = %request_id, error = %e, "Failed to parse DO response");
                return Response::error("Durable Object returned invalid response", 502);
            }
        }
    }

    Response::from_json(&serde_json::json!({
        "status": "accepted",
        "mode": "batched",
        "buffered_records": total_records,
        "buffered_bytes": total_bytes,
        "metric_types_routed": num_routed,
    }))
}

/// Route Arrow batches to a Durable Object for batching.
async fn route_to_batcher(
    env: &Env,
    signal_key: SignalKey,
    service_name: &str,
    batches: &[RecordBatch],
    first_timestamp_micros: i64,
) -> Result<Response> {
    let namespace = env.durable_object("BATCHER")?;
    let do_id_name = make_do_id(&signal_key, service_name);
    let id = namespace.id_from_name(&do_id_name)?;
    let stub = id.get_stub()?;

    let mut latest_records: i64 = 0;
    let mut latest_bytes: i64 = 0;
    let request_id = Uuid::new_v4().to_string();

    for (batch_index, batch) in batches.iter().enumerate() {
        let ipc_bytes = serialize_batch(batch)
            .map_err(|e| worker::Error::RustError(format!("IPC serialize failed: {}", e)))?;

        let headers = Headers::new();
        headers.set("Content-Type", "application/vnd.apache.arrow.stream")?;
        headers.set("X-Record-Count", &(batch.num_rows() as i64).to_string())?;
        headers.set("X-Request-Id", &request_id)?;
        headers.set("X-Batch-Index", &batch_index.to_string())?;
        headers.set(
            "X-First-Timestamp-Micros",
            &first_timestamp_micros.to_string(),
        )?;

        let mut init = RequestInit::new();
        init.with_method(Method::Post);
        init.with_headers(headers);
        init.with_body(Some(ipc_bytes.into()));

        let request =
            Request::new_with_init(&format!("http://do/ingest?name={}", do_id_name), &init)?;
        let mut response = stub.fetch_with_request(request).await?;

        if response.status_code() == 200 {
            if let Ok(body) = response.json::<serde_json::Value>().await {
                if let Some(records) = body.get("buffered_records").and_then(|v| v.as_i64()) {
                    latest_records = records;
                }
                if let Some(bytes) = body.get("buffered_bytes").and_then(|v| v.as_i64()) {
                    latest_bytes = bytes;
                }
            }
        }
    }

    Response::from_json(&serde_json::json!({
        "status": "accepted",
        "mode": "batched",
        "buffered_records": latest_records,
        "buffered_bytes": latest_bytes,
    }))
}
