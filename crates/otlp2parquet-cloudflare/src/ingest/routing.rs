//! Durable Object routing logic for batched signals.

use crate::{make_do_id, SignalKey, TraceContext};
use arrow::record_batch::RecordBatch;
use otlp2parquet_core::batch::ipc::serialize_batch;
use serde::Deserialize;
use worker::{Env, Headers, Method, Request, RequestInit, Response, Result};

/// Response from Durable Object ingest endpoint.
/// Must match `IngestResponse` in batcher.rs.
#[derive(Debug, Deserialize)]
pub(crate) struct DoIngestResponse {
    #[allow(dead_code)]
    pub status: String,
    pub buffered_records: i64,
    pub buffered_bytes: i64,
}

/// Route Arrow batches to a Durable Object for batching.
#[tracing::instrument(
    name = "otlp.route_to_do",
    skip(env, trace_ctx, batches),
    fields(
        signal_key = %signal_key,
        service_name = %service_name,
        batch_count = batches.len(),
        error = tracing::field::Empty,
    )
)]
pub async fn route_to_batcher(
    env: &Env,
    trace_ctx: &TraceContext,
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

    for (batch_index, batch) in batches.iter().enumerate() {
        let ipc_bytes = serialize_batch(batch)
            .map_err(|e| worker::Error::RustError(format!("IPC serialize failed: {}", e)))?;

        let headers = Headers::new();
        headers.set("Content-Type", "application/vnd.apache.arrow.stream")?;
        headers.set("X-Record-Count", &(batch.num_rows() as i64).to_string())?;
        headers.set("X-Request-Id", &trace_ctx.request_id)?;
        headers.set("X-Batch-Index", &batch_index.to_string())?;
        headers.set(
            "X-First-Timestamp-Micros",
            &first_timestamp_micros.to_string(),
        )?;
        // Propagate trace context headers to Durable Object
        headers.set("traceparent", &trace_ctx.traceparent())?;

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

/// Route a single metric batch to its DO.
pub(super) async fn route_single_metric(
    env: &Env,
    trace_ctx: &TraceContext,
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
    route_to_batcher(
        env,
        trace_ctx,
        signal_key,
        service_name,
        &[batch],
        first_timestamp,
    )
    .await
}

/// Aggregate buffered stats from multiple DO responses.
pub(super) async fn aggregate_do_responses(
    results: Vec<Response>,
    request_id: &str,
) -> Result<Response> {
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
