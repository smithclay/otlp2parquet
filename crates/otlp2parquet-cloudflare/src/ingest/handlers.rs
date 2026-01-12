//! Signal-specific handlers for OTLP batched ingestion.

use super::routing::{aggregate_do_responses, route_single_metric, route_to_batcher};
use super::{invalid_request_response, BatchContext};
use crate::{MetricType, SignalKey};
use futures::future::try_join_all;
use otlp2parquet_common::InputFormat;
use otlp2parquet_handlers::{
    decode_logs_partitioned, decode_metrics_partitioned, decode_traces_partitioned,
    report_skipped_metrics, ServiceGroupedBatches,
};
use worker::{Response, Result};

/// Handle batched logs - parse, convert to Arrow, route to DO.
#[tracing::instrument(
    name = "otlp.parse",
    skip(ctx, body),
    fields(
        request_id = %ctx.request_id,
        signal_type = "logs",
        service_name = tracing::field::Empty,
        record_count = tracing::field::Empty,
        bytes = body.len(),
        error = tracing::field::Empty,
    )
)]
pub async fn handle_batched_logs(
    ctx: &BatchContext<'_>,
    body: &[u8],
    format: InputFormat,
) -> Result<Response> {
    let grouped = match decode_logs_partitioned(body, format) {
        Ok(grouped) => grouped,
        Err(message) => {
            tracing::Span::current().record("error", message.as_str());
            tracing::error!(
                request_id = %ctx.request_id,
                error = %message,
                "Failed to parse logs for batching"
            );
            return Ok(invalid_request_response(
                format!("Failed to parse logs: {}", message),
                ctx.request_id,
            ));
        }
    };

    let mut responses = Vec::new();

    for pb in grouped.batches {
        if pb.batch.num_rows() == 0 {
            continue;
        }

        tracing::Span::current().record("service_name", pb.service_name.as_ref());
        tracing::Span::current().record("record_count", pb.record_count);

        tracing::debug!(
            request_id = %ctx.request_id,
            service = %pb.service_name,
            records = pb.record_count,
            "Batching logs"
        );

        let response = route_to_batcher(
            ctx.env,
            ctx.trace_ctx,
            SignalKey::Logs,
            &pb.service_name,
            &[pb.batch],
            pb.min_timestamp_micros,
        )
        .await?;

        responses.push(response);
    }

    aggregate_do_responses(responses, ctx.request_id).await
}

/// Handle batched traces - parse, convert to Arrow, route to DO.
#[tracing::instrument(
    name = "otlp.parse",
    skip(ctx, body),
    fields(
        request_id = %ctx.request_id,
        signal_type = "traces",
        service_name = tracing::field::Empty,
        record_count = tracing::field::Empty,
        bytes = body.len(),
        error = tracing::field::Empty,
    )
)]
pub async fn handle_batched_traces(
    ctx: &BatchContext<'_>,
    body: &[u8],
    format: InputFormat,
) -> Result<Response> {
    let grouped = match decode_traces_partitioned(body, format) {
        Ok(grouped) => grouped,
        Err(message) => {
            tracing::Span::current().record("error", message.as_str());
            tracing::error!(
                request_id = %ctx.request_id,
                error = %message,
                "Failed to parse traces for batching"
            );
            return Ok(invalid_request_response(
                format!("Failed to parse traces: {}", message),
                ctx.request_id,
            ));
        }
    };

    let mut responses = Vec::new();

    for pb in grouped.batches {
        if pb.batch.num_rows() == 0 {
            continue;
        }

        tracing::Span::current().record("service_name", pb.service_name.as_ref());
        tracing::Span::current().record("record_count", pb.record_count);

        tracing::debug!(
            request_id = %ctx.request_id,
            service = %pb.service_name,
            spans = pb.record_count,
            "Batching traces"
        );

        let response = route_to_batcher(
            ctx.env,
            ctx.trace_ctx,
            SignalKey::Traces,
            &pb.service_name,
            &[pb.batch],
            pb.min_timestamp_micros,
        )
        .await?;

        responses.push(response);
    }

    aggregate_do_responses(responses, ctx.request_id).await
}

/// Handle batched metrics - parse, convert to Arrow, route to DOs in parallel.
#[tracing::instrument(
    name = "otlp.parse",
    skip(ctx, body),
    fields(
        request_id = %ctx.request_id,
        signal_type = "metrics",
        service_name = tracing::field::Empty,
        record_count = tracing::field::Empty,
        bytes = body.len(),
        error = tracing::field::Empty,
    )
)]
pub async fn handle_batched_metrics(
    ctx: &BatchContext<'_>,
    body: &[u8],
    format: InputFormat,
) -> Result<Response> {
    let partitioned = match decode_metrics_partitioned(body, format) {
        Ok(result) => result,
        Err(message) => {
            tracing::Span::current().record("error", message.as_str());
            tracing::error!(
                request_id = %ctx.request_id,
                error = %message,
                "Failed to parse metrics for batching"
            );
            return Ok(invalid_request_response(
                format!("Failed to parse metrics: {}", message),
                ctx.request_id,
            ));
        }
    };

    report_skipped_metrics(&partitioned.skipped);

    let skipped = &partitioned.skipped;
    if partitioned.gauge.is_empty() && partitioned.sum.is_empty() {
        return Response::from_json(&serde_json::json!({
            "status": "accepted",
            "mode": "batched",
            "buffered_records": 0,
            "buffered_bytes": 0,
            "skipped_histograms": skipped.histograms,
            "skipped_exponential_histograms": skipped.exponential_histograms,
            "skipped_summaries": skipped.summaries,
            "skipped_invalid": skipped.nan_values + skipped.infinity_values + skipped.missing_values,
        }));
    }

    let mut responses = Vec::new();

    responses.extend(route_metric_batches(ctx, MetricType::Gauge, partitioned.gauge).await?);
    responses.extend(route_metric_batches(ctx, MetricType::Sum, partitioned.sum).await?);

    // Aggregate responses and include skipped metrics info
    let num_routed = responses.len();
    let mut total_records: i64 = 0;
    let mut total_bytes: i64 = 0;

    for mut response in responses {
        let status = response.status_code();
        if status != 200 {
            let body = response.text().await.unwrap_or_default();
            tracing::error!(
                request_id = %ctx.request_id,
                status,
                body_preview = %body.chars().take(200).collect::<String>(),
                "DO returned non-success status"
            );
            return Response::error(format!("Durable Object returned status {}", status), status);
        }

        if let Ok(body) = response.json::<serde_json::Value>().await {
            if let Some(records) = body.get("buffered_records").and_then(|v| v.as_i64()) {
                total_records = total_records.saturating_add(records);
            }
            if let Some(bytes) = body.get("buffered_bytes").and_then(|v| v.as_i64()) {
                total_bytes = total_bytes.saturating_add(bytes);
            }
        }
    }

    Response::from_json(&serde_json::json!({
        "status": "accepted",
        "mode": "batched",
        "buffered_records": total_records,
        "buffered_bytes": total_bytes,
        "metric_types_routed": num_routed,
        "skipped_histograms": skipped.histograms,
        "skipped_exponential_histograms": skipped.exponential_histograms,
        "skipped_summaries": skipped.summaries,
        "skipped_invalid": skipped.nan_values + skipped.infinity_values + skipped.missing_values,
    }))
}

async fn route_metric_batches(
    ctx: &BatchContext<'_>,
    metric_type: MetricType,
    grouped: ServiceGroupedBatches,
) -> Result<Vec<Response>> {
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

    let futures = grouped
        .batches
        .into_iter()
        .filter(|pb| pb.batch.num_rows() > 0)
        .map(|pb| {
            let service_name = pb.service_name.to_string();
            let min_timestamp = pb.min_timestamp_micros;
            async move {
                route_single_metric(
                    ctx.env,
                    ctx.trace_ctx,
                    ctx.request_id,
                    SignalKey::Metrics(metric_type),
                    &service_name,
                    pb.batch,
                    min_timestamp,
                )
                .await
            }
        })
        .collect::<Vec<_>>();

    try_join_all(futures).await
}
