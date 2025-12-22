//! Signal-specific handlers for OTLP batched ingestion.

use super::routing::{aggregate_do_responses, route_single_metric, route_to_batcher};
use super::{invalid_request_response, BatchContext};
use crate::{MetricType, SignalKey};
use otlp2parquet_core::InputFormat;
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
    let (batch, metadata) = match otlp2parquet_core::parse_otlp_to_arrow(body, format) {
        Ok(result) => result,
        Err(e) => {
            tracing::Span::current().record("error", e.to_string().as_str());
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

    tracing::Span::current().record("service_name", &*metadata.service_name);
    tracing::Span::current().record("record_count", metadata.record_count);

    tracing::debug!(
        request_id = %ctx.request_id,
        service = %metadata.service_name,
        records = metadata.record_count,
        "Batching logs"
    );

    route_to_batcher(
        ctx.env,
        ctx.trace_ctx,
        SignalKey::Logs,
        &metadata.service_name,
        &[batch],
        metadata.first_timestamp_micros,
    )
    .await
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
    use otlp2parquet_core::otlp::traces;

    let request = match traces::parse_otlp_trace_request(body, format) {
        Ok(result) => result,
        Err(e) => {
            tracing::Span::current().record("error", e.to_string().as_str());
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
            tracing::Span::current().record("error", e.to_string().as_str());
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

    tracing::Span::current().record("service_name", &*metadata.service_name);
    tracing::Span::current().record("record_count", metadata.span_count);

    tracing::debug!(
        request_id = %ctx.request_id,
        service = %metadata.service_name,
        spans = metadata.span_count,
        "Batching traces"
    );

    route_to_batcher(
        ctx.env,
        ctx.trace_ctx,
        SignalKey::Traces,
        &metadata.service_name,
        &batches,
        metadata.first_timestamp_micros,
    )
    .await
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
    use futures::future::try_join_all;
    use otlp2parquet_core::otlp::metrics;

    let request = match metrics::parse_otlp_request(body, format) {
        Ok(result) => result,
        Err(e) => {
            tracing::Span::current().record("error", e.to_string().as_str());
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
            tracing::Span::current().record("error", e.to_string().as_str());
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

    let total_record_count = metadata.gauge_count
        + metadata.sum_count
        + metadata.histogram_count
        + metadata.exponential_histogram_count
        + metadata.summary_count;

    tracing::Span::current().record("service_name", metadata.service_name());
    tracing::Span::current().record("record_count", total_record_count);

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
                ctx.trace_ctx,
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
