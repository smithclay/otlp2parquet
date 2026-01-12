//! Signal-specific handlers for OTLP batched ingestion.

use super::routing::{aggregate_do_responses, route_single_metric, route_to_batcher};
use super::{invalid_request_response, BatchContext};
use crate::{MetricType, SignalKey};
use futures::future::try_join_all;
use otlp2parquet_common::InputFormat;
use otlp2parquet_handlers::{
    decode_logs_values, decode_metrics_values, decode_traces_values, first_timestamp_micros,
    group_values_by_service, report_skipped_metrics,
};
use otlp2records::{
    apply_log_transform, apply_metric_transform, apply_trace_transform, gauge_schema, logs_schema,
    sum_schema, traces_schema,
};
use vrl::value::Value;
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
    let values = match decode_logs_values(body, format) {
        Ok(values) => values,
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

    let transformed = match apply_log_transform(values) {
        Ok(values) => values,
        Err(e) => {
            tracing::Span::current().record("error", e.to_string().as_str());
            tracing::error!(
                request_id = %ctx.request_id,
                error = ?e,
                "Failed to convert logs to Arrow"
            );
            return Ok(invalid_request_response(
                format!("Failed to convert logs: {}", e),
                ctx.request_id,
            ));
        }
    };

    let per_service_values = group_values_by_service(transformed);
    let mut responses = Vec::new();

    for (service_name, subset) in per_service_values {
        let batch = match otlp2records::values_to_arrow(&subset, &logs_schema()) {
            Ok(batch) => batch,
            Err(e) => {
                tracing::Span::current().record("error", e.to_string().as_str());
                tracing::error!(
                    request_id = %ctx.request_id,
                    error = ?e,
                    "Failed to convert logs to Arrow"
                );
                return Ok(invalid_request_response(
                    format!("Failed to convert logs: {}", e),
                    ctx.request_id,
                ));
            }
        };

        tracing::Span::current().record("service_name", &service_name);
        tracing::Span::current().record("record_count", subset.len());

        tracing::debug!(
            request_id = %ctx.request_id,
            service = %service_name,
            records = subset.len(),
            "Batching logs"
        );

        let response = route_to_batcher(
            ctx.env,
            ctx.trace_ctx,
            SignalKey::Logs,
            &service_name,
            &[batch],
            first_timestamp_micros(&subset),
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
    let values = match decode_traces_values(body, format) {
        Ok(values) => values,
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

    let transformed = match apply_trace_transform(values) {
        Ok(values) => values,
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

    let per_service_values = group_values_by_service(transformed);
    let mut responses = Vec::new();

    for (service_name, subset) in per_service_values {
        let batch = match otlp2records::values_to_arrow(&subset, &traces_schema()) {
            Ok(batch) => batch,
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

        tracing::Span::current().record("service_name", &service_name);
        tracing::Span::current().record("record_count", subset.len());

        tracing::debug!(
            request_id = %ctx.request_id,
            service = %service_name,
            spans = subset.len(),
            "Batching traces"
        );

        let response = route_to_batcher(
            ctx.env,
            ctx.trace_ctx,
            SignalKey::Traces,
            &service_name,
            &[batch],
            first_timestamp_micros(&subset),
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
    let decode_result = match decode_metrics_values(body, format) {
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

    report_skipped_metrics(&decode_result.skipped);

    let metric_values = match apply_metric_transform(decode_result.values) {
        Ok(values) => values,
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

    if metric_values.gauge.is_empty() && metric_values.sum.is_empty() {
        return Response::from_json(&serde_json::json!({
            "status": "accepted",
            "mode": "batched",
            "buffered_records": 0,
            "buffered_bytes": 0,
        }));
    }

    let mut responses = Vec::new();

    responses.extend(route_metric_values(ctx, MetricType::Gauge, metric_values.gauge).await?);
    responses.extend(route_metric_values(ctx, MetricType::Sum, metric_values.sum).await?);

    aggregate_do_responses(responses, ctx.request_id).await
}

async fn route_metric_values(
    ctx: &BatchContext<'_>,
    metric_type: MetricType,
    values: Vec<Value>,
) -> Result<Vec<Response>> {
    if values.is_empty() {
        return Ok(Vec::new());
    }

    let schema = match metric_type {
        MetricType::Gauge => gauge_schema(),
        MetricType::Sum => sum_schema(),
        _ => {
            tracing::warn!(
                metric_type = ?metric_type,
                count = values.len(),
                "Unsupported metric type - data not persisted"
            );
            return Ok(Vec::new());
        }
    };

    let futures = group_values_by_service(values)
        .into_iter()
        .map(|(service_name, subset)| {
            let schema = schema.clone();
            async move {
                let batch = otlp2records::values_to_arrow(&subset, &schema).map_err(|e| {
                    worker::Error::RustError(format!("Failed to convert metrics to Arrow: {}", e))
                })?;

                route_single_metric(
                    ctx.env,
                    ctx.trace_ctx,
                    ctx.request_id,
                    SignalKey::Metrics(metric_type),
                    &service_name,
                    batch,
                    first_timestamp_micros(&subset),
                )
                .await
            }
        })
        .collect::<Vec<_>>();

    try_join_all(futures).await
}
