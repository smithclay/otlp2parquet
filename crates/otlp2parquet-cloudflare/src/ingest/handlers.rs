//! Signal-specific handlers for OTLP batched ingestion.

use super::routing::{aggregate_do_responses, route_single_metric, route_to_batcher};
use super::{invalid_request_response, BatchContext};
use crate::{MetricType, SignalKey};
use futures::future::try_join_all;
use otlp2parquet_common::{
    count_skipped_metric_data_points, normalise_json_value, normalize_json_bytes, InputFormat,
};
use otlp2records::{
    apply_log_transform, apply_metric_transform, apply_trace_transform, decode_logs,
    decode_metrics, decode_traces, gauge_schema, logs_schema, sum_schema, traces_schema,
    DecodeMetricsResult, InputFormat as RecordsFormat, SkippedMetrics,
};
use std::collections::HashMap;
use vrl::value::{KeyString, Value};
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

    responses.extend(route_metric_values(ctx, MetricType::Gauge, &metric_values.gauge).await?);
    responses.extend(route_metric_values(ctx, MetricType::Sum, &metric_values.sum).await?);

    aggregate_do_responses(responses, ctx.request_id).await
}

async fn route_metric_values(
    ctx: &BatchContext<'_>,
    metric_type: MetricType,
    values: &[Value],
) -> Result<Vec<Response>> {
    if values.is_empty() {
        return Ok(Vec::new());
    }

    let schema = match metric_type {
        MetricType::Gauge => gauge_schema(),
        MetricType::Sum => sum_schema(),
        _ => return Ok(Vec::new()),
    };

    let futures = group_values_by_service(values.to_vec())
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

fn decode_logs_values(body: &[u8], format: InputFormat) -> Result<Vec<Value>, String> {
    match format {
        InputFormat::Jsonl => {
            decode_jsonl_values(body, |line| decode_logs(line, RecordsFormat::Json))
        }
        InputFormat::Json => {
            let normalized = normalize_json_bytes(body).map_err(|e| e.to_string())?;
            decode_logs(&normalized, RecordsFormat::Json).map_err(|e| e.to_string())
        }
        InputFormat::Protobuf => {
            decode_logs(body, to_records_format(format)).map_err(|e| e.to_string())
        }
    }
}

fn decode_traces_values(body: &[u8], format: InputFormat) -> Result<Vec<Value>, String> {
    match format {
        InputFormat::Jsonl => {
            decode_jsonl_values(body, |line| decode_traces(line, RecordsFormat::Json))
        }
        InputFormat::Json => {
            let normalized = normalize_json_bytes(body).map_err(|e| e.to_string())?;
            decode_traces(&normalized, RecordsFormat::Json).map_err(|e| e.to_string())
        }
        InputFormat::Protobuf => {
            decode_traces(body, to_records_format(format)).map_err(|e| e.to_string())
        }
    }
}

fn decode_metrics_values(body: &[u8], format: InputFormat) -> Result<DecodeMetricsResult, String> {
    match format {
        InputFormat::Jsonl => decode_jsonl_metrics(body),
        InputFormat::Json => decode_metrics_json(body),
        InputFormat::Protobuf => {
            decode_metrics(body, to_records_format(format)).map_err(|e| e.to_string())
        }
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

fn decode_jsonl_metrics(body: &[u8]) -> Result<DecodeMetricsResult, String> {
    let text = std::str::from_utf8(body).map_err(|e| e.to_string())?;
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
            serde_json::from_slice(trimmed.as_bytes()).map_err(|e| e.to_string())?;
        let counts = count_skipped_metric_data_points(&value);
        normalise_json_value(&mut value, None).map_err(|e| e.to_string())?;
        let normalized = serde_json::to_vec(&value).map_err(|e| e.to_string())?;
        let mut result = decode_metrics(&normalized, RecordsFormat::Json)
            .map_err(|e| format!("line {}: {}", line_num + 1, e))?;
        result.skipped.histograms += counts.histograms;
        result.skipped.exponential_histograms += counts.exponential_histograms;
        result.skipped.summaries += counts.summaries;
        values.extend(result.values);
        merge_skipped(&mut skipped, &result.skipped);
    }

    if !saw_line {
        return Err("jsonl payload contained no records".to_string());
    }

    Ok(DecodeMetricsResult { values, skipped })
}

fn decode_metrics_json(body: &[u8]) -> Result<DecodeMetricsResult, String> {
    let mut value: serde_json::Value = serde_json::from_slice(body).map_err(|e| e.to_string())?;
    let counts = count_skipped_metric_data_points(&value);
    normalise_json_value(&mut value, None).map_err(|e| e.to_string())?;
    let normalized = serde_json::to_vec(&value).map_err(|e| e.to_string())?;
    let mut result = decode_metrics(&normalized, RecordsFormat::Json).map_err(|e| e.to_string())?;
    result.skipped.histograms += counts.histograms;
    result.skipped.exponential_histograms += counts.exponential_histograms;
    result.skipped.summaries += counts.summaries;
    Ok(result)
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
