// JSON normalization for OTLP canonical JSON format
//
// Converts enum strings to numeric values and validates trace/span IDs.

use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine};
use serde_json::Value as JsonValue;

#[derive(Debug, Default, Clone, Copy)]
pub struct MetricSkipCounts {
    pub histograms: usize,
    pub exponential_histograms: usize,
    pub summaries: usize,
}

/// Normalize OTLP JSON bytes into canonical OTLP JSON bytes.
pub fn normalize_json_bytes(body: &[u8]) -> Result<Vec<u8>> {
    let mut value: JsonValue =
        serde_json::from_slice(body).context("Failed to parse OTLP JSON payload")?;
    normalise_json_value(&mut value, None)?;
    serde_json::to_vec(&value).context("Failed to serialize normalized OTLP JSON payload")
}

/// Normalize canonical OTLP JSON to ensure enum strings are numeric.
pub fn normalise_json_value(value: &mut JsonValue, key_hint: Option<&str>) -> Result<()> {
    match value {
        JsonValue::Object(map) => {
            for (key, val) in map.iter_mut() {
                normalise_json_value(val, Some(key.as_str()))?;
            }
            Ok(())
        }
        JsonValue::Array(values) => {
            for item in values.iter_mut() {
                normalise_json_value(item, key_hint)?;
            }
            Ok(())
        }
        JsonValue::String(current) => {
            if let Some(key) = key_hint {
                if let Some(converted) = convert_string_field(key, current)? {
                    *value = converted;
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

pub fn count_skipped_metric_data_points(value: &JsonValue) -> MetricSkipCounts {
    let mut counts = MetricSkipCounts::default();
    let resource_metrics = value
        .get("resourceMetrics")
        .or_else(|| value.get("resource_metrics"))
        .and_then(JsonValue::as_array);

    let Some(resource_metrics) = resource_metrics else {
        return counts;
    };

    for resource_metric in resource_metrics {
        let scope_metrics = resource_metric
            .get("scopeMetrics")
            .or_else(|| resource_metric.get("scope_metrics"))
            .and_then(JsonValue::as_array);
        let Some(scope_metrics) = scope_metrics else {
            continue;
        };

        for scope_metric in scope_metrics {
            let metrics = scope_metric.get("metrics").and_then(JsonValue::as_array);
            let Some(metrics) = metrics else {
                continue;
            };

            for metric in metrics {
                counts.histograms += count_metric_points(metric, "histogram", "dataPoints");
                counts.histograms += count_metric_points(metric, "histogram", "data_points");
                counts.exponential_histograms +=
                    count_metric_points(metric, "exponentialHistogram", "dataPoints");
                counts.exponential_histograms +=
                    count_metric_points(metric, "exponential_histogram", "data_points");
                counts.summaries += count_metric_points(metric, "summary", "dataPoints");
                counts.summaries += count_metric_points(metric, "summary", "data_points");
            }
        }
    }

    counts
}

fn count_metric_points(metric: &JsonValue, metric_key: &str, points_key: &str) -> usize {
    metric
        .get(metric_key)
        .and_then(|entry| entry.get(points_key))
        .and_then(JsonValue::as_array)
        .map(|points| points.len())
        .unwrap_or(0)
}

/// Convert string field values to their appropriate types.
fn convert_string_field(key: &str, value: &str) -> Result<Option<JsonValue>> {
    if value.is_empty() {
        return Ok(None);
    }

    match key {
        "traceId" | "spanId" | "parentSpanId" | "trace_id" | "span_id" | "parent_span_id" => {
            validate_id_field(value, key)?;
            Ok(None)
        }
        "severityNumber" | "severity_number" => {
            convert_enum_or_number(value, severity_number_from_str)
        }
        "aggregationTemporality" | "aggregation_temporality" => {
            convert_enum_or_number(value, aggregation_temporality_from_str)
        }
        "kind" => convert_enum_or_number(value, span_kind_from_str),
        "code" => convert_enum_or_number(value, status_code_from_str),
        _ => Ok(None),
    }
}

fn convert_enum_or_number(
    value: &str,
    map_enum: fn(&str) -> Option<i64>,
) -> Result<Option<JsonValue>> {
    if let Some(mapped) = map_enum(value) {
        return Ok(Some(JsonValue::Number(mapped.into())));
    }
    if let Ok(parsed) = value.parse::<i64>() {
        return Ok(Some(JsonValue::Number(parsed.into())));
    }
    Ok(None)
}

fn validate_id_field(value: &str, key: &str) -> Result<()> {
    if value.is_empty() {
        return Ok(());
    }

    if hex::decode(value).is_ok() {
        return Ok(());
    }

    if BASE64_STANDARD.decode(value).is_ok() {
        return Ok(());
    }

    Err(anyhow!("Failed to decode {}: expected hex or base64", key))
}

fn span_kind_from_str(value: &str) -> Option<i64> {
    match value {
        "SPAN_KIND_UNSPECIFIED" => Some(0),
        "SPAN_KIND_INTERNAL" => Some(1),
        "SPAN_KIND_SERVER" => Some(2),
        "SPAN_KIND_CLIENT" => Some(3),
        "SPAN_KIND_PRODUCER" => Some(4),
        "SPAN_KIND_CONSUMER" => Some(5),
        _ => None,
    }
}

fn status_code_from_str(value: &str) -> Option<i64> {
    match value {
        "STATUS_CODE_UNSET" => Some(0),
        "STATUS_CODE_OK" => Some(1),
        "STATUS_CODE_ERROR" => Some(2),
        _ => None,
    }
}

fn severity_number_from_str(value: &str) -> Option<i64> {
    match value {
        "SEVERITY_NUMBER_UNSPECIFIED" => Some(0),
        "SEVERITY_NUMBER_TRACE" => Some(1),
        "SEVERITY_NUMBER_TRACE2" => Some(2),
        "SEVERITY_NUMBER_TRACE3" => Some(3),
        "SEVERITY_NUMBER_TRACE4" => Some(4),
        "SEVERITY_NUMBER_DEBUG" => Some(5),
        "SEVERITY_NUMBER_DEBUG2" => Some(6),
        "SEVERITY_NUMBER_DEBUG3" => Some(7),
        "SEVERITY_NUMBER_DEBUG4" => Some(8),
        "SEVERITY_NUMBER_INFO" => Some(9),
        "SEVERITY_NUMBER_INFO2" => Some(10),
        "SEVERITY_NUMBER_INFO3" => Some(11),
        "SEVERITY_NUMBER_INFO4" => Some(12),
        "SEVERITY_NUMBER_WARN" => Some(13),
        "SEVERITY_NUMBER_WARN2" => Some(14),
        "SEVERITY_NUMBER_WARN3" => Some(15),
        "SEVERITY_NUMBER_WARN4" => Some(16),
        "SEVERITY_NUMBER_ERROR" => Some(17),
        "SEVERITY_NUMBER_ERROR2" => Some(18),
        "SEVERITY_NUMBER_ERROR3" => Some(19),
        "SEVERITY_NUMBER_ERROR4" => Some(20),
        "SEVERITY_NUMBER_FATAL" => Some(21),
        "SEVERITY_NUMBER_FATAL2" => Some(22),
        "SEVERITY_NUMBER_FATAL3" => Some(23),
        "SEVERITY_NUMBER_FATAL4" => Some(24),
        _ => None,
    }
}

fn aggregation_temporality_from_str(value: &str) -> Option<i64> {
    match value {
        "AGGREGATION_TEMPORALITY_UNSPECIFIED" => Some(0),
        "AGGREGATION_TEMPORALITY_DELTA" => Some(1),
        "AGGREGATION_TEMPORALITY_CUMULATIVE" => Some(2),
        _ => None,
    }
}
