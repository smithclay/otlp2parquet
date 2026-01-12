//! Shared codec utilities for OTLP decoding and value extraction.
//!
//! This module provides pure functions for decoding OTLP payloads and extracting
//! values that can be used across all platform handlers (server, lambda, cloudflare).

use otlp2parquet_common::{
    count_skipped_metric_data_points, normalise_json_value, normalize_json_bytes, InputFormat,
};
use otlp2records::{
    decode_logs, decode_metrics, decode_traces, DecodeMetricsResult, InputFormat as RecordsFormat,
    SkippedMetrics,
};
use std::collections::HashMap;
use vrl::value::{KeyString, Value};

// Re-export types that consumers need
pub use otlp2records::{
    DecodeMetricsResult as MetricsDecodeResult, SkippedMetrics as MetricsSkipped,
};
pub use vrl::value::Value as VrlValue;

/// Convert InputFormat to otlp2records RecordsFormat
pub fn to_records_format(format: InputFormat) -> RecordsFormat {
    match format {
        InputFormat::Protobuf => RecordsFormat::Protobuf,
        InputFormat::Json => RecordsFormat::Json,
        InputFormat::Jsonl => RecordsFormat::Json,
    }
}

/// Merge skip counts from one SkippedMetrics into another
pub fn merge_skipped(target: &mut SkippedMetrics, other: &SkippedMetrics) {
    target.histograms += other.histograms;
    target.exponential_histograms += other.exponential_histograms;
    target.summaries += other.summaries;
    target.nan_values += other.nan_values;
    target.infinity_values += other.infinity_values;
    target.missing_values += other.missing_values;
}

/// Report skipped metrics via tracing.
/// Uses warn level to ensure visibility in production logs.
pub fn report_skipped_metrics(skipped: &SkippedMetrics) {
    if skipped.has_skipped() {
        tracing::warn!(
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

/// Group values by service name while preserving insertion order.
///
/// Returns a vector of (service_name, values) tuples in the order
/// services were first encountered.
pub fn group_values_by_service(values: Vec<Value>) -> Vec<(String, Vec<Value>)> {
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

/// Extract service name from a Value, defaulting to "unknown".
///
/// Logs at debug level when falling back to "unknown" to aid debugging
/// of misconfigured OTLP sources.
pub fn extract_service_name(value: &Value) -> String {
    let key: KeyString = "service_name".into();
    if let Value::Object(map) = value {
        if let Some(Value::Bytes(bytes)) = map.get(&key) {
            if !bytes.is_empty() {
                return String::from_utf8_lossy(bytes).into_owned();
            }
        }
        // Field exists but is empty or wrong type
        tracing::debug!("service_name field missing or empty, using 'unknown'");
    } else {
        tracing::debug!("Value is not an Object, cannot extract service_name");
    }
    "unknown".to_string()
}

/// Extract the minimum timestamp from values, converting to microseconds.
///
/// Timestamps in the transformed values are in milliseconds.
/// Returns 0 (Unix epoch) if no valid timestamps found, with a debug log.
pub fn first_timestamp_micros(values: &[Value]) -> i64 {
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

    if min.is_none() && !values.is_empty() {
        tracing::debug!(
            value_count = values.len(),
            "No valid timestamps found in values, using epoch (0)"
        );
    }

    min.unwrap_or(0)
}

/// Convert a VRL Value to i64, handling Integer and Float types.
pub fn value_to_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Integer(i) => Some(*i),
        Value::Float(f) => Some(f.into_inner() as i64),
        _ => None,
    }
}

// =============================================================================
// Decode functions - return Result<T, String> for easy error wrapping
// =============================================================================

/// Decode logs from various formats.
/// Returns String errors for easy wrapping by platform-specific error types.
pub fn decode_logs_values(body: &[u8], format: InputFormat) -> Result<Vec<Value>, String> {
    match format {
        InputFormat::Jsonl => {
            decode_jsonl_values(body, |line| decode_logs(line, RecordsFormat::Json))
        }
        InputFormat::Json => {
            let normalized = normalize_json_bytes(body).map_err(|e| e.to_string())?;
            decode_logs(&normalized, RecordsFormat::Json).map_err(|e| e.to_string())
        }
        InputFormat::Protobuf => {
            decode_logs(body, RecordsFormat::Protobuf).map_err(|e| e.to_string())
        }
    }
}

/// Decode traces from various formats.
/// Returns String errors for easy wrapping by platform-specific error types.
pub fn decode_traces_values(body: &[u8], format: InputFormat) -> Result<Vec<Value>, String> {
    match format {
        InputFormat::Jsonl => {
            decode_jsonl_values(body, |line| decode_traces(line, RecordsFormat::Json))
        }
        InputFormat::Json => {
            let normalized = normalize_json_bytes(body).map_err(|e| e.to_string())?;
            decode_traces(&normalized, RecordsFormat::Json).map_err(|e| e.to_string())
        }
        InputFormat::Protobuf => {
            decode_traces(body, RecordsFormat::Protobuf).map_err(|e| e.to_string())
        }
    }
}

/// Decode metrics from various formats.
/// Returns String errors for easy wrapping by platform-specific error types.
pub fn decode_metrics_values(
    body: &[u8],
    format: InputFormat,
) -> Result<DecodeMetricsResult, String> {
    match format {
        InputFormat::Jsonl => decode_jsonl_metrics(body),
        InputFormat::Json => decode_metrics_json(body),
        InputFormat::Protobuf => {
            decode_metrics(body, RecordsFormat::Protobuf).map_err(|e| e.to_string())
        }
    }
}

/// Decode JSONL payload line by line using the provided decode function.
pub fn decode_jsonl_values<F>(body: &[u8], mut decode: F) -> Result<Vec<Value>, String>
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
        let normalized = normalize_json_bytes(trimmed.as_bytes())
            .map_err(|e| format!("line {}: {}", line_num + 1, e))?;
        let values = decode(&normalized).map_err(|e| format!("line {}: {}", line_num + 1, e))?;
        out.extend(values);
    }

    if !saw_line {
        return Err("jsonl payload contained no records".to_string());
    }

    Ok(out)
}

/// Decode JSONL metrics payload, tracking skipped metric types.
pub fn decode_jsonl_metrics(body: &[u8]) -> Result<DecodeMetricsResult, String> {
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
        let mut value: serde_json::Value = serde_json::from_slice(trimmed.as_bytes())
            .map_err(|e| format!("line {}: {}", line_num + 1, e))?;
        let counts = count_skipped_metric_data_points(&value);
        normalise_json_value(&mut value, None)
            .map_err(|e| format!("line {}: {}", line_num + 1, e))?;
        let normalized =
            serde_json::to_vec(&value).map_err(|e| format!("line {}: {}", line_num + 1, e))?;
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

/// Decode JSON metrics payload, tracking skipped metric types.
pub fn decode_metrics_json(body: &[u8]) -> Result<DecodeMetricsResult, String> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_records_format() {
        assert!(matches!(
            to_records_format(InputFormat::Protobuf),
            RecordsFormat::Protobuf
        ));
        assert!(matches!(
            to_records_format(InputFormat::Json),
            RecordsFormat::Json
        ));
        assert!(matches!(
            to_records_format(InputFormat::Jsonl),
            RecordsFormat::Json
        ));
    }

    #[test]
    fn test_merge_skipped() {
        let mut target = SkippedMetrics::default();
        let other = SkippedMetrics {
            histograms: 1,
            exponential_histograms: 2,
            summaries: 3,
            nan_values: 4,
            infinity_values: 5,
            missing_values: 6,
        };

        merge_skipped(&mut target, &other);

        assert_eq!(target.histograms, 1);
        assert_eq!(target.exponential_histograms, 2);
        assert_eq!(target.summaries, 3);
        assert_eq!(target.nan_values, 4);
        assert_eq!(target.infinity_values, 5);
        assert_eq!(target.missing_values, 6);
    }

    #[test]
    fn test_group_values_by_service_empty() {
        let values: Vec<Value> = vec![];
        let grouped = group_values_by_service(values);
        assert!(grouped.is_empty());
    }

    #[test]
    fn test_group_values_by_service_preserves_order() {
        let values = vec![
            make_test_value("service-a"),
            make_test_value("service-b"),
            make_test_value("service-a"),
        ];

        let grouped = group_values_by_service(values);
        assert_eq!(grouped.len(), 2);
        assert_eq!(grouped[0].0, "service-a");
        assert_eq!(grouped[1].0, "service-b");
        assert_eq!(grouped[0].1.len(), 2);
        assert_eq!(grouped[1].1.len(), 1);
    }

    #[test]
    fn test_extract_service_name_valid() {
        let value = make_test_value("my-service");
        assert_eq!(extract_service_name(&value), "my-service");
    }

    #[test]
    fn test_extract_service_name_missing() {
        let map = vrl::value::ObjectMap::new();
        let value = Value::Object(map);
        assert_eq!(extract_service_name(&value), "unknown");
    }

    #[test]
    fn test_extract_service_name_not_object() {
        let value = Value::Integer(42);
        assert_eq!(extract_service_name(&value), "unknown");
    }

    #[test]
    fn test_first_timestamp_micros_valid() {
        let values = vec![
            make_test_value_with_timestamp("svc", 2000),
            make_test_value_with_timestamp("svc", 1000),
            make_test_value_with_timestamp("svc", 3000),
        ];
        // Should find minimum (1000) and convert to micros (1_000_000)
        assert_eq!(first_timestamp_micros(&values), 1_000_000);
    }

    #[test]
    fn test_first_timestamp_micros_empty() {
        let values: Vec<Value> = vec![];
        assert_eq!(first_timestamp_micros(&values), 0);
    }

    #[test]
    fn test_value_to_i64_integer() {
        let value = Value::Integer(42);
        assert_eq!(value_to_i64(&value), Some(42));
    }

    #[test]
    fn test_value_to_i64_float() {
        // Use ordered_float::NotNan which is what VRL uses internally
        let value = Value::Float(ordered_float::NotNan::new(42.9).unwrap());
        assert_eq!(value_to_i64(&value), Some(42)); // truncates
    }

    #[test]
    fn test_value_to_i64_other() {
        let value = Value::Boolean(true);
        assert_eq!(value_to_i64(&value), None);
    }

    fn make_test_value(service_name: &str) -> Value {
        make_test_value_with_timestamp(service_name, 1_700_000_000)
    }

    fn make_test_value_with_timestamp(service_name: &str, timestamp: i64) -> Value {
        let mut map = vrl::value::ObjectMap::new();
        map.insert(
            "service_name".into(),
            Value::Bytes(bytes::Bytes::copy_from_slice(service_name.as_bytes())),
        );
        map.insert("timestamp".into(), Value::Integer(timestamp));
        Value::Object(map)
    }
}
