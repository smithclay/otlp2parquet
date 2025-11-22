// JSON normalization for OTLP canonical JSON format
//
// Handles conversion from canonical OTLP JSON (camelCase, string numbers)
// to the format expected by prost-generated structs (snake_case, actual numbers)
//
// Supports all OTLP signals:
// - Logs: LogRecord with body, attributes, resources
// - Traces: Span with events, links, status
// - Metrics: All 5 metric types (Gauge, Sum, Histogram, ExponentialHistogram, Summary)

use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine};
use hex::FromHex;
use serde_json::Value as JsonValue;
use std::borrow::Cow;
use std::mem;

use super::field_names::otlp;

// Field name constants for JSON normalization
const U64_FIELDS: &[&str] = &[
    otlp::TIME_UNIX_NANO,
    otlp::OBSERVED_TIME_UNIX_NANO,
    otlp::START_TIME_UNIX_NANO,
    otlp::END_TIME_UNIX_NANO,
    otlp::COUNT,      // Metrics: histogram/exp_histogram count
    otlp::ZERO_COUNT, // Metrics: exponential histogram zero count
    otlp::SCALE,      // Metrics: exponential histogram scale
];
const U32_FIELDS: &[&str] = &[
    otlp::DROPPED_ATTRIBUTES_COUNT,
    otlp::FLAGS,
    otlp::TRACE_FLAGS,
    otlp::DROPPED_EVENTS_COUNT,
    otlp::DROPPED_LINKS_COUNT,
];
const I64_FIELDS: &[&str] = &[
    otlp::INT_VALUE,
    otlp::AS_INT, // Metrics: integer value in data points
];
const F64_FIELDS: &[&str] = &[
    otlp::DOUBLE_VALUE,
    otlp::AS_DOUBLE, // Metrics: double value in data points
];
const ANYVALUE_VARIANTS: &[&str] = &[
    otlp::STRING_VALUE,
    otlp::BOOL_VALUE,
    otlp::INT_VALUE,
    otlp::DOUBLE_VALUE,
    otlp::ARRAY_VALUE,
    otlp::KVLIST_VALUE,
    otlp::BYTES_VALUE,
];

/// Normalize canonical OTLP JSON to prost-compatible format
///
/// Transformations:
/// - camelCase field names → snake_case
/// - AnyValue variants → PascalCase (for prost oneof enums)
/// - String numbers → actual JSON numbers
/// - Hex strings for trace_id/span_id → byte arrays
/// - Fill in missing required fields with defaults
pub(crate) fn normalise_json_value(value: &mut JsonValue, key_hint: Option<&str>) -> Result<()> {
    match value {
        JsonValue::Object(map) => {
            let original = mem::take(map);

            for (key, mut val) in original {
                let snake_key: Cow<'_, str> = if key.chars().any(|c| c.is_ascii_uppercase()) {
                    Cow::Owned(camel_to_snake_case(&key))
                } else {
                    Cow::Borrowed(key.as_str())
                };

                let is_anyvalue_variant = ANYVALUE_VARIANTS.contains(&snake_key.as_ref());

                let hint_storage;
                let hint_key = if is_anyvalue_variant {
                    hint_storage = snake_key.to_string();
                    hint_storage.as_str()
                } else {
                    snake_key.as_ref()
                };

                normalise_json_value(&mut val, Some(hint_key))?;

                let final_key = if is_anyvalue_variant {
                    snake_to_pascal_case(snake_key.as_ref())
                } else {
                    match snake_key {
                        Cow::Owned(s) => s,
                        Cow::Borrowed(_) => key,
                    }
                };

                map.insert(final_key, val);
            }

            // Fill in missing required fields based on context
            if let Some(otlp::LOG_RECORDS) = key_hint {
                map.entry(otlp::DROPPED_ATTRIBUTES_COUNT.to_string())
                    .or_insert_with(|| JsonValue::Number(serde_json::Number::from(0u32)));
                map.entry(otlp::FLAGS.to_string())
                    .or_insert_with(|| JsonValue::Number(serde_json::Number::from(0u32)));
                map.entry(otlp::OBSERVED_TIME_UNIX_NANO.to_string())
                    .or_insert_with(|| JsonValue::Number(serde_json::Number::from(0u64)));
                map.entry(otlp::TIME_UNIX_NANO.to_string())
                    .or_insert_with(|| JsonValue::Number(serde_json::Number::from(0u64)));
                map.entry(otlp::SEVERITY_NUMBER.to_string())
                    .or_insert_with(|| JsonValue::Number(serde_json::Number::from(0i32)));
                map.entry(otlp::SEVERITY_TEXT.to_string())
                    .or_insert_with(|| JsonValue::String(String::new()));
                map.entry(otlp::ATTRIBUTES.to_string())
                    .or_insert_with(|| JsonValue::Array(Vec::new()));
                map.entry(otlp::TRACE_ID.to_string())
                    .or_insert_with(|| JsonValue::Array(Vec::new()));
                map.entry(otlp::SPAN_ID.to_string())
                    .or_insert_with(|| JsonValue::Array(Vec::new()));
            }

            if let Some(otlp::SCOPE_LOGS) = key_hint {
                map.entry(otlp::SCHEMA_URL.to_string())
                    .or_insert_with(|| JsonValue::String(String::new()));
            }

            if let Some(otlp::SCOPE_SPANS) = key_hint {
                map.entry(otlp::SCHEMA_URL.to_string())
                    .or_insert_with(|| JsonValue::String(String::new()));
                map.entry(otlp::SPANS.to_string())
                    .or_insert_with(|| JsonValue::Array(Vec::new()));
            }

            if let Some(otlp::RESOURCE_LOGS) = key_hint {
                map.entry(otlp::SCHEMA_URL.to_string())
                    .or_insert_with(|| JsonValue::String(String::new()));
            }

            if let Some(otlp::RESOURCE_SPANS) = key_hint {
                map.entry(otlp::SCHEMA_URL.to_string())
                    .or_insert_with(|| JsonValue::String(String::new()));
                map.entry(otlp::SCOPE_SPANS.to_string())
                    .or_insert_with(|| JsonValue::Array(Vec::new()));
            }

            if let Some(otlp::VALUE) = key_hint {
                if !map.contains_key(otlp::VALUE) {
                    let inner = JsonValue::Object(std::mem::take(map));
                    map.insert(otlp::VALUE.to_string(), inner);
                }
            }

            if let Some(otlp::RESOURCE) = key_hint {
                map.entry(otlp::DROPPED_ATTRIBUTES_COUNT.to_string())
                    .or_insert_with(|| JsonValue::Number(serde_json::Number::from(0u32)));
                map.entry(otlp::ATTRIBUTES.to_string())
                    .or_insert_with(|| JsonValue::Array(Vec::new()));
            }

            if let Some(otlp::SCOPE) = key_hint {
                map.entry(otlp::DROPPED_ATTRIBUTES_COUNT.to_string())
                    .or_insert_with(|| JsonValue::Number(serde_json::Number::from(0u32)));
                map.entry(otlp::NAME.to_string())
                    .or_insert_with(|| JsonValue::String(String::new()));
                map.entry(otlp::VERSION.to_string())
                    .or_insert_with(|| JsonValue::String(String::new()));
                map.entry(otlp::ATTRIBUTES.to_string())
                    .or_insert_with(|| JsonValue::Array(Vec::new()));
            }

            if let Some(otlp::SPANS) = key_hint {
                map.entry(otlp::TRACE_ID.to_string())
                    .or_insert_with(|| JsonValue::Array(Vec::new()));
                map.entry(otlp::SPAN_ID.to_string())
                    .or_insert_with(|| JsonValue::Array(Vec::new()));
                map.entry(otlp::PARENT_SPAN_ID.to_string())
                    .or_insert_with(|| JsonValue::Array(Vec::new()));
                map.entry(otlp::TRACE_STATE.to_string())
                    .or_insert_with(|| JsonValue::String(String::new()));
                map.entry(otlp::FLAGS.to_string())
                    .or_insert_with(|| JsonValue::Number(serde_json::Number::from(0u32)));
                map.entry(otlp::NAME.to_string())
                    .or_insert_with(|| JsonValue::String(String::new()));
                map.entry(otlp::KIND.to_string())
                    .or_insert_with(|| JsonValue::Number(serde_json::Number::from(0)));
                map.entry(otlp::START_TIME_UNIX_NANO.to_string())
                    .or_insert_with(|| JsonValue::Number(serde_json::Number::from(0u64)));
                map.entry(otlp::END_TIME_UNIX_NANO.to_string())
                    .or_insert_with(|| JsonValue::Number(serde_json::Number::from(0u64)));
                map.entry(otlp::ATTRIBUTES.to_string())
                    .or_insert_with(|| JsonValue::Array(Vec::new()));
                map.entry(otlp::DROPPED_ATTRIBUTES_COUNT.to_string())
                    .or_insert_with(|| JsonValue::Number(serde_json::Number::from(0u32)));
                map.entry(otlp::EVENTS.to_string())
                    .or_insert_with(|| JsonValue::Array(Vec::new()));
                map.entry(otlp::DROPPED_EVENTS_COUNT.to_string())
                    .or_insert_with(|| JsonValue::Number(serde_json::Number::from(0u32)));
                map.entry(otlp::LINKS.to_string())
                    .or_insert_with(|| JsonValue::Array(Vec::new()));
                map.entry(otlp::DROPPED_LINKS_COUNT.to_string())
                    .or_insert_with(|| JsonValue::Number(serde_json::Number::from(0u32)));
                map.entry(otlp::STATUS.to_string()).or_insert_with(|| {
                    let mut status = serde_json::Map::with_capacity(2);
                    status.insert(
                        otlp::CODE.to_string(),
                        JsonValue::Number(serde_json::Number::from(0)),
                    );
                    status.insert(otlp::MESSAGE.to_string(), JsonValue::String(String::new()));
                    JsonValue::Object(status)
                });
            }

            if let Some(otlp::EVENTS) = key_hint {
                map.entry(otlp::TIME_UNIX_NANO.to_string())
                    .or_insert_with(|| JsonValue::Number(serde_json::Number::from(0u64)));
                map.entry(otlp::NAME.to_string())
                    .or_insert_with(|| JsonValue::String(String::new()));
                map.entry(otlp::ATTRIBUTES.to_string())
                    .or_insert_with(|| JsonValue::Array(Vec::new()));
                map.entry(otlp::DROPPED_ATTRIBUTES_COUNT.to_string())
                    .or_insert_with(|| JsonValue::Number(serde_json::Number::from(0u32)));
            }

            if let Some(otlp::LINKS) = key_hint {
                map.entry(otlp::TRACE_ID.to_string())
                    .or_insert_with(|| JsonValue::Array(Vec::new()));
                map.entry(otlp::SPAN_ID.to_string())
                    .or_insert_with(|| JsonValue::Array(Vec::new()));
                map.entry(otlp::TRACE_STATE.to_string())
                    .or_insert_with(|| JsonValue::String(String::new()));
                map.entry(otlp::ATTRIBUTES.to_string())
                    .or_insert_with(|| JsonValue::Array(Vec::new()));
                map.entry(otlp::DROPPED_ATTRIBUTES_COUNT.to_string())
                    .or_insert_with(|| JsonValue::Number(serde_json::Number::from(0u32)));
            }

            if let Some(otlp::STATUS) = key_hint {
                map.entry(otlp::CODE.to_string())
                    .or_insert_with(|| JsonValue::Number(serde_json::Number::from(0)));
                map.entry(otlp::MESSAGE.to_string())
                    .or_insert_with(|| JsonValue::String(String::new()));
            }

            // ================================================================
            // METRICS HANDLERS
            // ================================================================

            // ResourceMetrics: fill defaults
            if let Some(otlp::RESOURCE_METRICS) = key_hint {
                map.entry(otlp::SCHEMA_URL.to_string())
                    .or_insert_with(|| JsonValue::String(String::new()));
                map.entry(otlp::SCOPE_METRICS.to_string())
                    .or_insert_with(|| JsonValue::Array(Vec::new()));
            }

            // ScopeMetrics: fill defaults
            if let Some(otlp::SCOPE_METRICS) = key_hint {
                map.entry(otlp::SCHEMA_URL.to_string())
                    .or_insert_with(|| JsonValue::String(String::new()));
                map.entry(otlp::METRICS.to_string())
                    .or_insert_with(|| JsonValue::Array(Vec::new()));
            }

            // Metrics: Transform gauge/sum/histogram/exponential_histogram/summary
            // from OTLP JSON format to prost enum format
            //
            // OTLP JSON:  {"name": "...", "gauge": {"dataPoints": [...]}}
            // Prost format: {"name": "...", "data": {"Gauge": {"data_points": [...]}}}
            if let Some(otlp::METRICS) = key_hint {
                // Check for each metric type and transform to data field
                let metric_types = [
                    (otlp::GAUGE, "Gauge"),
                    (otlp::SUM, "Sum"),
                    (otlp::HISTOGRAM, "Histogram"),
                    (otlp::EXPONENTIAL_HISTOGRAM, "ExponentialHistogram"),
                    (otlp::SUMMARY, "Summary"),
                ];

                for (field_name, variant_name) in &metric_types {
                    if let Some(data_value) = map.remove(*field_name) {
                        // Wrap the value in a data object with the variant name
                        let mut data_map = serde_json::Map::new();
                        data_map.insert(variant_name.to_string(), data_value);
                        map.insert("data".to_string(), JsonValue::Object(data_map));
                        break; // Only one data type per metric
                    }
                }
            }

            // Data points: fill defaults and transform as_double/as_int to value field
            if let Some(otlp::DATA_POINTS) = key_hint {
                map.entry(otlp::TIME_UNIX_NANO.to_string())
                    .or_insert_with(|| JsonValue::Number(serde_json::Number::from(0u64)));
                map.entry(otlp::START_TIME_UNIX_NANO.to_string())
                    .or_insert_with(|| JsonValue::Number(serde_json::Number::from(0u64)));
                map.entry(otlp::ATTRIBUTES.to_string())
                    .or_insert_with(|| JsonValue::Array(Vec::new()));
                map.entry("exemplars".to_string())
                    .or_insert_with(|| JsonValue::Array(Vec::new()));
                map.entry("flags".to_string())
                    .or_insert_with(|| JsonValue::Number(serde_json::Number::from(0u32)));

                // For ExponentialHistogramDataPoint: add zero_threshold default
                // We detect exponential histogram data points by the presence of "scale" field
                if map.contains_key(otlp::SCALE) {
                    map.entry("zero_threshold".to_string()).or_insert_with(|| {
                        // 0.0 is always valid, but use fallback for safety
                        JsonValue::Number(
                            serde_json::Number::from_f64(0.0)
                                .unwrap_or_else(|| serde_json::Number::from(0u32)),
                        )
                    });
                }

                // Transform as_double/as_int to value field
                // OTLP JSON:  {"timeUnixNano": "...", "asDouble": 45.2}
                // Prost format: {"time_unix_nano": ..., "value": {"AsDouble": 45.2}}
                if let Some(as_double_value) = map.remove(otlp::AS_DOUBLE) {
                    let mut value_map = serde_json::Map::new();
                    value_map.insert("AsDouble".to_string(), as_double_value);
                    map.insert("value".to_string(), JsonValue::Object(value_map));
                } else if let Some(as_int_value) = map.remove(otlp::AS_INT) {
                    let mut value_map = serde_json::Map::new();
                    value_map.insert("AsInt".to_string(), as_int_value);
                    map.insert("value".to_string(), JsonValue::Object(value_map));
                }
            }

            // Gauge: fill data_points default
            if let Some(otlp::GAUGE) = key_hint {
                map.entry(otlp::DATA_POINTS.to_string())
                    .or_insert_with(|| JsonValue::Array(Vec::new()));
            }

            // Sum: fill defaults
            if let Some(otlp::SUM) = key_hint {
                map.entry(otlp::DATA_POINTS.to_string())
                    .or_insert_with(|| JsonValue::Array(Vec::new()));
                map.entry(otlp::AGGREGATION_TEMPORALITY.to_string())
                    .or_insert_with(|| JsonValue::Number(serde_json::Number::from(0)));
                map.entry(otlp::IS_MONOTONIC.to_string())
                    .or_insert_with(|| JsonValue::Bool(false));
            }

            // Histogram: fill defaults
            if let Some(otlp::HISTOGRAM) = key_hint {
                map.entry(otlp::DATA_POINTS.to_string())
                    .or_insert_with(|| JsonValue::Array(Vec::new()));
                map.entry(otlp::AGGREGATION_TEMPORALITY.to_string())
                    .or_insert_with(|| JsonValue::Number(serde_json::Number::from(0)));
            }

            // ExponentialHistogram: fill defaults
            if let Some(otlp::EXPONENTIAL_HISTOGRAM) = key_hint {
                map.entry(otlp::DATA_POINTS.to_string())
                    .or_insert_with(|| JsonValue::Array(Vec::new()));
                map.entry(otlp::AGGREGATION_TEMPORALITY.to_string())
                    .or_insert_with(|| JsonValue::Number(serde_json::Number::from(0)));
            }

            // Summary: fill defaults
            if let Some(otlp::SUMMARY) = key_hint {
                map.entry(otlp::DATA_POINTS.to_string())
                    .or_insert_with(|| JsonValue::Array(Vec::new()));
            }

            // Quantile values: add default quantile field (0.0 for minimum)
            if let Some(otlp::QUANTILE_VALUES) = key_hint {
                map.entry(otlp::QUANTILE.to_string()).or_insert_with(|| {
                    // 0.0 is always valid, but use fallback for safety
                    JsonValue::Number(
                        serde_json::Number::from_f64(0.0)
                            .unwrap_or_else(|| serde_json::Number::from(0u32)),
                    )
                });
                map.entry("value".to_string()).or_insert_with(|| {
                    // 0.0 is always valid, but use fallback for safety
                    JsonValue::Number(
                        serde_json::Number::from_f64(0.0)
                            .unwrap_or_else(|| serde_json::Number::from(0u32)),
                    )
                });
            }

            // Positive/Negative buckets for exponential histogram
            if matches!(key_hint, Some(otlp::POSITIVE) | Some(otlp::NEGATIVE)) {
                map.entry(otlp::POSITIVE_OFFSET.to_string()) // Both use "offset"
                    .or_insert_with(|| JsonValue::Number(serde_json::Number::from(0)));
                map.entry(otlp::BUCKET_COUNTS.to_string())
                    .or_insert_with(|| JsonValue::Array(Vec::new()));
            }

            Ok(())
        }
        JsonValue::Array(values) => {
            // Special handling for arrays of string numbers (e.g., bucket_counts)
            if let Some(otlp::BUCKET_COUNTS) = key_hint {
                // Convert array of string numbers to array of u64 numbers
                for item in values.iter_mut() {
                    if let JsonValue::String(s) = item {
                        let parsed = s.parse::<u64>().with_context(|| {
                            format!("Failed to parse '{}' as u64 in bucket_counts array", s)
                        })?;
                        *item = JsonValue::Number(serde_json::Number::from(parsed));
                    }
                }
            } else {
                // Standard recursive normalization for other arrays
                for item in values.iter_mut() {
                    normalise_json_value(item, key_hint)?;
                }
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

/// Convert string field values to their appropriate types
///
/// OTLP canonical JSON represents large integers as strings to avoid
/// JavaScript precision loss. We convert them back to numbers.
fn convert_string_field(key: &str, value: &str) -> Result<Option<JsonValue>> {
    if value.is_empty() {
        return Ok(None);
    }

    if U64_FIELDS.contains(&key) {
        let parsed = value
            .parse::<u64>()
            .with_context(|| format!("Failed to parse '{}' as u64 for field '{}'", value, key))?;
        return Ok(Some(JsonValue::Number(serde_json::Number::from(parsed))));
    }

    if U32_FIELDS.contains(&key) {
        let parsed = value
            .parse::<u32>()
            .with_context(|| format!("Failed to parse '{}' as u32 for field '{}'", value, key))?;
        return Ok(Some(JsonValue::Number(serde_json::Number::from(parsed))));
    }

    if I64_FIELDS.contains(&key) {
        let parsed = value
            .parse::<i64>()
            .with_context(|| format!("Failed to parse '{}' as i64 for field '{}'", value, key))?;
        return Ok(Some(JsonValue::Number(serde_json::Number::from(parsed))));
    }

    if F64_FIELDS.contains(&key) {
        let parsed = value
            .parse::<f64>()
            .with_context(|| format!("Failed to parse '{}' as f64 for field '{}'", value, key))?;
        let number = serde_json::Number::from_f64(parsed).ok_or_else(|| {
            anyhow!(
                "Invalid floating point value '{}' for field '{}'",
                value,
                key
            )
        })?;
        return Ok(Some(JsonValue::Number(number)));
    }

    // Convert hex-encoded or base64-encoded trace/span IDs to byte arrays
    if matches!(key, k if k == otlp::TRACE_ID || k == otlp::SPAN_ID || k == otlp::PARENT_SPAN_ID) {
        let bytes = if value.len().is_multiple_of(2) && value.chars().all(|c| c.is_ascii_hexdigit())
        {
            // Hex-encoded
            Vec::from_hex(value).with_context(|| {
                format!(
                    "Failed to decode hex string '{}' for field '{}'",
                    value, key
                )
            })?
        } else {
            // Try base64-encoded
            BASE64_STANDARD.decode(value).with_context(|| {
                format!(
                    "Failed to decode base64 string '{}' for field '{}'",
                    value, key
                )
            })?
        };
        let json_value =
            serde_json::to_value(bytes).context("Failed to encode OTLP id bytes as JSON array")?;
        return Ok(Some(json_value));
    }

    if key == otlp::KIND {
        let parsed = match value {
            "SPAN_KIND_UNSPECIFIED" => 0,
            "SPAN_KIND_INTERNAL" => 1,
            "SPAN_KIND_SERVER" => 2,
            "SPAN_KIND_CLIENT" => 3,
            "SPAN_KIND_PRODUCER" => 4,
            "SPAN_KIND_CONSUMER" => 5,
            _ => return Ok(None),
        };
        return Ok(Some(JsonValue::Number(serde_json::Number::from(parsed))));
    }

    if key == otlp::CODE {
        let number = match value {
            "STATUS_CODE_UNSET" => serde_json::Number::from(0),
            "STATUS_CODE_OK" => serde_json::Number::from(1),
            "STATUS_CODE_ERROR" => serde_json::Number::from(2),
            _ => match value.parse::<i32>() {
                Ok(parsed) => serde_json::Number::from(parsed),
                Err(_) => return Ok(None),
            },
        };

        return Ok(Some(JsonValue::Number(number)));
    }

    if key == otlp::SEVERITY_NUMBER {
        let parsed = match value {
            "SEVERITY_NUMBER_UNSPECIFIED" => 0,
            "SEVERITY_NUMBER_TRACE" => 1,
            "SEVERITY_NUMBER_TRACE2" => 2,
            "SEVERITY_NUMBER_TRACE3" => 3,
            "SEVERITY_NUMBER_TRACE4" => 4,
            "SEVERITY_NUMBER_DEBUG" => 5,
            "SEVERITY_NUMBER_DEBUG2" => 6,
            "SEVERITY_NUMBER_DEBUG3" => 7,
            "SEVERITY_NUMBER_DEBUG4" => 8,
            "SEVERITY_NUMBER_INFO" => 9,
            "SEVERITY_NUMBER_INFO2" => 10,
            "SEVERITY_NUMBER_INFO3" => 11,
            "SEVERITY_NUMBER_INFO4" => 12,
            "SEVERITY_NUMBER_WARN" => 13,
            "SEVERITY_NUMBER_WARN2" => 14,
            "SEVERITY_NUMBER_WARN3" => 15,
            "SEVERITY_NUMBER_WARN4" => 16,
            "SEVERITY_NUMBER_ERROR" => 17,
            "SEVERITY_NUMBER_ERROR2" => 18,
            "SEVERITY_NUMBER_ERROR3" => 19,
            "SEVERITY_NUMBER_ERROR4" => 20,
            "SEVERITY_NUMBER_FATAL" => 21,
            "SEVERITY_NUMBER_FATAL2" => 22,
            "SEVERITY_NUMBER_FATAL3" => 23,
            "SEVERITY_NUMBER_FATAL4" => 24,
            _ => return Ok(None),
        };
        return Ok(Some(JsonValue::Number(serde_json::Number::from(parsed))));
    }

    if key == otlp::AGGREGATION_TEMPORALITY {
        let parsed = match value {
            "AGGREGATION_TEMPORALITY_UNSPECIFIED" => 0,
            "AGGREGATION_TEMPORALITY_DELTA" => 1,
            "AGGREGATION_TEMPORALITY_CUMULATIVE" => 2,
            _ => return Ok(None),
        };
        return Ok(Some(JsonValue::Number(serde_json::Number::from(parsed))));
    }

    if key == otlp::ARRAY_VALUE && value.is_empty() {
        // handled via recursive structure, no special casing required
        return Ok(None);
    }

    Ok(None)
}

/// Convert camelCase to snake_case
fn camel_to_snake_case(input: &str) -> String {
    let mut result = String::with_capacity(input.len() + 4);
    let mut prev_underscore = false;
    for ch in input.chars() {
        if ch.is_ascii_uppercase() {
            if !result.is_empty() && !prev_underscore {
                result.push('_');
            }
            result.push(ch.to_ascii_lowercase());
            prev_underscore = false;
        } else {
            prev_underscore = ch == '_';
            result.push(ch);
        }
    }
    result
}

fn snake_to_pascal_case(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    let mut capitalize_next = true;
    for ch in input.chars() {
        if ch == '_' {
            capitalize_next = true;
            continue;
        }
        if capitalize_next {
            result.push(ch.to_ascii_uppercase());
            capitalize_next = false;
        } else {
            result.push(ch);
        }
    }
    result
}
