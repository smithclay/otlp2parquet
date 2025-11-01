// JSON normalization for OTLP canonical JSON format
//
// Handles conversion from canonical OTLP JSON (camelCase, string numbers)
// to the format expected by prost-generated structs (snake_case, actual numbers)

use anyhow::{anyhow, Context, Result};
use hex::FromHex;
use serde_json::Value as JsonValue;
use std::borrow::Cow;
use std::mem;

use super::field_names::otlp;

// Field name constants for JSON normalization
const U64_FIELDS: &[&str] = &[otlp::TIME_UNIX_NANO, otlp::OBSERVED_TIME_UNIX_NANO];
const U32_FIELDS: &[&str] = &[
    otlp::DROPPED_ATTRIBUTES_COUNT,
    otlp::FLAGS,
    otlp::TRACE_FLAGS,
];
const I64_FIELDS: &[&str] = &[otlp::INT_VALUE];
const F64_FIELDS: &[&str] = &[otlp::DOUBLE_VALUE];
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

            if let Some(otlp::RESOURCE_LOGS) = key_hint {
                map.entry(otlp::SCHEMA_URL.to_string())
                    .or_insert_with(|| JsonValue::String(String::new()));
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

    // Convert hex-encoded trace/span IDs to byte arrays
    if matches!(key, k if k == otlp::TRACE_ID || k == otlp::SPAN_ID)
        && value.len().is_multiple_of(2)
        && value.chars().all(|c| c.is_ascii_hexdigit())
    {
        let bytes = Vec::from_hex(value).with_context(|| {
            format!(
                "Failed to decode hex string '{}' for field '{}'",
                value, key
            )
        })?;
        let json_value =
            serde_json::to_value(bytes).context("Failed to encode OTLP id bytes as JSON array")?;
        return Ok(Some(json_value));
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

/// Convert snake_case to PascalCase
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
