// JSON normalization for OTLP canonical JSON format
//
// Handles conversion from canonical OTLP JSON (camelCase, string numbers)
// to the format expected by prost-generated structs (snake_case, actual numbers)

use anyhow::{anyhow, Context, Result};
use hex::FromHex;
use serde_json::Value as JsonValue;
use std::borrow::Cow;
use std::mem;

// Field name constants for JSON normalization
const U64_FIELDS: &[&str] = &["time_unix_nano", "observed_time_unix_nano"];
const U32_FIELDS: &[&str] = &["dropped_attributes_count", "flags", "trace_flags"];
const I64_FIELDS: &[&str] = &["int_value"];
const F64_FIELDS: &[&str] = &["double_value"];
const ANYVALUE_VARIANTS: &[&str] = &[
    "string_value",
    "bool_value",
    "int_value",
    "double_value",
    "array_value",
    "kvlist_value",
    "bytes_value",
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
            if let Some("log_records") = key_hint {
                map.entry("dropped_attributes_count".to_string())
                    .or_insert_with(|| JsonValue::Number(serde_json::Number::from(0u32)));
                map.entry("flags".to_string())
                    .or_insert_with(|| JsonValue::Number(serde_json::Number::from(0u32)));
                map.entry("observed_time_unix_nano".to_string())
                    .or_insert_with(|| JsonValue::Number(serde_json::Number::from(0u64)));
                map.entry("time_unix_nano".to_string())
                    .or_insert_with(|| JsonValue::Number(serde_json::Number::from(0u64)));
                map.entry("severity_number".to_string())
                    .or_insert_with(|| JsonValue::Number(serde_json::Number::from(0i32)));
                map.entry("severity_text".to_string())
                    .or_insert_with(|| JsonValue::String(String::new()));
                map.entry("attributes".to_string())
                    .or_insert_with(|| JsonValue::Array(Vec::new()));
                map.entry("trace_id".to_string())
                    .or_insert_with(|| JsonValue::Array(Vec::new()));
                map.entry("span_id".to_string())
                    .or_insert_with(|| JsonValue::Array(Vec::new()));
            }

            if let Some("scope_logs") = key_hint {
                map.entry("schema_url".to_string())
                    .or_insert_with(|| JsonValue::String(String::new()));
            }

            if let Some("resource_logs") = key_hint {
                map.entry("schema_url".to_string())
                    .or_insert_with(|| JsonValue::String(String::new()));
            }

            if let Some("resource") = key_hint {
                map.entry("dropped_attributes_count".to_string())
                    .or_insert_with(|| JsonValue::Number(serde_json::Number::from(0u32)));
                map.entry("attributes".to_string())
                    .or_insert_with(|| JsonValue::Array(Vec::new()));
            }

            if let Some("scope") = key_hint {
                map.entry("dropped_attributes_count".to_string())
                    .or_insert_with(|| JsonValue::Number(serde_json::Number::from(0u32)));
                map.entry("name".to_string())
                    .or_insert_with(|| JsonValue::String(String::new()));
                map.entry("version".to_string())
                    .or_insert_with(|| JsonValue::String(String::new()));
                map.entry("attributes".to_string())
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
    if matches!(key, "trace_id" | "span_id")
        && value.len() % 2 == 0
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

    if key == "array_value" && value.is_empty() {
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
