// AnyValue struct builder and conversion utilities
//
// Handles the complex logic of converting OTLP AnyValue to Arrow StructBuilder

use otlp2parquet_proto::opentelemetry::proto::common::v1::{any_value, AnyValue};
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};

/// Extract string value from an AnyValue, if it's a string variant
pub(crate) fn any_value_string(any_val: &AnyValue) -> Option<&str> {
    match any_val.value.as_ref()? {
        any_value::Value::StringValue(s) => Some(s.as_str()),
        _ => None,
    }
}

/// Convert OTLP AnyValue to serde_json::Value for JSON serialization
#[inline]
pub(crate) fn any_value_to_json_value(any_val: &AnyValue) -> JsonValue {
    match any_val.value.as_ref() {
        Some(any_value::Value::StringValue(s)) => JsonValue::String(s.clone()),
        Some(any_value::Value::BoolValue(b)) => JsonValue::Bool(*b),
        Some(any_value::Value::IntValue(i)) => JsonValue::Number(JsonNumber::from(*i)),
        Some(any_value::Value::DoubleValue(d)) => JsonNumber::from_f64(*d)
            .map(JsonValue::Number)
            .unwrap_or_else(|| JsonValue::String(d.to_string())),
        Some(any_value::Value::BytesValue(b)) => JsonValue::String(format!("bytes:{}", b.len())),
        Some(any_value::Value::ArrayValue(arr)) => {
            let mut values = Vec::with_capacity(arr.values.len());
            for val in &arr.values {
                values.push(any_value_to_json_value(val));
            }
            JsonValue::Array(values)
        }
        Some(any_value::Value::KvlistValue(kv)) => {
            let mut map = JsonMap::with_capacity(kv.values.len());
            for entry in &kv.values {
                let value = entry
                    .value
                    .as_ref()
                    .map(any_value_to_json_value)
                    .unwrap_or(JsonValue::Null);
                map.insert(entry.key.clone(), value);
            }
            JsonValue::Object(map)
        }
        None => JsonValue::Null,
    }
}
