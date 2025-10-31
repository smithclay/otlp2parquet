// AnyValue struct builder and conversion utilities
//
// Handles the complex logic of converting OTLP AnyValue to Arrow StructBuilder

use anyhow::{anyhow, Result};
use arrow::array::{
    BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, LargeStringBuilder, StringBuilder,
    StructBuilder,
};
use otlp2parquet_proto::opentelemetry::proto::common::v1::{any_value, AnyValue};
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};

/// Append an OTLP AnyValue to an Arrow StructBuilder
///
/// The struct has 7 fields:
/// - Type: string indicating the variant type
/// - StringValue, BoolValue, IntValue, DoubleValue, BytesValue: scalar values
/// - JsonValue: JSON-serialized representation of arrays and maps
pub(crate) fn append_any_value(
    builder: &mut StructBuilder,
    any_val: Option<&AnyValue>,
) -> Result<()> {
    const TYPE_INDEX: usize = 0;
    const STRING_INDEX: usize = 1;
    const BOOL_INDEX: usize = 2;
    const INT_INDEX: usize = 3;
    const DOUBLE_INDEX: usize = 4;
    const BYTES_INDEX: usize = 5;
    const JSON_INDEX: usize = 6;

    if let Some(any_val) = any_val {
        if let Some(inner) = any_val.value.as_ref() {
            let mut string_value: Option<&str> = None;
            let mut bool_value: Option<bool> = None;
            let mut int_value: Option<i64> = None;
            let mut double_value: Option<f64> = None;
            let mut bytes_value: Option<&[u8]> = None;
            let mut json_value: Option<String> = None;
            let type_name;

            match inner {
                any_value::Value::StringValue(s) => {
                    type_name = "string";
                    string_value = Some(s);
                }
                any_value::Value::BoolValue(b) => {
                    type_name = "bool";
                    bool_value = Some(*b);
                }
                any_value::Value::IntValue(i) => {
                    type_name = "int";
                    int_value = Some(*i);
                }
                any_value::Value::DoubleValue(d) => {
                    type_name = "double";
                    double_value = Some(*d);
                }
                any_value::Value::BytesValue(b) => {
                    type_name = "bytes";
                    bytes_value = Some(b);
                }
                any_value::Value::ArrayValue(arr) => {
                    type_name = "array";
                    let json =
                        JsonValue::Array(arr.values.iter().map(any_value_to_json_value).collect());
                    json_value = Some(serde_json::to_string(&json)?);
                }
                any_value::Value::KvlistValue(kv) => {
                    type_name = "kvlist";
                    let mut map = JsonMap::new();
                    for kv in &kv.values {
                        let value_json = kv
                            .value
                            .as_ref()
                            .map(any_value_to_json_value)
                            .unwrap_or(JsonValue::Null);
                        map.insert(kv.key.clone(), value_json);
                    }
                    json_value = Some(serde_json::to_string(&JsonValue::Object(map))?);
                }
            }

            {
                let type_builder = builder
                    .field_builder::<StringBuilder>(TYPE_INDEX)
                    .ok_or_else(|| anyhow!("schema: Type field missing in AnyValue builder"))?;
                type_builder.append_value(type_name);
            }

            {
                let string_builder = builder
                    .field_builder::<StringBuilder>(STRING_INDEX)
                    .ok_or_else(|| {
                        anyhow!("schema: StringValue field missing in AnyValue builder")
                    })?;
                if let Some(val) = string_value {
                    string_builder.append_value(val);
                } else {
                    string_builder.append_null();
                }
            }

            {
                let bool_builder = builder
                    .field_builder::<BooleanBuilder>(BOOL_INDEX)
                    .ok_or_else(|| {
                        anyhow!("schema: BoolValue field missing in AnyValue builder")
                    })?;
                if let Some(val) = bool_value {
                    bool_builder.append_value(val);
                } else {
                    bool_builder.append_null();
                }
            }

            {
                let int_builder = builder
                    .field_builder::<Int64Builder>(INT_INDEX)
                    .ok_or_else(|| anyhow!("schema: IntValue field missing in AnyValue builder"))?;
                if let Some(val) = int_value {
                    int_builder.append_value(val);
                } else {
                    int_builder.append_null();
                }
            }

            {
                let double_builder = builder
                    .field_builder::<Float64Builder>(DOUBLE_INDEX)
                    .ok_or_else(|| {
                        anyhow!("schema: DoubleValue field missing in AnyValue builder")
                    })?;
                if let Some(val) = double_value {
                    double_builder.append_value(val);
                } else {
                    double_builder.append_null();
                }
            }

            {
                let bytes_builder = builder
                    .field_builder::<BinaryBuilder>(BYTES_INDEX)
                    .ok_or_else(|| {
                        anyhow!("schema: BytesValue field missing in AnyValue builder")
                    })?;
                if let Some(val) = bytes_value {
                    bytes_builder.append_value(val);
                } else {
                    bytes_builder.append_null();
                }
            }

            {
                let json_builder = builder
                    .field_builder::<LargeStringBuilder>(JSON_INDEX)
                    .ok_or_else(|| {
                        anyhow!("schema: JsonValue field missing in AnyValue builder")
                    })?;
                if let Some(val) = json_value {
                    json_builder.append_value(&val);
                } else {
                    json_builder.append_null();
                }
            }

            builder.append(true);
            return Ok(());
        }
    }

    // Null case - append nulls to all fields
    builder
        .field_builder::<StringBuilder>(TYPE_INDEX)
        .ok_or_else(|| anyhow!("schema: Type field missing in AnyValue builder"))?
        .append_null();
    builder
        .field_builder::<StringBuilder>(STRING_INDEX)
        .ok_or_else(|| anyhow!("schema: StringValue field missing in AnyValue builder"))?
        .append_null();
    builder
        .field_builder::<BooleanBuilder>(BOOL_INDEX)
        .ok_or_else(|| anyhow!("schema: BoolValue field missing in AnyValue builder"))?
        .append_null();
    builder
        .field_builder::<Int64Builder>(INT_INDEX)
        .ok_or_else(|| anyhow!("schema: IntValue field missing in AnyValue builder"))?
        .append_null();
    builder
        .field_builder::<Float64Builder>(DOUBLE_INDEX)
        .ok_or_else(|| anyhow!("schema: DoubleValue field missing in AnyValue builder"))?
        .append_null();
    builder
        .field_builder::<BinaryBuilder>(BYTES_INDEX)
        .ok_or_else(|| anyhow!("schema: BytesValue field missing in AnyValue builder"))?
        .append_null();
    builder
        .field_builder::<LargeStringBuilder>(JSON_INDEX)
        .ok_or_else(|| anyhow!("schema: JsonValue field missing in AnyValue builder"))?
        .append_null();
    builder.append(false);
    Ok(())
}

/// Extract string value from an AnyValue, if it's a string variant
pub(crate) fn any_value_string(any_val: &AnyValue) -> Option<&str> {
    match any_val.value.as_ref()? {
        any_value::Value::StringValue(s) => Some(s.as_str()),
        _ => None,
    }
}

/// Convert OTLP AnyValue to serde_json::Value for JSON serialization
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
            JsonValue::Array(arr.values.iter().map(any_value_to_json_value).collect())
        }
        Some(any_value::Value::KvlistValue(kv)) => {
            let mut map = JsonMap::new();
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
