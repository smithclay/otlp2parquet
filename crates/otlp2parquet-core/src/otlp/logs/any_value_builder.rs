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

#[derive(Copy, Clone)]
struct AnyValueFieldIndexes {
    type_idx: usize,
    string_idx: usize,
    bool_idx: usize,
    int_idx: usize,
    double_idx: usize,
    bytes_idx: usize,
    json_idx: usize,
}

/// Append an OTLP AnyValue to an Arrow StructBuilder
///
/// The struct has 7 fields:
/// - Type: string indicating the variant type
/// - StringValue, BoolValue, IntValue, DoubleValue, BytesValue: scalar values
/// - JsonValue: JSON-serialized representation of arrays and maps
#[inline]
pub(super) fn append_any_value(
    builder: &mut StructBuilder,
    any_val: Option<&AnyValue>,
) -> Result<()> {
    const INDEXES: AnyValueFieldIndexes = AnyValueFieldIndexes {
        type_idx: 0,
        string_idx: 1,
        bool_idx: 2,
        int_idx: 3,
        double_idx: 4,
        bytes_idx: 5,
        json_idx: 6,
    };

    match any_val.and_then(|value| value.value.as_ref()) {
        Some(inner) => append_present_any_value(builder, inner, INDEXES),
        None => append_null_any_value(builder, INDEXES),
    }
}

/// Extract string value from an AnyValue, if it's a string variant
pub(super) fn any_value_string(any_val: &AnyValue) -> Option<&str> {
    match any_val.value.as_ref()? {
        any_value::Value::StringValue(s) => Some(s.as_str()),
        _ => None,
    }
}

/// Convert OTLP AnyValue to serde_json::Value for JSON serialization
#[inline]
pub(super) fn any_value_to_json_value(any_val: &AnyValue) -> JsonValue {
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

#[inline]
fn append_present_any_value(
    builder: &mut StructBuilder,
    inner: &any_value::Value,
    indexes: AnyValueFieldIndexes,
) -> Result<()> {
    let mut string_value: Option<&str> = None;
    let mut bool_value: Option<bool> = None;
    let mut int_value: Option<i64> = None;
    let mut double_value: Option<f64> = None;
    let mut bytes_value: Option<&[u8]> = None;
    let mut json_value: Option<String> = None;
    let type_name = match inner {
        any_value::Value::StringValue(s) => {
            string_value = Some(s);
            "string"
        }
        any_value::Value::BoolValue(b) => {
            bool_value = Some(*b);
            "bool"
        }
        any_value::Value::IntValue(i) => {
            int_value = Some(*i);
            "int"
        }
        any_value::Value::DoubleValue(d) => {
            double_value = Some(*d);
            "double"
        }
        any_value::Value::BytesValue(b) => {
            bytes_value = Some(b);
            "bytes"
        }
        any_value::Value::ArrayValue(arr) => {
            let mut values = Vec::with_capacity(arr.values.len());
            for val in &arr.values {
                values.push(any_value_to_json_value(val));
            }
            json_value = Some(serde_json::to_string(&JsonValue::Array(values))?);
            "array"
        }
        any_value::Value::KvlistValue(kv) => {
            let mut map = JsonMap::with_capacity(kv.values.len());
            for entry in &kv.values {
                let value_json = entry
                    .value
                    .as_ref()
                    .map(any_value_to_json_value)
                    .unwrap_or(JsonValue::Null);
                map.insert(entry.key.clone(), value_json);
            }
            json_value = Some(serde_json::to_string(&JsonValue::Object(map))?);
            "kvlist"
        }
    };

    append_required_string(builder, indexes.type_idx, type_name)?;
    append_optional_string(builder, indexes.string_idx, string_value)?;
    append_optional_bool(builder, indexes.bool_idx, bool_value)?;
    append_optional_i64(builder, indexes.int_idx, int_value)?;
    append_optional_f64(builder, indexes.double_idx, double_value)?;
    append_optional_bytes(builder, indexes.bytes_idx, bytes_value)?;
    append_optional_json(builder, indexes.json_idx, json_value.as_deref())?;
    builder.append(true);
    Ok(())
}

#[inline]
fn append_null_any_value(builder: &mut StructBuilder, indexes: AnyValueFieldIndexes) -> Result<()> {
    append_optional_string(builder, indexes.type_idx, None)?;
    append_optional_string(builder, indexes.string_idx, None)?;
    append_optional_bool(builder, indexes.bool_idx, None)?;
    append_optional_i64(builder, indexes.int_idx, None)?;
    append_optional_f64(builder, indexes.double_idx, None)?;
    append_optional_bytes(builder, indexes.bytes_idx, None)?;
    append_optional_json(builder, indexes.json_idx, None)?;
    builder.append(false);
    Ok(())
}

#[inline(always)]
fn append_required_string(builder: &mut StructBuilder, index: usize, value: &str) -> Result<()> {
    let field = builder
        .field_builder::<StringBuilder>(index)
        .ok_or_else(|| {
            anyhow!(
                "schema: expected string field at index {} in AnyValue builder",
                index
            )
        })?;
    field.append_value(value);
    Ok(())
}

#[inline(always)]
fn append_optional_string(
    builder: &mut StructBuilder,
    index: usize,
    value: Option<&str>,
) -> Result<()> {
    let field = builder
        .field_builder::<StringBuilder>(index)
        .ok_or_else(|| {
            anyhow!(
                "schema: expected string field at index {} in AnyValue builder",
                index
            )
        })?;
    match value {
        Some(val) => field.append_value(val),
        None => field.append_null(),
    }
    Ok(())
}

#[inline(always)]
fn append_optional_bool(
    builder: &mut StructBuilder,
    index: usize,
    value: Option<bool>,
) -> Result<()> {
    let field = builder
        .field_builder::<BooleanBuilder>(index)
        .ok_or_else(|| {
            anyhow!(
                "schema: expected bool field at index {} in AnyValue builder",
                index
            )
        })?;
    match value {
        Some(val) => field.append_value(val),
        None => field.append_null(),
    }
    Ok(())
}

#[inline(always)]
fn append_optional_i64(
    builder: &mut StructBuilder,
    index: usize,
    value: Option<i64>,
) -> Result<()> {
    let field = builder
        .field_builder::<Int64Builder>(index)
        .ok_or_else(|| {
            anyhow!(
                "schema: expected int field at index {} in AnyValue builder",
                index
            )
        })?;
    match value {
        Some(val) => field.append_value(val),
        None => field.append_null(),
    }
    Ok(())
}

#[inline(always)]
fn append_optional_f64(
    builder: &mut StructBuilder,
    index: usize,
    value: Option<f64>,
) -> Result<()> {
    let field = builder
        .field_builder::<Float64Builder>(index)
        .ok_or_else(|| {
            anyhow!(
                "schema: expected double field at index {} in AnyValue builder",
                index
            )
        })?;
    match value {
        Some(val) => field.append_value(val),
        None => field.append_null(),
    }
    Ok(())
}

#[inline(always)]
fn append_optional_bytes(
    builder: &mut StructBuilder,
    index: usize,
    value: Option<&[u8]>,
) -> Result<()> {
    let field = builder
        .field_builder::<BinaryBuilder>(index)
        .ok_or_else(|| {
            anyhow!(
                "schema: expected bytes field at index {} in AnyValue builder",
                index
            )
        })?;
    match value {
        Some(val) => field.append_value(val),
        None => field.append_null(),
    }
    Ok(())
}

#[inline(always)]
fn append_optional_json(
    builder: &mut StructBuilder,
    index: usize,
    value: Option<&str>,
) -> Result<()> {
    let field = builder
        .field_builder::<LargeStringBuilder>(index)
        .ok_or_else(|| {
            anyhow!(
                "schema: expected json field at index {} in AnyValue builder",
                index
            )
        })?;
    match value {
        Some(val) => field.append_value(val),
        None => field.append_null(),
    }
    Ok(())
}
