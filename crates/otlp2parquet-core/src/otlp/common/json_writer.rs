//! Zero-copy JSON writing utilities for OTLP attributes.
//!
//! This module provides direct JSON string writing to avoid intermediate
//! `serde_json::Map` allocations and key cloning. Instead of building a
//! `serde_json::Value` and then serializing it, we write directly to a
//! pre-allocated `String` buffer.

use std::borrow::Cow;
use std::fmt::Write;

use otlp2parquet_proto::opentelemetry::proto::common::v1::{any_value, AnyValue, KeyValue};

/// Empty JSON object constant to avoid allocation for empty attribute sets.
pub const EMPTY_JSON_OBJECT: &str = "{}";

/// Write a JSON-encoded object from KeyValue attributes directly to a String buffer.
///
/// This avoids:
/// - Allocating a `serde_json::Map`
/// - Cloning attribute keys
/// - Creating intermediate `serde_json::Value` objects
///
/// Returns `Cow::Borrowed` for empty attributes to avoid allocation.
#[inline]
pub fn keyvalue_to_json(attributes: &[KeyValue]) -> Cow<'static, str> {
    if attributes.is_empty() {
        return Cow::Borrowed(EMPTY_JSON_OBJECT);
    }

    // Pre-allocate: ~64 bytes per attribute is a reasonable estimate
    let mut buf = String::with_capacity(attributes.len() * 64);
    write_keyvalue_json(&mut buf, attributes);
    Cow::Owned(buf)
}

/// Write a JSON-encoded object from key-value pairs with optional AnyValue.
///
/// Used for resource attributes where some fields are extracted.
#[inline]
pub fn pairs_to_json(pairs: &[(&str, Option<&AnyValue>)]) -> Cow<'static, str> {
    if pairs.is_empty() {
        return Cow::Borrowed(EMPTY_JSON_OBJECT);
    }

    let mut buf = String::with_capacity(pairs.len() * 64);
    write_pairs_json(&mut buf, pairs);
    Cow::Owned(buf)
}

/// Write a JSON-encoded object from string key-value pairs.
///
/// Used for metrics resource attributes.
#[inline]
pub fn string_pairs_to_json(pairs: &[(String, String)]) -> Cow<'static, str> {
    if pairs.is_empty() {
        return Cow::Borrowed(EMPTY_JSON_OBJECT);
    }

    let mut buf = String::with_capacity(pairs.len() * 64);
    write_string_pairs_json(&mut buf, pairs);
    Cow::Owned(buf)
}

/// Convert an AnyValue body to JSON string.
///
/// Returns `None` if the body is `None`.
#[inline]
pub fn body_to_json(body: Option<&AnyValue>) -> Option<Cow<'static, str>> {
    body.map(|v| {
        let mut buf = String::with_capacity(256);
        write_any_value(&mut buf, v);
        Cow::Owned(buf)
    })
}

/// Write KeyValue attributes as JSON object to buffer.
fn write_keyvalue_json(buf: &mut String, attributes: &[KeyValue]) {
    buf.push('{');
    let mut first = true;
    for attr in attributes {
        if !first {
            buf.push(',');
        }
        first = false;
        write_json_string(buf, &attr.key);
        buf.push(':');
        match &attr.value {
            Some(v) => write_any_value(buf, v),
            None => buf.push_str("null"),
        }
    }
    buf.push('}');
}

/// Write pairs of (&str, Option<&AnyValue>) as JSON object to buffer.
fn write_pairs_json(buf: &mut String, pairs: &[(&str, Option<&AnyValue>)]) {
    buf.push('{');
    let mut first = true;
    for (key, value) in pairs {
        if !first {
            buf.push(',');
        }
        first = false;
        write_json_string(buf, key);
        buf.push(':');
        match value {
            Some(v) => write_any_value(buf, v),
            None => buf.push_str("null"),
        }
    }
    buf.push('}');
}

/// Write string pairs as JSON object to buffer.
fn write_string_pairs_json(buf: &mut String, pairs: &[(String, String)]) {
    buf.push('{');
    let mut first = true;
    for (key, value) in pairs {
        if !first {
            buf.push(',');
        }
        first = false;
        write_json_string(buf, key);
        buf.push(':');
        write_json_string(buf, value);
    }
    buf.push('}');
}

/// Write an AnyValue as JSON to the buffer.
#[inline]
pub fn write_any_value(buf: &mut String, value: &AnyValue) {
    match value.value.as_ref() {
        Some(any_value::Value::StringValue(s)) => write_json_string(buf, s),
        Some(any_value::Value::BoolValue(b)) => {
            buf.push_str(if *b { "true" } else { "false" });
        }
        Some(any_value::Value::IntValue(i)) => {
            // itoa is faster than write!, but we use write! for simplicity
            let _ = write!(buf, "{}", i);
        }
        Some(any_value::Value::DoubleValue(d)) => {
            if d.is_finite() {
                // Use ryu for fast float formatting if available, otherwise write!
                let _ = write!(buf, "{}", d);
            } else if d.is_nan() {
                // JSON doesn't support NaN, encode as string
                buf.push_str("\"NaN\"");
            } else if d.is_infinite() {
                buf.push_str(if *d > 0.0 {
                    "\"Infinity\""
                } else {
                    "\"-Infinity\""
                });
            }
        }
        Some(any_value::Value::BytesValue(b)) => {
            // Encode as "bytes:N" for compatibility with existing behavior
            let _ = write!(buf, "\"bytes:{}\"", b.len());
        }
        Some(any_value::Value::ArrayValue(arr)) => {
            buf.push('[');
            let mut first = true;
            for val in &arr.values {
                if !first {
                    buf.push(',');
                }
                first = false;
                write_any_value(buf, val);
            }
            buf.push(']');
        }
        Some(any_value::Value::KvlistValue(kv)) => {
            buf.push('{');
            let mut first = true;
            for entry in &kv.values {
                if !first {
                    buf.push(',');
                }
                first = false;
                write_json_string(buf, &entry.key);
                buf.push(':');
                match &entry.value {
                    Some(v) => write_any_value(buf, v),
                    None => buf.push_str("null"),
                }
            }
            buf.push('}');
        }
        None => buf.push_str("null"),
    }
}

/// Write a JSON-escaped string to the buffer.
///
/// This handles all JSON escape sequences per RFC 8259.
#[inline]
fn write_json_string(buf: &mut String, s: &str) {
    buf.push('"');
    for c in s.chars() {
        match c {
            '"' => buf.push_str("\\\""),
            '\\' => buf.push_str("\\\\"),
            '\n' => buf.push_str("\\n"),
            '\r' => buf.push_str("\\r"),
            '\t' => buf.push_str("\\t"),
            // Control characters (U+0000 through U+001F)
            c if c.is_control() => {
                let _ = write!(buf, "\\u{:04x}", c as u32);
            }
            c => buf.push(c),
        }
    }
    buf.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;
    use otlp2parquet_proto::opentelemetry::proto::common::v1::{
        any_value, AnyValue, ArrayValue, KeyValue, KeyValueList,
    };

    fn make_string_value(s: &str) -> AnyValue {
        AnyValue {
            value: Some(any_value::Value::StringValue(s.to_string())),
        }
    }

    fn make_int_value(i: i64) -> AnyValue {
        AnyValue {
            value: Some(any_value::Value::IntValue(i)),
        }
    }

    fn make_kv(key: &str, value: AnyValue) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(value),
        }
    }

    #[test]
    fn test_empty_keyvalue() {
        let result = keyvalue_to_json(&[]);
        assert_eq!(result.as_ref(), "{}");
        assert!(matches!(result, Cow::Borrowed(_)));
    }

    #[test]
    fn test_single_string_attribute() {
        let attrs = vec![make_kv("name", make_string_value("value"))];
        let result = keyvalue_to_json(&attrs);
        assert_eq!(result.as_ref(), r#"{"name":"value"}"#);
    }

    #[test]
    fn test_multiple_attributes() {
        let attrs = vec![
            make_kv("str", make_string_value("hello")),
            make_kv("num", make_int_value(42)),
        ];
        let result = keyvalue_to_json(&attrs);
        assert_eq!(result.as_ref(), r#"{"str":"hello","num":42}"#);
    }

    #[test]
    fn test_escaped_string() {
        let attrs = vec![make_kv(
            "msg",
            make_string_value("line1\nline2\ttab\"quote\\backslash"),
        )];
        let result = keyvalue_to_json(&attrs);
        assert_eq!(
            result.as_ref(),
            r#"{"msg":"line1\nline2\ttab\"quote\\backslash"}"#
        );
    }

    #[test]
    fn test_bool_values() {
        let attrs = vec![
            make_kv(
                "t",
                AnyValue {
                    value: Some(any_value::Value::BoolValue(true)),
                },
            ),
            make_kv(
                "f",
                AnyValue {
                    value: Some(any_value::Value::BoolValue(false)),
                },
            ),
        ];
        let result = keyvalue_to_json(&attrs);
        assert_eq!(result.as_ref(), r#"{"t":true,"f":false}"#);
    }

    #[test]
    fn test_double_value() {
        let attrs = vec![make_kv(
            "val",
            AnyValue {
                value: Some(any_value::Value::DoubleValue(1.23456)),
            },
        )];
        let result = keyvalue_to_json(&attrs);
        assert!(result.contains("1.23456"));
    }

    #[test]
    fn test_array_value() {
        let arr = ArrayValue {
            values: vec![make_int_value(1), make_int_value(2), make_int_value(3)],
        };
        let attrs = vec![make_kv(
            "arr",
            AnyValue {
                value: Some(any_value::Value::ArrayValue(arr)),
            },
        )];
        let result = keyvalue_to_json(&attrs);
        assert_eq!(result.as_ref(), r#"{"arr":[1,2,3]}"#);
    }

    #[test]
    fn test_kvlist_value() {
        let kvlist = KeyValueList {
            values: vec![make_kv("nested", make_string_value("value"))],
        };
        let attrs = vec![make_kv(
            "obj",
            AnyValue {
                value: Some(any_value::Value::KvlistValue(kvlist)),
            },
        )];
        let result = keyvalue_to_json(&attrs);
        assert_eq!(result.as_ref(), r#"{"obj":{"nested":"value"}}"#);
    }

    #[test]
    fn test_null_value() {
        let attrs = vec![KeyValue {
            key: "null_key".to_string(),
            value: None,
        }];
        let result = keyvalue_to_json(&attrs);
        assert_eq!(result.as_ref(), r#"{"null_key":null}"#);
    }

    #[test]
    fn test_bytes_value() {
        let attrs = vec![make_kv(
            "data",
            AnyValue {
                value: Some(any_value::Value::BytesValue(vec![1, 2, 3, 4, 5])),
            },
        )];
        let result = keyvalue_to_json(&attrs);
        assert_eq!(result.as_ref(), r#"{"data":"bytes:5"}"#);
    }

    #[test]
    fn test_pairs_to_json() {
        let val = make_string_value("test");
        let pairs: Vec<(&str, Option<&AnyValue>)> = vec![("key1", Some(&val)), ("key2", None)];
        let result = pairs_to_json(&pairs);
        assert_eq!(result.as_ref(), r#"{"key1":"test","key2":null}"#);
    }

    #[test]
    fn test_string_pairs_to_json() {
        let pairs = vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ];
        let result = string_pairs_to_json(&pairs);
        assert_eq!(result.as_ref(), r#"{"key1":"value1","key2":"value2"}"#);
    }

    #[test]
    fn test_body_to_json_string() {
        let body = make_string_value("log message");
        let result = body_to_json(Some(&body));
        assert_eq!(result.unwrap().as_ref(), r#""log message""#);
    }

    #[test]
    fn test_body_to_json_none() {
        let result = body_to_json(None);
        assert!(result.is_none());
    }

    #[test]
    fn test_control_characters() {
        let attrs = vec![make_kv("ctrl", make_string_value("\x00\x01\x1f"))];
        let result = keyvalue_to_json(&attrs);
        assert_eq!(result.as_ref(), r#"{"ctrl":"\u0000\u0001\u001f"}"#);
    }

    #[test]
    fn test_special_floats() {
        // NaN
        let attrs = vec![make_kv(
            "nan",
            AnyValue {
                value: Some(any_value::Value::DoubleValue(f64::NAN)),
            },
        )];
        let result = keyvalue_to_json(&attrs);
        assert_eq!(result.as_ref(), r#"{"nan":"NaN"}"#);

        // Infinity
        let attrs = vec![make_kv(
            "inf",
            AnyValue {
                value: Some(any_value::Value::DoubleValue(f64::INFINITY)),
            },
        )];
        let result = keyvalue_to_json(&attrs);
        assert_eq!(result.as_ref(), r#"{"inf":"Infinity"}"#);

        // Negative infinity
        let attrs = vec![make_kv(
            "ninf",
            AnyValue {
                value: Some(any_value::Value::DoubleValue(f64::NEG_INFINITY)),
            },
        )];
        let result = keyvalue_to_json(&attrs);
        assert_eq!(result.as_ref(), r#"{"ninf":"-Infinity"}"#);
    }
}
