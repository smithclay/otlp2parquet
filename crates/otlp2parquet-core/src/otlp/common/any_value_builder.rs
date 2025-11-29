// AnyValue struct builder and conversion utilities
//
// Handles the complex logic of converting OTLP AnyValue to Arrow StructBuilder

use otlp2parquet_proto::opentelemetry::proto::common::v1::{any_value, AnyValue};

/// Extract string value from an AnyValue, if it's a string variant
pub(crate) fn any_value_string(any_val: &AnyValue) -> Option<&str> {
    match any_val.value.as_ref()? {
        any_value::Value::StringValue(s) => Some(s.as_str()),
        _ => None,
    }
}
