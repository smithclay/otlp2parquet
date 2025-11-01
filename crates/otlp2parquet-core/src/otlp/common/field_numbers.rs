//! Protocol field number constants for OTLP protobuf messages.
//!
//! These constants define the field numbers used in OpenTelemetry Protocol (OTLP)
//! protobuf messages. They are primarily used for validation, error messages, and
//! low-level protobuf parsing.
//!
//! Reference: https://github.com/open-telemetry/opentelemetry-proto
//! Inspired by: https://github.com/open-telemetry/otel-arrow/tree/main/rust

/// Protobuf wire type constants
pub mod wire_types {
    /// Variable-length integer (int32, int64, uint32, uint64, sint32, sint64, bool, enum)
    pub const VARINT: u64 = 0;
    /// 64-bit fixed-length (fixed64, sfixed64, double)
    pub const FIXED64: u64 = 1;
    /// Length-delimited (string, bytes, embedded messages, packed repeated fields)
    pub const LEN: u64 = 2;
    /// 32-bit fixed-length (fixed32, sfixed32, float)
    pub const FIXED32: u64 = 5;
}

/// Common OTLP field numbers shared across logs, traces, and metrics
pub mod common {
    /// AnyValue string_value field
    pub const ANY_VALUE_STRING_VALUE: u64 = 1;
    /// AnyValue bool_value field
    pub const ANY_VALUE_BOOL_VALUE: u64 = 2;
    /// AnyValue int_value field
    pub const ANY_VALUE_INT_VALUE: u64 = 3;
    /// AnyValue double_value field
    pub const ANY_VALUE_DOUBLE_VALUE: u64 = 4;
    /// AnyValue array_value field
    pub const ANY_VALUE_ARRAY_VALUE: u64 = 5;
    /// AnyValue kvlist_value field (key-value list for struct/map types)
    pub const ANY_VALUE_KVLIST_VALUE: u64 = 6;
    /// AnyValue bytes_value field
    pub const ANY_VALUE_BYTES_VALUE: u64 = 7;

    /// KeyValue key field
    pub const KEY_VALUE_KEY: u64 = 1;
    /// KeyValue value field
    pub const KEY_VALUE_VALUE: u64 = 2;

    /// Resource attributes field
    pub const RESOURCE_ATTRIBUTES: u64 = 1;
    /// Resource dropped_attributes_count field
    pub const RESOURCE_DROPPED_ATTRIBUTES_COUNT: u64 = 2;

    /// InstrumentationScope name field
    pub const SCOPE_NAME: u64 = 1;
    /// InstrumentationScope version field
    pub const SCOPE_VERSION: u64 = 2;
    /// InstrumentationScope attributes field
    pub const SCOPE_ATTRIBUTES: u64 = 3;
    /// InstrumentationScope dropped_attributes_count field
    pub const SCOPE_DROPPED_ATTRIBUTES_COUNT: u64 = 4;
}

/// Log-specific OTLP field numbers
pub mod logs {
    /// LogRecord time_unix_nano field
    pub const LOG_RECORD_TIME_UNIX_NANO: u64 = 1;
    /// LogRecord severity_number field
    pub const LOG_RECORD_SEVERITY_NUMBER: u64 = 2;
    /// LogRecord severity_text field
    pub const LOG_RECORD_SEVERITY_TEXT: u64 = 3;
    /// LogRecord body field (AnyValue)
    pub const LOG_RECORD_BODY: u64 = 5;
    /// LogRecord attributes field (repeated KeyValue)
    pub const LOG_RECORD_ATTRIBUTES: u64 = 6;
    /// LogRecord dropped_attributes_count field
    pub const LOG_RECORD_DROPPED_ATTRIBUTES_COUNT: u64 = 7;
    /// LogRecord flags field (W3C trace flags)
    pub const LOG_RECORD_FLAGS: u64 = 8;
    /// LogRecord trace_id field (16 bytes)
    pub const LOG_RECORD_TRACE_ID: u64 = 9;
    /// LogRecord span_id field (8 bytes)
    pub const LOG_RECORD_SPAN_ID: u64 = 10;
    /// LogRecord observed_time_unix_nano field
    pub const LOG_RECORD_OBSERVED_TIME_UNIX_NANO: u64 = 11;

    /// ScopeLogs scope field
    pub const SCOPE_LOGS_SCOPE: u64 = 1;
    /// ScopeLogs log_records field
    pub const SCOPE_LOGS_LOG_RECORDS: u64 = 2;
    /// ScopeLogs schema_url field
    pub const SCOPE_LOGS_SCHEMA_URL: u64 = 3;

    /// ResourceLogs resource field
    pub const RESOURCE_LOGS_RESOURCE: u64 = 1;
    /// ResourceLogs scope_logs field
    pub const RESOURCE_LOGS_SCOPE_LOGS: u64 = 2;
    /// ResourceLogs schema_url field
    pub const RESOURCE_LOGS_SCHEMA_URL: u64 = 3;

    /// ExportLogsServiceRequest resource_logs field
    pub const EXPORT_LOGS_SERVICE_REQUEST_RESOURCE_LOGS: u64 = 1;
}

/// Size constants for OTLP binary fields
pub mod sizes {
    /// Trace ID size in bytes (128-bit)
    pub const TRACE_ID_BYTES: usize = 16;
    /// Span ID size in bytes (64-bit)
    pub const SPAN_ID_BYTES: usize = 8;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wire_type_constants() {
        assert_eq!(wire_types::VARINT, 0);
        assert_eq!(wire_types::FIXED64, 1);
        assert_eq!(wire_types::LEN, 2);
        assert_eq!(wire_types::FIXED32, 5);
    }

    #[test]
    fn test_any_value_field_numbers() {
        assert_eq!(common::ANY_VALUE_STRING_VALUE, 1);
        assert_eq!(common::ANY_VALUE_BOOL_VALUE, 2);
        assert_eq!(common::ANY_VALUE_INT_VALUE, 3);
        assert_eq!(common::ANY_VALUE_DOUBLE_VALUE, 4);
        assert_eq!(common::ANY_VALUE_ARRAY_VALUE, 5);
        assert_eq!(common::ANY_VALUE_KVLIST_VALUE, 6);
        assert_eq!(common::ANY_VALUE_BYTES_VALUE, 7);
    }

    #[test]
    fn test_size_constants() {
        assert_eq!(sizes::TRACE_ID_BYTES, 16);
        assert_eq!(sizes::SPAN_ID_BYTES, 8);
    }
}
