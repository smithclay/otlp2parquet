// Arrow builder construction helpers
//
// Provides utilities for creating Arrow builders with the correct schema

/// Size of OpenTelemetry TraceId in bytes (128 bits)
pub(crate) const TRACE_ID_SIZE: i32 = 16;

/// Size of OpenTelemetry SpanId in bytes (64 bits)
pub(crate) const SPAN_ID_SIZE: i32 = 8;
