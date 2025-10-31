// OTLP to Arrow conversion
//
// This module handles converting OpenTelemetry Protocol (OTLP) log records
// into Arrow RecordBatches using the ClickHouse-compatible schema.

pub mod format;
pub mod to_arrow;

// Submodules
mod any_value_builder;
mod builder_helpers;
mod json_normalizer;

pub use format::{parse_otlp_request, InputFormat};
pub use to_arrow::{ArrowConverter, LogMetadata};
