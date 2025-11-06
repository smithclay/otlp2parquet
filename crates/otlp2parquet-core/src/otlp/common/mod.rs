//! Shared OTLP ingestion helpers used by multiple signal types.

/// Placeholder used when a resource omits `service.name`.
pub const UNKNOWN_SERVICE_NAME: &str = "unknown";

pub mod any_value_builder;
pub mod builder_helpers;
pub mod field_names;
pub mod field_numbers;
pub mod format;
pub mod json_normalizer;

pub use format::{parse_request, InputFormat, JsonNormalizer, OtlpSignalRequest};
