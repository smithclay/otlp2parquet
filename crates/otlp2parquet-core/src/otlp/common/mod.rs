//! Shared OTLP ingestion helpers used by multiple signal types.

/// Placeholder used when a resource omits `service.name`.
pub const UNKNOWN_SERVICE_NAME: &str = "unknown";

pub mod any_value_builder;
pub mod builder_helpers;
pub mod field_names;
pub mod field_numbers;
pub mod format;
pub mod json_normalizer;
pub mod json_writer;

pub use format::{parse_request, InputFormat, JsonNormalizer, OtlpSignalRequest};
pub use json_writer::{body_to_json, keyvalue_to_json, pairs_to_json, string_pairs_to_json};
