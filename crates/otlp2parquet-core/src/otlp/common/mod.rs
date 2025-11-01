//! Shared OTLP ingestion helpers used by multiple signal types.

pub mod any_value_builder;
pub mod builder_helpers;
pub mod field_names;
pub mod field_numbers;
pub mod format;
pub mod json_normalizer;

pub use format::{parse_request, InputFormat, JsonNormalizer, OtlpSignalRequest};
