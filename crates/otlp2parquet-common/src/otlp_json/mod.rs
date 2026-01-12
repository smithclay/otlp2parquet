//! OTLP JSON normalization utilities.

pub mod field_names;
pub mod json_normalizer;

pub use json_normalizer::{
    count_skipped_metric_data_points, normalise_json_value, normalize_json_bytes, MetricSkipCounts,
};
