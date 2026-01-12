//! Shared signal processing and error handling for OTLP ingestion
//!
//! This crate provides unified error types and signal processing functions
//! used across Cloudflare Workers, Lambda, and Server platforms.

pub mod codec;
pub mod error;
pub mod processor;

pub use codec::{
    decode_jsonl_metrics, decode_jsonl_values, decode_logs_values, decode_metrics_json,
    decode_metrics_values, decode_traces_values, extract_service_name, first_timestamp_micros,
    group_values_by_service, merge_skipped, report_skipped_metrics, to_records_format,
    value_to_i64,
};
pub use error::OtlpError;
pub use processor::{process_logs, process_metrics, process_traces, ProcessingResult};
