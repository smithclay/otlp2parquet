//! Shared signal processing and error handling for OTLP ingestion
//!
//! This crate provides unified error types and signal processing functions
//! used across Cloudflare Workers, Lambda, and Server platforms.

pub mod codec;
pub mod error;
pub mod processor;

pub use codec::{
    decode_logs_partitioned, decode_metrics_partitioned, decode_traces_partitioned,
    report_skipped_metrics, PartitionedBatch, PartitionedMetrics, ServiceGroupedBatches,
    SkippedMetrics,
};
// Re-export InputFormat from otlp2records
pub use error::OtlpError;
pub use otlp2records::InputFormat;
pub use processor::{process_logs, process_metrics, process_traces, ProcessingResult};
