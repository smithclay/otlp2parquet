//! Shared signal processing and error handling for OTLP ingestion
//!
//! This crate provides unified error types and signal processing functions
//! used across Cloudflare Workers, Lambda, and Server platforms.

pub mod error;
pub mod processor;

pub use error::OtlpError;
pub use processor::{ProcessingResult, ProcessorConfig};
