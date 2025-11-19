//! OTLP to Parquet writer with icepick integration
//!
//! This crate provides a unified writer interface for writing OTLP data
//! to Parquet files with optional Apache Iceberg catalog integration.

mod init;
mod platform;
mod table_mapping;
mod writer;

pub use init::{
    initialize_cloudflare_writer, initialize_lambda_writer, initialize_server_writer,
    initialize_writer,
};
pub use platform::{detect_platform, Platform};
pub use writer::{IcepickWriter, OtlpWriter, WriteResult};

// Re-export commonly used types
pub use icepick;
pub use otlp2parquet_core;
