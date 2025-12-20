//! OTLP to Parquet writer with icepick integration
//!
//! This crate provides a unified writer interface for writing OTLP data
//! to Parquet files with optional Apache Iceberg catalog integration.

// Allow large error types - rich diagnostic messages (including redacted
// credentials for debugging auth failures) are more valuable than smaller
// error sizes on the error path.
#![allow(clippy::result_large_err)]

mod catalog;
mod error;
mod storage;
mod table_mapping;
mod write;

pub use catalog::{ensure_namespace, initialize_catalog, CatalogConfig, CatalogType};
pub use error::{redact_secret, ErrorCode, Result, WriterError};
pub use storage::{get_operator_clone, initialize_storage};
pub use table_mapping::{set_table_name_overrides, table_name_for_signal};
pub use write::{
    write_batch, write_multi_batch, RetryPolicy, WriteBatchRequest, WriteMultiBatchRequest,
};

// Re-export commonly used types for convenience
pub use icepick;
pub use otlp2parquet_core;
