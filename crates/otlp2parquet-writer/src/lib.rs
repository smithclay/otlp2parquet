//! OTLP to Parquet writer with icepick integration
//!
//! This crate provides a unified writer interface for writing OTLP data
//! to Parquet files with optional Apache Iceberg catalog integration.

mod catalog;
mod error;
mod storage;
mod table_mapping;
mod write;

pub use catalog::{ensure_namespace, initialize_catalog, CatalogConfig, CatalogType};
pub use error::{Result, WriterError};
pub use storage::initialize_storage;
pub use table_mapping::table_name_for_signal;
pub use write::{write_batch, RetryPolicy, WriteBatchRequest};

// Re-export commonly used types for convenience
pub use icepick;
pub use otlp2parquet_core;
