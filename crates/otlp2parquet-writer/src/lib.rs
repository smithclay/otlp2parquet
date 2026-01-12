//! OTLP to Parquet writer.
//!
//! This crate provides a unified writer interface for writing OTLP data
//! to Parquet files.

// Allow large error types - rich diagnostic messages (including redacted
// credentials for debugging auth failures) are more valuable than smaller
// error sizes on the error path.
#![allow(clippy::result_large_err)]

mod encoding;
mod error;
mod storage;
mod write;

pub use encoding::set_parquet_row_group_size;
pub use error::{Result, WriterError};
pub use storage::{get_operator_clone, initialize_storage};
pub use write::{write_batch, write_multi_batch, WriteBatchRequest, WriteMultiBatchRequest};
