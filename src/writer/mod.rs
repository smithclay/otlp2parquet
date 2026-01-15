//! OTLP to Parquet writer.
//!
//! Provides a unified writer interface for writing OTLP data to Parquet files.

// Allow large error types - rich diagnostic messages are more valuable on error paths.
#![allow(clippy::result_large_err)]

mod error;
mod storage;
mod write;

pub use storage::initialize_storage;
pub use write::{write_batch, WriteBatchRequest};
