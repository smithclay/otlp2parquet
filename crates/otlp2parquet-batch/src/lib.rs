//! Batching utilities for otlp2parquet.

pub mod batch;
pub mod ipc;

pub use batch::{BatchConfig, BatchManager, CompletedBatch};
