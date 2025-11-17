//! Shared types used across storage and iceberg crates
//!
//! These types are defined here to avoid circular dependencies

use std::sync::Arc;

/// OpenTelemetry signal types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SignalType {
    /// Logs signal
    Logs,
    /// Traces signal
    Traces,
    /// Metrics signal
    Metrics,
}

/// Blake3 content hash for deduplication
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Blake3Hash([u8; 32]);

impl Blake3Hash {
    pub fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }
}

/// Result of writing a Parquet file
///
/// Contains metadata needed for both storage tracking and Iceberg catalog commits
#[derive(Clone)]
pub struct ParquetWriteResult {
    /// Path where the file was written
    pub path: String,
    /// Blake3 content hash
    pub hash: Blake3Hash,
    /// File size in bytes
    pub file_size: u64,
    /// Number of rows written
    pub row_count: i64,
    /// Arrow schema used
    pub arrow_schema: Arc<arrow::datatypes::Schema>,
    /// Parquet metadata (for Iceberg DataFile construction)
    pub parquet_metadata: Arc<parquet::file::metadata::ParquetMetaData>,
    /// Timestamp when write completed
    pub completed_at: chrono::DateTime<chrono::Utc>,
}
