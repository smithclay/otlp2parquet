//! Core writer types and implementations

use anyhow::Result;
use arrow::array::RecordBatch;
use async_trait::async_trait;
use otlp2parquet_core::SignalType;
use std::sync::Arc;

/// Result of a write operation
#[derive(Clone, Debug)]
pub struct WriteResult {
    /// Path where the file was written
    pub path: String,
    /// File size in bytes
    pub file_size: u64,
    /// Number of rows written
    pub row_count: i64,
    /// Signal type
    pub signal_type: SignalType,
    /// Timestamp when write completed
    pub completed_at: chrono::DateTime<chrono::Utc>,
}

/// Configuration for the writer
#[derive(Clone, Debug)]
pub struct WriterConfig {
    /// Base path for writing files
    pub base_path: String,
    /// Optional catalog configuration
    pub catalog_config: Option<CatalogConfig>,
    /// Whether to enable table caching
    pub enable_table_cache: bool,
}

/// Catalog configuration
#[derive(Clone, Debug)]
pub struct CatalogConfig {
    /// Catalog type (S3Tables, R2Catalog, etc.)
    pub catalog_type: String,
    /// Catalog-specific settings
    pub settings: std::collections::HashMap<String, String>,
}

/// Trait for writing OTLP data to Parquet files
#[async_trait]
pub trait OtlpWriter: Send + Sync {
    /// Write a single RecordBatch to storage
    async fn write_batch(
        &self,
        batch: RecordBatch,
        signal_type: SignalType,
        service_name: &str,
    ) -> Result<WriteResult>;

    /// Write multiple RecordBatches to storage
    async fn write_batches(
        &self,
        batches: Vec<RecordBatch>,
        signal_type: SignalType,
        service_name: &str,
    ) -> Result<Vec<WriteResult>>;
}

/// Icepick-based writer implementation
pub struct IcepickWriter {
    /// Writer configuration
    #[allow(dead_code)]
    config: Arc<WriterConfig>,
    // TODO: Add catalog client field
    // TODO: Add table cache field
}

impl IcepickWriter {
    /// Create a new IcepickWriter
    pub fn new(config: WriterConfig) -> Result<Self> {
        Ok(Self {
            config: Arc::new(config),
        })
    }
}

#[async_trait]
impl OtlpWriter for IcepickWriter {
    async fn write_batch(
        &self,
        _batch: RecordBatch,
        _signal_type: SignalType,
        _service_name: &str,
    ) -> Result<WriteResult> {
        // TODO: Implement
        anyhow::bail!("Not yet implemented")
    }

    async fn write_batches(
        &self,
        _batches: Vec<RecordBatch>,
        _signal_type: SignalType,
        _service_name: &str,
    ) -> Result<Vec<WriteResult>> {
        // TODO: Implement
        anyhow::bail!("Not yet implemented")
    }
}
