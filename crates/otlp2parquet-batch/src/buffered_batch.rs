// Buffered batch accumulation logic
//
// Accumulates Arrow RecordBatches and merges them when flushing

use anyhow::{bail, Result};
use arrow::array::RecordBatch;
use otlp2parquet_core::otlp::LogMetadata;
use std::time::Instant;

use crate::{BatchConfig, CompletedBatch};

/// Buffered batch accumulating Arrow RecordBatches
#[derive(Debug)]
pub(crate) struct BufferedBatch {
    batches: Vec<RecordBatch>,
    total_rows: usize,
    total_bytes: usize, // Approximate size for flushing decisions
    first_timestamp: i64,
    service_name: String,
    created_at: Instant,
}

impl BufferedBatch {
    pub fn new(metadata: &LogMetadata) -> Self {
        Self {
            batches: Vec::new(),
            total_rows: 0,
            total_bytes: 0,
            first_timestamp: if metadata.first_timestamp_nanos > 0 {
                metadata.first_timestamp_nanos
            } else {
                i64::MAX
            },
            service_name: metadata.service_name.clone(),
            created_at: Instant::now(),
        }
    }

    pub fn add(&mut self, batch: RecordBatch, metadata: &LogMetadata, approx_bytes: usize) {
        if metadata.first_timestamp_nanos > 0 {
            self.first_timestamp = self.first_timestamp.min(metadata.first_timestamp_nanos);
        }
        self.total_rows += metadata.record_count;
        self.total_bytes += approx_bytes;
        self.batches.push(batch);
    }

    pub fn should_flush(&self, cfg: &BatchConfig) -> bool {
        self.total_rows >= cfg.max_rows
            || self.total_bytes >= cfg.max_bytes
            || self.created_at.elapsed() >= cfg.max_age
    }

    pub fn finalize(self) -> Result<CompletedBatch> {
        if self.batches.is_empty() {
            bail!("Cannot finalize empty batch");
        }

        let metadata = LogMetadata {
            service_name: self.service_name,
            first_timestamp_nanos: if self.first_timestamp == i64::MAX {
                0
            } else {
                self.first_timestamp
            },
            record_count: self.total_rows,
        };

        Ok(CompletedBatch {
            batches: self.batches,
            metadata,
        })
    }
}
