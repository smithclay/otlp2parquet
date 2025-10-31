// Buffered batch accumulation logic
//
// Accumulates Arrow RecordBatches and merges them when flushing

use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use otlp2parquet_core::otlp::LogMetadata;
use std::time::Instant;

use crate::preview::RequestPreview;
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
    pub fn new(preview: &RequestPreview) -> Self {
        Self {
            batches: Vec::new(),
            total_rows: 0,
            total_bytes: 0,
            first_timestamp: if preview.first_timestamp > 0 {
                preview.first_timestamp
            } else {
                i64::MAX
            },
            service_name: preview.service_name.clone(),
            created_at: Instant::now(),
        }
    }

    pub fn add(&mut self, batch: RecordBatch, preview: &RequestPreview) {
        if preview.first_timestamp > 0 {
            self.first_timestamp = self.first_timestamp.min(preview.first_timestamp);
        }
        self.total_rows += preview.record_count;
        self.total_bytes += preview.encoded_len; // Approximate
        self.batches.push(batch);
    }

    pub fn should_flush(&self, cfg: &BatchConfig) -> bool {
        self.total_rows >= cfg.max_rows
            || self.total_bytes >= cfg.max_bytes
            || self.created_at.elapsed() >= cfg.max_age
    }

    pub fn finalize(self) -> Result<CompletedBatch> {
        // Merge all accumulated batches using Arrow's efficient concatenation
        let merged_batch = if self.batches.is_empty() {
            anyhow::bail!("Cannot finalize empty batch");
        } else if self.batches.len() == 1 {
            self.batches.into_iter().next().unwrap()
        } else {
            let schema = self.batches[0].schema();
            arrow::compute::concat_batches(&schema, &self.batches)
                .context("Failed to concatenate Arrow batches")?
        };

        let metadata = LogMetadata {
            service_name: self.service_name,
            first_timestamp_nanos: if self.first_timestamp == i64::MAX {
                0
            } else {
                self.first_timestamp
            },
            record_count: merged_batch.num_rows(),
        };

        Ok(CompletedBatch {
            batch: merged_batch,
            metadata,
        })
    }
}
