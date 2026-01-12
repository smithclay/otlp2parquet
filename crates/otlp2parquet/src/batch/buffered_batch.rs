// Buffered batch accumulation logic
//
// Accumulates Arrow RecordBatches and merges them when flushing

use anyhow::{bail, Result};
use arrow::array::RecordBatch;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Instant;

use super::{BatchConfig, BatchMetadata, CompletedBatch};

/// Buffered batch accumulating Arrow RecordBatches
#[derive(Debug)]
pub(crate) struct BufferedBatch<M: BatchMetadata> {
    batches: Vec<RecordBatch>,
    total_rows: usize,
    total_bytes: usize, // Approximate size for flushing decisions
    first_timestamp: i64,
    service_name: Arc<str>,
    created_at: Instant,
    _marker: PhantomData<M>,
}

impl<M: BatchMetadata> BufferedBatch<M> {
    pub fn new(metadata: &M) -> Self {
        Self {
            batches: Vec::new(),
            total_rows: 0,
            total_bytes: 0,
            first_timestamp: if metadata.first_timestamp_micros() > 0 {
                metadata.first_timestamp_micros()
            } else {
                i64::MAX
            },
            service_name: Arc::clone(metadata.service_name()),
            created_at: Instant::now(),
            _marker: PhantomData,
        }
    }

    pub fn add_batches(&mut self, batches: Vec<RecordBatch>, metadata: &M, approx_bytes: usize) {
        if metadata.first_timestamp_micros() > 0 {
            self.first_timestamp = self.first_timestamp.min(metadata.first_timestamp_micros());
        }
        self.total_rows += metadata.record_count();
        self.total_bytes += approx_bytes;
        self.batches.extend(batches);
    }

    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    pub fn should_flush(&self, cfg: &BatchConfig) -> bool {
        self.total_rows >= cfg.max_rows
            || self.total_bytes >= cfg.max_bytes
            || self.created_at.elapsed() >= cfg.max_age
    }

    pub fn finalize(self) -> Result<CompletedBatch<M>> {
        if self.batches.is_empty() {
            bail!("Cannot finalize empty batch");
        }

        let metadata = M::aggregate(
            self.service_name,
            if self.first_timestamp == i64::MAX {
                0
            } else {
                self.first_timestamp
            },
            self.total_rows,
        );

        Ok(CompletedBatch {
            batches: self.batches,
            metadata,
        })
    }
}
