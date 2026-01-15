//! In-memory batch accumulation for server mode.
//!
//! Accumulates Arrow batches in memory and merges them into larger Arrow batches.
//! This reduces the number of storage writes and improves compression efficiency.
//!
//! Note: This module provides the batching infrastructure for when `batch.enabled=true`
//! in the server config. Currently the handlers write directly per-request, but this
//! infrastructure is available for future use.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use arrow::array::RecordBatch;
use otlp2records::PartitionedBatch;
use parking_lot::Mutex;

mod buffered_batch;

use buffered_batch::BufferedBatch;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct BatchKey {
    service: String,
    minute_bucket: i64,
}

impl BatchKey {
    fn from_metadata<M: BatchMetadata>(metadata: &M) -> Self {
        let bucket = if metadata.first_timestamp_micros() > 0 {
            // Metadata timestamps are stored in microseconds; bucket by minute in micros.
            metadata.first_timestamp_micros() / 60_000_000
        } else {
            0
        };

        Self {
            service: metadata.service_name().as_ref().to_string(),
            minute_bucket: bucket,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BatchConfig {
    pub max_rows: usize,
    pub max_bytes: usize,
    pub max_age: Duration,
}

/// Metadata extracted during OTLP parsing for log batches.
#[derive(Debug, Clone)]
pub struct LogMetadata {
    pub service_name: Arc<str>,
    // Stored in microseconds to align with Parquet expectations.
    pub first_timestamp_micros: i64,
    pub record_count: usize,
}

/// Metadata required by the batching layer.
pub trait BatchMetadata: Clone {
    fn service_name(&self) -> &Arc<str>;
    /// Stored in microseconds.
    fn first_timestamp_micros(&self) -> i64;
    fn record_count(&self) -> usize;
    fn aggregate(service_name: Arc<str>, first_timestamp_micros: i64, record_count: usize) -> Self;
}

impl BatchMetadata for LogMetadata {
    fn service_name(&self) -> &Arc<str> {
        &self.service_name
    }

    fn first_timestamp_micros(&self) -> i64 {
        self.first_timestamp_micros
    }

    fn record_count(&self) -> usize {
        self.record_count
    }

    fn aggregate(service_name: Arc<str>, first_timestamp_micros: i64, record_count: usize) -> Self {
        Self {
            service_name,
            first_timestamp_micros,
            record_count,
        }
    }
}

/// Signal-specific logic used by the batching layer.
pub trait SignalProcessor {
    type Request;
    type Metadata: BatchMetadata;

    fn estimate_row_count(request: &Self::Request) -> usize;
    fn convert_request(
        request: &Self::Request,
        capacity_hint: usize,
    ) -> Result<(Vec<RecordBatch>, Self::Metadata)>;
}

type BatchIngestResult<M> = Result<(Vec<CompletedBatch<M>>, M)>;

/// Log-specific signal processor using Arrow-native PartitionedBatch.
#[derive(Debug, Clone, Copy, Default)]
pub struct LogSignalProcessor;

impl SignalProcessor for LogSignalProcessor {
    type Request = PartitionedBatch;
    type Metadata = LogMetadata;

    fn estimate_row_count(request: &Self::Request) -> usize {
        request.record_count
    }

    fn convert_request(
        request: &Self::Request,
        _capacity_hint: usize,
    ) -> Result<(Vec<RecordBatch>, Self::Metadata)> {
        let metadata = LogMetadata {
            service_name: Arc::clone(&request.service_name),
            first_timestamp_micros: request.min_timestamp_micros,
            record_count: request.record_count,
        };
        Ok((vec![request.batch.clone()], metadata))
    }
}

/// Completed batch ready for storage
///
/// Contains merged Arrow RecordBatch + metadata.
/// Hashing and serialization happen in the storage layer.
#[derive(Debug)]
pub struct CompletedBatch<M: BatchMetadata = LogMetadata> {
    pub batches: Vec<RecordBatch>,
    pub metadata: M,
}

/// Thread-safe batch orchestrator shared across handlers.
pub struct BatchManager<P: SignalProcessor = LogSignalProcessor> {
    config: BatchConfig,
    inner: Arc<Mutex<BatchState<P>>>,
    _marker: PhantomData<P>,
}

#[derive(Debug)]
struct BatchState<P: SignalProcessor> {
    batches: HashMap<BatchKey, BufferedBatch<P::Metadata>>,
    total_bytes: usize,
}

impl<P: SignalProcessor> BatchManager<P> {
    pub fn new(config: BatchConfig) -> Self {
        Self {
            config,
            inner: Arc::new(Mutex::new(BatchState {
                batches: HashMap::new(),
                total_bytes: 0,
            })),
            _marker: PhantomData,
        }
    }

    pub fn ingest(
        &self,
        request: &P::Request,
        approx_bytes: usize,
    ) -> BatchIngestResult<P::Metadata> {
        let capacity_hint = P::estimate_row_count(request);
        let (batches, metadata) = P::convert_request(request, capacity_hint)?;

        if metadata.record_count() == 0 {
            return Ok((Vec::new(), metadata));
        }

        let key = BatchKey::from_metadata(&metadata);
        let mut guard = self.inner.lock();
        let max_pending_bytes = self
            .config
            .max_bytes
            .saturating_mul(8)
            .max(self.config.max_bytes);

        let prospective_total = guard.total_bytes.saturating_add(approx_bytes);
        if prospective_total > max_pending_bytes {
            anyhow::bail!(
                "backpressure: buffered batches exceed limit ({} > {})",
                prospective_total,
                max_pending_bytes
            );
        }

        // Scope the mutable borrow to avoid holding it across flush/remove.
        let flush_now = {
            let buffered = guard
                .batches
                .entry(key.clone())
                .or_insert_with(|| BufferedBatch::new(&metadata));
            buffered.add_batches(batches, &metadata, approx_bytes);
            buffered.should_flush(&self.config)
        };

        guard.total_bytes = prospective_total;

        let mut completed = Vec::new();
        if flush_now {
            let batch = guard
                .batches
                .remove(&key)
                .ok_or_else(|| anyhow!("batch evicted before flush: {:?}", key))?;
            guard.total_bytes = guard.total_bytes.saturating_sub(batch.total_bytes());
            completed.push(batch.finalize()?);
        }

        drop(guard);

        Ok((completed, metadata))
    }

    pub fn drain_expired(&self) -> Result<Vec<CompletedBatch<P::Metadata>>> {
        let mut guard = self.inner.lock();
        let mut completed = Vec::new();
        let keys: Vec<BatchKey> = guard
            .batches
            .iter()
            .filter(|(_, batch)| batch.should_flush(&self.config))
            .map(|(key, _)| key.clone())
            .collect();

        for key in keys {
            if let Some(batch) = guard.batches.remove(&key) {
                guard.total_bytes = guard.total_bytes.saturating_sub(batch.total_bytes());
                completed.push(batch.finalize()?);
            }
        }

        Ok(completed)
    }

    pub fn drain_all(&self) -> Result<Vec<CompletedBatch<P::Metadata>>> {
        let mut guard = self.inner.lock();
        let drained: Vec<_> = guard.batches.drain().collect();
        guard.total_bytes = 0;
        drop(guard);

        drained
            .into_iter()
            .map(|(_, batch)| batch.finalize())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray, TimestampMillisecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc as StdArc;

    fn create_test_batch(service_name: &str, record_count: usize) -> PartitionedBatch {
        let schema = StdArc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("service_name", DataType::Utf8, true),
            Field::new("severity_number", DataType::Int64, true),
        ]));

        let timestamps: Vec<i64> = (0..record_count)
            .map(|i| 1_700_000_000_000 + i as i64)
            .collect();
        let services: Vec<&str> = vec![service_name; record_count];
        let severities: Vec<i64> = vec![9; record_count];

        let batch = RecordBatch::try_new(
            schema,
            vec![
                StdArc::new(TimestampMillisecondArray::from(timestamps.clone())),
                StdArc::new(StringArray::from(services)),
                StdArc::new(Int64Array::from(severities)),
            ],
        )
        .unwrap();

        PartitionedBatch {
            batch,
            service_name: Arc::from(service_name),
            min_timestamp_micros: timestamps[0] * 1000, // Convert ms to us
            record_count,
        }
    }

    #[test]
    fn test_batch_manager_accumulation() {
        let config = BatchConfig {
            max_rows: 100,
            max_bytes: 1024 * 1024,
            max_age: Duration::from_secs(10),
        };
        let manager = BatchManager::<LogSignalProcessor>::new(config);

        // First request - should not flush
        let request1 = create_test_batch("test-service", 10);
        let approx1 = 320; // Approximate bytes
        let (completed1, _meta1) = manager.ingest(&request1, approx1).unwrap();
        assert_eq!(completed1.len(), 0); // Not flushed yet

        // Second request - should not flush (total 20 rows)
        let request2 = create_test_batch("test-service", 10);
        let approx2 = 320;
        let (completed2, _meta2) = manager.ingest(&request2, approx2).unwrap();
        assert_eq!(completed2.len(), 0); // Still not flushed

        // Third test with smaller limit - should flush when hitting threshold
        let config_small = BatchConfig {
            max_rows: 20,
            max_bytes: 1024 * 1024,
            max_age: Duration::from_secs(10),
        };
        let manager_small = BatchManager::<LogSignalProcessor>::new(config_small);

        let req1 = create_test_batch("test-service", 10);
        let approx_small_1 = 320;
        let (c1, _) = manager_small.ingest(&req1, approx_small_1).unwrap();
        assert_eq!(c1.len(), 0); // 10 rows < 20, no flush

        let req2 = create_test_batch("test-service", 10);
        let approx_small_2 = 320;
        let (c2, _) = manager_small.ingest(&req2, approx_small_2).unwrap();
        assert_eq!(c2.len(), 1); // 10 + 10 = 20 rows, should flush!
        assert_eq!(
            c2[0].batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            20
        );
    }
}
