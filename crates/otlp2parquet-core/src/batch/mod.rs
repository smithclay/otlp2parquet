// otlp2parquet-batch - Optimization layer for batching
//
// Accumulates OTLP requests in memory and merges them into larger Arrow batches.
// This reduces the number of storage writes and improves compression efficiency.
//

pub mod ipc;

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use crate::otlp::traces::{TraceArrowConverter, TraceMetadata, TraceRequest};
use crate::otlp::LogMetadata;
use crate::{convert_request_to_arrow_with_capacity, estimate_request_row_count};
use anyhow::{anyhow, Context, Result};
use arrow::array::RecordBatch;
use otlp2parquet_proto::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;
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

impl BatchMetadata for TraceMetadata {
    fn service_name(&self) -> &Arc<str> {
        &self.service_name
    }

    fn first_timestamp_micros(&self) -> i64 {
        self.first_timestamp_micros
    }

    fn record_count(&self) -> usize {
        self.span_count
    }

    fn aggregate(service_name: Arc<str>, first_timestamp_micros: i64, record_count: usize) -> Self {
        Self {
            service_name,
            first_timestamp_micros,
            span_count: record_count,
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

/// Log-specific signal processor.
#[derive(Debug, Clone, Copy, Default)]
pub struct LogSignalProcessor;

impl SignalProcessor for LogSignalProcessor {
    type Request = ExportLogsServiceRequest;
    type Metadata = LogMetadata;

    fn estimate_row_count(request: &Self::Request) -> usize {
        estimate_request_row_count(request)
    }

    fn convert_request(
        request: &Self::Request,
        capacity_hint: usize,
    ) -> Result<(Vec<RecordBatch>, Self::Metadata)> {
        let (batch, metadata) = convert_request_to_arrow_with_capacity(request, capacity_hint)
            .context("Failed to convert OTLP request to Arrow")?;
        Ok((vec![batch], metadata))
    }
}

/// Trace-specific signal processor placeholder.
#[derive(Debug, Clone, Copy, Default)]
pub struct TraceSignalProcessor;

impl SignalProcessor for TraceSignalProcessor {
    type Request = TraceRequest;
    type Metadata = TraceMetadata;

    fn estimate_row_count(request: &Self::Request) -> usize {
        request
            .resource_spans
            .iter()
            .map(|resource_spans| {
                resource_spans
                    .scope_spans
                    .iter()
                    .map(|scope_spans| scope_spans.spans.len())
                    .sum::<usize>()
            })
            .sum()
    }

    fn convert_request(
        request: &Self::Request,
        _capacity_hint: usize,
    ) -> Result<(Vec<RecordBatch>, Self::Metadata)> {
        let (batches, metadata) = TraceArrowConverter::convert(request)?;
        Ok((batches, metadata))
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

/// Lightweight helper when batching is disabled entirely.
///
/// Converts each OTLP request directly to an Arrow batch without accumulation.
#[derive(Debug, Clone)]
pub struct PassthroughBatcher<P: SignalProcessor = LogSignalProcessor>(PhantomData<P>);

impl<P: SignalProcessor> Default for PassthroughBatcher<P> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<P: SignalProcessor> PassthroughBatcher<P> {
    pub fn ingest(&self, request: &P::Request) -> Result<CompletedBatch<P::Metadata>> {
        let capacity_hint = P::estimate_row_count(request);
        let (batches, metadata) = P::convert_request(request, capacity_hint)?;

        Ok(CompletedBatch { batches, metadata })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use otlp2parquet_proto::opentelemetry::proto::common::v1::{any_value, AnyValue, KeyValue};
    use otlp2parquet_proto::opentelemetry::proto::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use otlp2parquet_proto::opentelemetry::proto::resource::v1::Resource;
    use prost::Message;

    fn create_test_request(service_name: &str, record_count: usize) -> ExportLogsServiceRequest {
        let mut records = Vec::new();
        for i in 0..record_count {
            records.push(LogRecord {
                time_unix_nano: 1_000_000_000 + i as u64,
                body: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(format!("log {}", i))),
                }),
                ..Default::default()
            });
        }

        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(service_name.to_string())),
                        }),
                    }],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    log_records: records,
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    #[test]
    fn test_passthrough_batcher() {
        let batcher = PassthroughBatcher::<LogSignalProcessor>::default();
        let request = create_test_request("test-service", 10);

        let result = batcher.ingest(&request);
        assert!(result.is_ok());

        let completed = result.unwrap();
        assert_eq!(completed.batches[0].num_rows(), 10);
        assert_eq!(completed.metadata.service_name.as_ref(), "test-service");
        assert_eq!(completed.metadata.record_count, 10);
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
        let request1 = create_test_request("test-service", 10);
        let approx1 = request1.encoded_len();
        let (completed1, _meta1) = manager.ingest(&request1, approx1).unwrap();
        assert_eq!(completed1.len(), 0); // Not flushed yet

        // Second request - should not flush (total 20 rows)
        let request2 = create_test_request("test-service", 10);
        let approx2 = request2.encoded_len();
        let (completed2, _meta2) = manager.ingest(&request2, approx2).unwrap();
        assert_eq!(completed2.len(), 0); // Still not flushed

        // Third test with smaller limit - should flush when hitting threshold
        let config_small = BatchConfig {
            max_rows: 20,
            max_bytes: 1024 * 1024,
            max_age: Duration::from_secs(10),
        };
        let manager_small = BatchManager::<LogSignalProcessor>::new(config_small);

        let req1 = create_test_request("test-service", 10);
        let approx_small_1 = req1.encoded_len();
        let (c1, _) = manager_small.ingest(&req1, approx_small_1).unwrap();
        assert_eq!(c1.len(), 0); // 10 rows < 20, no flush

        let req2 = create_test_request("test-service", 10);
        let approx_small_2 = req2.encoded_len();
        let (c2, _) = manager_small.ingest(&req2, approx_small_2).unwrap();
        assert_eq!(c2.len(), 1); // 10 + 10 = 20 rows, should flush!
        assert_eq!(
            c2[0].batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            20
        );
    }
}
