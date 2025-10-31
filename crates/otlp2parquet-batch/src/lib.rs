// otlp2parquet-batch - Optimization layer for batching
//
// Accumulates OTLP requests in memory and merges them into larger Arrow batches.
// This reduces the number of storage writes and improves compression efficiency.
//

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use arrow::array::RecordBatch;
use otlp2parquet_core::convert_request_to_arrow;
use otlp2parquet_core::otlp::LogMetadata;
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
    fn from_metadata(metadata: &LogMetadata) -> Self {
        let bucket = if metadata.first_timestamp_nanos > 0 {
            metadata.first_timestamp_nanos / 60_000_000_000
        } else {
            0
        };

        Self {
            service: metadata.service_name.clone(),
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

impl BatchConfig {
    pub fn from_env(default_rows: usize, default_bytes: usize, default_age_secs: u64) -> Self {
        fn parse_env<T: std::str::FromStr>(key: &str) -> Option<T> {
            std::env::var(key).ok()?.parse().ok()
        }

        let max_rows = parse_env("BATCH_MAX_ROWS").unwrap_or(default_rows);
        let max_bytes = parse_env("BATCH_MAX_BYTES").unwrap_or(default_bytes);
        let max_age = parse_env("BATCH_MAX_AGE_SECS")
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(default_age_secs));

        Self {
            max_rows,
            max_bytes,
            max_age,
        }
    }
}

/// Completed batch ready for storage
///
/// Contains merged Arrow RecordBatch + metadata.
/// Hashing and serialization happen in the storage layer.
#[derive(Debug)]
pub struct CompletedBatch {
    pub batches: Vec<RecordBatch>,
    pub metadata: LogMetadata,
}

/// Thread-safe batch orchestrator shared across handlers.
pub struct BatchManager {
    config: BatchConfig,
    inner: Arc<Mutex<HashMap<BatchKey, BufferedBatch>>>,
}

impl BatchManager {
    pub fn new(config: BatchConfig) -> Self {
        Self {
            config,
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn ingest(
        &self,
        request: ExportLogsServiceRequest,
        approx_bytes: usize,
    ) -> Result<(Vec<CompletedBatch>, LogMetadata)> {
        let (batch, metadata) = convert_request_to_arrow(&request)
            .context("Failed to convert OTLP request to Arrow")?;

        if metadata.record_count == 0 {
            return Ok((Vec::new(), metadata));
        }

        let key = BatchKey::from_metadata(&metadata);
        let mut guard = self.inner.lock();
        let buffered = guard
            .entry(key.clone())
            .or_insert_with(|| BufferedBatch::new(&metadata));
        buffered.add(batch, &metadata, approx_bytes);

        let mut completed = Vec::new();
        if buffered.should_flush(&self.config) {
            let batch = guard
                .remove(&key)
                .ok_or_else(|| anyhow!("batch evicted before flush: {:?}", key))?;
            completed.push(batch.finalize()?);
        }

        Ok((completed, metadata))
    }

    pub fn drain_expired(&self) -> Result<Vec<CompletedBatch>> {
        let mut guard = self.inner.lock();
        let mut completed = Vec::new();
        let keys: Vec<BatchKey> = guard
            .iter()
            .filter(|(_, batch)| batch.should_flush(&self.config))
            .map(|(key, _)| key.clone())
            .collect();

        for key in keys {
            if let Some(batch) = guard.remove(&key) {
                completed.push(batch.finalize()?);
            }
        }

        Ok(completed)
    }
}

/// Lightweight helper when batching is disabled entirely.
///
/// Converts each OTLP request directly to an Arrow batch without accumulation.
#[derive(Debug, Clone)]
pub struct PassthroughBatcher;

impl PassthroughBatcher {
    pub fn ingest(&self, request: ExportLogsServiceRequest) -> Result<CompletedBatch> {
        // Parse to Arrow using new core API
        let (batch, metadata) = convert_request_to_arrow(&request)
            .context("Failed to convert OTLP request to Arrow")?;

        Ok(CompletedBatch {
            batches: vec![batch],
            metadata,
        })
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
        let batcher = PassthroughBatcher;
        let request = create_test_request("test-service", 10);

        let result = batcher.ingest(request);
        assert!(result.is_ok());

        let completed = result.unwrap();
        assert_eq!(completed.batches[0].num_rows(), 10);
        assert_eq!(completed.metadata.service_name, "test-service");
        assert_eq!(completed.metadata.record_count, 10);
    }

    #[test]
    fn test_batch_manager_accumulation() {
        let config = BatchConfig {
            max_rows: 100,
            max_bytes: 1024 * 1024,
            max_age: Duration::from_secs(10),
        };
        let manager = BatchManager::new(config);

        // First request - should not flush
        let request1 = create_test_request("test-service", 10);
        let approx1 = request1.encoded_len();
        let (completed1, _meta1) = manager.ingest(request1, approx1).unwrap();
        assert_eq!(completed1.len(), 0); // Not flushed yet

        // Second request - should not flush (total 20 rows)
        let request2 = create_test_request("test-service", 10);
        let approx2 = request2.encoded_len();
        let (completed2, _meta2) = manager.ingest(request2, approx2).unwrap();
        assert_eq!(completed2.len(), 0); // Still not flushed

        // Third test with smaller limit - should flush when hitting threshold
        let config_small = BatchConfig {
            max_rows: 20,
            max_bytes: 1024 * 1024,
            max_age: Duration::from_secs(10),
        };
        let manager_small = BatchManager::new(config_small);

        let req1 = create_test_request("test-service", 10);
        let approx_small_1 = req1.encoded_len();
        let (c1, _) = manager_small.ingest(req1, approx_small_1).unwrap();
        assert_eq!(c1.len(), 0); // 10 rows < 20, no flush

        let req2 = create_test_request("test-service", 10);
        let approx_small_2 = req2.encoded_len();
        let (c2, _) = manager_small.ingest(req2, approx_small_2).unwrap();
        assert_eq!(c2.len(), 1); // 10 + 10 = 20 rows, should flush!
        assert_eq!(
            c2[0].batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            20
        );
    }
}
