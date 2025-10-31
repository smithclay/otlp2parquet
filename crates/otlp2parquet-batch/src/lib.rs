// otlp2parquet-batch - Optimization layer for batching
//
// Accumulates OTLP requests in memory and merges them into larger Arrow batches.
// This reduces the number of storage writes and improves compression efficiency.
//
// Philosophy (Fred Brooks): This is the "optimization" layer between essence (core)
// and accident (storage). It's optional - systems can bypass batching entirely.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use otlp2parquet_core::otlp::LogMetadata;
use otlp2parquet_core::{parse_otlp_to_arrow, InputFormat};
use otlp2parquet_proto::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;
use otlp2parquet_proto::opentelemetry::proto::common::v1::any_value;
use parking_lot::Mutex;
use prost::Message;

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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct BatchKey {
    service: String,
    hour_bucket: i64,
}

#[derive(Debug, Clone)]
struct RequestPreview {
    service_name: String,
    first_timestamp: i64,
    record_count: usize,
    encoded_len: usize,
}

impl RequestPreview {
    fn from_request(req: &ExportLogsServiceRequest) -> Self {
        let mut service_name = String::new();
        let mut first_ts = 0_i64;
        let mut record_count = 0_usize;

        for resource_logs in &req.resource_logs {
            if let Some(resource) = &resource_logs.resource {
                for attr in &resource.attributes {
                    if attr.key == "service.name" {
                        if let Some(any_value::Value::StringValue(value)) =
                            attr.value.as_ref().and_then(|v| v.value.as_ref())
                        {
                            service_name = value.clone();
                        }
                    }
                }
            }

            for scope_logs in &resource_logs.scope_logs {
                for record in &scope_logs.log_records {
                    record_count += 1;
                    let ts = record.time_unix_nano as i64;
                    if ts > 0 {
                        if first_ts == 0 {
                            first_ts = ts;
                        } else {
                            first_ts = first_ts.min(ts);
                        }
                    }
                }
            }
        }

        if service_name.is_empty() {
            service_name = "unknown".to_string();
        }

        Self {
            service_name,
            first_timestamp: first_ts,
            record_count,
            encoded_len: req.encoded_len(),
        }
    }

    fn batch_key(&self) -> BatchKey {
        let hour_bucket = if self.first_timestamp > 0 {
            self.first_timestamp / 3_600_000_000_000
        } else {
            0
        };

        BatchKey {
            service: self.service_name.clone(),
            hour_bucket,
        }
    }
}

/// Buffered batch accumulating Arrow RecordBatches
#[derive(Debug)]
struct BufferedBatch {
    batches: Vec<RecordBatch>,
    total_rows: usize,
    total_bytes: usize, // Approximate size for flushing decisions
    first_timestamp: i64,
    service_name: String,
    created_at: Instant,
}

impl BufferedBatch {
    fn new(preview: &RequestPreview) -> Self {
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

    fn add(&mut self, batch: RecordBatch, preview: &RequestPreview) {
        if preview.first_timestamp > 0 {
            self.first_timestamp = self.first_timestamp.min(preview.first_timestamp);
        }
        self.total_rows += preview.record_count;
        self.total_bytes += preview.encoded_len; // Approximate
        self.batches.push(batch);
    }

    fn should_flush(&self, cfg: &BatchConfig) -> bool {
        self.total_rows >= cfg.max_rows
            || self.total_bytes >= cfg.max_bytes
            || self.created_at.elapsed() >= cfg.max_age
    }

    fn finalize(self) -> Result<CompletedBatch> {
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

/// Completed batch ready for storage
///
/// Contains merged Arrow RecordBatch + metadata.
/// Hashing and serialization happen in the storage layer.
#[derive(Debug)]
pub struct CompletedBatch {
    pub batch: RecordBatch,
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
    ) -> Result<(Vec<CompletedBatch>, LogMetadata)> {
        let preview = RequestPreview::from_request(&request);
        if preview.record_count == 0 {
            return Ok((
                Vec::new(),
                LogMetadata {
                    service_name: preview.service_name,
                    first_timestamp_nanos: preview.first_timestamp,
                    record_count: 0,
                },
            ));
        }

        // Convert OTLP request to Arrow batch
        let encoded = request.encode_to_vec();
        let (batch, _meta) = parse_otlp_to_arrow(&encoded, InputFormat::Protobuf)
            .context("Failed to parse OTLP request to Arrow")?;

        let key = preview.batch_key();
        let mut guard = self.inner.lock();
        let buffered = guard
            .entry(key.clone())
            .or_insert_with(|| BufferedBatch::new(&preview));
        buffered.add(batch, &preview);

        let mut completed = Vec::new();
        if buffered.should_flush(&self.config) {
            let batch = guard.remove(&key).expect("batch removed during flush");
            completed.push(batch.finalize()?);
        }

        Ok((
            completed,
            LogMetadata {
                service_name: preview.service_name,
                first_timestamp_nanos: preview.first_timestamp,
                record_count: preview.record_count,
            },
        ))
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
        // Encode request to bytes for parsing
        let encoded = request.encode_to_vec();

        // Parse to Arrow using new core API
        let (batch, metadata) = parse_otlp_to_arrow(&encoded, InputFormat::Protobuf)
            .context("Failed to parse OTLP request to Arrow")?;

        Ok(CompletedBatch { batch, metadata })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use otlp2parquet_proto::opentelemetry::proto::common::v1::{AnyValue, KeyValue};
    use otlp2parquet_proto::opentelemetry::proto::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use otlp2parquet_proto::opentelemetry::proto::resource::v1::Resource;

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
        assert_eq!(completed.batch.num_rows(), 10);
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
        let (completed1, _meta1) = manager.ingest(request1).unwrap();
        assert_eq!(completed1.len(), 0); // Not flushed yet

        // Second request - should not flush (total 20 rows)
        let request2 = create_test_request("test-service", 10);
        let (completed2, _meta2) = manager.ingest(request2).unwrap();
        assert_eq!(completed2.len(), 0); // Still not flushed

        // Third test with smaller limit - should flush when hitting threshold
        let config_small = BatchConfig {
            max_rows: 20,
            max_bytes: 1024 * 1024,
            max_age: Duration::from_secs(10),
        };
        let manager_small = BatchManager::new(config_small);

        let req1 = create_test_request("test-service", 10);
        let (c1, _) = manager_small.ingest(req1).unwrap();
        assert_eq!(c1.len(), 0); // 10 rows < 20, no flush

        let req2 = create_test_request("test-service", 10);
        let (c2, _) = manager_small.ingest(req2).unwrap();
        assert_eq!(c2.len(), 1); // 10 + 10 = 20 rows, should flush!
        assert_eq!(c2[0].batch.num_rows(), 20);
    }
}
