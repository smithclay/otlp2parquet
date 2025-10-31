use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ::parquet::arrow::arrow_writer::ArrowWriter;
use anyhow::{Context, Result};
use blake3::Hash as Blake3Hash;
use otlp2parquet_core::otlp::LogMetadata;
use otlp2parquet_core::{self, ProcessingOptions};
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

#[derive(Debug)]
struct BufferedBatch {
    requests: Vec<ExportLogsServiceRequest>,
    total_rows: usize,
    total_bytes: usize,
    first_timestamp: i64,
    service_name: String,
    created_at: Instant,
}

impl BufferedBatch {
    fn new(preview: &RequestPreview) -> Self {
        Self {
            requests: Vec::new(),
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

    fn add(&mut self, req: ExportLogsServiceRequest, preview: &RequestPreview) {
        if preview.first_timestamp > 0 {
            self.first_timestamp = self.first_timestamp.min(preview.first_timestamp);
        }
        self.total_rows += preview.record_count;
        self.total_bytes += preview.encoded_len;
        self.requests.push(req);
    }

    fn should_flush(&self, cfg: &BatchConfig) -> bool {
        self.total_rows >= cfg.max_rows
            || self.total_bytes >= cfg.max_bytes
            || self.created_at.elapsed() >= cfg.max_age
    }

    fn finalize(self, options: &ProcessingOptions) -> Result<CompletedBatch> {
        let schema = otlp2parquet_core::schema::otel_logs_schema_arc();
        let properties = otlp2parquet_core::parquet::writer::writer_properties().clone();
        let cursor = Cursor::new(Vec::with_capacity(self.total_bytes.max(4 * 1024 * 1024)));
        let mut writer = ArrowWriter::try_new(cursor, schema, Some(properties))
            .context("failed to initialise batch arrow writer")?;

        let mut aggregate = LogMetadata {
            service_name: self.service_name.clone(),
            first_timestamp_nanos: if self.first_timestamp == i64::MAX {
                0
            } else {
                self.first_timestamp
            },
            record_count: 0,
        };

        for request in &self.requests {
            let meta =
                otlp2parquet_core::write_request_into_arrow_writer(&mut writer, request, options)?;
            aggregate.record_count += meta.record_count;
            if meta.first_timestamp_nanos > 0 {
                aggregate.first_timestamp_nanos = if aggregate.first_timestamp_nanos == 0 {
                    meta.first_timestamp_nanos
                } else {
                    aggregate
                        .first_timestamp_nanos
                        .min(meta.first_timestamp_nanos)
                };
            }
            if aggregate.service_name == "unknown" && meta.service_name != "unknown" {
                aggregate.service_name = meta.service_name;
            }
        }

        let cursor = writer.into_inner()?;
        let mut bytes = cursor.into_inner();
        bytes.shrink_to_fit();
        let content_hash = blake3::hash(&bytes);

        Ok(CompletedBatch {
            bytes,
            metadata: aggregate,
            content_hash,
        })
    }
}

#[derive(Debug)]
pub struct CompletedBatch {
    pub bytes: Vec<u8>,
    pub metadata: LogMetadata,
    pub content_hash: Blake3Hash,
}

/// Thread-safe batch orchestrator shared across handlers.
pub struct BatchManager {
    config: BatchConfig,
    options: ProcessingOptions,
    inner: Arc<Mutex<HashMap<BatchKey, BufferedBatch>>>,
}

impl BatchManager {
    pub fn new(config: BatchConfig, options: ProcessingOptions) -> Self {
        Self {
            config,
            options,
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn options(&self) -> &ProcessingOptions {
        &self.options
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

        let key = preview.batch_key();
        let mut guard = self.inner.lock();
        let batch = guard
            .entry(key.clone())
            .or_insert_with(|| BufferedBatch::new(&preview));
        batch.add(request, &preview);

        let mut completed = Vec::new();
        if batch.should_flush(&self.config) {
            let batch = guard.remove(&key).expect("batch removed during flush");
            completed.push(batch.finalize(&self.options)?);
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
                completed.push(batch.finalize(&self.options)?);
            }
        }

        Ok(completed)
    }
}

/// Lightweight helper when batching is disabled entirely.
#[derive(Debug, Clone)]
pub struct PassthroughBatcher;

impl PassthroughBatcher {
    pub fn ingest(
        &self,
        request: ExportLogsServiceRequest,
        options: &ProcessingOptions,
    ) -> Result<CompletedBatch> {
        let schema = otlp2parquet_core::schema::otel_logs_schema_arc();
        let properties = otlp2parquet_core::parquet::writer::writer_properties().clone();
        let cursor = Cursor::new(Vec::new());
        let mut writer = ArrowWriter::try_new(cursor, schema, Some(properties))?;
        let metadata =
            otlp2parquet_core::write_request_into_arrow_writer(&mut writer, &request, options)?;
        let cursor = writer.into_inner()?;
        let bytes = cursor.into_inner();
        let content_hash = blake3::hash(&bytes);

        Ok(CompletedBatch {
            bytes,
            metadata,
            content_hash,
        })
    }
}
