/// Result of processing a signal request
#[derive(Debug, Clone, PartialEq)]
pub struct ProcessingResult {
    pub paths_written: Vec<String>,
    pub records_processed: usize,
    pub batches_flushed: usize,
}

/// Configuration for signal processing
pub struct ProcessorConfig<'a> {
    pub catalog: Option<&'a dyn otlp2parquet_writer::icepick::catalog::Catalog>,
    pub namespace: &'a str,
    pub snapshot_timestamp_ms: Option<i64>,
    pub retry_policy: RetryPolicy,
}

use crate::error::OtlpError;
use otlp2parquet_core::batch::LogSignalProcessor;
use otlp2parquet_core::{otlp, InputFormat, SignalType};
use otlp2parquet_writer::{RetryPolicy, WriteBatchRequest};

/// Process OTLP logs request
pub async fn process_logs(
    body: &[u8],
    format: InputFormat,
    config: ProcessorConfig<'_>,
) -> Result<ProcessingResult, OtlpError> {
    // Parse OTLP request
    let request =
        otlp::parse_otlp_request(body, format).map_err(|e| OtlpError::InvalidRequest {
            message: format!("Failed to parse OTLP logs request: {}", e),
            hint: Some(
                "Ensure the request body contains valid OTLP protobuf, JSON, or JSONL format."
                    .into(),
            ),
        })?;

    // Split by service name
    let per_service_requests = otlp::logs::split_request_by_service(request);

    // Convert to Arrow via PassthroughBatcher
    let passthrough = otlp2parquet_core::batch::PassthroughBatcher::<LogSignalProcessor>::default();
    let mut batches = Vec::new();
    let mut total_records = 0;

    for subset in per_service_requests {
        let batch = passthrough
            .ingest(&subset)
            .map_err(|e| OtlpError::ConversionFailed {
                signal: "logs".into(),
                message: e.to_string(),
            })?;
        total_records += batch.metadata.record_count;
        batches.push(batch);
    }

    // Write batches
    let mut paths = Vec::new();
    let batch_count = batches.len();
    for batch in batches {
        for record_batch in &batch.batches {
            let path = otlp2parquet_writer::write_batch(WriteBatchRequest {
                catalog: config.catalog,
                namespace: config.namespace,
                batch: record_batch,
                signal_type: SignalType::Logs,
                metric_type: None,
                service_name: &batch.metadata.service_name,
                timestamp_micros: batch.metadata.first_timestamp_micros,
                snapshot_timestamp_ms: config.snapshot_timestamp_ms,
                retry_policy: config.retry_policy,
            })
            .await
            .map_err(|e| OtlpError::StorageFailed {
                message: e.to_string(),
            })?;

            paths.push(path);
        }
    }

    Ok(ProcessingResult {
        paths_written: paths,
        records_processed: total_records,
        batches_flushed: batch_count,
    })
}

/// Process OTLP traces request
pub async fn process_traces(
    body: &[u8],
    format: InputFormat,
    config: ProcessorConfig<'_>,
) -> Result<ProcessingResult, OtlpError> {
    // Parse OTLP traces request
    let request = otlp::traces::parse_otlp_trace_request(body, format).map_err(|e| {
        OtlpError::InvalidRequest {
            message: format!("Failed to parse OTLP traces request: {}", e),
            hint: Some(
                "Ensure the request body contains valid OTLP protobuf, JSON, or JSONL format."
                    .into(),
            ),
        }
    })?;

    // Split by service name
    let per_service_requests = otlp::traces::split_request_by_service(request);

    let mut paths = Vec::new();
    let mut spans_processed = 0;

    for subset in per_service_requests {
        let (batches, metadata) =
            otlp::traces::TraceArrowConverter::convert(&subset).map_err(|e| {
                OtlpError::ConversionFailed {
                    signal: "traces".into(),
                    message: e.to_string(),
                }
            })?;

        if batches.is_empty() || metadata.span_count == 0 {
            continue;
        }

        spans_processed += metadata.span_count;

        // Write traces
        for batch in &batches {
            let path = otlp2parquet_writer::write_batch(WriteBatchRequest {
                catalog: config.catalog,
                namespace: config.namespace,
                batch,
                signal_type: SignalType::Traces,
                metric_type: None,
                service_name: metadata.service_name.as_ref(),
                timestamp_micros: metadata.first_timestamp_micros,
                snapshot_timestamp_ms: config.snapshot_timestamp_ms,
                retry_policy: config.retry_policy,
            })
            .await
            .map_err(|e| OtlpError::StorageFailed {
                message: e.to_string(),
            })?;

            paths.push(path);
        }
    }

    let batch_count = paths.len();
    Ok(ProcessingResult {
        paths_written: paths,
        records_processed: spans_processed,
        batches_flushed: batch_count,
    })
}

/// Process OTLP metrics request
pub async fn process_metrics(
    body: &[u8],
    format: InputFormat,
    config: ProcessorConfig<'_>,
) -> Result<ProcessingResult, OtlpError> {
    // Parse OTLP metrics request
    let request =
        otlp::metrics::parse_otlp_request(body, format).map_err(|e| OtlpError::InvalidRequest {
            message: format!("Failed to parse OTLP metrics request: {}", e),
            hint: Some(
                "Ensure the request body contains valid OTLP protobuf, JSON, or JSONL format."
                    .into(),
            ),
        })?;

    // Split by service name
    let per_service_requests = otlp::metrics::split_request_by_service(request);
    let converter = otlp::metrics::ArrowConverter::new();

    let mut paths = Vec::new();
    let mut total_data_points = 0;

    for subset in per_service_requests {
        let (batches_by_type, metadata) =
            converter
                .convert(subset)
                .map_err(|e| OtlpError::ConversionFailed {
                    signal: "metrics".into(),
                    message: e.to_string(),
                })?;

        // Skip empty subsets to avoid wasted work
        if batches_by_type.is_empty() {
            continue;
        }

        // Count total data points
        total_data_points += metadata.gauge_count
            + metadata.sum_count
            + metadata.histogram_count
            + metadata.exponential_histogram_count
            + metadata.summary_count;

        let service_name = metadata.service_name().to_string();

        // Write each metric type
        for (metric_type, batch) in batches_by_type {
            let timestamp_micros = metadata
                .first_timestamp_for(&metric_type)
                .unwrap_or_default();

            let path = otlp2parquet_writer::write_batch(WriteBatchRequest {
                catalog: config.catalog,
                namespace: config.namespace,
                batch: &batch,
                signal_type: SignalType::Metrics,
                metric_type: Some(&metric_type),
                service_name: &service_name,
                timestamp_micros,
                snapshot_timestamp_ms: config.snapshot_timestamp_ms,
                retry_policy: config.retry_policy,
            })
            .await
            .map_err(|e| OtlpError::StorageFailed {
                message: e.to_string(),
            })?;

            paths.push(path);
        }
    }

    let batch_count = paths.len();
    Ok(ProcessingResult {
        paths_written: paths,
        records_processed: total_data_points,
        batches_flushed: batch_count,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_processing_result_creation() {
        let result = ProcessingResult {
            paths_written: vec!["path1".to_string(), "path2".to_string()],
            records_processed: 100,
            batches_flushed: 2,
        };

        assert_eq!(result.paths_written.len(), 2);
        assert_eq!(result.records_processed, 100);
        assert_eq!(result.batches_flushed, 2);
    }

    #[tokio::test]
    async fn test_process_logs_invalid_request() {
        use otlp2parquet_core::InputFormat;

        let invalid_data = b"not valid otlp data";
        let config = ProcessorConfig {
            catalog: None,
            namespace: "test",
            snapshot_timestamp_ms: None,
            retry_policy: RetryPolicy::default(),
        };

        let result = process_logs(invalid_data, InputFormat::Protobuf, config).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.error_type(), "InvalidRequest");
        assert!(err.message().contains("Failed to parse OTLP logs request"));
    }

    #[tokio::test]
    async fn test_process_logs_valid_request() {
        use otlp2parquet_core::config::{FsConfig, StorageBackend, StorageConfig};
        use otlp2parquet_core::InputFormat;

        // Read test data
        let test_data = std::fs::read("../../testdata/logs.pb").expect("Failed to read testdata");

        // Initialize storage for plain Parquet mode
        let temp_dir = std::env::temp_dir().join("otlp2parquet-handlers-test");
        std::fs::create_dir_all(&temp_dir).ok();

        let runtime_config = otlp2parquet_core::config::RuntimeConfig {
            batch: Default::default(),
            request: Default::default(),
            storage: StorageConfig {
                backend: StorageBackend::Fs,
                parquet_row_group_size: 100_000,
                fs: Some(FsConfig {
                    path: temp_dir.to_string_lossy().to_string(),
                }),
                s3: None,
                r2: None,
            },
            catalog_mode: Default::default(),
            server: None,
            lambda: None,
            cloudflare: None,
            iceberg: None,
        };

        otlp2parquet_writer::initialize_storage(&runtime_config)
            .expect("Failed to initialize storage");

        let config = ProcessorConfig {
            catalog: None,
            namespace: "test",
            snapshot_timestamp_ms: None,
            retry_policy: RetryPolicy::default(),
        };

        let result = process_logs(&test_data, InputFormat::Protobuf, config)
            .await
            .expect("Failed to process logs");

        assert!(result.records_processed > 0);
        assert!(!result.paths_written.is_empty());
        assert_eq!(result.batches_flushed, result.paths_written.len());

        // Cleanup
        std::fs::remove_dir_all(&temp_dir).ok();
    }

    #[tokio::test]
    async fn test_process_traces_invalid_request() {
        use otlp2parquet_core::InputFormat;

        let invalid_data = b"not valid otlp data";
        let config = ProcessorConfig {
            catalog: None,
            namespace: "test",
            snapshot_timestamp_ms: None,
            retry_policy: RetryPolicy::default(),
        };

        let result = process_traces(invalid_data, InputFormat::Protobuf, config).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.error_type(), "InvalidRequest");
        assert!(err
            .message()
            .contains("Failed to parse OTLP traces request"));
    }

    #[tokio::test]
    async fn test_process_traces_valid_request() {
        use otlp2parquet_core::config::{FsConfig, StorageBackend, StorageConfig};
        use otlp2parquet_core::InputFormat;

        // Read test data
        let test_data = std::fs::read("../../testdata/traces.pb").expect("Failed to read testdata");

        // Initialize storage
        let temp_dir = std::env::temp_dir().join("otlp2parquet-handlers-test-traces");
        std::fs::create_dir_all(&temp_dir).ok();

        let runtime_config = otlp2parquet_core::config::RuntimeConfig {
            batch: Default::default(),
            request: Default::default(),
            storage: StorageConfig {
                backend: StorageBackend::Fs,
                parquet_row_group_size: 100_000,
                fs: Some(FsConfig {
                    path: temp_dir.to_string_lossy().to_string(),
                }),
                s3: None,
                r2: None,
            },
            catalog_mode: Default::default(),
            server: None,
            lambda: None,
            cloudflare: None,
            iceberg: None,
        };

        otlp2parquet_writer::initialize_storage(&runtime_config)
            .expect("Failed to initialize storage");

        let config = ProcessorConfig {
            catalog: None,
            namespace: "test",
            snapshot_timestamp_ms: None,
            retry_policy: RetryPolicy::default(),
        };

        let result = process_traces(&test_data, InputFormat::Protobuf, config)
            .await
            .expect("Failed to process traces");

        assert!(result.records_processed > 0);
        assert!(!result.paths_written.is_empty());

        // Cleanup
        std::fs::remove_dir_all(&temp_dir).ok();
    }

    #[tokio::test]
    async fn test_process_metrics_invalid_request() {
        use otlp2parquet_core::InputFormat;

        let invalid_data = b"not valid otlp data";
        let config = ProcessorConfig {
            catalog: None,
            namespace: "test",
            snapshot_timestamp_ms: None,
            retry_policy: RetryPolicy::default(),
        };

        let result = process_metrics(invalid_data, InputFormat::Protobuf, config).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.error_type(), "InvalidRequest");
        assert!(err
            .message()
            .contains("Failed to parse OTLP metrics request"));
    }

    #[tokio::test]
    async fn test_process_metrics_valid_request() {
        use otlp2parquet_core::config::{FsConfig, StorageBackend, StorageConfig};
        use otlp2parquet_core::InputFormat;

        // Read test data
        let test_data =
            std::fs::read("../../testdata/metrics_gauge.pb").expect("Failed to read testdata");

        // Initialize storage
        let temp_dir = std::env::temp_dir().join("otlp2parquet-handlers-test-metrics");
        std::fs::create_dir_all(&temp_dir).ok();

        let runtime_config = otlp2parquet_core::config::RuntimeConfig {
            batch: Default::default(),
            request: Default::default(),
            storage: StorageConfig {
                backend: StorageBackend::Fs,
                parquet_row_group_size: 100_000,
                fs: Some(FsConfig {
                    path: temp_dir.to_string_lossy().to_string(),
                }),
                s3: None,
                r2: None,
            },
            catalog_mode: Default::default(),
            server: None,
            lambda: None,
            cloudflare: None,
            iceberg: None,
        };

        otlp2parquet_writer::initialize_storage(&runtime_config)
            .expect("Failed to initialize storage");

        let config = ProcessorConfig {
            catalog: None,
            namespace: "test",
            snapshot_timestamp_ms: None,
            retry_policy: RetryPolicy::default(),
        };

        let result = process_metrics(&test_data, InputFormat::Protobuf, config)
            .await
            .expect("Failed to process metrics");

        assert!(result.records_processed > 0);
        assert!(!result.paths_written.is_empty());

        // Cleanup
        std::fs::remove_dir_all(&temp_dir).ok();
    }
}
