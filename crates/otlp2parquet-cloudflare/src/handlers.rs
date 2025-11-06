// Request handlers for OTLP signals (logs, traces, metrics)

use arrow::array::{Array, RecordBatch, StringArray, TimestampNanosecondArray};
use otlp2parquet_batch::{LogSignalProcessor, PassthroughBatcher};
use otlp2parquet_core::otlp;
use serde_json::json;
use std::sync::Arc;
use worker::{console_error, Response, Result};

use crate::parquet;

/// Handle logs request
pub async fn handle_logs_request(
    body_bytes: &[u8],
    format: otlp2parquet_core::InputFormat,
    content_type: Option<&str>,
    storage: &Arc<otlp2parquet_storage::opendal_storage::OpenDalStorage>,
) -> Result<Response> {
    // Cloudflare Workers: Always use passthrough (no batching)
    // Batching requires time-based operations which aren't supported in WASM
    let passthrough: PassthroughBatcher<LogSignalProcessor> = PassthroughBatcher::default();

    // Process logs
    let request = otlp::parse_otlp_request(body_bytes, format).map_err(|e| {
        console_error!(
            "Failed to parse OTLP logs (format: {:?}, content-type: {:?}): {:?}",
            format,
            content_type,
            e
        );
        worker::Error::RustError(format!("Processing error: {}", e))
    })?;

    let per_service_requests = otlp::logs::split_request_by_service(request);
    let mut uploads = Vec::new();
    let mut total_records = 0usize;

    for subset in per_service_requests {
        let batch = passthrough.ingest(&subset).map_err(|err| {
            console_error!("Failed to convert OTLP to Arrow: {:?}", err);
            worker::Error::RustError(format!("Encoding error: {}", err))
        })?;
        total_records += batch.metadata.record_count;
        uploads.push(batch);
    }

    let mut uploaded_paths = Vec::new();
    for batch in uploads {
        // WASM-specific: Write Parquet synchronously, then upload via OpenDAL
        // (parquet_opendal doesn't work in WASM due to tokio::spawn requiring Send)
        let parquet_bytes = parquet::write_batches_to_parquet(&batch.batches).map_err(|e| {
            console_error!("Failed to serialize Parquet: {:?}", e);
            worker::Error::RustError(format!("Parquet serialization error: {}", e))
        })?;

        // Compute Blake3 hash for content-addressable storage
        let hash_bytes = blake3::hash(&parquet_bytes);
        let hash_hex = hex::encode(hash_bytes.as_bytes());

        // Generate partition path with hash
        let partition_path = otlp2parquet_storage::partition::generate_partition_path(
            &batch.metadata.service_name,
            batch.metadata.first_timestamp_nanos,
            &hash_hex,
        );

        // Upload to R2 via OpenDAL (this DOES work in WASM)
        storage
            .write(&partition_path, parquet_bytes)
            .await
            .map_err(|e| {
                console_error!("Failed to write to R2: {:?}", e);
                worker::Error::RustError(format!("Storage error: {}", e))
            })?;

        uploaded_paths.push(partition_path);
    }

    let response_body = json!({
        "status": "ok",
        "records_processed": total_records,
        "flush_count": uploaded_paths.len(),
        "partitions": uploaded_paths,
    });

    Response::from_json(&response_body)
}

/// Handle traces request
pub async fn handle_traces_request(
    body_bytes: &[u8],
    format: otlp2parquet_core::InputFormat,
    content_type: Option<&str>,
    storage: &Arc<otlp2parquet_storage::opendal_storage::OpenDalStorage>,
) -> Result<Response> {
    // Parse OTLP traces request
    let request = otlp::traces::parse_otlp_trace_request(body_bytes, format).map_err(|e| {
        console_error!(
            "Failed to parse OTLP traces (format: {:?}, content-type: {:?}): {:?}",
            format,
            content_type,
            e
        );
        worker::Error::RustError(format!("Processing error: {}", e))
    })?;

    let per_service_requests = otlp::traces::split_request_by_service(request);
    let mut uploaded_paths = Vec::new();
    let mut spans_processed = 0usize;

    for subset in per_service_requests {
        let (batches, metadata) =
            otlp::traces::TraceArrowConverter::convert(&subset).map_err(|e| {
                console_error!("Failed to convert OTLP traces to Arrow: {:?}", e);
                worker::Error::RustError(format!("Encoding error: {}", e))
            })?;

        if batches.is_empty() || metadata.span_count == 0 {
            continue;
        }

        spans_processed += metadata.span_count;

        let parquet_bytes = parquet::write_batches_to_parquet(&batches).map_err(|e| {
            console_error!("Failed to serialize traces Parquet: {:?}", e);
            worker::Error::RustError(format!("Parquet serialization error: {}", e))
        })?;

        let hash_bytes = blake3::hash(&parquet_bytes);
        let hash_hex = hex::encode(hash_bytes.as_bytes());

        let partition_path = otlp2parquet_storage::partition::generate_partition_path_with_signal(
            "traces",
            metadata.service_name.as_ref(),
            metadata.first_timestamp_nanos,
            &hash_hex,
            None,
        );

        storage
            .write(&partition_path, parquet_bytes)
            .await
            .map_err(|e| {
                console_error!("Failed to write traces to R2: {:?}", e);
                worker::Error::RustError(format!("Storage error: {}", e))
            })?;

        uploaded_paths.push(partition_path);
    }

    if spans_processed == 0 {
        let response = Response::from_json(&json!({
            "status": "ok",
            "message": "No trace spans to process",
        }))?;
        return Ok(response);
    }

    let response_body = json!({
        "status": "ok",
        "spans_processed": spans_processed,
        "partitions": uploaded_paths,
    });

    Response::from_json(&response_body)
}

/// Handle metrics request (separate from logs due to multiple batches per type)
pub async fn handle_metrics_request(
    body_bytes: &[u8],
    format: otlp2parquet_core::InputFormat,
    content_type: Option<&str>,
    storage: &Arc<otlp2parquet_storage::opendal_storage::OpenDalStorage>,
) -> Result<Response> {
    // Parse OTLP metrics request
    let request = otlp::metrics::parse_otlp_request(body_bytes, format).map_err(|e| {
        console_error!(
            "Failed to parse OTLP metrics (format: {:?}, content-type: {:?}): {:?}",
            format,
            content_type,
            e
        );
        worker::Error::RustError(format!("Processing error: {}", e))
    })?;

    let per_service_requests = otlp::metrics::split_request_by_service(request);
    let converter = otlp::metrics::ArrowConverter::new();
    let mut aggregated = otlp::metrics::MetricsMetadata::default();
    let mut uploaded_paths = Vec::new();

    for subset in per_service_requests {
        let (batches_by_type, subset_metadata) = converter.convert(subset).map_err(|e| {
            console_error!("Failed to convert OTLP metrics to Arrow: {:?}", e);
            worker::Error::RustError(format!("Encoding error: {}", e))
        })?;

        aggregated.resource_metrics_count += subset_metadata.resource_metrics_count;
        aggregated.scope_metrics_count += subset_metadata.scope_metrics_count;
        aggregated.gauge_count += subset_metadata.gauge_count;
        aggregated.sum_count += subset_metadata.sum_count;
        aggregated.histogram_count += subset_metadata.histogram_count;
        aggregated.exponential_histogram_count += subset_metadata.exponential_histogram_count;
        aggregated.summary_count += subset_metadata.summary_count;

        for (metric_type, batch) in batches_by_type {
            let service_name = extract_service_name(&batch);
            let timestamp_nanos = extract_first_timestamp(&batch);

            let parquet_bytes = parquet::write_batches_to_parquet(&[batch]).map_err(|e| {
                console_error!(
                    "Failed to serialize {} metrics Parquet: {:?}",
                    metric_type,
                    e
                );
                worker::Error::RustError(format!("Parquet serialization error: {}", e))
            })?;

            let hash_bytes = blake3::hash(&parquet_bytes);
            let hash_hex = hex::encode(hash_bytes.as_bytes());

            let partition_path =
                otlp2parquet_storage::partition::generate_partition_path_with_signal(
                    "metrics",
                    &service_name,
                    timestamp_nanos,
                    &hash_hex,
                    Some(&metric_type),
                );

            storage
                .write(&partition_path, parquet_bytes)
                .await
                .map_err(|e| {
                    console_error!("Failed to write {} metrics to R2: {:?}", metric_type, e);
                    worker::Error::RustError(format!("Storage error: {}", e))
                })?;

            uploaded_paths.push(partition_path);
        }
    }

    if uploaded_paths.is_empty() {
        let response = Response::from_json(&json!({
            "status": "ok",
            "message": "No metrics data points to process",
        }))?;
        return Ok(response);
    }

    let total_data_points = aggregated.gauge_count
        + aggregated.sum_count
        + aggregated.histogram_count
        + aggregated.exponential_histogram_count
        + aggregated.summary_count;

    let response_body = json!({
        "status": "ok",
        "data_points_processed": total_data_points,
        "gauge_count": aggregated.gauge_count,
        "sum_count": aggregated.sum_count,
        "histogram_count": aggregated.histogram_count,
        "exponential_histogram_count": aggregated.exponential_histogram_count,
        "summary_count": aggregated.summary_count,
        "partitions": uploaded_paths,
    });

    Response::from_json(&response_body)
}

fn extract_service_name(batch: &RecordBatch) -> String {
    let fallback = otlp::common::UNKNOWN_SERVICE_NAME;

    if let Some(array) = batch.column(1).as_any().downcast_ref::<StringArray>() {
        for idx in 0..array.len() {
            if array.is_valid(idx) {
                let value = array.value(idx);
                if !value.is_empty() {
                    return value.to_string();
                }
            }
        }
    }

    fallback.to_string()
}

fn extract_first_timestamp(batch: &RecordBatch) -> i64 {
    if let Some(array) = batch
        .column(0)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
    {
        let mut min_value = i64::MAX;
        for idx in 0..array.len() {
            if array.is_valid(idx) {
                let value = array.value(idx);
                if value < min_value {
                    min_value = value;
                }
            }
        }

        if min_value != i64::MAX {
            return min_value;
        }
    }

    0
}
