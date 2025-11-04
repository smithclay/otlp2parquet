// Request handlers for OTLP signals (logs, traces, metrics)

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

    // Convert directly to Arrow (no batching)
    let batch = passthrough.ingest(&request).map_err(|err| {
        console_error!("Failed to convert OTLP to Arrow: {:?}", err);
        worker::Error::RustError(format!("Encoding error: {}", err))
    })?;

    let metadata = batch.metadata.clone();
    let uploads = vec![batch];

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
        "records_processed": metadata.record_count,
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

    // Convert to Arrow
    let (batches, metadata) =
        otlp::traces::TraceArrowConverter::convert(&request).map_err(|e| {
            console_error!("Failed to convert OTLP traces to Arrow: {:?}", e);
            worker::Error::RustError(format!("Encoding error: {}", e))
        })?;

    if batches.is_empty() || metadata.span_count == 0 {
        let response = Response::from_json(&json!({
            "status": "ok",
            "message": "No trace spans to process",
        }))?;
        return Ok(response);
    }

    // Write Parquet synchronously (WASM-compatible)
    let parquet_bytes = parquet::write_batches_to_parquet(&batches).map_err(|e| {
        console_error!("Failed to serialize traces Parquet: {:?}", e);
        worker::Error::RustError(format!("Parquet serialization error: {}", e))
    })?;

    // Compute Blake3 hash
    let hash_bytes = blake3::hash(&parquet_bytes);
    let hash_hex = hex::encode(hash_bytes.as_bytes());

    // Generate partition path for traces
    let partition_path = otlp2parquet_storage::partition::generate_partition_path_with_signal(
        "traces",
        metadata.service_name.as_ref(),
        metadata.first_timestamp_nanos,
        &hash_hex,
        None, // No subdirectory for traces (unlike metrics)
    );

    // Upload to R2
    storage
        .write(&partition_path, parquet_bytes)
        .await
        .map_err(|e| {
            console_error!("Failed to write traces to R2: {:?}", e);
            worker::Error::RustError(format!("Storage error: {}", e))
        })?;

    let response_body = json!({
        "status": "ok",
        "spans_processed": metadata.span_count,
        "partitions": vec![partition_path],
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

    // Convert to Arrow (returns multiple batches, one per metric type)
    let converter = otlp::metrics::ArrowConverter::new();
    let (batches_by_type, metadata) = converter.convert(request).map_err(|e| {
        console_error!("Failed to convert OTLP metrics to Arrow: {:?}", e);
        worker::Error::RustError(format!("Encoding error: {}", e))
    })?;

    if batches_by_type.is_empty() {
        let response = Response::from_json(&json!({
            "status": "ok",
            "message": "No metrics data points to process",
        }))?;
        return Ok(response);
    }

    // Count total data points across all metric types
    let total_data_points = metadata.gauge_count
        + metadata.sum_count
        + metadata.histogram_count
        + metadata.exponential_histogram_count
        + metadata.summary_count;

    // Write each metric type batch to its own Parquet file
    let mut uploaded_paths = Vec::new();
    for (metric_type, batch) in batches_by_type {
        let service_name = "default"; // TODO: Extract from metadata
        let timestamp_nanos = 0; // TODO: Extract first timestamp from batch

        // Write Parquet synchronously (WASM-compatible)
        let parquet_bytes = parquet::write_batches_to_parquet(&[batch]).map_err(|e| {
            console_error!(
                "Failed to serialize {} metrics Parquet: {:?}",
                metric_type,
                e
            );
            worker::Error::RustError(format!("Parquet serialization error: {}", e))
        })?;

        // Compute Blake3 hash
        let hash_bytes = blake3::hash(&parquet_bytes);
        let hash_hex = hex::encode(hash_bytes.as_bytes());

        // Generate partition path with metric type subdirectory
        let partition_path = otlp2parquet_storage::partition::generate_partition_path_with_signal(
            "metrics",
            service_name,
            timestamp_nanos,
            &hash_hex,
            Some(&metric_type),
        );

        // Upload to R2
        storage
            .write(&partition_path, parquet_bytes)
            .await
            .map_err(|e| {
                console_error!("Failed to write {} metrics to R2: {:?}", metric_type, e);
                worker::Error::RustError(format!("Storage error: {}", e))
            })?;

        uploaded_paths.push(partition_path);
    }

    let response_body = json!({
        "status": "ok",
        "data_points_processed": total_data_points,
        "gauge_count": metadata.gauge_count,
        "sum_count": metadata.sum_count,
        "histogram_count": metadata.histogram_count,
        "exponential_histogram_count": metadata.exponential_histogram_count,
        "summary_count": metadata.summary_count,
        "partitions": uploaded_paths,
    });

    Response::from_json(&response_body)
}
