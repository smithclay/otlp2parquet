// Cloudflare Workers runtime adapter
//
// Uses OpenDAL S3 for R2 storage and handles incoming requests via Worker fetch events
//
// Philosophy: Use OpenDAL S3 with R2-compatible endpoint
// Worker crate provides the runtime, OpenDAL provides storage abstraction
// Entry point is #[event(fetch)] macro, not main()

use once_cell::sync::OnceCell;
use otlp2parquet_batch::{LogSignalProcessor, PassthroughBatcher};
use otlp2parquet_core::otlp;
use serde_json::json;
use std::sync::Arc;
use worker::*;

static STORAGE: OnceCell<Arc<otlp2parquet_storage::opendal_storage::OpenDalStorage>> =
    OnceCell::new();

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum SignalKind {
    Logs,
    Traces,
    Metrics,
}

/// WASM-specific Parquet writer helper
///
/// Why synchronous approach for WASM:
/// - parquet_opendal uses tokio::spawn which requires Send trait
/// - Send is not available in single-threaded wasm32-unknown-unknown target
/// - OpenDAL S3 service DOES work in WASM for R2 uploads
/// - Solution: Write Parquet synchronously to Vec<u8>, then upload via OpenDAL
///
/// This is platform-specific "accidental complexity" (storage format + I/O)
/// and belongs in the platform layer, not core.
mod wasm_parquet {
    use anyhow::{bail, Result};
    use arrow::array::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    /// Write one or more RecordBatches to Parquet bytes synchronously (WASM-compatible)
    pub fn write_batches_to_parquet(batches: &[RecordBatch]) -> Result<Vec<u8>> {
        if batches.is_empty() {
            bail!("cannot write empty batch list");
        }

        let mut buffer = Vec::new();
        let props = writer_properties();
        let mut writer = ArrowWriter::try_new(&mut buffer, batches[0].schema(), Some(props))?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.close()?;
        Ok(buffer)
    }

    /// Platform-specific writer properties for WASM (Snappy compression only)
    fn writer_properties() -> WriterProperties {
        use parquet::basic::Compression;
        use parquet::file::properties::EnabledStatistics;

        WriterProperties::builder()
            .set_dictionary_enabled(true)
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_compression(Compression::SNAPPY)
            .set_data_page_size_limit(256 * 1024)
            .set_write_batch_size(32 * 1024)
            .set_max_row_group_size(32 * 1024)
            .set_dictionary_page_size_limit(128 * 1024)
            .build()
    }
}

/// Cloudflare Workers entry point
#[event(fetch)]
pub async fn main(req: Request, env: Env, ctx: Context) -> Result<Response> {
    console_log!("Cloudflare Workers OTLP endpoint started");
    handle_otlp_request(req, env, ctx).await
}

/// Handle OTLP HTTP POST request and write to R2
async fn handle_otlp_request(mut req: Request, env: Env, _ctx: Context) -> Result<Response> {
    // Only accept POST requests to /v1/logs
    if req.method() != Method::Post {
        return Response::error("Method not allowed", 405);
    }

    let path = req.path();
    let signal = match path.as_str() {
        "/v1/logs" => SignalKind::Logs,
        "/v1/traces" => SignalKind::Traces,
        "/v1/metrics" => SignalKind::Metrics,
        _ => return Response::error("Not found", 404),
    };

    let storage = if let Some(existing) = STORAGE.get() {
        existing.clone()
    } else {
        // Generic S3-compatible storage configuration
        // Supports R2 (production), MinIO (local dev), or any S3-compatible endpoint
        let bucket = env
            .var("OTLP2PARQUET_S3_BUCKET")
            .map_err(|e| {
                console_error!(
                    "OTLP2PARQUET_S3_BUCKET environment variable not set: {:?}",
                    e
                );
                e
            })?
            .to_string();

        let region = env
            .var("OTLP2PARQUET_S3_REGION")
            .ok()
            .map(|v| v.to_string())
            .unwrap_or_else(|| "auto".to_string());

        // Optional: custom S3 endpoint (for MinIO, R2, etc.)
        // If not set, OpenDAL uses AWS S3 defaults
        let endpoint = env
            .var("OTLP2PARQUET_S3_ENDPOINT")
            .ok()
            .map(|v| v.to_string());

        let access_key_id = env
            .var("OTLP2PARQUET_S3_ACCESS_KEY_ID")
            .ok()
            .map(|v| v.to_string());

        let secret_access_key = env
            .secret("OTLP2PARQUET_S3_SECRET_ACCESS_KEY")
            .or_else(|_| env.var("OTLP2PARQUET_S3_SECRET_ACCESS_KEY"))
            .ok()
            .map(|v| v.to_string());

        console_log!(
            "Initializing S3-compatible storage: bucket={}, region={}, endpoint={:?}",
            bucket,
            region,
            endpoint.as_deref().unwrap_or("default")
        );

        let instance = Arc::new(
            otlp2parquet_storage::opendal_storage::OpenDalStorage::new_s3(
                &bucket,
                &region,
                endpoint.as_deref(),
                access_key_id.as_deref(),
                secret_access_key.as_deref(),
            )
            .map_err(|e| {
                console_error!("Failed to initialize OpenDAL S3 storage: {:?}", e);
                worker::Error::RustError(format!("Storage initialization error: {}", e))
            })?,
        );

        let _ = STORAGE.set(instance.clone());
        instance
    };

    // Cloudflare Workers: Always use passthrough (no batching)
    // Batching requires time-based operations which aren't supported in WASM
    let passthrough: PassthroughBatcher<LogSignalProcessor> = PassthroughBatcher::default();
    let max_payload_bytes = 1024 * 1024; // 1MB max payload

    let content_type_header = req.headers().get("content-type").ok().flatten();
    let content_type = content_type_header.as_deref();
    let format = otlp2parquet_core::InputFormat::from_content_type(content_type);

    let body_bytes = req.bytes().await.map_err(|e| {
        console_error!("Failed to read request body: {:?}", e);
        e
    })?;

    if body_bytes.len() > max_payload_bytes {
        return Response::error("Payload too large", 413);
    }

    if matches!(signal, SignalKind::Traces) {
        let response = Response::from_json(&json!({
            "error": "OTLP trace ingestion not implemented yet",
        }))?
        .with_status(501);
        return Ok(response);
    }

    // Handle metrics separately
    if matches!(signal, SignalKind::Metrics) {
        return handle_metrics_request(&body_bytes, format, content_type, &storage).await;
    }

    // Process logs
    let request = otlp::parse_otlp_request(&body_bytes, format).map_err(|e| {
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
        let parquet_bytes =
            wasm_parquet::write_batches_to_parquet(&batch.batches).map_err(|e| {
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

/// Handle metrics request (separate from logs due to multiple batches per type)
async fn handle_metrics_request(
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
        let parquet_bytes = wasm_parquet::write_batches_to_parquet(&[batch]).map_err(|e| {
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
