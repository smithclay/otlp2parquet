// Cloudflare Workers runtime adapter
//
// Uses OpenDAL S3 for R2 storage and handles incoming requests via Worker fetch events
//
// Philosophy: Use OpenDAL S3 with R2-compatible endpoint
// Worker crate provides the runtime, OpenDAL provides storage abstraction
// Entry point is #[event(fetch)] macro, not main()

use once_cell::sync::OnceCell;
use otlp2parquet_batch::{BatchConfig, BatchManager, CompletedBatch, PassthroughBatcher};
use otlp2parquet_core::otlp;
use otlp2parquet_core::ProcessingOptions;
use serde_json::json;
use std::sync::Arc;
use worker::*;

static STORAGE: OnceCell<Arc<otlp2parquet_storage::opendal_storage::OpenDalStorage>> =
    OnceCell::new();
static PROCESSING_OPTIONS: OnceCell<ProcessingOptions> = OnceCell::new();
static BATCHER: OnceCell<Option<Arc<BatchManager>>> = OnceCell::new();
static MAX_PAYLOAD_BYTES: OnceCell<usize> = OnceCell::new();

/// Handle OTLP HTTP POST request and write to R2
pub async fn handle_otlp_request(mut req: Request, env: Env, _ctx: Context) -> Result<Response> {
    // Only accept POST requests to /v1/logs
    if req.method() != Method::Post {
        return Response::error("Method not allowed", 405);
    }

    let path = req.path();
    if path != "/v1/logs" {
        return Response::error("Not found", 404);
    }

    let storage = if let Some(existing) = STORAGE.get() {
        existing.clone()
    } else {
        let bucket = env
            .var("R2_BUCKET")
            .map_err(|e| {
                console_error!("R2_BUCKET environment variable not set: {:?}", e);
                e
            })?
            .to_string();

        let account_id = env
            .var("R2_ACCOUNT_ID")
            .map_err(|e| {
                console_error!("R2_ACCOUNT_ID environment variable not set: {:?}", e);
                e
            })?
            .to_string();

        let access_key_id = env
            .var("R2_ACCESS_KEY_ID")
            .map_err(|e| {
                console_error!("R2_ACCESS_KEY_ID environment variable not set: {:?}", e);
                e
            })?
            .to_string();

        let secret_access_key = env
            .secret("R2_SECRET_ACCESS_KEY")
            .map_err(|e| {
                console_error!("R2_SECRET_ACCESS_KEY secret not set: {:?}", e);
                e
            })?
            .to_string();

        let instance = Arc::new(
            otlp2parquet_storage::opendal_storage::OpenDalStorage::new_r2(
                &bucket,
                &account_id,
                &access_key_id,
                &secret_access_key,
            )
            .map_err(|e| {
                console_error!("Failed to initialize OpenDAL R2 storage: {:?}", e);
                worker::Error::RustError(format!("Storage initialization error: {}", e))
            })?,
        );

        let _ = STORAGE.set(instance.clone());
        instance
    };

    let processing_options = PROCESSING_OPTIONS
        .get_or_init(processing_options_from_env)
        .clone();

    let batcher = BATCHER
        .get_or_init(|| {
            let cfg = BatchConfig::from_env(100_000, 64 * 1024 * 1024, 5);
            if cfg.max_rows == 0 || cfg.max_bytes == 0 {
                console_log!(
                    "Cloudflare batching disabled (max_rows={} max_bytes={})",
                    cfg.max_rows,
                    cfg.max_bytes
                );
                None
            } else {
                console_log!(
                    "Cloudflare batching enabled (max_rows={} max_bytes={} max_age={}s, row_group_rows={})",
                    cfg.max_rows,
                    cfg.max_bytes,
                    cfg.max_age.as_secs(),
                    PROCESSING_OPTIONS.get().map(|p| p.max_rows_per_batch).unwrap_or(processing_options.max_rows_per_batch)
                );
                let opts = PROCESSING_OPTIONS
                    .get()
                    .cloned()
                    .unwrap_or_else(processing_options_from_env);
                Some(Arc::new(BatchManager::new(cfg, opts)))
            }
        })
        .clone();

    let max_payload_bytes =
        *MAX_PAYLOAD_BYTES.get_or_init(|| max_payload_bytes_from_env(1024 * 1024));
    let passthrough = PassthroughBatcher;

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

    let request = otlp::parse_otlp_request(&body_bytes, format).map_err(|e| {
        console_error!(
            "Failed to parse OTLP logs (format: {:?}, content-type: {:?}): {:?}",
            format,
            content_type,
            e
        );
        worker::Error::RustError(format!("Processing error: {}", e))
    })?;

    let mut uploads: Vec<CompletedBatch> = Vec::new();
    let metadata;

    if let Some(manager) = batcher.as_ref() {
        match manager.drain_expired() {
            Ok(mut expired) => uploads.append(&mut expired),
            Err(err) => {
                console_error!("Failed to flush expired batches: {:?}", err);
                return Response::error("Internal batching failure", 500);
            }
        }

        match manager.ingest(request) {
            Ok((mut ready, meta)) => {
                uploads.append(&mut ready);
                metadata = meta;
            }
            Err(err) => {
                console_error!("Batch enqueue failed: {:?}", err);
                return Response::error("Internal batching failure", 500);
            }
        }
    } else {
        match passthrough.ingest(request, &processing_options) {
            Ok(batch) => {
                metadata = batch.metadata.clone();
                uploads.push(batch);
            }
            Err(err) => {
                console_error!("Failed to encode Parquet: {:?}", err);
                return Response::error("Internal encoding failure", 500);
            }
        }
    }

    let mut uploaded_paths = Vec::new();
    for batch in uploads {
        let hash_hex = batch.content_hash.to_hex().to_string();
        let partition_path = otlp2parquet_storage::partition::generate_partition_path(
            &batch.metadata.service_name,
            batch.metadata.first_timestamp_nanos,
            &hash_hex,
        );

        storage
            .write(&partition_path, batch.bytes)
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

/// Platform-specific helper: Read processing options from environment
fn processing_options_from_env() -> ProcessingOptions {
    let max_rows = std::env::var("ROW_GROUP_MAX_ROWS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .map(|rows| rows.max(1024))
        .unwrap_or(32 * 1024);

    ProcessingOptions {
        max_rows_per_batch: max_rows,
    }
}

/// Platform-specific helper: Read max payload bytes from environment
fn max_payload_bytes_from_env(default: usize) -> usize {
    std::env::var("MAX_PAYLOAD_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default)
}
