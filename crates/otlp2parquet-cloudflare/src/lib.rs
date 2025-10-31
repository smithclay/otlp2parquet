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
use serde_json::json;
use std::sync::Arc;
use worker::*;

static STORAGE: OnceCell<Arc<otlp2parquet_storage::opendal_storage::OpenDalStorage>> =
    OnceCell::new();
static BATCHER: OnceCell<Option<Arc<BatchManager>>> = OnceCell::new();
static MAX_PAYLOAD_BYTES: OnceCell<usize> = OnceCell::new();

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
    use arrow::array::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    /// Write RecordBatch to Parquet bytes synchronously (WASM-compatible)
    pub fn write_batch_to_parquet(batch: &RecordBatch) -> anyhow::Result<Vec<u8>> {
        let mut buffer = Vec::new();
        let props = writer_properties();
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props))?;
        writer.write(batch)?;
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
                    "Cloudflare batching enabled (max_rows={} max_bytes={} max_age={}s)",
                    cfg.max_rows,
                    cfg.max_bytes,
                    cfg.max_age.as_secs()
                );
                Some(Arc::new(BatchManager::new(cfg)))
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
        match passthrough.ingest(request) {
            Ok(batch) => {
                metadata = batch.metadata.clone();
                uploads.push(batch);
            }
            Err(err) => {
                console_error!("Failed to convert OTLP to Arrow: {:?}", err);
                return Response::error("Internal encoding failure", 500);
            }
        }
    }

    let mut uploaded_paths = Vec::new();
    for batch in uploads {
        // WASM-specific: Write Parquet synchronously, then upload via OpenDAL
        // (parquet_opendal doesn't work in WASM due to tokio::spawn requiring Send)
        let parquet_bytes = wasm_parquet::write_batch_to_parquet(&batch.batch).map_err(|e| {
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

/// Platform-specific helper: Read max payload bytes from environment
fn max_payload_bytes_from_env(default: usize) -> usize {
    std::env::var("MAX_PAYLOAD_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default)
}
