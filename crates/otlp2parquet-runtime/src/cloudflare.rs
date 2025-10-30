// Cloudflare Workers runtime adapter
//
// Uses OpenDAL S3 for R2 storage and handles incoming requests via Worker fetch events
//
// Philosophy: Use OpenDAL S3 with R2-compatible endpoint
// Worker crate provides the runtime, OpenDAL provides storage abstraction
// Entry point is #[event(fetch)] macro, not main()

#[cfg(feature = "cloudflare")]
use worker::*;

// Note: R2Storage removed - now using OpenDalStorage with S3-compatible R2 endpoint

/// Handle OTLP HTTP POST request and write to R2
#[cfg(feature = "cloudflare")]
pub async fn handle_otlp_request(mut req: Request, env: Env, _ctx: Context) -> Result<Response> {
    // Only accept POST requests to /v1/logs
    if req.method() != Method::Post {
        return Response::error("Method not allowed", 405);
    }

    let path = req.path();
    if path != "/v1/logs" {
        return Response::error("Not found", 404);
    }

    // Get R2 configuration from environment variables
    // Note: R2 uses S3-compatible API, so we use OpenDAL's S3 service
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

    // Initialize OpenDAL S3 storage with R2 endpoint
    let storage = crate::opendal_storage::OpenDalStorage::new_r2(
        &bucket,
        &account_id,
        &access_key_id,
        &secret_access_key,
    )
    .map_err(|e| {
        console_error!("Failed to initialize OpenDAL R2 storage: {:?}", e);
        worker::Error::RustError(format!("Storage initialization error: {}", e))
    })?;

    // Read request body
    let body_bytes = req.bytes().await.map_err(|e| {
        console_error!("Failed to read request body: {:?}", e);
        e
    })?;

    // Process OTLP logs (PURE - no I/O, deterministic)
    let mut parquet_bytes = Vec::new();
    let metadata = otlp2parquet_core::process_otlp_logs_into(&body_bytes, &mut parquet_bytes)
        .map_err(|e| {
            console_error!("Failed to process OTLP logs: {:?}", e);
            worker::Error::RustError(format!("Processing error: {}", e))
        })?;

    // Generate partition path (ACCIDENT - platform-specific storage decision)
    let partition_path = crate::partition::generate_partition_path(
        &metadata.service_name,
        metadata.first_timestamp_nanos,
    );

    // Write to R2 via OpenDAL
    storage
        .write(&partition_path, parquet_bytes)
        .await
        .map_err(|e| {
            console_error!("Failed to write to R2: {:?}", e);
            worker::Error::RustError(format!("Storage error: {}", e))
        })?;

    // Return success response
    Response::ok("OK")
}
