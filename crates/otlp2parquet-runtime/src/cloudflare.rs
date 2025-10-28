// Cloudflare Workers runtime adapter
//
// Uses R2 for storage and handles incoming requests via Worker fetch events
//
// Philosophy: Use worker crate's JS-based runtime
// No tokio - single-threaded JavaScript-style execution model
// Entry point is #[event(fetch)] macro, not main()

#[cfg(feature = "cloudflare")]
use worker::*;

#[cfg(feature = "cloudflare")]
pub struct R2Storage {
    bucket: Bucket,
}

#[cfg(feature = "cloudflare")]
impl R2Storage {
    pub fn new(bucket: Bucket) -> Self {
        Self { bucket }
    }

    /// Write parquet data to R2 (async, uses worker runtime)
    pub async fn write(&self, path: &str, data: Vec<u8>) -> anyhow::Result<()> {
        self.bucket
            .put(path, data)
            .execute()
            .await
            .map_err(|e| anyhow::anyhow!("R2 write error: {}", e))?;
        Ok(())
    }
}

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

    // Get R2 bucket from environment
    let bucket = env.bucket("LOGS_BUCKET").map_err(|e| {
        console_error!("Failed to get R2 bucket: {:?}", e);
        e
    })?;

    let storage = R2Storage::new(bucket);

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
    let path = crate::partition::generate_partition_path(
        &metadata.service_name,
        metadata.first_timestamp_nanos,
    );

    // Write to R2
    storage.write(&path, parquet_bytes).await.map_err(|e| {
        console_error!("Failed to write to R2: {:?}", e);
        worker::Error::RustError(format!("Storage error: {}", e))
    })?;

    // Return success response
    Response::ok("OK")
}
