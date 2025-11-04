// Cloudflare Workers runtime adapter
//
// Uses OpenDAL S3 for R2 storage and handles incoming requests via Worker fetch events
//
// Philosophy: Use OpenDAL S3 with R2-compatible endpoint
// Worker crate provides the runtime, OpenDAL provides storage abstraction
// Entry point is #[event(fetch)] macro, not main()

mod auth;
mod handlers;
mod parquet;

use once_cell::sync::OnceCell;
use std::sync::Arc;
use worker::*;

static STORAGE: OnceCell<Arc<otlp2parquet_storage::opendal_storage::OpenDalStorage>> =
    OnceCell::new();
static CONFIG: OnceCell<otlp2parquet_config::RuntimeConfig> = OnceCell::new();

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum SignalKind {
    Logs,
    Traces,
    Metrics,
}

/// Cloudflare Workers entry point
#[event(fetch)]
pub async fn main(req: Request, env: Env, ctx: Context) -> Result<Response> {
    console_log!("Cloudflare Workers OTLP endpoint started");
    handle_otlp_request(req, env, ctx).await
}

/// Handle OTLP HTTP POST request and write to R2
async fn handle_otlp_request(mut req: Request, env: Env, _ctx: Context) -> Result<Response> {
    // Only accept POST requests
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

    // Check basic authentication if enabled
    if let Err(response) = auth::check_basic_auth(&req, &env) {
        return Ok(response);
    }

    // Load config once (with platform detection and env var overrides)
    let config = CONFIG.get_or_try_init(|| {
        otlp2parquet_config::RuntimeConfig::load().map_err(|e| {
            console_error!("Failed to load configuration: {:?}", e);
            worker::Error::RustError(format!("Config error: {}", e))
        })
    })?;

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

    // Use configurable max payload size (default 10MB for CF Workers)
    // Can be overridden via OTLP2PARQUET_MAX_PAYLOAD_BYTES env var
    let max_payload_bytes = config.request.max_payload_bytes;

    console_log!(
        "Processing {} request (max payload: {} MB)",
        match signal {
            SignalKind::Logs => "logs",
            SignalKind::Traces => "traces",
            SignalKind::Metrics => "metrics",
        },
        max_payload_bytes / (1024 * 1024)
    );

    let content_type_header = req.headers().get("content-type").ok().flatten();
    let content_type = content_type_header.as_deref();
    let format = otlp2parquet_core::InputFormat::from_content_type(content_type);

    let body_bytes = req.bytes().await.map_err(|e| {
        console_error!("Failed to read request body: {:?}", e);
        e
    })?;

    if body_bytes.len() > max_payload_bytes {
        console_error!(
            "Payload too large: {} bytes (max: {} bytes / {} MB)",
            body_bytes.len(),
            max_payload_bytes,
            max_payload_bytes / (1024 * 1024)
        );
        return Response::error(
            format!(
                "Payload too large: {} bytes exceeds limit of {} MB",
                body_bytes.len(),
                max_payload_bytes / (1024 * 1024)
            ),
            413,
        );
    }

    // Route to appropriate handler based on signal type
    match signal {
        SignalKind::Logs => {
            handlers::handle_logs_request(&body_bytes, format, content_type, &storage).await
        }
        SignalKind::Traces => {
            handlers::handle_traces_request(&body_bytes, format, content_type, &storage).await
        }
        SignalKind::Metrics => {
            handlers::handle_metrics_request(&body_bytes, format, content_type, &storage).await
        }
    }
}
