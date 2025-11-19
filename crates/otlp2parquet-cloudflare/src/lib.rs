// Cloudflare Workers runtime adapter
//
// Uses icepick with OpenDAL S3 for R2 storage and handles incoming requests via Worker fetch events
//
// Philosophy: Use otlp2parquet-writer for unified write path
// Worker crate provides the runtime, icepick provides storage abstraction
// Entry point is #[event(fetch)] macro, not main()

mod auth;
mod handlers;

use once_cell::sync::OnceCell;
use otlp2parquet_config::{EnvSource, Platform, RuntimeConfig, ENV_PREFIX};
use otlp2parquet_writer::IcepickWriter;
use std::sync::Arc;
use worker::*;

static WRITER: OnceCell<Arc<IcepickWriter>> = OnceCell::new();
static CONFIG: OnceCell<RuntimeConfig> = OnceCell::new();

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
    let config = CONFIG.get_or_try_init(|| load_worker_config(&env))?;

    let writer = if let Some(existing) = WRITER.get() {
        existing.clone()
    } else {
        // Generic S3-compatible storage configuration
        // Supports R2 (production), MinIO (local dev), or any S3-compatible endpoint
        // Extract R2 Data Catalog configuration from environment variables
        let account_id = env
            .var("CLOUDFLARE_ACCOUNT_ID")
            .map_err(|e| {
                console_error!(
                    "CLOUDFLARE_ACCOUNT_ID environment variable not set: {:?}",
                    e
                );
                worker::Error::RustError(
                    "CLOUDFLARE_ACCOUNT_ID environment variable required".to_string(),
                )
            })?
            .to_string();

        let bucket_name = env
            .var("CLOUDFLARE_BUCKET_NAME")
            .map_err(|e| {
                console_error!(
                    "CLOUDFLARE_BUCKET_NAME environment variable not set: {:?}",
                    e
                );
                worker::Error::RustError(
                    "CLOUDFLARE_BUCKET_NAME environment variable required".to_string(),
                )
            })?
            .to_string();

        let api_token = env
            .secret("CLOUDFLARE_API_TOKEN")
            .or_else(|_| env.var("CLOUDFLARE_API_TOKEN"))
            .map_err(|e| {
                console_error!(
                    "CLOUDFLARE_API_TOKEN environment variable/secret not set: {:?}",
                    e
                );
                worker::Error::RustError(
                    "CLOUDFLARE_API_TOKEN environment variable required".to_string(),
                )
            })?
            .to_string();

        let namespace = env.var("OTLP_NAMESPACE").ok().map(|v| v.to_string());

        console_log!(
            "Initializing R2 Data Catalog: account={}, bucket={}, namespace={:?}",
            account_id,
            bucket_name,
            namespace.as_deref().unwrap_or("otlp")
        );

        let instance = Arc::new(
            otlp2parquet_writer::initialize_cloudflare_writer(
                &account_id,
                &bucket_name,
                &api_token,
                namespace,
            )
            .await
            .map_err(|e| {
                console_error!("Failed to initialize writer: {:?}", e);
                worker::Error::RustError(format!("Writer initialization error: {}", e))
            })?,
        );

        let _ = WRITER.set(instance.clone());
        instance
    };

    // Use configurable max payload size (default 10MB for CF Workers)
    // Can be overridden via OTLP2PARQUET_MAX_PAYLOAD_BYTES env var
    let max_payload_bytes = env
        .var("OTLP2PARQUET_MAX_PAYLOAD_BYTES")
        .ok()
        .and_then(|val| val.to_string().parse::<usize>().ok())
        .unwrap_or(config.request.max_payload_bytes);

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
            handlers::handle_logs_request(&body_bytes, format, content_type, &writer).await
        }
        SignalKind::Traces => {
            handlers::handle_traces_request(&body_bytes, format, content_type, &writer).await
        }
        SignalKind::Metrics => {
            handlers::handle_metrics_request(&body_bytes, format, content_type, &writer).await
        }
    }
}

fn load_worker_config(env: &Env) -> Result<RuntimeConfig> {
    let provider = WorkerEnvSource { env };
    let inline = provider.get("CONFIG_CONTENT");
    RuntimeConfig::load_for_platform_with_env(
        Platform::CloudflareWorkers,
        inline.as_deref(),
        &provider,
    )
    .map_err(|e| {
        console_error!("Failed to load configuration: {:?}", e);
        worker::Error::RustError(format!("Config error: {}", e))
    })
}

struct WorkerEnvSource<'a> {
    env: &'a Env,
}

impl<'a> EnvSource for WorkerEnvSource<'a> {
    fn get(&self, key: &str) -> Option<String> {
        let binding = format!("{}{}", ENV_PREFIX, key);
        if let Ok(val) = self.env.var(&binding) {
            return Some(val.to_string());
        }
        if key.contains("SECRET") {
            if let Ok(secret) = self.env.secret(&binding) {
                return Some(secret.to_string());
            }
        }
        None
    }
}
