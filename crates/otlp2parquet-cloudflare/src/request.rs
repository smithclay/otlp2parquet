//! HTTP request handling for the Cloudflare Worker.
//!
//! Contains the main OTLP request handler and supporting functions.

use crate::do_config::apply_namespace_fallback;
use crate::do_config::WorkerEnvSource;
use crate::{auth, catalog_worker, errors, handlers, ingest, TraceContext};
use flate2::read::GzDecoder;
use once_cell::sync::OnceCell;
use otlp2parquet_core::config::{CatalogMode, Platform, RuntimeConfig};
use otlp2parquet_writer::set_table_name_overrides;
use serde::Deserialize;
use std::io::Read;
use uuid::Uuid;
use worker::*;

static CONFIG: OnceCell<RuntimeConfig> = OnceCell::new();

/// Payload for Durable Object receipt callbacks.
#[derive(Debug, Deserialize, serde::Serialize)]
struct ReceiptPayload {
    path: String,
    table: String,
    rows: usize,
    timestamp_ms: i64,
}

/// Decompress gzip-encoded request body if Content-Encoding header indicates gzip.
fn maybe_decompress(
    data: &[u8],
    content_encoding: Option<&str>,
    request_id: Option<&str>,
) -> std::result::Result<Vec<u8>, Response> {
    match content_encoding {
        Some(enc) if enc.eq_ignore_ascii_case("gzip") => {
            let mut decoder = GzDecoder::new(data);
            let mut decompressed = Vec::new();
            decoder.read_to_end(&mut decompressed).map_err(|e| {
                let error = errors::OtlpErrorKind::InvalidRequest(format!(
                    "gzip decompression failed: {}",
                    e
                ));
                let status_code = error.status_code();
                errors::ErrorResponse::from_error(error, request_id.map(String::from))
                    .into_response(status_code)
                    .unwrap_or_else(|_| Response::error("Decompression failed", 400).unwrap())
            })?;
            Ok(decompressed)
        }
        _ => Ok(data.to_vec()),
    }
}

/// Internal endpoint for Durable Objects to submit KV receipts.
/// This is only callable via the SELF service binding (internal to the Worker).
#[tracing::instrument(
    name = "otlp.receipt",
    skip(req, env),
    fields(
        path = tracing::field::Empty,
        table = tracing::field::Empty,
    )
)]
async fn handle_receipt(mut req: Request, env: &Env) -> Result<Response> {
    tracing::debug!("handle_receipt called");

    let kv = env.kv("PENDING_FILES").map_err(|e| {
        tracing::error!(error = %e, "PENDING_FILES KV binding failed");
        worker::Error::RustError("PENDING_FILES KV not bound".to_string())
    })?;
    tracing::debug!("Got KV binding");

    let payload: ReceiptPayload = req.json().await.map_err(|e| {
        tracing::error!(error = %e, "Receipt JSON parse failed");
        worker::Error::RustError(format!("Invalid receipt payload: {}", e))
    })?;
    tracing::debug!(path = %payload.path, table = %payload.table, "Parsed receipt payload");

    // Record fields in the span
    tracing::Span::current().record("path", payload.path.as_str());
    tracing::Span::current().record("table", payload.table.as_str());

    let key = format!("pending:{}:{}", payload.timestamp_ms, Uuid::new_v4());
    let value = serde_json::to_string(&payload)
        .map_err(|e| worker::Error::RustError(format!("Serialize receipt failed: {}", e)))?;

    tracing::debug!(key = %key, "Writing receipt to KV");
    kv.put(&key, value)
        .map_err(|e| {
            tracing::error!(error = %e, "KV put init failed");
            worker::Error::RustError(format!("KV put init failed: {}", e))
        })?
        .execute()
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "KV execute failed");
            worker::Error::RustError(format!("KV receipt write failed: {}", e))
        })?;

    tracing::debug!("Successfully stored receipt in KV");
    Response::ok("ok")
}

/// Internal endpoint for smoke tests to trigger catalog sync immediately.
async fn handle_sync_catalog(env: &Env) -> Result<Response> {
    tracing::info!("handle_sync_catalog called (test hook)");
    match catalog_worker::sync_catalog_with_report(env).await {
        Ok(report) => {
            let body = serde_json::to_string(&report).unwrap_or_else(|_| "ok".to_string());
            Response::ok(body)
        }
        Err(e) => {
            tracing::error!(error = ?e, "Catalog sync failed via test hook");
            Response::error(format!("sync_catalog failed: {}", e), 500)
        }
    }
}

/// Handle OTLP HTTP POST request and write to R2.
///
/// This is the main request handler called by the Worker fetch event.
#[tracing::instrument(
    name = "otlp.request",
    skip(req, env, _ctx),
    fields(
        request_id = tracing::field::Empty,
        signal_type = tracing::field::Empty,
        service_name = tracing::field::Empty,
        record_count = tracing::field::Empty,
        bytes = tracing::field::Empty,
        error = tracing::field::Empty,
    )
)]
pub(crate) async fn handle(mut req: Request, env: Env, _ctx: Context) -> Result<Response> {
    tracing::debug!("handle_otlp_request: start");

    // Extract headers into owned Strings for TraceContext
    let request_id_header = req.headers().get("x-request-id").ok().flatten();
    let traceparent_header = req.headers().get("traceparent").ok().flatten();

    // Extract or create trace context from incoming headers
    let trace_ctx = TraceContext::from_headers(|name| match name {
        "x-request-id" | "X-Request-Id" => request_id_header.as_deref(),
        "traceparent" | "Traceparent" => traceparent_header.as_deref(),
        _ => None,
    });

    // Record request_id in the span
    tracing::Span::current().record("request_id", trace_ctx.request_id.as_str());
    tracing::debug!(request_id = %trace_ctx.request_id, "Processing request");

    // Only accept POST requests
    if req.method() != Method::Post {
        tracing::Span::current().record("error", "method_not_allowed");
        return Response::error("Method not allowed", 405);
    }

    let path = req.path();
    tracing::debug!(path = %path, "Request path");
    if path == "/__internal/receipt" {
        return handle_receipt(req, &env).await;
    } else if path == "/__internal/sync_catalog" {
        return handle_sync_catalog(&env).await;
    }

    // Validate signal path
    let signal = match path.as_str() {
        "/v1/logs" => "logs",
        "/v1/traces" => "traces",
        "/v1/metrics" => "metrics",
        _ => {
            tracing::Span::current().record("error", "not_found");
            return Response::error("Not found", 404);
        }
    };

    // Record signal_type in the span
    tracing::Span::current().record("signal_type", signal);

    tracing::debug!("Checking auth");
    // Check basic authentication if enabled
    if let Err(response) = auth::check_basic_auth(&req, &env, Some(&trace_ctx.request_id)) {
        tracing::Span::current().record("error", "auth_failed");
        return Ok(response);
    }

    tracing::debug!("Loading config");
    // Load config once (with platform detection and env var overrides)
    let config = CONFIG
        .get_or_try_init(|| load_worker_config(&env))
        .map_err(|e| {
            tracing::Span::current().record("error", "config_error");
            tracing::error!(request_id = %trace_ctx.request_id, error = ?e, "Failed to load configuration");
            e
        })?;
    tracing::debug!("Config loaded successfully");

    // Initialize storage operator in writer crate if not already done
    // Catalog is handled by Cron Worker, not hot path
    // Just ensure storage is initialized for plain Parquet writes
    otlp2parquet_writer::initialize_storage(config).map_err(|e| {
        tracing::Span::current().record("error", "storage_init_failed");
        tracing::error!(request_id = %trace_ctx.request_id, error = ?e, "Failed to initialize storage");
        let error = errors::OtlpErrorKind::StorageError(format!(
            "Failed to initialize R2 storage: {}. Verify AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and OTLP2PARQUET_R2_BUCKET are correctly configured.",
            e
        ));
        let error_response = errors::ErrorResponse::from_error(error, Some(trace_ctx.request_id.clone()));
        worker::Error::RustError(serde_json::to_string(&error_response).unwrap_or_default())
    })?;

    // Use configurable max payload size (default 10MB for CF Workers)
    // Can be overridden via OTLP2PARQUET_MAX_PAYLOAD_BYTES env var
    let max_payload_bytes = env
        .var("OTLP2PARQUET_MAX_PAYLOAD_BYTES")
        .ok()
        .and_then(|val| val.to_string().parse::<usize>().ok())
        .unwrap_or(config.request.max_payload_bytes);

    tracing::debug!(
        signal,
        max_payload_mb = max_payload_bytes / (1024 * 1024),
        "Processing request"
    );

    let content_type_header = req.headers().get("content-type").ok().flatten();
    let content_type = content_type_header.as_deref();
    let format = otlp2parquet_core::InputFormat::from_content_type(content_type);
    let content_encoding = req.headers().get("content-encoding").ok().flatten();

    let raw_body = req.bytes().await.map_err(|e| {
        tracing::error!(error = ?e, "Failed to read request body");
        e
    })?;

    // Decompress if gzip-encoded (OTel collectors typically send gzip by default)
    let body_bytes = match maybe_decompress(
        &raw_body,
        content_encoding.as_deref(),
        Some(&trace_ctx.request_id),
    ) {
        Ok(bytes) => bytes,
        Err(response) => return Ok(response),
    };

    // Record bytes in the span
    tracing::Span::current().record("bytes", body_bytes.len());

    // Check payload size AFTER decompression (prevents zip bombs)
    if body_bytes.len() > max_payload_bytes {
        tracing::Span::current().record("error", "payload_too_large");
        tracing::error!(
            payload_bytes = body_bytes.len(),
            max_bytes = max_payload_bytes,
            max_mb = max_payload_bytes / (1024 * 1024),
            "Payload too large"
        );
        let error = errors::OtlpErrorKind::PayloadTooLarge(format!(
            "Payload too large: {} bytes exceeds limit of {} MB",
            body_bytes.len(),
            max_payload_bytes / (1024 * 1024)
        ));
        let status_code = error.status_code();
        return errors::ErrorResponse::from_error(error, Some(trace_ctx.request_id.clone()))
            .into_response(status_code);
    }

    // Check if batching is enabled (DO-based batching for Workers)
    let batching_enabled = config.batch.enabled;

    // Route to batching or direct handler based on config
    if batching_enabled {
        let ctx = ingest::BatchContext {
            env: &env,
            request_id: &trace_ctx.request_id,
            trace_ctx: &trace_ctx,
        };
        return match signal {
            "logs" => ingest::handle_batched_logs(&ctx, &body_bytes, format).await,
            "traces" => ingest::handle_batched_traces(&ctx, &body_bytes, format).await,
            "metrics" => ingest::handle_batched_metrics(&ctx, &body_bytes, format).await,
            _ => unreachable!("signal validated above"),
        };
    }

    // Direct handler mode (no batching)
    // Initialize catalog for direct registration when catalog mode is Iceberg
    let namespace = catalog_worker::resolve_namespace(config, &env);
    let catalog = if config.catalog_mode == CatalogMode::Iceberg {
        tracing::debug!("Initializing catalog for direct registration (non-batching mode)");
        match catalog_worker::init_catalog_from_env(&env, config, &namespace).await {
            Ok(cat) => Some(cat),
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "Failed to initialize catalog, proceeding without catalog registration"
                );
                None
            }
        }
    } else {
        None
    };

    let catalog_ref = catalog.as_deref();

    match signal {
        "logs" => {
            handlers::handle_logs_request(
                &body_bytes,
                format,
                content_type,
                &namespace,
                &trace_ctx.request_id,
                catalog_ref,
            )
            .await
        }
        "traces" => {
            handlers::handle_traces_request(
                &body_bytes,
                format,
                content_type,
                &namespace,
                &trace_ctx.request_id,
                catalog_ref,
            )
            .await
        }
        "metrics" => {
            handlers::handle_metrics_request(
                &body_bytes,
                format,
                content_type,
                &namespace,
                &trace_ctx.request_id,
                catalog_ref,
            )
            .await
        }
        _ => unreachable!("signal validated above"),
    }
}

fn load_worker_config(env: &Env) -> Result<RuntimeConfig> {
    use otlp2parquet_core::config::EnvSource;

    tracing::debug!("load_worker_config: starting");
    let provider = WorkerEnvSource { env };
    let inline = provider.get("CONFIG_CONTENT");

    tracing::debug!("Loading config for Platform::CloudflareWorkers");
    let mut config = RuntimeConfig::load_for_platform_with_env(
        Platform::CloudflareWorkers,
        inline.as_deref(),
        &provider,
    )
    .map_err(|e| {
        tracing::error!(error = ?e, "Failed to load configuration");
        worker::Error::RustError(format!("Config error: {}", e))
    })?;

    // Ensure namespace is picked up even if other Iceberg envs were missing
    apply_namespace_fallback(env, &mut config);

    tracing::debug!(
        catalog_mode = ?config.catalog_mode,
        storage_backend = ?config.storage.backend,
        "Config loaded successfully"
    );

    if let Some(iceberg) = config.iceberg.as_ref() {
        set_table_name_overrides(iceberg.to_tables_map());
    }

    if let Some(ref r2) = config.storage.r2 {
        tracing::debug!(
            bucket = %r2.bucket,
            account_id = %r2.account_id,
            access_key_id_set = !r2.access_key_id.is_empty(),
            secret_access_key_set = !r2.secret_access_key.is_empty(),
            endpoint = ?r2.endpoint,
            "R2 config loaded"
        );
    } else {
        tracing::debug!("R2 config: None");
    }

    Ok(config)
}
