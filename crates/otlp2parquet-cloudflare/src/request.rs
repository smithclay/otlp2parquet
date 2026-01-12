//! HTTP request handling for the Cloudflare Worker.
//!
//! Contains the main OTLP request handler and supporting functions.

use crate::do_config::WorkerEnvSource;
use crate::{auth, errors, handlers, TraceContext};
use flate2::read::GzDecoder;
use once_cell::sync::OnceCell;
use otlp2parquet_common::config::{Platform, RuntimeConfig};
use std::io::Read;
use worker::*;

static CONFIG: OnceCell<RuntimeConfig> = OnceCell::new();

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
    let format = otlp2parquet_common::InputFormat::from_content_type(content_type);
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

    // Process signals directly - write to R2 per-request
    match signal {
        "logs" => {
            handlers::handle_logs_request(&body_bytes, format, content_type, &trace_ctx.request_id)
                .await
        }
        "traces" => {
            handlers::handle_traces_request(
                &body_bytes,
                format,
                content_type,
                &trace_ctx.request_id,
            )
            .await
        }
        "metrics" => {
            handlers::handle_metrics_request(
                &body_bytes,
                format,
                content_type,
                &trace_ctx.request_id,
            )
            .await
        }
        _ => unreachable!("signal validated above"),
    }
}

fn load_worker_config(env: &Env) -> Result<RuntimeConfig> {
    use otlp2parquet_common::config::EnvSource;

    tracing::debug!("load_worker_config: starting");
    let provider = WorkerEnvSource { env };
    let inline = provider.get("CONFIG_CONTENT");

    tracing::debug!("Loading config for Platform::CloudflareWorkers");
    let config = RuntimeConfig::load_for_platform_with_env(
        Platform::CloudflareWorkers,
        inline.as_deref(),
        &provider,
    )
    .map_err(|e| {
        tracing::error!(error = ?e, "Failed to load configuration");
        worker::Error::RustError(format!("Config error: {}", e))
    })?;

    tracing::debug!(
        storage_backend = ?config.storage.backend,
        "Config loaded successfully"
    );

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
