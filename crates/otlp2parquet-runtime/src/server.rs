// Server mode - Full-featured HTTP server with multi-backend storage
//
// This is the default/general-purpose mode that can run anywhere:
// - Docker containers
// - Kubernetes
// - Local development
// - VM instances
//
// Features:
// - Axum HTTP server (HTTP/1.1, HTTP/2)
// - Multi-backend storage (S3, R2, Filesystem, GCS)
// - Structured logging with tracing
// - Graceful shutdown
// - Production-ready

use crate::batcher::{
    max_payload_bytes_from_env, processing_options_from_env, BatchConfig, BatchManager,
    CompletedBatch, PassthroughBatcher,
};
use anyhow::{Context, Result};
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use metrics::{counter, histogram};
use otlp2parquet_core::{otlp, InputFormat, ProcessingOptions};
use serde_json::json;
use std::sync::Arc;
use std::time::Instant;
use tokio::signal;
use tracing::{debug, error, info, warn};

use crate::opendal_storage::OpenDalStorage;

/// Application state shared across all requests
#[derive(Clone)]
struct AppState {
    storage: Arc<OpenDalStorage>,
    batcher: Option<Arc<BatchManager>>,
    passthrough: PassthroughBatcher,
    processing_options: ProcessingOptions,
    max_payload_bytes: usize,
}

/// Error type that implements IntoResponse
struct AppError {
    status: StatusCode,
    error: anyhow::Error,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        error!("Request error: {:?}", self.error);
        (
            self.status,
            Json(json!({
                "error": self.error.to_string(),
            })),
        )
            .into_response()
    }
}

impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            error: err.into(),
        }
    }
}

impl AppError {
    fn with_status(status: StatusCode, error: anyhow::Error) -> Self {
        Self { status, error }
    }
}

/// POST /v1/logs - OTLP log ingestion endpoint
async fn handle_logs(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, AppError> {
    // Detect input format from Content-Type header
    let content_type = headers.get("content-type").and_then(|v| v.to_str().ok());
    let format = InputFormat::from_content_type(content_type);

    debug!(
        "Received OTLP logs request ({} bytes, format: {:?}, content-type: {:?})",
        body.len(),
        format,
        content_type
    );

    let max_payload = state.max_payload_bytes;
    if body.len() > max_payload {
        counter!("otlp.ingest.rejected", 1);
        return Err(AppError::with_status(
            StatusCode::PAYLOAD_TOO_LARGE,
            anyhow::anyhow!("payload {} exceeds limit {}", body.len(), max_payload),
        ));
    }

    let start = Instant::now();
    counter!("otlp.ingest.requests", 1);
    histogram!("otlp.ingest.bytes", body.len() as f64);

    let request =
        otlp::parse_otlp_request(&body, format).context("Failed to parse OTLP request payload")?;

    let mut uploads: Vec<CompletedBatch> = Vec::new();
    let metadata;

    if let Some(batcher) = &state.batcher {
        let mut expired = batcher
            .drain_expired()
            .context("Failed to flush expired batches")?;
        uploads.append(&mut expired);

        let (mut ready, meta) = batcher.ingest(request).context("Failed to enqueue batch")?;
        uploads.append(&mut ready);
        metadata = meta;
    } else {
        let batch = state
            .passthrough
            .ingest(request, &state.processing_options)
            .context("Failed to write Parquet batch")?;
        metadata = batch.metadata.clone();
        uploads.push(batch);
    }

    counter!("otlp.ingest.records", metadata.record_count as u64);

    let mut uploaded_paths = Vec::new();
    for batch in uploads {
        let hash_hex = batch.content_hash.to_hex().to_string();
        let partition_path = crate::partition::generate_partition_path(
            &batch.metadata.service_name,
            batch.metadata.first_timestamp_nanos,
            &hash_hex,
        );

        let size_bytes = batch.bytes.len();
        state
            .storage
            .write(&partition_path, batch.bytes)
            .await
            .context("Failed to write to storage")?;

        counter!("otlp.batch.flushes", 1);
        histogram!("otlp.batch.rows", batch.metadata.record_count as f64);
        histogram!("otlp.batch.size_bytes", size_bytes as f64);
        info!(
            "Committed batch path={} service={} rows={}",
            partition_path, batch.metadata.service_name, batch.metadata.record_count
        );
        uploaded_paths.push(partition_path);
    }

    histogram!(
        "otlp.ingest.latency_ms",
        start.elapsed().as_secs_f64() * 1000.0
    );

    let response = Json(json!({
        "status": "ok",
        "records_processed": metadata.record_count,
        "flush_count": uploaded_paths.len(),
        "partitions": uploaded_paths,
    }));

    Ok((StatusCode::OK, response).into_response())
}

/// GET /health - Basic health check
async fn health_check() -> impl IntoResponse {
    (StatusCode::OK, Json(json!({"status": "healthy"})))
}

/// GET /ready - Readiness check (includes storage connectivity)
async fn ready_check(State(state): State<AppState>) -> impl IntoResponse {
    // Test storage connectivity by listing (basic check)
    match state.storage.list("").await {
        Ok(_) => (
            StatusCode::OK,
            Json(json!({"status": "ready", "storage": "connected"})),
        ),
        Err(e) => {
            warn!("Storage readiness check failed: {}", e);
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(
                    json!({"status": "not ready", "storage": "disconnected", "error": e.to_string()}),
                ),
            )
        }
    }
}

/// Initialize storage backend based on STORAGE_BACKEND env var
fn init_storage() -> Result<Arc<OpenDalStorage>> {
    let backend = std::env::var("STORAGE_BACKEND").unwrap_or_else(|_| "fs".to_string());

    info!("Initializing storage backend: {}", backend);

    let storage = match backend.as_str() {
        "fs" => {
            let path = std::env::var("STORAGE_PATH").unwrap_or_else(|_| "./data".to_string());
            info!("Using filesystem storage at: {}", path);
            OpenDalStorage::new_fs(&path)?
        }
        "s3" => {
            let bucket =
                std::env::var("S3_BUCKET").context("S3_BUCKET env var required for s3 backend")?;
            let region =
                std::env::var("S3_REGION").context("S3_REGION env var required for s3 backend")?;
            let endpoint = std::env::var("S3_ENDPOINT").ok();

            // Credentials auto-discovered from environment (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
            // or IAM role (recommended for production)
            info!("Using S3 storage: bucket={}, region={}", bucket, region);
            OpenDalStorage::new_s3(&bucket, &region, endpoint.as_deref(), None, None)?
        }
        "r2" => {
            let bucket =
                std::env::var("R2_BUCKET").context("R2_BUCKET env var required for r2 backend")?;
            let account_id = std::env::var("R2_ACCOUNT_ID")
                .context("R2_ACCOUNT_ID env var required for r2 backend")?;
            let access_key_id = std::env::var("R2_ACCESS_KEY_ID")
                .context("R2_ACCESS_KEY_ID env var required for r2 backend")?;
            let secret_access_key = std::env::var("R2_SECRET_ACCESS_KEY")
                .context("R2_SECRET_ACCESS_KEY env var required for r2 backend")?;

            info!(
                "Using R2 storage: account={}, bucket={}",
                account_id, bucket
            );
            OpenDalStorage::new_r2(&bucket, &account_id, &access_key_id, &secret_access_key)?
        }
        _ => {
            anyhow::bail!(
                "Unsupported storage backend: {}. Supported: fs, s3, r2",
                backend
            );
        }
    };

    Ok(Arc::new(storage))
}

/// Initialize tracing/logging
fn init_tracing() {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};

    // Default to INFO level, override with LOG_LEVEL env var
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    // Check if we should use JSON output (for structured logging in production)
    let json_logs = std::env::var("LOG_FORMAT")
        .map(|f| f == "json")
        .unwrap_or(false);

    let registry = tracing_subscriber::registry().with(env_filter);

    if json_logs {
        registry.with(fmt::layer().json()).init();
    } else {
        registry.with(fmt::layer()).init();
    }
}

/// Graceful shutdown handler
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!("Received Ctrl+C, starting graceful shutdown...");
        },
        _ = terminate => {
            info!("Received SIGTERM, starting graceful shutdown...");
        },
    }
}

/// Entry point for server mode
#[cfg(feature = "server")]
pub async fn run() -> Result<()> {
    // Initialize tracing
    init_tracing();

    info!("Server mode - full-featured HTTP server with multi-backend storage");

    // Get configuration
    let addr = std::env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());

    // Initialize storage
    let storage = init_storage()?;

    // Configure batching + row group behaviour
    let processing_options = processing_options_from_env();
    let batch_config = BatchConfig::from_env(200_000, 128 * 1024 * 1024, 10);
    let batcher = if batch_config.max_rows == 0 || batch_config.max_bytes == 0 {
        info!(
            "Batching disabled (max_rows={} max_bytes={})",
            batch_config.max_rows, batch_config.max_bytes
        );
        None
    } else {
        let cfg = batch_config.clone();
        info!(
            "Batching enabled (max_rows={} max_bytes={} max_age={}s, row_group_rows={})",
            cfg.max_rows,
            cfg.max_bytes,
            cfg.max_age.as_secs(),
            processing_options.max_rows_per_batch
        );
        Some(Arc::new(BatchManager::new(cfg, processing_options.clone())))
    };

    let max_payload_bytes = max_payload_bytes_from_env(8 * 1024 * 1024);
    info!("Max payload size set to {} bytes", max_payload_bytes);

    // Create app state
    let state = AppState {
        storage,
        batcher,
        passthrough: PassthroughBatcher,
        processing_options,
        max_payload_bytes,
    };

    // Build router
    let app = Router::new()
        .route("/v1/logs", post(handle_logs))
        .route("/health", get(health_check))
        .route("/ready", get(ready_check))
        .with_state(state);

    // Create TCP listener
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .context(format!("Failed to bind to {}", addr))?;

    info!("OTLP HTTP endpoint listening on http://{}", addr);
    info!("Routes:");
    info!("  POST http://{}/v1/logs - OTLP log ingestion", addr);
    info!("  GET  http://{}/health  - Health check", addr);
    info!("  GET  http://{}/ready   - Readiness check", addr);
    info!("Press Ctrl+C or send SIGTERM to stop");

    // Start server with graceful shutdown
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("Server error")?;

    info!("Server shutdown complete");

    Ok(())
}
