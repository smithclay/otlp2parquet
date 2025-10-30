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

use anyhow::{Context, Result};
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde_json::json;
use std::sync::Arc;
use tokio::signal;
use tracing::{error, info, warn};

use crate::opendal_storage::OpenDalStorage;

/// Application state shared across all requests
#[derive(Clone)]
struct AppState {
    storage: Arc<OpenDalStorage>,
}

/// Error type that implements IntoResponse
struct AppError(anyhow::Error);

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        error!("Request error: {:?}", self.0);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "error": self.0.to_string(),
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
        Self(err.into())
    }
}

/// POST /v1/logs - OTLP log ingestion endpoint
async fn handle_logs(
    State(state): State<AppState>,
    body: axum::body::Bytes,
) -> Result<Json<serde_json::Value>, AppError> {
    info!("Received OTLP logs request ({} bytes)", body.len());

    // Process OTLP logs (PURE - no I/O, deterministic)
    let mut parquet_bytes = Vec::new();
    let metadata = otlp2parquet_core::process_otlp_logs_into(&body, &mut parquet_bytes)
        .context("Failed to process OTLP logs")?;

    info!(
        "Processed {} log records from service '{}'",
        metadata.record_count, metadata.service_name
    );

    // Generate partition path (platform-specific storage decision)
    let partition_path = crate::partition::generate_partition_path(
        &metadata.service_name,
        metadata.first_timestamp_nanos,
    );

    info!("Writing to partition: {}", partition_path);

    // Write to storage (async)
    state
        .storage
        .write(&partition_path, parquet_bytes)
        .await
        .context("Failed to write to storage")?;

    info!("Successfully wrote parquet file to storage");

    Ok(Json(json!({
        "status": "ok",
        "records_processed": metadata.record_count,
        "partition_path": partition_path,
    })))
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

    // Create app state
    let state = AppState { storage };

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
