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
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use otlp2parquet_batch::{BatchConfig, BatchManager, PassthroughBatcher};
use otlp2parquet_storage::opendal_storage::OpenDalStorage;
use otlp2parquet_storage::ParquetWriter;
use serde_json::json;
use std::sync::Arc;
use tokio::signal;
use tracing::{error, info};

mod handlers;
mod init;

use handlers::{handle_logs, health_check, ready_check};
use init::{init_storage, init_tracing, max_payload_bytes_from_env};

/// Application state shared across all requests
#[derive(Clone)]
pub(crate) struct AppState {
    pub storage: Arc<OpenDalStorage>,
    pub parquet_writer: Arc<ParquetWriter>,
    pub batcher: Option<Arc<BatchManager>>,
    pub passthrough: PassthroughBatcher,
    pub max_payload_bytes: usize,
}

/// Error type that implements IntoResponse
pub(crate) struct AppError {
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
    pub fn with_status(status: StatusCode, error: anyhow::Error) -> Self {
        Self { status, error }
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
pub async fn run() -> Result<()> {
    // Initialize tracing
    init_tracing();

    info!("Server mode - full-featured HTTP server with multi-backend storage");

    // Get configuration
    let addr = std::env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());

    // Initialize storage
    let storage = init_storage()?;
    let parquet_writer = Arc::new(ParquetWriter::new(storage.operator().clone()));

    // Configure batching
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
            "Batching enabled (max_rows={} max_bytes={} max_age={}s)",
            cfg.max_rows,
            cfg.max_bytes,
            cfg.max_age.as_secs()
        );
        Some(Arc::new(BatchManager::new(cfg)))
    };

    let max_payload_bytes = max_payload_bytes_from_env(8 * 1024 * 1024);
    info!("Max payload size set to {} bytes", max_payload_bytes);

    // Create app state
    let state = AppState {
        storage,
        parquet_writer,
        batcher,
        passthrough: PassthroughBatcher,
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
