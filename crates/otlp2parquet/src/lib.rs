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
use otlp2parquet_common::config::RuntimeConfig;

mod batch;

use batch::{BatchConfig, BatchManager};
use otlp2parquet_writer::set_parquet_row_group_size;
use serde_json::json;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tower_http::decompression::RequestDecompressionLayer;
use tracing::{debug, error, info, warn};

mod handlers;
mod init;

pub mod connect;
pub mod deploy;

use handlers::{handle_logs, handle_metrics, handle_traces, health_check, ready_check};
pub use init::init_tracing;
use init::init_writer;

/// Application state shared across all requests
#[derive(Clone)]
pub(crate) struct AppState {
    pub batcher: Option<Arc<BatchManager>>,
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

impl AppError {
    pub fn with_status(status: StatusCode, error: anyhow::Error) -> Self {
        Self { status, error }
    }

    pub fn bad_request<E>(error: E) -> Self
    where
        E: Into<anyhow::Error>,
    {
        Self {
            status: StatusCode::BAD_REQUEST,
            error: error.into(),
        }
    }

    pub fn internal<E>(error: E) -> Self
    where
        E: Into<anyhow::Error>,
    {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            error: error.into(),
        }
    }
}

/// Graceful shutdown handler
async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = signal::ctrl_c().await {
            tracing::error!("Failed to install Ctrl+C handler: {}", e);
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(e) => {
                tracing::error!("Failed to install SIGTERM handler: {}", e);
            }
        }
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

/// Entry point for server mode (loads config automatically)
pub async fn run() -> Result<()> {
    let config = RuntimeConfig::load().context("Failed to load configuration")?;
    run_with_config(config).await
}

/// Entry point for server mode with pre-loaded configuration (for CLI usage)
pub async fn run_with_config(config: RuntimeConfig) -> Result<()> {
    // Initialize tracing with config
    init_tracing(&config);

    // Configure Parquet writer properties before first use
    set_parquet_row_group_size(config.storage.parquet_row_group_size);

    info!("Server mode - full-featured HTTP server with multi-backend storage");

    // Get listen address from config
    let addr = config
        .server
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("server config required"))?
        .listen_addr
        .clone();

    // Initialize storage
    init_writer(&config)?;

    // Configure batching
    let batch_config = BatchConfig {
        max_rows: config.batch.max_rows,
        max_bytes: config.batch.max_bytes,
        max_age: Duration::from_secs(config.batch.max_age_secs),
    };

    let batcher = if !config.batch.enabled {
        info!("Batching disabled by configuration");
        None
    } else {
        info!(
            "Batching enabled (max_rows={} max_bytes={} max_age={}s)",
            batch_config.max_rows,
            batch_config.max_bytes,
            batch_config.max_age.as_secs()
        );
        Some(Arc::new(BatchManager::new(batch_config)))
    };

    let max_payload_bytes = config.request.max_payload_bytes;
    info!("Max payload size set to {} bytes", max_payload_bytes);

    // Create app state
    let state = AppState {
        batcher,
        max_payload_bytes,
    };

    let router_state = state.clone();

    // Build router with gzip decompression support
    // OTel collectors typically send gzip-compressed payloads by default
    let app = Router::new()
        .route("/v1/logs", post(handle_logs))
        .route("/v1/traces", post(handle_traces))
        .route("/v1/metrics", post(handle_metrics))
        .route("/health", get(health_check))
        .route("/ready", get(ready_check))
        .layer(RequestDecompressionLayer::new().gzip(true))
        .with_state(router_state);

    // Create TCP listener
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .context(format!("Failed to bind to {}", addr))?;

    info!("OTLP HTTP endpoint listening on http://{}", addr);
    info!("Routes:");
    info!("  POST http://{}/v1/logs    - OTLP log ingestion", addr);
    info!("  POST http://{}/v1/metrics - OTLP metrics ingestion", addr);
    info!("  POST http://{}/v1/traces  - OTLP trace ingestion", addr);
    info!("  GET  http://{}/health     - Health check", addr);
    info!("  GET  http://{}/ready      - Readiness check", addr);
    info!("Press Ctrl+C or send SIGTERM to stop");

    // Spawn background flush task if batching is enabled
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let flush_handle = if state.batcher.is_some() {
        let flush_state = state.clone();
        let flush_shutdown = Arc::clone(&shutdown_flag);
        let flush_interval =
            Duration::from_secs(config.batch.max_age_secs.max(1) / 2).max(Duration::from_secs(1));
        Some(tokio::spawn(async move {
            run_background_flush(flush_state, flush_shutdown, flush_interval).await;
        }))
    } else {
        None
    };

    // Start server with graceful shutdown
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("Server error")?;

    // Signal background task to stop and wait for it
    shutdown_flag.store(true, Ordering::SeqCst);
    if let Some(handle) = flush_handle {
        let _ = handle.await;
    }

    flush_pending_batches(&state).await?;

    info!("Server shutdown complete");

    Ok(())
}

async fn flush_pending_batches(state: &AppState) -> Result<()> {
    if let Some(batcher) = &state.batcher {
        let pending = batcher
            .drain_all()
            .context("Failed to drain pending log batches during shutdown")?;

        if pending.is_empty() {
            return Ok(());
        }

        info!(
            batch_count = pending.len(),
            "Flushing buffered log batches before shutdown"
        );

        for completed in pending {
            let rows = completed.metadata.record_count;
            let service = completed.metadata.service_name.as_ref().to_string();
            match handlers::persist_log_batch(&completed).await {
                Ok(paths) => {
                    for path in paths {
                        info!(
                            path = %path,
                            service_name = %service,
                            rows,
                            "Flushed pending batch"
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        service_name = %service,
                        rows,
                        "Failed to flush pending batch during shutdown"
                    );
                }
            }
        }
    }

    Ok(())
}

/// Background task that periodically flushes expired batches
async fn run_background_flush(state: AppState, shutdown: Arc<AtomicBool>, interval: Duration) {
    debug!(
        "Background flush task started (interval={}s)",
        interval.as_secs()
    );

    while !shutdown.load(Ordering::SeqCst) {
        tokio::time::sleep(interval).await;

        if shutdown.load(Ordering::SeqCst) {
            break;
        }

        if let Some(batcher) = &state.batcher {
            match batcher.drain_expired() {
                Ok(expired) => {
                    for completed in expired {
                        let rows = completed.metadata.record_count;
                        let service = completed.metadata.service_name.as_ref().to_string();
                        match handlers::persist_log_batch(&completed).await {
                            Ok(paths) => {
                                for path in &paths {
                                    info!(
                                        path = %path,
                                        service_name = %service,
                                        rows,
                                        "Flushed expired batch"
                                    );
                                }
                            }
                            Err(e) => {
                                warn!(
                                    error = %e,
                                    service_name = %service,
                                    rows,
                                    "Failed to flush expired batch"
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!(error = %e, "Failed to drain expired batches");
                }
            }
        }
    }

    debug!("Background flush task stopped");
}
