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
use otlp2parquet_config::RuntimeConfig;
use otlp2parquet_storage::iceberg::IcebergCommitter;
use otlp2parquet_storage::opendal_storage::OpenDalStorage;
use otlp2parquet_storage::{set_parquet_row_group_size, ParquetWriter};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tracing::{error, info};

mod handlers;
mod init;

use handlers::{handle_logs, handle_metrics, handle_traces, health_check, ready_check};
use init::{init_storage, init_tracing};

/// Application state shared across all requests
#[derive(Clone)]
pub(crate) struct AppState {
    pub storage: Arc<OpenDalStorage>,
    pub parquet_writer: Arc<ParquetWriter>,
    pub batcher: Option<Arc<BatchManager>>,
    pub passthrough: PassthroughBatcher,
    pub max_payload_bytes: usize,
    pub iceberg_committer: Option<Arc<IcebergCommitter>>,
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
    // Load configuration
    let config = RuntimeConfig::load().context("Failed to load configuration")?;

    // Initialize tracing with config
    init_tracing(&config);

    // Configure Parquet writer properties before first use
    set_parquet_row_group_size(config.storage.parquet_row_group_size);

    info!("Server mode - full-featured HTTP server with multi-backend storage");

    // Get listen address from config
    let addr = config
        .server
        .as_ref()
        .expect("server config required")
        .listen_addr
        .clone();

    // Initialize storage
    let storage = init_storage(&config)?;
    let parquet_writer = Arc::new(ParquetWriter::new(storage.operator().clone()));

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

    // Initialize Iceberg committer if configured
    let iceberg_committer = {
        use otlp2parquet_storage::iceberg::init::{initialize_committer, InitResult};
        match initialize_committer().await {
            InitResult::Success {
                committer,
                table_count,
            } => {
                info!(
                    "Iceberg catalog integration enabled with {} tables",
                    table_count
                );
                Some(committer)
            }
            InitResult::NotConfigured(msg) => {
                info!("Iceberg catalog not configured: {}", msg);
                None
            }
            InitResult::CatalogError(msg) => {
                info!(
                    "Failed to create Iceberg catalog: {} - continuing without Iceberg",
                    msg
                );
                None
            }
            InitResult::NamespaceError(msg) => {
                info!(
                    "Failed to parse Iceberg namespace: {} - continuing without Iceberg",
                    msg
                );
                None
            }
        }
    };

    // Create app state
    let state = AppState {
        storage,
        parquet_writer,
        batcher,
        passthrough: PassthroughBatcher::default(),
        max_payload_bytes,
        iceberg_committer,
    };

    // Build router
    let app = Router::new()
        .route("/v1/logs", post(handle_logs))
        .route("/v1/traces", post(handle_traces))
        .route("/v1/metrics", post(handle_metrics))
        .route("/health", get(health_check))
        .route("/ready", get(ready_check))
        .with_state(state);

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

    // Start server with graceful shutdown
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("Server error")?;

    info!("Server shutdown complete");

    Ok(())
}
