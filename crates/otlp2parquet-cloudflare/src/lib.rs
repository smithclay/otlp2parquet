//! Cloudflare Workers runtime adapter for otlp2parquet.
//!
//! Uses icepick with OpenDAL S3 for R2 storage and handles incoming requests via Worker fetch events.
//!
//! Philosophy: Use otlp2parquet-writer for unified write path.
//! Worker crate provides the runtime, icepick provides storage abstraction.
//! Entry point is #[event(fetch)] macro, not main().

mod auth;
mod batched;
mod batcher;
mod catalog_worker;
mod do_config;
mod errors;
mod handlers;
mod request;

// Re-export Durable Object classes at crate root for worker-build
pub use batcher::OtlpBatcherLegacy; // Migration stub for renamed old class
pub use batcher::OtlpBatcherV2;
pub use otlp2parquet_core::{MetricType, SignalKey};

use tracing_subscriber::fmt::format::Pretty;
use tracing_subscriber::fmt::time::UtcTime;
use tracing_subscriber::prelude::*;
use tracing_web::{performance_layer, MakeConsoleWriter};
use worker::*;

/// Durable Object ID separator between signal key and service name.
pub(crate) const DO_ID_SEPARATOR: char = '|';

/// Version suffix for DO IDs to force fresh instances when needed.
/// Increment this to invalidate all existing DO instances and create new ones.
/// v3: Fixed set_alarm to use offset instead of absolute timestamp
const DO_ID_VERSION: &str = "v3";

/// Create DO ID name from signal key and service name.
/// Format: "{signal_key}|{service_name}|{version}"
/// Example: "logs|my-service|v2" or "metrics:gauge|my-service|v2"
pub(crate) fn make_do_id(signal_key: &SignalKey, service_name: &str) -> String {
    format!(
        "{}{}{}{}{}",
        signal_key, DO_ID_SEPARATOR, service_name, DO_ID_SEPARATOR, DO_ID_VERSION
    )
}

/// Parse DO ID into (signal_key_str, service_name).
/// Returns the raw signal string which can be parsed with SignalKey::from_str().
pub(crate) fn parse_do_id(id: &str) -> Option<(&str, &str)> {
    id.split_once(DO_ID_SEPARATOR)
}

/// Initialize tracing subscriber for Cloudflare Workers.
/// Uses tracing-web to output structured JSON logs to the Workers console.
/// Must be called via #[event(start)] to run once on worker initialization.
#[event(start)]
fn init_tracing() {
    // JSON formatting layer that writes to the Workers console
    let fmt_layer = tracing_subscriber::fmt::layer()
        .json()
        .flatten_event(true) // Put message at top level for $metadata.message in dashboard
        .with_ansi(false) // ANSI codes not supported in Workers console
        .with_timer(UtcTime::rfc_3339())
        .with_writer(MakeConsoleWriter);

    // Performance layer for timing spans (note: spans have identical start/end
    // times unless they encompass I/O due to Workers platform restrictions)
    let perf_layer = performance_layer().with_details_from_fields(Pretty::default());

    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(perf_layer)
        .init();
}

/// Cloudflare Workers entry point.
#[event(fetch)]
pub async fn main(req: Request, env: Env, ctx: Context) -> Result<Response> {
    tracing::debug!("Worker fetch handler called");
    let result = request::handle(req, env, ctx).await;
    tracing::debug!("request::handle returned");
    result
}

/// Cron-triggered catalog sync handler.
/// Runs every 5 minutes to commit pending Parquet files to Iceberg.
#[event(scheduled)]
pub async fn scheduled(_event: ScheduledEvent, env: Env, _ctx: ScheduleContext) {
    tracing::debug!("Scheduled event triggered");

    if let Err(e) = catalog_worker::sync_catalog(&env).await {
        tracing::error!(error = ?e, "Catalog sync failed");
    }
}
