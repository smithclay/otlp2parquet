//! Cloudflare Workers runtime adapter for otlp2parquet.
//!
//! Uses OpenDAL S3 for R2 storage and handles incoming requests via Worker fetch events.
//!
//! Philosophy: Use otlp2parquet-writer for unified write path.
//! Worker crate provides the runtime.
//! Entry point is #[event(fetch)] macro, not main().

mod auth;
mod do_config;
mod errors;
mod handlers;
mod request;
mod tracing_context;

pub use tracing_context::TraceContext;

use tracing_subscriber::fmt::format::Pretty;
use tracing_subscriber::fmt::time::UtcTime;
use tracing_subscriber::prelude::*;
use tracing_web::{performance_layer, MakeConsoleWriter};
use worker::*;

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
