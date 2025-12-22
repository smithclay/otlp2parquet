//! HTTP handlers for OTLP signal ingestion.

mod handlers;
mod routing;

pub use handlers::{handle_batched_logs, handle_batched_metrics, handle_batched_traces};
#[allow(unused_imports)]
pub use routing::route_to_batcher;

use crate::TraceContext;
use worker::Env;

/// Context for batched request handling.
pub struct BatchContext<'a> {
    pub env: &'a Env,
    pub request_id: &'a str,
    /// TraceContext for propagating distributed tracing headers to Durable Objects.
    /// Will be used in subsequent tasks for header propagation.
    #[allow(dead_code)]
    pub trace_ctx: &'a TraceContext,
}

/// Convert an error message to a structured InvalidRequest error response.
fn invalid_request_response(message: String, request_id: &str) -> worker::Response {
    use crate::errors;
    let error = errors::OtlpErrorKind::InvalidRequest(message);
    let status_code = error.status_code();
    errors::ErrorResponse::from_error(error, Some(request_id.to_string()))
        .into_response(status_code)
        .unwrap_or_else(|_| worker::Response::error("Invalid request", 400).unwrap())
}
