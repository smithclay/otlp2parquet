// AWS Lambda runtime adapter
//
// Uses OpenDAL S3 for storage and handles Lambda function events
//
// Philosophy: Use lambda_runtime's provided tokio
// We don't add our own tokio - lambda_runtime provides it

use lambda_runtime::{service_fn, Error, LambdaEvent};
use otlp2parquet_common::config::{RuntimeConfig, StorageBackend};
use otlp2parquet_writer::set_parquet_row_group_size;
use std::sync::Arc;

mod handlers;
mod response;

use handlers::{canonical_path, handle_http_request};
use response::{
    build_api_gateway_v1_response, build_api_gateway_v2_response, build_function_url_response,
};
pub(crate) use response::{HttpLambdaResponse, HttpRequestEvent, HttpResponseData};

/// Lambda handler for OTLP HTTP requests
async fn handle_request(
    event: LambdaEvent<HttpRequestEvent>,
    state: Arc<LambdaState>,
) -> Result<HttpLambdaResponse, Error> {
    let (request, _context) = event.into_parts();

    match request {
        HttpRequestEvent::ApiGatewayV1(boxed_request) => {
            let request = &*boxed_request;
            let method = request.http_method.as_str();
            let path = canonical_path(request.path.as_deref());

            // Extract Content-Type header from API Gateway v1 request
            let content_type = request
                .headers
                .get("content-type")
                .and_then(|v| v.to_str().ok());

            // Extract Content-Encoding header for gzip decompression
            let content_encoding = request
                .headers
                .get("content-encoding")
                .and_then(|v| v.to_str().ok());

            let response = handle_http_request(
                method,
                &path,
                request.body.as_deref(),
                request.is_base64_encoded,
                content_type,
                content_encoding,
                &state,
            )
            .await;
            Ok(build_api_gateway_v1_response(response))
        }
        HttpRequestEvent::ApiGatewayV2(boxed_request) => {
            let request = &*boxed_request;
            let method = request.request_context.http.method.as_str();
            let path = canonical_path(request.raw_path.as_deref());

            // Extract Content-Type header from API Gateway v2 (HTTP API) request
            let content_type = request
                .headers
                .get("content-type")
                .and_then(|v| v.to_str().ok());

            // Extract Content-Encoding header for gzip decompression
            let content_encoding = request
                .headers
                .get("content-encoding")
                .and_then(|v| v.to_str().ok());

            let response = handle_http_request(
                method,
                &path,
                request.body.as_deref(),
                request.is_base64_encoded,
                content_type,
                content_encoding,
                &state,
            )
            .await;
            Ok(build_api_gateway_v2_response(response))
        }
        HttpRequestEvent::FunctionUrl(boxed_request) => {
            let request = &*boxed_request;
            let method = request
                .request_context
                .http
                .method
                .as_deref()
                .unwrap_or("GET");
            let path = canonical_path(
                request
                    .raw_path
                    .as_deref()
                    .or(request.request_context.http.path.as_deref()),
            );

            // Extract Content-Type header from Function URL request
            let content_type = request
                .headers
                .get("content-type")
                .and_then(|v| v.to_str().ok());

            // Extract Content-Encoding header for gzip decompression
            let content_encoding = request
                .headers
                .get("content-encoding")
                .and_then(|v| v.to_str().ok());

            let response = handle_http_request(
                method,
                &path,
                request.body.as_deref(),
                request.is_base64_encoded,
                content_type,
                content_encoding,
                &state,
            )
            .await;
            Ok(build_function_url_response(response))
        }
    }
}

#[derive(Clone)]
pub(crate) struct LambdaState {
    pub max_payload_bytes: usize,
}

/// Lambda runtime entry point
pub async fn run() -> Result<(), Error> {
    // Initialize tracing subscriber for CloudWatch logs
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .without_time() // Lambda adds timestamps
        .init();

    // Log version information
    let version = env!("CARGO_PKG_VERSION");
    let git_hash = env!("GIT_HASH");
    let build_timestamp = env!("BUILD_TIMESTAMP");

    tracing::info!(
        "otlp2parquet Lambda v{} (git:{}, built:{})",
        version,
        git_hash,
        build_timestamp
    );
    tracing::info!("Lambda runtime - using lambda_runtime's tokio + OpenDAL S3");

    // Load configuration
    let config = RuntimeConfig::load()
        .map_err(|e| lambda_runtime::Error::from(format!("Failed to load configuration: {}", e)))?;

    // Validate storage backend is S3
    if config.storage.backend != StorageBackend::S3 {
        return Err(lambda_runtime::Error::from(format!(
            "Lambda requires S3 storage backend, got: {}",
            config.storage.backend
        )));
    }

    set_parquet_row_group_size(config.storage.parquet_row_group_size);

    // Lambda: Always use passthrough (no batching)
    // Event-driven/stateless architecture makes batching ineffective
    tracing::info!("Lambda using passthrough mode (no batching)");

    let max_payload_bytes = config.request.max_payload_bytes;
    tracing::info!("Lambda payload cap set to {} bytes", max_payload_bytes);

    otlp2parquet_writer::initialize_storage(&config)
        .map_err(|e| Error::from(format!("Failed to initialize storage: {}", e)))?;

    let state = Arc::new(LambdaState { max_payload_bytes });

    lambda_runtime::run(service_fn(move |event: LambdaEvent<HttpRequestEvent>| {
        let state = state.clone();
        async move { handle_request(event, state).await }
    }))
    .await
}
