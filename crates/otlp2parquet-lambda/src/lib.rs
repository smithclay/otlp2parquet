// AWS Lambda runtime adapter
//
// Uses OpenDAL S3 for storage and handles Lambda function events
//
// Philosophy: Use lambda_runtime's provided tokio
// We don't add our own tokio - lambda_runtime provides it

use lambda_runtime::{service_fn, Error, LambdaEvent};
use otlp2parquet_batch::PassthroughBatcher;
use otlp2parquet_config::{RuntimeConfig, StorageBackend};
use otlp2parquet_core::parquet::encoding::set_parquet_row_group_size;
use std::sync::Arc;

mod handlers;
mod response;

use handlers::{canonical_path, handle_http_request};
use response::{build_api_gateway_response, build_function_url_response};
pub(crate) use response::{HttpLambdaResponse, HttpRequestEvent, HttpResponseData};

/// Lambda handler for OTLP HTTP requests
async fn handle_request(
    event: LambdaEvent<HttpRequestEvent>,
    state: Arc<LambdaState>,
) -> Result<HttpLambdaResponse, Error> {
    let (request, _context) = event.into_parts();

    match request {
        HttpRequestEvent::ApiGateway(boxed_request) => {
            let request = &*boxed_request;
            let method = request.http_method.as_str();
            let path = canonical_path(request.path.as_deref());

            // Extract Content-Type header from API Gateway request
            let content_type = request
                .headers
                .get("content-type")
                .and_then(|v| v.to_str().ok());

            let response = handle_http_request(
                method,
                &path,
                request.body.as_deref(),
                request.is_base64_encoded,
                content_type,
                &state,
            )
            .await;
            Ok(build_api_gateway_response(response))
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

            let response = handle_http_request(
                method,
                &path,
                request.body.as_deref(),
                request.is_base64_encoded,
                content_type,
                &state,
            )
            .await;
            Ok(build_function_url_response(response))
        }
    }
}

// Old Writer enum removed - now using otlp2parquet_writer::OtlpWriter trait

#[derive(Clone)]
pub(crate) struct LambdaState {
    pub writer: Arc<dyn otlp2parquet_writer::OtlpWriter>,
    pub passthrough: PassthroughBatcher,
    pub max_payload_bytes: usize,
}

/// Lambda runtime entry point
pub async fn run() -> Result<(), Error> {
    // Initialize tracing subscriber for CloudWatch logs
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .without_time() // Lambda adds timestamps
        .init();

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

    // Initialize writer using new icepick-based implementation
    let bucket_arn = config
        .iceberg
        .as_ref()
        .and_then(|ic| ic.bucket_arn.as_ref())
        .ok_or_else(|| {
            Error::from("Lambda requires iceberg.bucket_arn configuration for S3 Tables")
        })?;

    let writer = otlp2parquet_writer::initialize_lambda_writer(bucket_arn, String::new())
        .await
        .map_err(|e| Error::from(format!("Failed to initialize writer: {}", e)))?;

    let state = Arc::new(LambdaState {
        writer: Arc::new(writer),
        passthrough: PassthroughBatcher::default(),
        max_payload_bytes,
    });

    lambda_runtime::run(service_fn(move |event: LambdaEvent<HttpRequestEvent>| {
        let state = state.clone();
        async move { handle_request(event, state).await }
    }))
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use otlp2parquet_config::{IcebergConfig, Platform};

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        use std::sync::{Mutex, OnceLock};

        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    #[test]
    fn lambda_requires_bucket_arn_configuration() {
        // Lambda now always uses icepick with S3 Tables, which requires bucket_arn
        let mut config = RuntimeConfig::from_platform_defaults(Platform::Lambda);

        // Without bucket_arn, initialization should fail
        assert!(
            config.iceberg.is_none()
                || config
                    .iceberg
                    .as_ref()
                    .and_then(|ic| ic.bucket_arn.as_ref())
                    .is_none()
        );

        // With bucket_arn, config is valid
        config.iceberg = Some(IcebergConfig {
            bucket_arn: Some(
                "arn:aws:s3tables:us-west-2:123456789012:bucket/test-bucket".to_string(),
            ),
            ..Default::default()
        });

        assert!(config
            .iceberg
            .as_ref()
            .and_then(|ic| ic.bucket_arn.as_ref())
            .is_some());
    }

    #[test]
    fn lambda_bucket_arn_respects_env_override() {
        let _guard = env_lock();
        let test_arn = "arn:aws:s3tables:us-east-1:999999999999:bucket/my-bucket";
        std::env::set_var("OTLP2PARQUET_ICEBERG_BUCKET_ARN", test_arn);

        let config =
            RuntimeConfig::load_for_platform(Platform::Lambda).expect("config load should succeed");

        let bucket_arn = config
            .iceberg
            .as_ref()
            .and_then(|ic| ic.bucket_arn.as_ref())
            .expect("bucket_arn should be set from env");

        assert_eq!(bucket_arn, test_arn);

        std::env::remove_var("OTLP2PARQUET_ICEBERG_BUCKET_ARN");
    }
}
