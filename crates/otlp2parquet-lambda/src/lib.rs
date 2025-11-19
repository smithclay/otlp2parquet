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

// Imports for REST catalog support
use icepick::catalog::Catalog;

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

            let response = handle_http_request(
                method,
                &path,
                request.body.as_deref(),
                request.is_base64_encoded,
                content_type,
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

            let response = handle_http_request(
                method,
                &path,
                request.body.as_deref(),
                request.is_base64_encoded,
                content_type,
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

    // Initialize writer - support both REST catalog and S3 Tables
    let iceberg_cfg = config.iceberg.as_ref().ok_or_else(|| {
        Error::from("Lambda requires iceberg configuration (rest_uri or bucket_arn)")
    })?;

    let writer = if !iceberg_cfg.rest_uri.is_empty() {
        // REST catalog (Nessie, Glue REST, etc.)
        tracing::info!(
            "Initializing Lambda with REST catalog: endpoint={}, namespace={:?}",
            iceberg_cfg.rest_uri,
            iceberg_cfg.namespace
        );

        // Build REST catalog
        let builder = icepick::RestCatalog::builder("otlp2parquet", &iceberg_cfg.rest_uri);

        // Create S3 operator for storage
        let s3_config = config.storage.s3.as_ref().ok_or_else(|| {
            Error::from("Lambda with REST catalog requires S3 storage configuration")
        })?;

        let mut s3_builder = opendal::services::S3::default()
            .bucket(&s3_config.bucket)
            .region(&s3_config.region);

        if let Some(endpoint) = &s3_config.endpoint {
            tracing::info!("Using custom S3 endpoint: {}", endpoint);
            s3_builder = s3_builder.endpoint(endpoint);
        }

        let operator = opendal::Operator::new(s3_builder)
            .map_err(|e| Error::from(format!("Failed to build S3 operator: {}", e)))?
            .finish();

        let file_io = icepick::FileIO::new(operator);

        // Build catalog with file_io
        let catalog = builder
            .with_file_io(file_io.clone())
            .with_bearer_token("") // Anonymous/no auth for Nessie
            .build()
            .map_err(|e| Error::from(format!("Failed to create REST catalog: {}", e)))?;

        // Get namespace from config
        let namespace = iceberg_cfg
            .namespace
            .clone()
            .unwrap_or_else(|| "otlp".to_string());

        // Create namespace if it doesn't exist
        let namespace_ident = icepick::NamespaceIdent::new(vec![namespace.clone()]);
        match catalog
            .create_namespace(&namespace_ident, Default::default())
            .await
        {
            Ok(_) => tracing::info!("Created namespace: {}", namespace),
            Err(e) => tracing::info!(
                "Namespace creation result for '{}': {} (may already exist)",
                namespace,
                e
            ),
        }

        // Use the catalog we created with the specified namespace
        let _ = file_io; // FileIO not needed with AppendOnlyTableWriter
        otlp2parquet_writer::IcepickWriter::new(Arc::new(catalog), namespace)
            .map_err(|e| Error::from(format!("Failed to initialize writer: {}", e)))?
    } else if let Some(bucket_arn) = &iceberg_cfg.bucket_arn {
        // S3 Tables catalog (AWS managed)
        tracing::info!("Initializing Lambda with S3 Tables catalog: {}", bucket_arn);
        otlp2parquet_writer::initialize_lambda_writer(bucket_arn, String::new())
            .await
            .map_err(|e| Error::from(format!("Failed to initialize writer: {}", e)))?
    } else {
        return Err(Error::from(
            "Lambda requires either iceberg.rest_uri or iceberg.bucket_arn configuration",
        ));
    };

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
