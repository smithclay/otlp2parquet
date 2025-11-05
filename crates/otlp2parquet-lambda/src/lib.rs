// AWS Lambda runtime adapter
//
// Uses OpenDAL S3 for storage and handles Lambda function events
//
// Philosophy: Use lambda_runtime's provided tokio
// We don't add our own tokio - lambda_runtime provides it

use anyhow::Result;
use lambda_runtime::{service_fn, Error, LambdaEvent};
use otlp2parquet_batch::PassthroughBatcher;
use otlp2parquet_config::{RuntimeConfig, StorageBackend};
use otlp2parquet_iceberg::IcebergCommitter;
use otlp2parquet_storage::{set_parquet_row_group_size, ParquetWriter};
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

#[derive(Clone)]
pub(crate) struct LambdaState {
    pub parquet_writer: Arc<ParquetWriter>,
    pub passthrough: PassthroughBatcher,
    pub max_payload_bytes: usize,
    pub iceberg_committer: Option<Arc<IcebergCommitter>>,
}

/// Lambda runtime entry point
pub async fn run() -> Result<(), Error> {
    println!("Lambda runtime - using lambda_runtime's tokio + OpenDAL S3");

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

    let s3 = config
        .storage
        .s3
        .as_ref()
        .ok_or_else(|| lambda_runtime::Error::from("S3 configuration required for Lambda"))?;

    set_parquet_row_group_size(config.storage.parquet_row_group_size);

    // Initialize OpenDAL S3 storage
    // OpenDAL automatically discovers AWS credentials from:
    // - IAM role (preferred for Lambda)
    // - Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    // - AWS credentials file
    let storage = Arc::new(
        otlp2parquet_storage::opendal_storage::OpenDalStorage::new_s3(
            &s3.bucket,
            &s3.region,
            s3.endpoint.as_deref(),
            None,
            None,
        )
        .map_err(|e| lambda_runtime::Error::from(format!("Failed to initialize storage: {}", e)))?,
    );
    let parquet_writer = Arc::new(ParquetWriter::new(storage.operator().clone()));

    // Lambda: Always use passthrough (no batching)
    // Event-driven/stateless architecture makes batching ineffective
    println!("Lambda using passthrough mode (no batching)");

    let max_payload_bytes = config.request.max_payload_bytes;
    println!("Lambda payload cap set to {} bytes", max_payload_bytes);

    // Initialize Iceberg committer if configured
    let iceberg_committer = match otlp2parquet_iceberg::IcebergRestConfig::from_env() {
        Ok(config) => match otlp2parquet_iceberg::create_rest_catalog(&config).await {
            Ok(catalog) => match config.namespace_ident() {
                Ok(namespace) => {
                    let table_ident = otlp2parquet_iceberg::IcebergTableIdentifier::new(
                        namespace,
                        config.table.clone(),
                    );
                    let committer = otlp2parquet_iceberg::IcebergCommitter::new(
                        catalog,
                        table_ident,
                        config.clone(),
                    );
                    println!("Iceberg catalog integration enabled");
                    Some(Arc::new(committer))
                }
                Err(e) => {
                    println!(
                        "Failed to parse Iceberg namespace: {} - continuing without Iceberg",
                        e
                    );
                    None
                }
            },
            Err(e) => {
                println!(
                    "Failed to create Iceberg catalog: {} - continuing without Iceberg",
                    e
                );
                None
            }
        },
        Err(e) => {
            // Not configured or failed to load config - log and continue without Iceberg
            println!(
                "Iceberg catalog not configured (OTLP2PARQUET_ICEBERG_REST_URI not set): {}",
                e
            );
            None
        }
    };

    let state = Arc::new(LambdaState {
        parquet_writer,
        passthrough: PassthroughBatcher::default(),
        max_payload_bytes,
        iceberg_committer,
    });

    lambda_runtime::run(service_fn(move |event: LambdaEvent<HttpRequestEvent>| {
        let state = state.clone();
        async move { handle_request(event, state).await }
    }))
    .await
}
