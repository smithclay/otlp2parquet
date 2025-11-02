// AWS Lambda runtime adapter
//
// Uses OpenDAL S3 for storage and handles Lambda function events
//
// Philosophy: Use lambda_runtime's provided tokio
// We don't add our own tokio - lambda_runtime provides it

use anyhow::Result;
use lambda_runtime::{service_fn, Error, LambdaEvent};
use otlp2parquet_batch::{BatchConfig, BatchManager, PassthroughBatcher};
use otlp2parquet_config::{RuntimeConfig, StorageBackend};
use otlp2parquet_storage::ParquetWriter;
use std::sync::Arc;
use std::time::Duration;

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
    pub batcher: Option<Arc<BatchManager>>,
    pub passthrough: PassthroughBatcher,
    pub max_payload_bytes: usize,
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

    let batch_config = BatchConfig {
        max_rows: config.batch.max_rows,
        max_bytes: config.batch.max_bytes,
        max_age: Duration::from_secs(config.batch.max_age_secs),
    };

    let batcher = if !config.batch.enabled {
        println!("Lambda batching disabled by configuration");
        None
    } else {
        println!(
            "Lambda batching enabled (max_rows={} max_bytes={} max_age={}s)",
            batch_config.max_rows,
            batch_config.max_bytes,
            batch_config.max_age.as_secs()
        );
        Some(Arc::new(BatchManager::new(batch_config)))
    };

    let max_payload_bytes = config.request.max_payload_bytes;
    println!("Lambda payload cap set to {} bytes", max_payload_bytes);

    let state = Arc::new(LambdaState {
        parquet_writer,
        batcher,
        passthrough: PassthroughBatcher::default(),
        max_payload_bytes,
    });

    lambda_runtime::run(service_fn(move |event: LambdaEvent<HttpRequestEvent>| {
        let state = state.clone();
        async move { handle_request(event, state).await }
    }))
    .await
}
