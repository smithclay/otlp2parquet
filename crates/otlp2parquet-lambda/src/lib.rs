// AWS Lambda runtime adapter
//
// Uses OpenDAL S3 for storage and handles Lambda function events
//
// Philosophy: Use lambda_runtime's provided tokio
// We don't add our own tokio - lambda_runtime provides it

use anyhow::Result;
use aws_lambda_events::{
    apigw::{ApiGatewayProxyRequest, ApiGatewayProxyResponse},
    encodings::Body,
    http::{header::CONTENT_TYPE, HeaderValue},
    lambda_function_urls::{LambdaFunctionUrlRequest, LambdaFunctionUrlResponse},
};
use base64::Engine;
use lambda_runtime::{service_fn, Error, LambdaEvent};
use otlp2parquet_core::otlp;
use otlp2parquet_core::ProcessingOptions;
use otlp2parquet_runtime::batcher::{
    max_payload_bytes_from_env, processing_options_from_env, BatchConfig, BatchManager,
    CompletedBatch, PassthroughBatcher,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::borrow::Cow;
use std::sync::Arc;

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

fn canonical_path(path: Option<&str>) -> String {
    let raw = path.unwrap_or("/");
    raw.split('?').next().unwrap_or("/").to_string()
}

async fn handle_http_request(
    method: &str,
    path: &str,
    body: Option<&str>,
    is_base64_encoded: bool,
    content_type: Option<&str>,
    state: &LambdaState,
) -> HttpResponseData {
    let method = method.trim().to_ascii_uppercase();
    match method.as_str() {
        "POST" => handle_post(path, body, is_base64_encoded, content_type, state).await,
        "GET" => handle_get(path),
        _ => HttpResponseData::json(405, json!({ "error": "method not allowed" }).to_string()),
    }
}

const HEALTHY_TEXT: &str = "Healthy";

async fn handle_post(
    path: &str,
    body: Option<&str>,
    is_base64_encoded: bool,
    content_type: Option<&str>,
    state: &LambdaState,
) -> HttpResponseData {
    if path != "/v1/logs" {
        return HttpResponseData::json(404, json!({ "error": "not found" }).to_string());
    }

    let body = match decode_body(body, is_base64_encoded) {
        Ok(bytes) => bytes,
        Err(response) => return response,
    };

    if body.len() > state.max_payload_bytes {
        return HttpResponseData::json(
            413,
            json!({
                "error": "payload too large",
                "limit_bytes": state.max_payload_bytes,
            })
            .to_string(),
        );
    }

    // Detect input format from Content-Type header
    let format = otlp2parquet_core::InputFormat::from_content_type(content_type);

    let request = match otlp::parse_otlp_request(body.as_ref(), format) {
        Ok(req) => req,
        Err(err) => {
            eprintln!(
                "Failed to parse OTLP logs (format: {:?}, content-type: {:?}): {}",
                format, content_type, err
            );
            return HttpResponseData::json(
                400,
                json!({ "error": "invalid OTLP payload" }).to_string(),
            );
        }
    };

    let mut uploads: Vec<CompletedBatch> = Vec::new();
    let metadata;

    if let Some(batcher) = &state.batcher {
        match batcher.drain_expired() {
            Ok(mut expired) => uploads.append(&mut expired),
            Err(err) => {
                eprintln!("Failed to flush expired batches: {}", err);
                return HttpResponseData::json(
                    500,
                    json!({ "error": "internal batching failure" }).to_string(),
                );
            }
        }

        match batcher.ingest(request) {
            Ok((mut ready, meta)) => {
                uploads.append(&mut ready);
                metadata = meta;
            }
            Err(err) => {
                eprintln!("Batch enqueue failed: {}", err);
                return HttpResponseData::json(
                    500,
                    json!({ "error": "internal batching failure" }).to_string(),
                );
            }
        }
    } else {
        match state.passthrough.ingest(request, &state.processing_options) {
            Ok(batch) => {
                metadata = batch.metadata.clone();
                uploads.push(batch);
            }
            Err(err) => {
                eprintln!("Failed to encode Parquet: {}", err);
                return HttpResponseData::json(
                    500,
                    json!({ "error": "internal encoding failure" }).to_string(),
                );
            }
        }
    }

    let mut uploaded_paths = Vec::new();
    for batch in uploads {
        let hash_hex = batch.content_hash.to_hex().to_string();
        let partition_path = otlp2parquet_runtime::partition::generate_partition_path(
            &batch.metadata.service_name,
            batch.metadata.first_timestamp_nanos,
            &hash_hex,
        );

        if let Err(err) = state.storage.write(&partition_path, batch.bytes).await {
            eprintln!("Failed to write to storage: {}", err);
            return HttpResponseData::json(
                500,
                json!({ "error": "internal storage failure" }).to_string(),
            );
        }

        uploaded_paths.push(partition_path);
    }

    HttpResponseData::json(
        200,
        json!({
            "status": "ok",
            "records_processed": metadata.record_count,
            "flush_count": uploaded_paths.len(),
            "partitions": uploaded_paths,
        })
        .to_string(),
    )
}

fn handle_get(path: &str) -> HttpResponseData {
    match path {
        "/health" => HttpResponseData::text(200, HEALTHY_TEXT.to_string()),
        _ => HttpResponseData::json(404, json!({ "error": "not found" }).to_string()),
    }
}

fn decode_body<'a>(
    body: Option<&'a str>,
    is_base64_encoded: bool,
) -> Result<Cow<'a, [u8]>, HttpResponseData> {
    let body = body.ok_or_else(|| {
        HttpResponseData::json(400, json!({ "error": "missing request body" }).to_string())
    })?;

    if is_base64_encoded {
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(body.as_bytes())
            .map_err(|_| {
                HttpResponseData::json(400, json!({ "error": "invalid base64 body" }).to_string())
            })?;
        Ok(Cow::Owned(decoded))
    } else {
        Ok(Cow::Borrowed(body.as_bytes()))
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum HttpRequestEvent {
    ApiGateway(Box<ApiGatewayProxyRequest>),
    FunctionUrl(Box<LambdaFunctionUrlRequest>),
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum HttpLambdaResponse {
    ApiGateway(ApiGatewayProxyResponse),
    FunctionUrl(LambdaFunctionUrlResponse),
}

#[derive(Clone)]
struct LambdaState {
    storage: Arc<otlp2parquet_runtime::opendal_storage::OpenDalStorage>,
    batcher: Option<Arc<BatchManager>>,
    passthrough: PassthroughBatcher,
    processing_options: ProcessingOptions,
    max_payload_bytes: usize,
}

struct HttpResponseData {
    status_code: u16,
    body: String,
    content_type: &'static str,
}

impl HttpResponseData {
    fn json(status_code: u16, body: String) -> Self {
        Self {
            status_code,
            body,
            content_type: "application/json",
        }
    }

    fn text(status_code: u16, body: String) -> Self {
        Self {
            status_code,
            body,
            content_type: "text/plain; charset=utf-8",
        }
    }
}

fn build_api_gateway_response(data: HttpResponseData) -> HttpLambdaResponse {
    let mut response = ApiGatewayProxyResponse {
        status_code: data.status_code as i64,
        headers: Default::default(),
        multi_value_headers: Default::default(),
        body: Some(Body::Text(data.body)),
        is_base64_encoded: false,
    };
    response
        .headers
        .insert(CONTENT_TYPE, HeaderValue::from_static(data.content_type));
    HttpLambdaResponse::ApiGateway(response)
}

fn build_function_url_response(data: HttpResponseData) -> HttpLambdaResponse {
    let mut headers = aws_lambda_events::http::HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(data.content_type));
    HttpLambdaResponse::FunctionUrl(LambdaFunctionUrlResponse {
        status_code: data.status_code as i64,
        headers,
        body: Some(data.body),
        is_base64_encoded: false,
        cookies: Vec::new(),
    })
}

/// Lambda runtime entry point
pub async fn run() -> Result<(), Error> {
    println!("Lambda runtime - using lambda_runtime's tokio + OpenDAL S3");

    // Get configuration from environment
    let bucket = std::env::var("LOGS_BUCKET").unwrap_or_else(|_| "otlp-logs".to_string());
    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());

    // Initialize OpenDAL S3 storage
    // OpenDAL automatically discovers AWS credentials from:
    // - IAM role (preferred for Lambda)
    // - Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    // - AWS credentials file
    let storage = Arc::new(
        otlp2parquet_runtime::opendal_storage::OpenDalStorage::new_s3(
            &bucket, &region, None, None, None,
        )
        .map_err(|e| lambda_runtime::Error::from(format!("Failed to initialize storage: {}", e)))?,
    );

    let processing_options = processing_options_from_env();
    let batch_config = BatchConfig::from_env(200_000, 128 * 1024 * 1024, 10);
    let batcher = if batch_config.max_rows == 0 || batch_config.max_bytes == 0 {
        println!(
            "Lambda batching disabled (max_rows={}, max_bytes={})",
            batch_config.max_rows, batch_config.max_bytes
        );
        None
    } else {
        println!(
            "Lambda batching enabled (max_rows={} max_bytes={} max_age={}s, row_group_rows={})",
            batch_config.max_rows,
            batch_config.max_bytes,
            batch_config.max_age.as_secs(),
            processing_options.max_rows_per_batch
        );
        Some(Arc::new(BatchManager::new(
            batch_config,
            processing_options.clone(),
        )))
    };

    let max_payload_bytes = max_payload_bytes_from_env(6 * 1024 * 1024);
    println!("Lambda payload cap set to {} bytes", max_payload_bytes);

    let state = Arc::new(LambdaState {
        storage: storage.clone(),
        batcher,
        passthrough: PassthroughBatcher,
        processing_options,
        max_payload_bytes,
    });

    lambda_runtime::run(service_fn(move |event: LambdaEvent<HttpRequestEvent>| {
        let state = state.clone();
        async move { handle_request(event, state).await }
    }))
    .await
}
