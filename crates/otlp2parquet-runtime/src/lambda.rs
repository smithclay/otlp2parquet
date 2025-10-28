// AWS Lambda runtime adapter
//
// Uses S3 for storage and handles Lambda function events
//
// Philosophy: Use lambda_runtime's provided tokio
// We don't add our own tokio - lambda_runtime provides it

#[cfg(feature = "lambda")]
use anyhow::Result;
#[cfg(feature = "lambda")]
use aws_lambda_events::{
    apigw::{ApiGatewayProxyRequest, ApiGatewayProxyResponse},
    encodings::Body,
    http::{header::CONTENT_TYPE, HeaderValue},
    lambda_function_urls::{LambdaFunctionUrlRequest, LambdaFunctionUrlResponse},
};
#[cfg(feature = "lambda")]
use aws_sdk_s3::Client;
#[cfg(feature = "lambda")]
use base64::Engine;
#[cfg(feature = "lambda")]
use lambda_runtime::{service_fn, Error, LambdaEvent};
#[cfg(feature = "lambda")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "lambda")]
use serde_json::json;
#[cfg(feature = "lambda")]
use std::sync::Arc;

#[cfg(feature = "lambda")]
pub struct S3Storage {
    client: Client,
    bucket: String,
}

#[cfg(feature = "lambda")]
impl S3Storage {
    pub fn new(client: Client, bucket: String) -> Self {
        Self { client, bucket }
    }

    /// Write parquet data to S3 (async, uses lambda_runtime's tokio)
    pub async fn write(&self, path: &str, data: Vec<u8>) -> Result<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(path)
            .body(data.into())
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("S3 write error: {}", e))?;

        Ok(())
    }
}

/// Lambda handler for OTLP HTTP requests
#[cfg(feature = "lambda")]
async fn handle_request(
    event: LambdaEvent<HttpRequestEvent>,
    storage: Arc<S3Storage>,
) -> Result<HttpLambdaResponse, Error> {
    let (request, _context) = event.into_parts();

    match request {
        HttpRequestEvent::ApiGateway(boxed_request) => {
            let request = &*boxed_request;
            let method = request.http_method.as_str();
            let path = canonical_path(request.path.as_deref());
            let response = handle_http_request(
                method,
                &path,
                request.body.as_deref(),
                request.is_base64_encoded,
                &storage,
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
            let response = handle_http_request(
                method,
                &path,
                request.body.as_deref(),
                request.is_base64_encoded,
                &storage,
            )
            .await;
            Ok(build_function_url_response(response))
        }
    }
}

#[cfg(feature = "lambda")]
fn canonical_path(path: Option<&str>) -> String {
    let raw = path.unwrap_or("/");
    raw.split('?').next().unwrap_or("/").to_string()
}

#[cfg(feature = "lambda")]
async fn handle_http_request(
    method: &str,
    path: &str,
    body: Option<&str>,
    is_base64_encoded: bool,
    storage: &S3Storage,
) -> HttpResponseData {
    let method = method.trim().to_ascii_uppercase();
    match method.as_str() {
        "POST" => handle_post(path, body, is_base64_encoded, storage).await,
        "GET" => handle_get(path),
        _ => HttpResponseData::json(405, json!({ "error": "method not allowed" }).to_string()),
    }
}

#[cfg(feature = "lambda")]
const OK_JSON_BODY: &str = r#"{"status":"ok"}"#;
#[cfg(feature = "lambda")]
const HEALTHY_TEXT: &str = "Healthy";

#[cfg(feature = "lambda")]
async fn handle_post(
    path: &str,
    body: Option<&str>,
    is_base64_encoded: bool,
    storage: &S3Storage,
) -> HttpResponseData {
    if path != "/v1/logs" {
        return HttpResponseData::json(404, json!({ "error": "not found" }).to_string());
    }

    let body = match decode_body(body, is_base64_encoded) {
        Ok(bytes) => bytes,
        Err(response) => return response,
    };

    let mut parquet_bytes = Vec::new();
    let metadata = match otlp2parquet_core::process_otlp_logs_into(&body, &mut parquet_bytes) {
        Ok(metadata) => metadata,
        Err(err) => {
            eprintln!("Failed to process OTLP logs: {}", err);
            return HttpResponseData::json(
                400,
                json!({ "error": "invalid OTLP payload" }).to_string(),
            );
        }
    };

    let partition_path = crate::partition::generate_partition_path(
        &metadata.service_name,
        metadata.first_timestamp_nanos,
    );

    if let Err(err) = storage.write(&partition_path, parquet_bytes).await {
        eprintln!("Failed to write to S3: {}", err);
        return HttpResponseData::json(
            500,
            json!({ "error": "internal storage failure" }).to_string(),
        );
    }

    HttpResponseData::json(200, OK_JSON_BODY.to_string())
}

#[cfg(feature = "lambda")]
fn handle_get(path: &str) -> HttpResponseData {
    match path {
        "/health" => HttpResponseData::text(200, HEALTHY_TEXT.to_string()),
        _ => HttpResponseData::json(404, json!({ "error": "not found" }).to_string()),
    }
}

#[cfg(feature = "lambda")]
fn decode_body(body: Option<&str>, is_base64_encoded: bool) -> Result<Vec<u8>, HttpResponseData> {
    let body = body.ok_or_else(|| {
        HttpResponseData::json(400, json!({ "error": "missing request body" }).to_string())
    })?;

    if is_base64_encoded {
        base64::engine::general_purpose::STANDARD
            .decode(body.as_bytes())
            .map_err(|_| {
                HttpResponseData::json(400, json!({ "error": "invalid base64 body" }).to_string())
            })
    } else {
        Ok(body.as_bytes().to_vec())
    }
}

#[cfg(feature = "lambda")]
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum HttpRequestEvent {
    ApiGateway(Box<ApiGatewayProxyRequest>),
    FunctionUrl(Box<LambdaFunctionUrlRequest>),
}

#[cfg(feature = "lambda")]
#[derive(Debug, Serialize)]
#[serde(untagged)]
enum HttpLambdaResponse {
    ApiGateway(ApiGatewayProxyResponse),
    FunctionUrl(LambdaFunctionUrlResponse),
}

#[cfg(feature = "lambda")]
struct HttpResponseData {
    status_code: u16,
    body: String,
    content_type: &'static str,
}

#[cfg(feature = "lambda")]
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

#[cfg(feature = "lambda")]
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

#[cfg(feature = "lambda")]
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
#[cfg(feature = "lambda")]
pub async fn run() -> Result<(), Error> {
    println!("Lambda runtime - using lambda_runtime's tokio");

    // Get bucket name from environment
    let bucket = std::env::var("LOGS_BUCKET").unwrap_or_else(|_| "otlp-logs".to_string());

    // Initialize AWS SDK
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .load()
        .await;
    let s3_client = aws_sdk_s3::Client::new(&config);
    let storage = Arc::new(S3Storage::new(s3_client, bucket));

    // Run Lambda runtime
    lambda_runtime::run(service_fn(move |event: LambdaEvent<HttpRequestEvent>| {
        let storage = storage.clone();
        async move { handle_request(event, storage).await }
    }))
    .await
}
