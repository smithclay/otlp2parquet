// AWS Lambda runtime adapter
//
// Uses S3 for storage and handles Lambda function events
//
// Philosophy: Use lambda_runtime's provided tokio
// We don't add our own tokio - lambda_runtime provides it

#[cfg(feature = "lambda")]
use anyhow::{Context as AnyhowContext, Result};
#[cfg(feature = "lambda")]
use aws_lambda_events::apigw::{ApiGatewayProxyRequest, ApiGatewayProxyResponse};
#[cfg(feature = "lambda")]
use aws_sdk_s3::Client;
#[cfg(feature = "lambda")]
use lambda_runtime::{service_fn, Error, LambdaEvent};
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
    event: LambdaEvent<ApiGatewayProxyRequest>,
    storage: Arc<S3Storage>,
) -> Result<ApiGatewayProxyResponse, Error> {
    let (request, _context) = event.into_parts();

    // Only accept POST requests to /v1/logs
    let method = request.http_method.to_uppercase();
    if method != "POST" {
        return Ok(ApiGatewayProxyResponse {
            status_code: 405,
            headers: Default::default(),
            multi_value_headers: Default::default(),
            body: Some("Method not allowed".to_string()),
            is_base64_encoded: false,
        });
    }

    let path = request.path.unwrap_or_default();
    if path != "/v1/logs" {
        return Ok(ApiGatewayProxyResponse {
            status_code: 404,
            headers: Default::default(),
            multi_value_headers: Default::default(),
            body: Some("Not found".to_string()),
            is_base64_encoded: false,
        });
    }

    // Get request body
    let body = request.body.context("Missing request body")?;

    // Decode base64 if needed
    let body_bytes = if request.is_base64_encoded {
        use base64::Engine;
        base64::engine::general_purpose::STANDARD
            .decode(&body)
            .context("Failed to decode base64 body")?
    } else {
        body.into_bytes()
    };

    // Process OTLP logs (PURE - no I/O, deterministic)
    let mut parquet_bytes = Vec::new();
    let metadata = otlp2parquet_core::process_otlp_logs_into(&body_bytes, &mut parquet_bytes)
        .context("Failed to process OTLP logs")?;

    // Generate partition path (ACCIDENT - platform-specific storage decision)
    let path = crate::partition::generate_partition_path(
        &metadata.service_name,
        metadata.first_timestamp_nanos,
    );

    // Write to S3
    storage
        .write(&path, parquet_bytes)
        .await
        .context("Failed to write to S3")?;

    // Return success response
    Ok(ApiGatewayProxyResponse {
        status_code: 200,
        headers: Default::default(),
        multi_value_headers: Default::default(),
        body: Some(json!({"status": "ok"}).to_string()),
        is_base64_encoded: false,
    })
}

/// Lambda runtime entry point
#[cfg(feature = "lambda")]
pub async fn run() -> Result<(), Error> {
    println!("Lambda runtime - using lambda_runtime's tokio");

    // Get bucket name from environment
    let bucket = std::env::var("LOGS_BUCKET").unwrap_or_else(|_| "otlp-logs".to_string());

    // Initialize AWS SDK
    let config = aws_config::load_from_env().await;
    let s3_client = aws_sdk_s3::Client::new(&config);
    let storage = Arc::new(S3Storage::new(s3_client, bucket));

    // Run Lambda runtime
    lambda_runtime::run(service_fn(
        move |event: LambdaEvent<ApiGatewayProxyRequest>| {
            let storage = storage.clone();
            async move { handle_request(event, storage).await }
        },
    ))
    .await
}
