// HTTP request handlers for Lambda
//
// Handles the core logic of processing OTLP requests and generating responses

use anyhow::Result;
use base64::Engine;
use otlp2parquet_batch::CompletedBatch;
use otlp2parquet_core::otlp;
use serde_json::json;
use std::borrow::Cow;

use crate::{HttpResponseData, LambdaState};

const HEALTHY_TEXT: &str = "Healthy";

/// Handle incoming HTTP request based on method and path
pub(crate) async fn handle_http_request(
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

/// Handle POST requests - OTLP log ingestion
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

        match batcher.ingest(&request, body.len()) {
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
        match state.passthrough.ingest(&request) {
            Ok(batch) => {
                metadata = batch.metadata.clone();
                uploads.push(batch);
            }
            Err(err) => {
                eprintln!("Failed to convert OTLP to Arrow: {}", err);
                return HttpResponseData::json(
                    500,
                    json!({ "error": "internal encoding failure" }).to_string(),
                );
            }
        }
    }

    let mut uploaded_paths = Vec::new();
    for batch in uploads {
        // Write RecordBatch to Parquet and upload (hash computed in storage layer)
        match state
            .parquet_writer
            .write_batches_with_hash(
                &batch.batches,
                &batch.metadata.service_name,
                batch.metadata.first_timestamp_nanos,
            )
            .await
        {
            Ok((partition_path, _hash)) => {
                uploaded_paths.push(partition_path);
            }
            Err(err) => {
                eprintln!("Failed to write Parquet to storage: {}", err);
                return HttpResponseData::json(
                    500,
                    json!({ "error": "internal storage failure" }).to_string(),
                );
            }
        }
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

/// Handle GET requests - health checks
fn handle_get(path: &str) -> HttpResponseData {
    match path {
        "/health" => HttpResponseData::text(200, HEALTHY_TEXT.to_string()),
        _ => HttpResponseData::json(404, json!({ "error": "not found" }).to_string()),
    }
}

/// Decode request body, handling base64 encoding
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

/// Extract canonical path from request (strip query string)
pub(crate) fn canonical_path(path: Option<&str>) -> String {
    let raw = path.unwrap_or("/");
    raw.split('?').next().unwrap_or("/").to_string()
}
