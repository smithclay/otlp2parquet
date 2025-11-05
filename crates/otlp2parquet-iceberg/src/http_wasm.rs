//! WASM HTTP client implementation for Cloudflare Workers
//!
//! Uses worker::Fetch API for HTTP operations in Workers runtime.
//! TODO: Add authentication mechanism for S3 Tables/Iceberg endpoints.

use crate::http::{HttpClient, HttpResponse};
use anyhow::{Context, Result};
use async_trait::async_trait;
use worker::{Fetch, Headers, Method, Request, RequestInit};

/// WASM HTTP client using Cloudflare Workers Fetch API
///
/// TODO: Add authentication support for Iceberg REST endpoints
pub struct WasmHttpClient {
    // Reserved for future configuration
    _config: (),
}

impl WasmHttpClient {
    /// Create a new WasmHttpClient
    ///
    /// # Parameters
    /// - `_base_url`: Base URL of the REST catalog (reserved for future auth detection)
    pub fn new(_base_url: &str) -> Result<Self> {
        Ok(Self { _config: () })
    }
}

#[async_trait(?Send)]
impl HttpClient for WasmHttpClient {
    async fn request(
        &self,
        method: &str,
        url: &str,
        headers: Vec<(String, String)>,
        body: Option<Vec<u8>>,
    ) -> Result<HttpResponse> {
        // Parse method
        let fetch_method = match method.to_uppercase().as_str() {
            "GET" => Method::Get,
            "POST" => Method::Post,
            "PUT" => Method::Put,
            "DELETE" => Method::Delete,
            "HEAD" => Method::Head,
            "OPTIONS" => Method::Options,
            "PATCH" => Method::Patch,
            _ => return Err(anyhow::anyhow!("Unsupported HTTP method: {}", method)),
        };

        // Build headers
        let mut fetch_headers = Headers::new();
        for (name, value) in &headers {
            fetch_headers
                .set(name, value)
                .map_err(|e| anyhow::anyhow!("Failed to set header {}: {:?}", name, e))?;
        }

        // Add User-Agent if not present
        if !headers
            .iter()
            .any(|(k, _)| k.eq_ignore_ascii_case("user-agent"))
        {
            fetch_headers
                .set("User-Agent", "otlp2parquet-iceberg-wasm/0.1")
                .ok();
        }

        // Build request init
        let mut init = RequestInit::new();
        init.with_method(fetch_method).with_headers(fetch_headers);

        // Add body if present
        if let Some(body_bytes) = body {
            init.with_body(Some(worker::wasm_bindgen::JsValue::from(
                js_sys::Uint8Array::from(&body_bytes[..]),
            )));
        }

        // Create request
        let request = Request::new_with_init(url, &init)
            .map_err(|e| anyhow::anyhow!("Failed to create request: {:?}", e))?;

        // Execute fetch
        let mut response = Fetch::Request(request)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Fetch failed: {:?}", e))?;

        // Extract status
        let status = response.status_code();

        // Extract headers
        let response_headers: Vec<(String, String)> = response
            .headers()
            .entries()
            .into_iter()
            .filter_map(|(k, v)| Some((k.ok()?, v.as_string()?)))
            .collect();

        // Extract body
        let body = response
            .bytes()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read response body: {:?}", e))?;

        Ok(HttpResponse {
            status,
            headers: response_headers,
            body,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_client() {
        let client = WasmHttpClient::new("https://catalog.example.com/v1").unwrap();
        // Just verify client was created successfully
        assert!(std::mem::size_of_val(&client) > 0);
    }

    #[test]
    fn test_create_client_aws_endpoint() {
        // Should succeed even for AWS endpoints (auth not yet implemented)
        let _client =
            WasmHttpClient::new("https://s3tables.us-east-1.amazonaws.com/iceberg").unwrap();
        // Just verify client was created successfully
    }
}
