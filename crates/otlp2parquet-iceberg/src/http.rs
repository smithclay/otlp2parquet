//! Platform-agnostic HTTP client abstraction
//!
//! Provides a simple HTTP interface that can be implemented for different platforms:
//! - Native (Lambda/Server): reqwest with AWS SigV4
//! - WASM (Workers): reqwest with wasm-bindgen fetch

use anyhow::{Context, Result};
use async_trait::async_trait;

/// HTTP response from the Iceberg catalog
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
    /// HTTP status code
    pub status: u16,
    /// Response headers (name, value pairs)
    pub headers: Vec<(String, String)>,
    /// Response body bytes
    pub body: Vec<u8>,
}

impl HttpResponse {
    /// Check if the response status indicates success (2xx)
    pub fn is_success(&self) -> bool {
        self.status >= 200 && self.status < 300
    }

    /// Get the response body as a UTF-8 string
    pub fn body_string(&self) -> Result<String> {
        String::from_utf8(self.body.clone())
            .map_err(|e| anyhow::anyhow!("Invalid UTF-8 in response body: {}", e))
    }

    /// Parse the response body as JSON
    pub fn json<T: serde::de::DeserializeOwned>(&self) -> Result<T> {
        serde_json::from_slice(&self.body)
            .map_err(|e| anyhow::anyhow!("Failed to parse JSON response: {}", e))
    }

    /// Get a header value by name (case-insensitive)
    pub fn header(&self, name: &str) -> Option<&str> {
        let name_lower = name.to_lowercase();
        self.headers
            .iter()
            .find(|(k, _)| k.to_lowercase() == name_lower)
            .map(|(_, v)| v.as_str())
    }
}

/// Platform-agnostic HTTP client trait
///
/// Implementations provide platform-specific HTTP requests while maintaining
/// a simple, common interface for the Iceberg catalog client.
#[async_trait]
pub trait HttpClient: Send + Sync {
    /// Execute an HTTP request
    ///
    /// # Parameters
    /// - `method`: HTTP method (GET, POST, PUT, DELETE, etc.)
    /// - `url`: Full URL to request
    /// - `headers`: Request headers as (name, value) pairs
    /// - `body`: Optional request body bytes
    ///
    /// # Returns
    /// Returns the HTTP response or an error
    async fn request(
        &self,
        method: &str,
        url: &str,
        headers: Vec<(String, String)>,
        body: Option<Vec<u8>>,
    ) -> Result<HttpResponse>;

    /// Convenience method for GET requests
    async fn get(&self, url: &str, headers: Vec<(String, String)>) -> Result<HttpResponse> {
        self.request("GET", url, headers, None).await
    }

    /// Convenience method for POST requests
    async fn post(
        &self,
        url: &str,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    ) -> Result<HttpResponse> {
        self.request("POST", url, headers, Some(body)).await
    }

    /// Convenience method for PUT requests
    async fn put(
        &self,
        url: &str,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    ) -> Result<HttpResponse> {
        self.request("PUT", url, headers, Some(body)).await
    }

    /// Convenience method for DELETE requests
    async fn delete(&self, url: &str, headers: Vec<(String, String)>) -> Result<HttpResponse> {
        self.request("DELETE", url, headers, None).await
    }
}

/// Unified HTTP client using reqwest
///
/// Works on both native and WASM targets. Reqwest automatically switches
/// between native (hyper + rustls) and WASM (wasm-bindgen fetch) implementations.
///
/// TODO: Add AWS SigV4 signing support for S3 Tables endpoints
pub struct ReqwestHttpClient {
    /// Reqwest HTTP client
    client: reqwest::Client,
}

impl ReqwestHttpClient {
    /// Create a new ReqwestHttpClient
    ///
    /// # Parameters
    /// - `_base_url`: Base URL of the REST catalog (reserved for future SigV4 detection)
    pub async fn new(_base_url: &str) -> Result<Self> {
        let mut builder = reqwest::Client::builder();

        // Timeout only works on native (WASM uses browser's timeout)
        #[cfg(not(target_arch = "wasm32"))]
        {
            builder = builder.timeout(std::time::Duration::from_secs(30));
        }

        let client = builder
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to create reqwest client: {}", e))?;

        Ok(Self { client })
    }

    /// Create a client with custom configuration
    pub fn with_client(client: reqwest::Client) -> Self {
        Self { client }
    }
}

// Native: async_trait with Send
#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl HttpClient for ReqwestHttpClient {
    async fn request(
        &self,
        method: &str,
        url: &str,
        mut headers: Vec<(String, String)>,
        body: Option<Vec<u8>>,
    ) -> Result<HttpResponse> {
        // Add User-Agent if not present
        if !headers
            .iter()
            .any(|(k, _)| k.eq_ignore_ascii_case("user-agent"))
        {
            headers.push((
                "User-Agent".to_string(),
                "otlp2parquet-iceberg/0.1".to_string(),
            ));
        }

        // Build reqwest request
        let mut request_builder = self
            .client
            .request(method.parse().context("Invalid HTTP method")?, url);

        // Add headers
        for (name, value) in &headers {
            request_builder = request_builder.header(name, value);
        }

        // Add body if present
        if let Some(body_bytes) = body {
            request_builder = request_builder.body(body_bytes);
        }

        // Execute request
        let response = request_builder
            .send()
            .await
            .context("HTTP request failed")?;

        // Extract response details
        let status = response.status().as_u16();
        let response_headers: Vec<(String, String)> = response
            .headers()
            .iter()
            .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();
        let body = response
            .bytes()
            .await
            .context("Failed to read response body")?
            .to_vec();

        Ok(HttpResponse {
            status,
            headers: response_headers,
            body,
        })
    }
}

// WASM: async_trait with ?Send
#[cfg(target_arch = "wasm32")]
#[async_trait(?Send)]
impl HttpClient for ReqwestHttpClient {
    async fn request(
        &self,
        method: &str,
        url: &str,
        mut headers: Vec<(String, String)>,
        body: Option<Vec<u8>>,
    ) -> Result<HttpResponse> {
        // Add User-Agent if not present
        if !headers
            .iter()
            .any(|(k, _)| k.eq_ignore_ascii_case("user-agent"))
        {
            headers.push((
                "User-Agent".to_string(),
                "otlp2parquet-iceberg-wasm/0.1".to_string(),
            ));
        }

        // Build reqwest request
        let mut request_builder = self.client.request(
            method
                .parse()
                .map_err(|e| anyhow::anyhow!("Invalid HTTP method: {}", e))?,
            url,
        );

        // Add headers
        for (name, value) in &headers {
            request_builder = request_builder.header(name, value);
        }

        // Add body if present
        if let Some(body_bytes) = body {
            request_builder = request_builder.body(body_bytes);
        }

        // Execute request
        let response = request_builder
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("HTTP request failed: {}", e))?;

        // Extract response details
        let status = response.status().as_u16();
        let response_headers: Vec<(String, String)> = response
            .headers()
            .iter()
            .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();
        let body = response
            .bytes()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read response body: {}", e))?
            .to_vec();

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
    fn test_http_response_is_success() {
        let response = HttpResponse {
            status: 200,
            headers: vec![],
            body: vec![],
        };
        assert!(response.is_success());

        let response = HttpResponse {
            status: 299,
            headers: vec![],
            body: vec![],
        };
        assert!(response.is_success());

        let response = HttpResponse {
            status: 404,
            headers: vec![],
            body: vec![],
        };
        assert!(!response.is_success());
    }

    #[test]
    fn test_http_response_body_string() {
        let response = HttpResponse {
            status: 200,
            headers: vec![],
            body: b"Hello, world!".to_vec(),
        };
        assert_eq!(response.body_string().unwrap(), "Hello, world!");
    }

    #[test]
    fn test_http_response_json() {
        let response = HttpResponse {
            status: 200,
            headers: vec![],
            body: br#"{"key":"value"}"#.to_vec(),
        };

        let parsed: serde_json::Value = response.json().unwrap();
        assert_eq!(parsed["key"], "value");
    }

    #[test]
    fn test_http_response_header() {
        let response = HttpResponse {
            status: 200,
            headers: vec![
                ("Content-Type".to_string(), "application/json".to_string()),
                ("X-Custom".to_string(), "test".to_string()),
            ],
            body: vec![],
        };

        assert_eq!(response.header("Content-Type").unwrap(), "application/json");
        assert_eq!(response.header("content-type").unwrap(), "application/json"); // case-insensitive
        assert_eq!(response.header("X-Custom").unwrap(), "test");
        assert!(response.header("Missing").is_none());
    }

    // Only run construction test on native (WASM needs runtime)
    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn test_create_client() {
        let client = ReqwestHttpClient::new("https://catalog.example.com/v1")
            .await
            .unwrap();
        // Just verify client was created successfully
        assert!(std::mem::size_of_val(&client.client) > 0);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn test_create_client_aws_endpoint() {
        // Should succeed even for AWS endpoints (signing not yet implemented)
        let _client = ReqwestHttpClient::new("https://s3tables.us-east-1.amazonaws.com/iceberg")
            .await
            .unwrap();
        // Just verify client was created successfully
    }
}
