//! Native HTTP client implementation for Lambda and Server
//!
//! Uses reqwest for HTTP operations.
//! TODO: Add AWS SigV4 signing for S3 Tables authentication

use crate::http::{HttpClient, HttpResponse};
use anyhow::{Context, Result};
use async_trait::async_trait;

/// Native HTTP client using reqwest
///
/// TODO: Add AWS SigV4 signing support for S3 Tables endpoints
pub struct NativeHttpClient {
    /// Reqwest HTTP client
    client: reqwest::Client,
}

impl NativeHttpClient {
    /// Create a new NativeHttpClient
    ///
    /// # Parameters
    /// - `_base_url`: Base URL of the REST catalog (reserved for future SigV4 detection)
    pub async fn new(_base_url: &str) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .context("Failed to create reqwest client")?;

        Ok(Self { client })
    }

    /// Create a client with custom configuration
    pub fn with_client(client: reqwest::Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl HttpClient for NativeHttpClient {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_client() {
        let client = NativeHttpClient::new("https://catalog.example.com/v1")
            .await
            .unwrap();
        // Just verify client was created successfully
        assert!(std::mem::size_of_val(&client.client) > 0);
    }

    #[tokio::test]
    async fn test_create_client_aws_endpoint() {
        // Should succeed even for AWS endpoints (signing not yet implemented)
        let _client = NativeHttpClient::new("https://s3tables.us-east-1.amazonaws.com/iceberg")
            .await
            .unwrap();
        // Just verify client was created successfully
    }
}
