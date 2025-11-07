//! Platform-agnostic HTTP client abstraction
//!
//! Provides a simple HTTP interface that can be implemented for different platforms:
//! - Native (Lambda/Server): reqwest with AWS SigV4
//! - WASM (Workers): reqwest with wasm-bindgen fetch

#[cfg(not(target_arch = "wasm32"))]
use anyhow::Context;
use anyhow::Result;
use async_trait::async_trait;

#[cfg(not(target_arch = "wasm32"))]
use aws_credential_types::provider::ProvideCredentials;
#[cfg(not(target_arch = "wasm32"))]
use aws_sigv4::http_request::{sign, SignableBody, SignableRequest, SigningSettings};
#[cfg(not(target_arch = "wasm32"))]
use aws_sigv4::sign::v4;

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
#[cfg(not(target_arch = "wasm32"))]
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

#[cfg(target_arch = "wasm32")]
#[async_trait(?Send)]
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
/// On native targets, automatically signs requests to S3 Tables endpoints using AWS SigV4
pub struct ReqwestHttpClient {
    /// Reqwest HTTP client
    client: reqwest::Client,
    /// AWS credentials provider for SigV4 signing (native only)
    #[cfg(not(target_arch = "wasm32"))]
    credentials_provider: Option<aws_credential_types::provider::SharedCredentialsProvider>,
}

impl ReqwestHttpClient {
    /// Create a new ReqwestHttpClient
    ///
    /// On native platforms, detects S3 Tables endpoints and initializes AWS credentials
    /// for SigV4 signing. On WASM, creates a basic client.
    ///
    /// # Parameters
    /// - `base_url`: Base URL of the REST catalog (used to detect S3 Tables endpoints)
    #[cfg_attr(target_arch = "wasm32", allow(unused_variables))]
    pub async fn new(base_url: &str) -> Result<Self> {
        #[allow(unused_mut)]
        let mut builder = reqwest::Client::builder();

        // Timeout only works on native (WASM uses browser's timeout)
        #[cfg(not(target_arch = "wasm32"))]
        {
            builder = builder.timeout(std::time::Duration::from_secs(30));
        }

        let client = builder
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to create reqwest client: {}", e))?;

        #[cfg(not(target_arch = "wasm32"))]
        {
            // Check if this is an S3 Tables endpoint
            let credentials_provider = if Self::is_s3_tables_endpoint(base_url) {
                // Load AWS credentials from environment (Lambda execution role, EC2 instance profile, etc.)
                // Use from_env() which works with Lambda's tokio runtime
                let config = aws_config::from_env().load().await;
                Some(config.credentials_provider().unwrap())
            } else {
                None
            };

            Ok(Self {
                client,
                credentials_provider,
            })
        }

        #[cfg(target_arch = "wasm32")]
        {
            Ok(Self { client })
        }
    }

    /// Create a client with custom configuration (WASM only)
    #[cfg(target_arch = "wasm32")]
    pub fn with_client(client: reqwest::Client) -> Self {
        Self { client }
    }

    /// Check if a URL is an S3 Tables endpoint
    #[cfg(not(target_arch = "wasm32"))]
    fn is_s3_tables_endpoint(url: &str) -> bool {
        url.contains("s3tables") && url.contains("amazonaws.com")
    }

    /// Extract AWS region from S3 Tables URL
    /// Example: https://s3tables.us-west-2.amazonaws.com/iceberg -> us-west-2
    #[cfg(not(target_arch = "wasm32"))]
    fn extract_region(url: &str) -> Option<String> {
        url.parse::<url::Url>()
            .ok()
            .and_then(|parsed| parsed.host_str().map(String::from))
            .and_then(|host| {
                // Pattern: s3tables.{region}.amazonaws.com
                let parts: Vec<&str> = host.split('.').collect();
                if parts.len() >= 3 && parts[0] == "s3tables" && parts[2] == "amazonaws" {
                    Some(parts[1].to_string())
                } else {
                    None
                }
            })
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

        // Apply AWS SigV4 signing if credentials are available
        if let Some(ref creds_provider) = self.credentials_provider {
            let region = Self::extract_region(url)
                .ok_or_else(|| anyhow::anyhow!("Failed to extract AWS region from URL: {}", url))?;

            // Get credentials
            let credentials = creds_provider
                .provide_credentials()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to get AWS credentials: {}", e))?;

            // Build signable request
            let parsed_url = url
                .parse::<url::Url>()
                .context("Failed to parse URL for signing")?;

            let signable_headers: Vec<(&str, &str)> = headers
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_str()))
                .collect();

            let signable_body = if let Some(ref body_bytes) = body {
                SignableBody::Bytes(body_bytes)
            } else {
                SignableBody::Bytes(&[])
            };

            let signable_request = SignableRequest::new(
                method,
                parsed_url.as_str(),
                signable_headers.into_iter(),
                signable_body,
            )
            .expect("Failed to create signable request");

            // Build identity from credentials
            let identity = aws_credential_types::Credentials::new(
                credentials.access_key_id(),
                credentials.secret_access_key(),
                credentials.session_token().map(String::from),
                None, // expiration
                "lambda-execution-role",
            )
            .into();

            // Configure signing params using v4 module
            let signing_params = v4::SigningParams::builder()
                .identity(&identity)
                .region(&region)
                .name("s3tables")
                .time(std::time::SystemTime::now())
                .settings(SigningSettings::default())
                .build()
                .map_err(|e| anyhow::anyhow!("Failed to build signing params: {}", e))?
                .into();

            // Sign the request
            let (signing_instructions, _signature) = sign(signable_request, &signing_params)
                .map_err(|e| anyhow::anyhow!("Failed to sign request: {}", e))?
                .into_parts();

            // Apply signing headers
            for (name, value) in signing_instructions.headers() {
                headers.push((name.to_string(), value.to_string()));
            }
        }

        // Build reqwest request
        let mut request_builder = self
            .client
            .request(method.parse().context("Invalid HTTP method")?, url);

        // Add headers (including signed headers if applicable)
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
