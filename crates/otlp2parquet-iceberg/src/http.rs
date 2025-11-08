//! HTTP client abstraction

use anyhow::Result;
use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl HttpResponse {
    pub fn is_success(&self) -> bool {
        self.status >= 200 && self.status < 300
    }

    pub fn body_string(&self) -> Result<String> {
        String::from_utf8(self.body.clone()).map_err(|e| anyhow::anyhow!("Invalid UTF-8: {}", e))
    }

    pub fn json<T: serde::de::DeserializeOwned>(&self) -> Result<T> {
        serde_json::from_slice(&self.body)
            .map_err(|e| anyhow::anyhow!("Failed to parse JSON: {}", e))
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        let name_lower = name.to_lowercase();
        self.headers
            .iter()
            .find(|(k, _)| k.to_lowercase() == name_lower)
            .map(|(_, v)| v.as_str())
    }
}

#[async_trait]
pub trait HttpClient: Send + Sync {
    async fn request(
        &self,
        method: &str,
        url: &str,
        headers: Vec<(String, String)>,
        body: Option<Vec<u8>>,
    ) -> Result<HttpResponse>;

    async fn get(&self, url: &str, headers: Vec<(String, String)>) -> Result<HttpResponse> {
        self.request("GET", url, headers, None).await
    }

    async fn post(
        &self,
        url: &str,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    ) -> Result<HttpResponse> {
        self.request("POST", url, headers, Some(body)).await
    }
}

/// Simple reqwest-based HTTP client (no authentication)
pub struct ReqwestHttpClient {
    client: reqwest::Client,
}

impl ReqwestHttpClient {
    pub fn new() -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to create client: {}", e))?;
        Ok(Self { client })
    }
}

#[async_trait]
impl HttpClient for ReqwestHttpClient {
    async fn request(
        &self,
        method: &str,
        url: &str,
        headers: Vec<(String, String)>,
        body: Option<Vec<u8>>,
    ) -> Result<HttpResponse> {
        use anyhow::Context;

        let mut builder = self
            .client
            .request(method.parse().context("Invalid HTTP method")?, url);

        for (name, value) in &headers {
            builder = builder.header(name, value);
        }

        if let Some(body_bytes) = body {
            builder = builder.body(body_bytes);
        }

        let response = builder.send().await.context("HTTP request failed")?;

        let status = response.status().as_u16();
        let response_headers: Vec<(String, String)> = response
            .headers()
            .iter()
            .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();
        let body = response
            .bytes()
            .await
            .context("Failed to read body")?
            .to_vec();

        Ok(HttpResponse {
            status,
            headers: response_headers,
            body,
        })
    }
}
