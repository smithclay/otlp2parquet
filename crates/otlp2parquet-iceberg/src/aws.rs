//! AWS SigV4-authenticated HTTP client for S3 Tables

use crate::http::{HttpClient, HttpResponse};
use anyhow::{Context, Result};
use async_trait::async_trait;
use aws_credential_types::provider::ProvideCredentials;
use aws_sigv4::http_request::{sign, SignableBody, SignableRequest, SigningSettings};
use aws_sigv4::sign::v4;

/// HTTP client that signs requests with AWS SigV4
pub struct AwsSigV4HttpClient {
    client: reqwest::Client,
    credentials_provider: aws_credential_types::provider::SharedCredentialsProvider,
    region: String,
}

impl AwsSigV4HttpClient {
    /// Create new AWS SigV4 client
    ///
    /// Loads credentials from environment (Lambda execution role, EC2 instance profile, etc.)
    pub async fn new(region: &str) -> Result<Self> {
        let config = aws_config::from_env().load().await;
        let credentials_provider = config
            .credentials_provider()
            .ok_or_else(|| anyhow::anyhow!("No AWS credentials provider available"))?;

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to create reqwest client: {}", e))?;

        Ok(Self {
            client,
            credentials_provider,
            region: region.to_string(),
        })
    }
}

#[async_trait]
impl HttpClient for AwsSigV4HttpClient {
    async fn request(
        &self,
        method: &str,
        url: &str,
        mut headers: Vec<(String, String)>,
        body: Option<Vec<u8>>,
    ) -> Result<HttpResponse> {
        // Get AWS credentials
        let credentials = self
            .credentials_provider
            .provide_credentials()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get AWS credentials: {}", e))?;

        // Build signable request
        let parsed_url = url.parse::<url::Url>().context("Failed to parse URL")?;

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
            None,
            "lambda-execution-role",
        )
        .into();

        // Configure signing params
        let signing_params = v4::SigningParams::builder()
            .identity(&identity)
            .region(&self.region)
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

        // Build and execute reqwest request
        let mut request_builder = self
            .client
            .request(method.parse().context("Invalid HTTP method")?, url);

        for (name, value) in &headers {
            request_builder = request_builder.header(name, value);
        }

        if let Some(body_bytes) = body {
            request_builder = request_builder.body(body_bytes);
        }

        let response = request_builder
            .send()
            .await
            .context("HTTP request failed")?;

        // Extract response
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
