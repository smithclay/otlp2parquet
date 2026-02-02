// Forwarding module - forwards OTLP requests to additional endpoints
//
// Similar to Datadog's additional_endpoints feature, this allows forwarding
// incoming OTLP data to multiple destinations unchanged.

use crate::config::ForwardingConfig;
use crate::SignalType;
use bytes::Bytes;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use std::time::Duration;
use tracing::{debug, warn};

/// Forwarder handles forwarding OTLP requests to additional endpoints
#[derive(Clone)]
pub struct Forwarder {
    client: reqwest::Client,
    endpoints: Vec<EndpointConfig>,
    blocking: bool,
}

/// Internal endpoint configuration with pre-parsed headers
#[derive(Clone)]
struct EndpointConfig {
    base_url: String,
    headers: HeaderMap,
}

impl Forwarder {
    /// Create a new Forwarder from the forwarding configuration
    pub fn new(config: &ForwardingConfig) -> Result<Self, anyhow::Error> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .gzip(true)
            .build()?;

        let mut endpoints = Vec::with_capacity(config.additional_endpoints.len());
        for ep in &config.additional_endpoints {
            let headers = parse_headers(&ep.headers)?;
            endpoints.push(EndpointConfig {
                base_url: ep.endpoint.trim_end_matches('/').to_string(),
                headers,
            });
        }

        Ok(Self {
            client,
            endpoints,
            blocking: config.blocking,
        })
    }

    /// Check if forwarding should block the response
    pub fn is_blocking(&self) -> bool {
        self.blocking
    }

    /// Forward a request to all configured endpoints
    /// Returns the number of successful forwards
    pub async fn forward(
        &self,
        signal: SignalType,
        content_type: Option<&str>,
        body: Bytes,
    ) -> usize {
        if self.endpoints.is_empty() {
            return 0;
        }

        let path = signal_to_path(signal);
        let content_type = content_type.unwrap_or("application/x-protobuf");

        let mut success_count = 0;

        for endpoint in self.endpoints.iter() {
            let url = format!("{}{}", endpoint.base_url, path);
            match self
                .forward_to_endpoint(&url, &endpoint.headers, content_type, body.clone())
                .await
            {
                Ok(status) => {
                    if status.is_success() {
                        debug!(
                            url = %url,
                            status = %status,
                            signal = %signal.as_str(),
                            "Successfully forwarded request"
                        );
                        success_count += 1;
                    } else {
                        warn!(
                            url = %url,
                            status = %status,
                            signal = %signal.as_str(),
                            "Forward request returned non-success status"
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        url = %url,
                        error = %e,
                        signal = %signal.as_str(),
                        "Failed to forward request"
                    );
                }
            }
        }

        success_count
    }

    /// Forward to a single endpoint
    async fn forward_to_endpoint(
        &self,
        url: &str,
        headers: &HeaderMap,
        content_type: &str,
        body: Bytes,
    ) -> Result<reqwest::StatusCode, reqwest::Error> {
        let response = self
            .client
            .post(url)
            .headers(headers.clone())
            .header("Content-Type", content_type)
            .body(body)
            .send()
            .await?;

        Ok(response.status())
    }

    /// Get the number of configured endpoints
    pub fn endpoint_count(&self) -> usize {
        self.endpoints.len()
    }
}

/// Parse a HashMap of string headers into a HeaderMap
fn parse_headers(
    headers: &std::collections::HashMap<String, String>,
) -> Result<HeaderMap, anyhow::Error> {
    let mut header_map = HeaderMap::new();
    for (key, value) in headers {
        let name = HeaderName::try_from(key.as_str())
            .map_err(|e| anyhow::anyhow!("Invalid header name '{}': {}", key, e))?;
        let val = HeaderValue::from_str(value)
            .map_err(|e| anyhow::anyhow!("Invalid header value for '{}': {}", key, e))?;
        header_map.insert(name, val);
    }
    Ok(header_map)
}

/// Convert a signal type to its OTLP HTTP path
fn signal_to_path(signal: SignalType) -> &'static str {
    match signal {
        SignalType::Logs => "/v1/logs",
        SignalType::Traces => "/v1/traces",
        SignalType::Metrics => "/v1/metrics",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AdditionalEndpoint;
    use std::collections::HashMap;

    #[test]
    fn test_parse_headers() {
        let mut headers = HashMap::new();
        headers.insert("Authorization".to_string(), "Bearer token123".to_string());
        headers.insert("X-Custom-Header".to_string(), "custom-value".to_string());

        let header_map = parse_headers(&headers).unwrap();
        assert_eq!(header_map.len(), 2);
        assert_eq!(header_map.get("Authorization").unwrap(), "Bearer token123");
        assert_eq!(header_map.get("X-Custom-Header").unwrap(), "custom-value");
    }

    #[test]
    fn test_signal_to_path() {
        assert_eq!(signal_to_path(SignalType::Logs), "/v1/logs");
        assert_eq!(signal_to_path(SignalType::Traces), "/v1/traces");
        assert_eq!(signal_to_path(SignalType::Metrics), "/v1/metrics");
    }

    #[test]
    fn test_forwarder_no_endpoints() {
        let config = ForwardingConfig::default();
        let forwarder = Forwarder::new(&config).unwrap();
        assert_eq!(forwarder.endpoint_count(), 0);
    }

    #[test]
    fn test_forwarder_with_endpoints() {
        let config = ForwardingConfig {
            additional_endpoints: vec![AdditionalEndpoint {
                endpoint: "https://example.com:4318".to_string(),
                headers: HashMap::new(),
            }],
            ..Default::default()
        };
        let forwarder = Forwarder::new(&config).unwrap();
        assert_eq!(forwarder.endpoint_count(), 1);
    }
}
