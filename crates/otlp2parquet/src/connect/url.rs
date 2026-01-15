//! URL resolution utilities for connect commands

use anyhow::Result;

const DEFAULT_ENDPOINT: &str = "http://localhost:4318";

/// Resolve the OTLP endpoint URL from provided argument or default
pub async fn resolve_endpoint_url(url: Option<&str>) -> Result<String> {
    match url {
        Some(u) => Ok(u.to_string()),
        None => Ok(DEFAULT_ENDPOINT.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_resolve_with_url() {
        let result = resolve_endpoint_url(Some("https://example.com")).await;
        assert_eq!(result.unwrap(), "https://example.com");
    }

    #[tokio::test]
    async fn test_resolve_without_url() {
        let result = resolve_endpoint_url(None).await;
        assert_eq!(result.unwrap(), "http://localhost:4318");
    }
}
