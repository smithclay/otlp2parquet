//! URL resolution utilities for connect commands

use anyhow::Result;

const DEFAULT_ENDPOINT: &str = "http://localhost:4318";

/// Resolve the OTLP endpoint URL from provided argument or default.
pub fn resolve_endpoint_url(url: Option<&str>) -> Result<String> {
    Ok(url
        .map(str::to_string)
        .unwrap_or_else(|| DEFAULT_ENDPOINT.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_with_url() {
        let result = resolve_endpoint_url(Some("https://example.com"));
        assert_eq!(result.unwrap(), "https://example.com");
    }

    #[test]
    fn test_resolve_without_url() {
        let result = resolve_endpoint_url(None);
        assert_eq!(result.unwrap(), "http://localhost:4318");
    }
}
