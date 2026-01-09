//! URL resolution utilities for connect commands

use anyhow::Result;

const DEFAULT_ENDPOINT: &str = "http://localhost:4318";

/// Resolve the worker/server URL from provided argument or default
pub async fn resolve_worker_url(url: Option<&str>) -> Result<String> {
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
        let result = resolve_worker_url(Some("https://my-worker.workers.dev")).await;
        assert_eq!(result.unwrap(), "https://my-worker.workers.dev");
    }

    #[tokio::test]
    async fn test_resolve_without_url() {
        let result = resolve_worker_url(None).await;
        assert_eq!(result.unwrap(), "http://localhost:4318");
    }
}
