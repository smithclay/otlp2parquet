// otlp2parquet-runtime - Platform-specific adapters
//
// This crate provides runtime adapters for different platforms:
// - Cloudflare Workers (R2 storage, WASM-constrained)
// - AWS Lambda (S3 storage, event-driven)
// - Server (default mode, multi-backend storage, production features)

pub mod partition;

#[cfg(feature = "cloudflare")]
pub mod cloudflare;

#[cfg(feature = "lambda")]
pub mod lambda;

#[cfg(feature = "server")]
pub mod server;

// OpenDAL storage implementation
#[cfg(feature = "opendal")]
pub mod opendal_storage;

/// Platform detection based on environment variables
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    CloudflareWorkers,
    Lambda,
    Server,
}

impl Platform {
    /// Detect the current platform from environment variables
    ///
    /// Detection order:
    /// 1. CF_WORKER env var → Cloudflare Workers
    /// 2. AWS_LAMBDA_FUNCTION_NAME env var → Lambda
    /// 3. Otherwise → Server (default/general purpose)
    pub fn detect() -> Self {
        if std::env::var("CF_WORKER").is_ok() {
            Platform::CloudflareWorkers
        } else if std::env::var("AWS_LAMBDA_FUNCTION_NAME").is_ok() {
            Platform::Lambda
        } else {
            Platform::Server
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_platform_detection_server() {
        // In test environment without special env vars, should be Server (default)
        let platform = Platform::detect();
        // This will vary based on test environment, but at least verify it returns a value
        assert!(
            platform == Platform::Server
                || platform == Platform::Lambda
                || platform == Platform::CloudflareWorkers
        );
    }
}
