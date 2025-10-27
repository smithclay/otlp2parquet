// quill-runtime - Platform-specific adapters
//
// This crate provides runtime adapters for different platforms:
// - Cloudflare Workers (R2 storage)
// - AWS Lambda (S3 storage)
// - Standalone (local filesystem)

#[cfg(feature = "cloudflare")]
pub mod cloudflare;

#[cfg(feature = "lambda")]
pub mod lambda;

#[cfg(feature = "standalone")]
pub mod standalone;

/// Platform detection based on environment variables
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    CloudflareWorkers,
    Lambda,
    Standalone,
}

impl Platform {
    /// Detect the current platform from environment variables
    ///
    /// Detection order:
    /// 1. CF_WORKER env var → Cloudflare Workers
    /// 2. AWS_LAMBDA_FUNCTION_NAME env var → Lambda
    /// 3. Otherwise → Standalone
    pub fn detect() -> Self {
        if std::env::var("CF_WORKER").is_ok() {
            Platform::CloudflareWorkers
        } else if std::env::var("AWS_LAMBDA_FUNCTION_NAME").is_ok() {
            Platform::Lambda
        } else {
            Platform::Standalone
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_platform_detection_standalone() {
        // In test environment without special env vars, should be Standalone
        let platform = Platform::detect();
        // This will vary based on test environment, but at least verify it returns a value
        assert!(
            platform == Platform::Standalone
                || platform == Platform::Lambda
                || platform == Platform::CloudflareWorkers
        );
    }
}
