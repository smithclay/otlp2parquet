//! Platform detection for runtime environment

/// Platform type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    /// Cloudflare Workers
    CloudflareWorkers,
    /// AWS Lambda
    Lambda,
    /// Server (default)
    Server,
}

/// Detect the current platform based on environment variables
///
/// Detection order:
/// 1. CF_WORKER env var -> CloudflareWorkers
/// 2. AWS_LAMBDA_FUNCTION_NAME env var -> Lambda
/// 3. Default -> Server
pub fn detect_platform() -> Platform {
    if std::env::var("CF_WORKER").is_ok() {
        Platform::CloudflareWorkers
    } else if std::env::var("AWS_LAMBDA_FUNCTION_NAME").is_ok() {
        Platform::Lambda
    } else {
        Platform::Server
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_platform_detection_server() {
        // Default is Server when no env vars set
        std::env::remove_var("CF_WORKER");
        std::env::remove_var("AWS_LAMBDA_FUNCTION_NAME");
        assert_eq!(detect_platform(), Platform::Server);
    }
}
