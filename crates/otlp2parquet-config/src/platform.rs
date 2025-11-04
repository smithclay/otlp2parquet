// Platform detection based on environment variables
//
// Auto-detects runtime environment:
// - Cloudflare Workers: CF_WORKER env var present
// - AWS Lambda: AWS_LAMBDA_FUNCTION_NAME env var present
// - Server: Neither present (default)

use std::env;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    Server,
    Lambda,
    CloudflareWorkers,
}

impl Platform {
    /// Auto-detect the current platform based on environment variables
    pub fn detect() -> Self {
        if env::var("CF_WORKER").is_ok() {
            Platform::CloudflareWorkers
        } else if env::var("AWS_LAMBDA_FUNCTION_NAME").is_ok() {
            Platform::Lambda
        } else {
            Platform::Server
        }
    }

    /// Get platform-specific defaults
    pub fn defaults(&self) -> PlatformDefaults {
        match self {
            Platform::Server => PlatformDefaults {
                batch_max_rows: 200_000,
                batch_max_bytes: 128 * 1024 * 1024, // 128 MB
                batch_max_age_secs: 10,
                max_payload_bytes: 8 * 1024 * 1024, // 8 MB
                storage_backend: "fs",
            },
            Platform::Lambda => PlatformDefaults {
                batch_max_rows: 200_000,
                batch_max_bytes: 128 * 1024 * 1024, // 128 MB
                batch_max_age_secs: 10,
                max_payload_bytes: 6 * 1024 * 1024, // 6 MB
                storage_backend: "s3",
            },
            Platform::CloudflareWorkers => PlatformDefaults {
                batch_max_rows: 100_000,
                batch_max_bytes: 64 * 1024 * 1024, // 64 MB
                batch_max_age_secs: 5,
                max_payload_bytes: 10 * 1024 * 1024, // 10 MB
                storage_backend: "r2",
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct PlatformDefaults {
    pub batch_max_rows: usize,
    pub batch_max_bytes: usize,
    pub batch_max_age_secs: u64,
    pub max_payload_bytes: usize,
    pub storage_backend: &'static str,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_platform_defaults() {
        let server = Platform::Server.defaults();
        assert_eq!(server.batch_max_rows, 200_000);
        assert_eq!(server.storage_backend, "fs");

        let lambda = Platform::Lambda.defaults();
        assert_eq!(lambda.batch_max_rows, 200_000);
        assert_eq!(lambda.storage_backend, "s3");

        let cf = Platform::CloudflareWorkers.defaults();
        assert_eq!(cf.batch_max_rows, 100_000);
        assert_eq!(cf.storage_backend, "r2");
    }
}
