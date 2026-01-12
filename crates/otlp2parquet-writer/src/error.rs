//! Error types for the OTLP writer crate

use thiserror::Error;

/// Redact a secret for safe logging, showing first 4 and last 4 characters.
///
/// Examples:
/// - "lTkWygojZsXFtfv07Rlzw80moyduOwJcZJ63grtT" -> "lTkW...grtT"
/// - "short" -> "s***t" (for secrets < 10 chars)
/// - "" -> "(empty)"
#[allow(dead_code)] // Used for error messages, kept for future use
fn redact_secret(secret: &str) -> String {
    if secret.is_empty() {
        return "(empty)".to_string();
    }
    if secret.len() < 10 {
        // For short secrets, show first and last char only
        let first = secret.chars().next().unwrap_or('?');
        let last = secret.chars().last().unwrap_or('?');
        return format!("{}***{}", first, last);
    }
    // Show first 4 and last 4 characters
    let prefix: String = secret.chars().take(4).collect();
    let suffix: String = secret
        .chars()
        .rev()
        .take(4)
        .collect::<String>()
        .chars()
        .rev()
        .collect();
    format!("{}...{}", prefix, suffix)
}

/// Error codes for programmatic handling
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    /// E004: Configuration missing or invalid
    E004InvalidConfig,
    /// E005: Write operation failed
    E005WriteFailure,
    /// E006: Platform not supported for operation
    E006UnsupportedPlatform,
}

impl ErrorCode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::E004InvalidConfig => "E004",
            Self::E005WriteFailure => "E005",
            Self::E006UnsupportedPlatform => "E006",
        }
    }

    pub fn docs_url(&self) -> String {
        format!(
            "https://smithclay.github.io/otlp2parquet/troubleshooting.html#{}",
            self.as_str().to_lowercase()
        )
    }
}

/// Errors that can occur during OTLP writing operations
#[derive(Debug, Error)]
pub enum WriterError {
    /// Invalid configuration provided
    #[error("[{code}] Invalid configuration: {message}\n\nSee: {docs_url}")]
    InvalidConfig {
        code: &'static str,
        message: String,
        docs_url: String,
    },

    /// Write operation failed
    #[error("[{code}] Write operation failed: {message}\n\nSee: {docs_url}")]
    WriteFailure {
        code: &'static str,
        message: String,
        docs_url: String,
    },

    /// Platform not supported for this operation
    #[error("[{code}] Platform not supported: {message}\n\nSupported platforms: {supported}\n\nSee: {docs_url}")]
    UnsupportedPlatform {
        code: &'static str,
        message: String,
        supported: String,
        docs_url: String,
    },
}

impl WriterError {
    /// Create an invalid config error with error code
    pub fn invalid_config(message: String) -> Self {
        let code_enum = ErrorCode::E004InvalidConfig;
        Self::InvalidConfig {
            code: code_enum.as_str(),
            message,
            docs_url: code_enum.docs_url(),
        }
    }

    /// Create a write failure error with error code
    pub fn write_failure(message: String) -> Self {
        let code_enum = ErrorCode::E005WriteFailure;
        Self::WriteFailure {
            code: code_enum.as_str(),
            message,
            docs_url: code_enum.docs_url(),
        }
    }

    /// Create an unsupported platform error with error code
    pub fn unsupported_platform(message: String, supported: String) -> Self {
        let code_enum = ErrorCode::E006UnsupportedPlatform;
        Self::UnsupportedPlatform {
            code: code_enum.as_str(),
            message,
            supported,
            docs_url: code_enum.docs_url(),
        }
    }
}

/// Result type alias for WriterError
pub type Result<T> = std::result::Result<T, WriterError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redact_secret_empty() {
        assert_eq!(redact_secret(""), "(empty)");
    }

    #[test]
    fn test_redact_secret_short() {
        assert_eq!(redact_secret("abc"), "a***c");
        assert_eq!(redact_secret("12345678"), "1***8");
    }

    #[test]
    fn test_redact_secret_long() {
        // Token-like secret
        assert_eq!(
            redact_secret("lTkWygojZsXFtfv07Rlzw80moyduOwJcZJ63grtT"),
            "lTkW...grtT"
        );
        // Access key ID
        assert_eq!(
            redact_secret("e12dcf5a655bfd1917be71c51eb60f35"),
            "e12d...0f35"
        );
    }

    #[test]
    fn test_redact_secret_boundary() {
        // Exactly 10 chars - should use long format
        assert_eq!(redact_secret("1234567890"), "1234...7890");
        // 9 chars - should use short format
        assert_eq!(redact_secret("123456789"), "1***9");
    }
}
