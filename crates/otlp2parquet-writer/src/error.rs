//! Error types for the OTLP writer crate

use otlp2parquet_core::SignalType;
use thiserror::Error;

/// Redact a secret for safe logging, showing first 4 and last 4 characters.
///
/// Examples:
/// - "lTkWygojZsXFtfv07Rlzw80moyduOwJcZJ63grtT" -> "lTkW...grtT"
/// - "short" -> "s]***[t" (for secrets < 10 chars)
/// - "" -> "(empty)"
pub fn redact_secret(secret: &str) -> String {
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
    /// E001: Catalog unreachable or connection failed
    E001CatalogUnreachable,
    /// E002: Invalid credentials for catalog or storage
    E002InvalidCredentials,
    /// E003: Schema incompatible with existing table
    E003SchemaIncompatible,
    /// E004: Configuration missing or invalid
    E004InvalidConfig,
    /// E005: Write operation failed
    E005WriteFailure,
    /// E006: Platform not supported for operation
    E006UnsupportedPlatform,
    /// E007: Table operation failed
    E007TableOperation,
}

impl ErrorCode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::E001CatalogUnreachable => "E001",
            Self::E002InvalidCredentials => "E002",
            Self::E003SchemaIncompatible => "E003",
            Self::E004InvalidConfig => "E004",
            Self::E005WriteFailure => "E005",
            Self::E006UnsupportedPlatform => "E006",
            Self::E007TableOperation => "E007",
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
    /// Catalog initialization failed
    #[error("[{code}] Catalog initialization failed for '{catalog_type}' at '{endpoint}': {reason}\n\nCredentials used:\n{credentials_used}\n\nTroubleshooting:\n  • Verify catalog is reachable: curl {endpoint}\n  • Check credentials are valid and have correct permissions\n  • Ensure the secret values match your working configuration\n  • Ensure catalog service is running\n\nSee: {docs_url}")]
    CatalogInit {
        code: &'static str,
        catalog_type: String,
        endpoint: String,
        reason: String,
        /// Redacted credentials for debugging (e.g., "api_token: lTkW...grtT")
        credentials_used: String,
        docs_url: String,
    },

    /// Table operation failed
    #[error("[{code}] Table operation failed for '{table}': {reason}\n\nSee: {docs_url}")]
    TableOperation {
        code: &'static str,
        /// The table name
        table: String,
        /// The reason for failure
        reason: String,
        docs_url: String,
    },

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

    /// Invalid table name for the given signal type
    #[error("Invalid table name for signal: {signal:?}, metric_type: {metric_type:?}")]
    InvalidTableName {
        /// The signal type
        signal: SignalType,
        /// The optional metric type
        metric_type: Option<String>,
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
    /// Create a catalog init error with error code and redacted credentials
    pub fn catalog_init_with_credentials(
        catalog_type: String,
        endpoint: String,
        reason: String,
        credentials_used: String,
    ) -> Self {
        let code_enum = ErrorCode::E001CatalogUnreachable;
        Self::CatalogInit {
            code: code_enum.as_str(),
            catalog_type,
            endpoint,
            reason,
            credentials_used,
            docs_url: code_enum.docs_url(),
        }
    }

    /// Create a catalog init error with error code (no credentials info)
    pub fn catalog_init(catalog_type: String, endpoint: String, reason: String) -> Self {
        Self::catalog_init_with_credentials(
            catalog_type,
            endpoint,
            reason,
            "  (no credential info available)".to_string(),
        )
    }

    /// Create a table operation error with error code
    pub fn table_operation(table: String, reason: String) -> Self {
        let code_enum = ErrorCode::E007TableOperation;
        Self::TableOperation {
            code: code_enum.as_str(),
            table,
            reason,
            docs_url: code_enum.docs_url(),
        }
    }

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
