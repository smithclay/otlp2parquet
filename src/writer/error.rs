//! Error types for Parquet writer operations.

use thiserror::Error;

/// Error codes for programmatic handling
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    /// E004: Configuration missing or invalid
    E004InvalidConfig,
    /// E005: Write operation failed
    E005WriteFailure,
}

impl ErrorCode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::E004InvalidConfig => "E004",
            Self::E005WriteFailure => "E005",
        }
    }

    pub fn docs_url(&self) -> String {
        format!(
            "https://smithclay.github.io/otlp2parquet/troubleshooting.html#{}",
            self.as_str().to_lowercase()
        )
    }
}

/// Errors that can occur during Parquet writing operations
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
}

/// Result type alias for WriterError
pub type Result<T> = std::result::Result<T, WriterError>;
