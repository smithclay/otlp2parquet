//! Error types for the OTLP writer crate

use otlp2parquet_core::SignalType;
use thiserror::Error;

/// Errors that can occur during OTLP writing operations
#[derive(Debug, Error)]
pub enum WriterError {
    /// Catalog initialization failed
    #[error("Catalog initialization failed: {0}")]
    CatalogInit(String),

    /// Table operation failed
    #[error("Table operation failed for '{table}': {reason}")]
    TableOperation {
        /// The table name
        table: String,
        /// The reason for failure
        reason: String,
    },

    /// Invalid configuration provided
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    /// Write operation failed
    #[error("Write operation failed: {0}")]
    WriteFailure(String),

    /// Invalid table name for the given signal type
    #[error("Invalid table name for signal: {signal:?}, metric_type: {metric_type:?}")]
    InvalidTableName {
        /// The signal type
        signal: SignalType,
        /// The optional metric type
        metric_type: Option<String>,
    },

    /// Platform not supported for this operation
    #[error("Platform not supported: {0}")]
    UnsupportedPlatform(String),
}

/// Result type alias for WriterError
pub type Result<T> = std::result::Result<T, WriterError>;
