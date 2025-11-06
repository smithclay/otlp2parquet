//! Iceberg REST API protocol types

pub mod requests;
pub mod responses;

// Re-export commonly used types
pub use requests::{CommitTransactionRequest, SnapshotRefType, TableUpdate};
pub use responses::{CommitTransactionResponse, ErrorResponse};
