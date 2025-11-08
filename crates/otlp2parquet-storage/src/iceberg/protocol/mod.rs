//! Iceberg REST API protocol types

pub mod requests;
pub mod responses;

// Re-export commonly used types
pub use requests::{
    CommitTableRequest, CommitTransactionRequest, Snapshot, SnapshotRefType, TableRequirement,
    TableUpdate,
};
pub use responses::{CommitTransactionResponse, ErrorResponse};
