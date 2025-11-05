//! Iceberg REST API response types
//!
//! Based on: https://iceberg.apache.org/docs/latest/rest-api/

use serde::{Deserialize, Serialize};

/// Response from committing a transaction
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CommitTransactionResponse {
    /// Metadata location after the commit
    pub metadata_location: String,

    /// Metadata (optional in some catalogs)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

/// Error response from Iceberg REST API
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorResponse {
    /// Error message
    pub message: String,

    /// Error type/code
    #[serde(rename = "type")]
    pub error_type: String,

    /// HTTP status code
    pub code: u16,

    /// Additional error details
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stack: Option<Vec<String>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_commit_response_serialization() {
        let response = CommitTransactionResponse {
            metadata_location: "s3://bucket/warehouse/db/table/metadata/v2.metadata.json"
                .to_string(),
            metadata: None,
        };

        let json = serde_json::to_string(&response).unwrap();
        let deserialized: CommitTransactionResponse = serde_json::from_str(&json).unwrap();

        assert_eq!(response, deserialized);
    }

    #[test]
    fn test_error_response_serialization() {
        let error = ErrorResponse {
            message: "Table not found".to_string(),
            error_type: "NoSuchTableException".to_string(),
            code: 404,
            stack: None,
        };

        let json = serde_json::to_string(&error).unwrap();
        let deserialized: ErrorResponse = serde_json::from_str(&json).unwrap();

        assert_eq!(error, deserialized);
    }
}
