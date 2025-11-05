//! Iceberg REST API request types
//!
//! Based on: <https://iceberg.apache.org/docs/latest/rest-api/>

use crate::types::DataFile;
use serde::{Deserialize, Serialize};

/// Request to commit a transaction to an Iceberg table
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CommitTransactionRequest {
    /// List of table changes to apply
    pub table_changes: Vec<TableUpdate>,
}

/// A table update operation
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case")]
pub enum TableUpdate {
    /// Append data files to the table
    #[serde(rename_all = "kebab-case")]
    AppendFiles {
        /// Data files to append
        data_files: Vec<DataFile>,
    },
    /// Add a new snapshot
    #[serde(rename_all = "kebab-case")]
    SetSnapshotRef {
        /// Reference name (e.g., "main")
        ref_name: String,
        /// Snapshot ID to set
        snapshot_id: i64,
        /// Reference type
        #[serde(rename = "type")]
        ref_type: SnapshotRefType,
    },
}

/// Type of snapshot reference
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SnapshotRefType {
    /// Branch reference
    Branch,
    /// Tag reference
    Tag,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DataContentType, DataFile, DataFileFormat};
    use std::collections::HashMap;

    #[test]
    fn test_append_files_serialization() {
        let data_file = DataFile::builder()
            .content(DataContentType::Data)
            .file_path("s3://bucket/file.parquet")
            .file_format(DataFileFormat::Parquet)
            .record_count(100)
            .file_size_in_bytes(5000)
            .partition(HashMap::new())
            .build()
            .unwrap();

        let request = CommitTransactionRequest {
            table_changes: vec![TableUpdate::AppendFiles {
                data_files: vec![data_file],
            }],
        };

        let json = serde_json::to_string(&request).unwrap();
        let deserialized: CommitTransactionRequest = serde_json::from_str(&json).unwrap();

        assert_eq!(request, deserialized);
    }

    #[test]
    fn test_append_files_has_action_field() {
        let data_file = DataFile::builder()
            .content(DataContentType::Data)
            .file_path("s3://bucket/file.parquet")
            .file_format(DataFileFormat::Parquet)
            .record_count(100)
            .file_size_in_bytes(5000)
            .partition(HashMap::new())
            .build()
            .unwrap();

        let request = CommitTransactionRequest {
            table_changes: vec![TableUpdate::AppendFiles {
                data_files: vec![data_file],
            }],
        };

        let json = serde_json::to_value(&request).unwrap();
        let action = json["table-changes"][0]["action"].as_str().unwrap();
        assert_eq!(action, "append-files");
    }
}
