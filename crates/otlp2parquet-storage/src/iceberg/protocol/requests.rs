//! Iceberg REST API request types
//!
//! Based on: <https://iceberg.apache.org/docs/latest/rest-api/>

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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
    /// Add a new snapshot to the table
    #[serde(rename_all = "kebab-case")]
    AddSnapshot {
        /// Snapshot to add
        snapshot: Snapshot,
    },
    /// Set a snapshot reference (branch or tag)
    #[serde(rename_all = "kebab-case")]
    SetSnapshotRef {
        /// Reference name (e.g., "main")
        #[serde(rename = "ref-name")]
        ref_name: String,
        /// Snapshot ID to set
        #[serde(rename = "snapshot-id")]
        snapshot_id: i64,
        /// Reference type
        #[serde(rename = "type")]
        ref_type: String,
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

/// Request to update an Iceberg table (via POST /v1/{prefix}/namespaces/{namespace}/tables/{table})
///
/// Used for manifest-based commits with optimistic concurrency control.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CommitTableRequest {
    /// Requirements that must be met for the update to succeed
    pub requirements: Vec<TableRequirement>,
    /// Updates to apply to the table
    pub updates: Vec<TableUpdate>,
}

/// Table requirements for optimistic concurrency control
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum TableRequirement {
    /// Assert that a reference points to a specific snapshot
    AssertRefSnapshotId {
        /// Reference name (e.g., "main")
        #[serde(rename = "ref")]
        ref_name: String,
        /// Expected snapshot ID (None means reference should not exist)
        #[serde(rename = "snapshot-id")]
        snapshot_id: Option<i64>,
    },
}

/// Iceberg snapshot metadata
///
/// Represents a point-in-time state of the table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Snapshot {
    /// Unique snapshot identifier
    pub snapshot_id: i64,
    /// Parent snapshot ID (None for first snapshot)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_snapshot_id: Option<i64>,
    /// Timestamp in milliseconds since epoch
    pub timestamp_ms: i64,
    /// Path to the manifest list file
    pub manifest_list: String,
    /// Summary metadata (e.g., operation type)
    pub summary: HashMap<String, String>,
    /// Sequence number (optional, for Iceberg v2)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequence_number: Option<i64>,
    /// Schema ID at the time of snapshot (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_id: Option<i32>,
}
