//! Iceberg table metadata types
//!
//! Minimal implementation of Iceberg table metadata for REST API compatibility.
//! Based on Iceberg Table Spec v2: <https://iceberg.apache.org/spec/#table-metadata>

use super::schema::Schema;
use serde::{Deserialize, Serialize};

/// Iceberg table metadata response
///
/// Returned by the REST API when loading a table.
/// Contains schema, partition specs, sort orders, and snapshot information.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct TableMetadata {
    /// Table format version (1 or 2)
    pub format_version: i32,

    /// Unique table identifier (UUID)
    pub table_uuid: String,

    /// Base location for the table's metadata and data files
    pub location: String,

    /// ID of the current schema
    pub current_schema_id: i32,

    /// List of schemas, current schema is identified by current-schema-id
    pub schemas: Vec<Schema>,

    /// ID of the table's current partition spec
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_spec_id: Option<i32>,

    /// List of partition specs
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_specs: Option<Vec<serde_json::Value>>,

    /// ID of the current snapshot, or None if no snapshots
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_snapshot_id: Option<i64>,

    /// List of snapshots
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshots: Option<Vec<serde_json::Value>>,

    /// Timestamp of the most recent change (milliseconds since epoch)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_updated_ms: Option<i64>,

    /// Table properties
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<std::collections::HashMap<String, String>>,

    /// Default sort order ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_sort_order_id: Option<i32>,

    /// List of sort orders
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_orders: Option<Vec<serde_json::Value>>,

    /// Last assigned column ID (AWS S3 Tables field)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_column_id: Option<i32>,

    /// Last assigned partition ID (AWS S3 Tables field)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_partition_id: Option<i32>,

    /// Last assigned sequence number (Iceberg v2 field)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_sequence_number: Option<i64>,

    /// Metadata log (history of metadata file locations)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_log: Option<Vec<serde_json::Value>>,

    /// Partition statistics
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_statistics: Option<Vec<serde_json::Value>>,

    /// Snapshot references (branches/tags)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refs: Option<serde_json::Value>,

    /// Snapshot log (history of snapshot changes)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_log: Option<Vec<serde_json::Value>>,

    /// Table statistics
    #[serde(skip_serializing_if = "Option::is_none")]
    pub statistics: Option<Vec<serde_json::Value>>,
}

impl TableMetadata {
    /// Get the current schema
    pub fn current_schema(&self) -> Option<&Schema> {
        self.schemas
            .iter()
            .find(|s| s.schema_id == self.current_schema_id)
    }

    /// Get schema by ID
    pub fn schema_by_id(&self, schema_id: i32) -> Option<&Schema> {
        self.schemas.iter().find(|s| s.schema_id == schema_id)
    }
}

/// Response from loading a table via REST API
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct LoadTableResponse {
    /// Table metadata
    #[serde(rename = "metadata")]
    pub table_metadata: TableMetadata,

    /// Metadata location (S3/GCS/etc path to metadata.json)
    pub metadata_location: String,

    /// Configuration properties (AWS S3 Tables returns structured config)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<serde_json::Value>,
}

/// Response from catalog config endpoint (GET /v1/config)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CatalogConfig {
    /// Default configuration values
    pub defaults: std::collections::HashMap<String, String>,

    /// Configuration overrides
    #[serde(skip_serializing_if = "Option::is_none")]
    pub overrides: Option<std::collections::HashMap<String, String>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iceberg::types::schema::{NestedField, Schema, Type};

    #[test]
    fn test_table_metadata_current_schema() {
        let schema1 = Schema {
            schema_id: 0,
            fields: vec![NestedField {
                id: 1,
                name: "id".to_string(),
                required: true,
                field_type: Type::Primitive("long".to_string()),
                doc: None,
            }],
            identifier_field_ids: None,
        };

        let schema2 = Schema {
            schema_id: 1,
            fields: vec![NestedField {
                id: 1,
                name: "id".to_string(),
                required: true,
                field_type: Type::Primitive("long".to_string()),
                doc: None,
            }],
            identifier_field_ids: None,
        };

        let metadata = TableMetadata {
            format_version: 2,
            table_uuid: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            location: "s3://bucket/warehouse/db/table".to_string(),
            current_schema_id: 1,
            schemas: vec![schema1, schema2.clone()],
            default_spec_id: Some(0),
            partition_specs: None,
            current_snapshot_id: None,
            snapshots: None,
            last_updated_ms: None,
            properties: None,
            default_sort_order_id: None,
            sort_orders: None,
            last_column_id: None,
            last_partition_id: None,
            last_sequence_number: None,
            metadata_log: None,
            partition_statistics: None,
            refs: None,
            snapshot_log: None,
            statistics: None,
        };

        let current = metadata.current_schema().unwrap();
        assert_eq!(current.schema_id, 1);
        assert_eq!(current, &schema2);
    }

    #[test]
    fn test_table_metadata_serialization() {
        let metadata = TableMetadata {
            format_version: 2,
            table_uuid: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            location: "s3://bucket/warehouse/db/table".to_string(),
            current_schema_id: 0,
            schemas: vec![Schema {
                schema_id: 0,
                fields: vec![],
                identifier_field_ids: None,
            }],
            default_spec_id: Some(0),
            partition_specs: None,
            current_snapshot_id: None,
            snapshots: None,
            last_updated_ms: Some(1609459200000),
            properties: None,
            default_sort_order_id: None,
            sort_orders: None,
            last_column_id: None,
            last_partition_id: None,
            last_sequence_number: None,
            metadata_log: None,
            partition_statistics: None,
            refs: None,
            snapshot_log: None,
            statistics: None,
        };

        let json = serde_json::to_string(&metadata).unwrap();
        let deserialized: TableMetadata = serde_json::from_str(&json).unwrap();

        assert_eq!(metadata, deserialized);
    }

    #[test]
    fn test_load_table_response_serialization() {
        let response = LoadTableResponse {
            table_metadata: TableMetadata {
                format_version: 2,
                table_uuid: "550e8400-e29b-41d4-a716-446655440000".to_string(),
                location: "s3://bucket/warehouse/db/table".to_string(),
                current_schema_id: 0,
                schemas: vec![Schema {
                    schema_id: 0,
                    fields: vec![],
                    identifier_field_ids: None,
                }],
                default_spec_id: None,
                partition_specs: None,
                current_snapshot_id: None,
                snapshots: None,
                last_updated_ms: None,
                properties: None,
                default_sort_order_id: None,
                sort_orders: None,
                last_column_id: None,
                last_partition_id: None,
                last_sequence_number: None,
                metadata_log: None,
                partition_statistics: None,
                refs: None,
                snapshot_log: None,
                statistics: None,
            },
            metadata_location: "s3://bucket/warehouse/db/table/metadata/v1.metadata.json"
                .to_string(),
            config: None,
        };

        let json = serde_json::to_string(&response).unwrap();
        let deserialized: LoadTableResponse = serde_json::from_str(&json).unwrap();

        assert_eq!(response, deserialized);
    }
}
