//! Iceberg manifest file generation
//!
//! Implements Avro-based manifest and manifest-list file generation for Iceberg table commits.
//! Based on Iceberg Table Spec v2: <https://iceberg.apache.org/spec/#manifests>

#[cfg(not(target_arch = "wasm32"))]
use anyhow::{Context, Result};
#[cfg(not(target_arch = "wasm32"))]
use apache_avro::{types::Value as AvroValue, Schema as AvroSchema, Writer as AvroWriter};
#[cfg(not(target_arch = "wasm32"))]
use opendal::Operator;
#[cfg(not(target_arch = "wasm32"))]
use std::collections::HashMap;
#[cfg(not(target_arch = "wasm32"))]
use uuid::Uuid;

#[cfg(not(target_arch = "wasm32"))]
use crate::types::datafile::DataFile;

/// Avro schema for Iceberg manifest entry
/// Based on: https://iceberg.apache.org/spec/#manifests
#[cfg(not(target_arch = "wasm32"))]
const MANIFEST_ENTRY_SCHEMA: &str = r#"{
  "type": "record",
  "name": "manifest_entry",
  "fields": [
    {"name": "status", "type": "int"},
    {"name": "snapshot_id", "type": ["null", "long"], "default": null},
    {"name": "data_file", "type": {
      "type": "record",
      "name": "r2",
      "fields": [
        {"name": "file_path", "type": "string"},
        {"name": "file_format", "type": "string"},
        {"name": "partition", "type": {"type": "map", "values": "string"}},
        {"name": "record_count", "type": "long"},
        {"name": "file_size_in_bytes", "type": "long"},
        {"name": "column_sizes", "type": ["null", {"type": "map", "values": "long"}], "default": null},
        {"name": "value_counts", "type": ["null", {"type": "map", "values": "long"}], "default": null},
        {"name": "null_value_counts", "type": ["null", {"type": "map", "values": "long"}], "default": null},
        {"name": "nan_value_counts", "type": ["null", {"type": "map", "values": "long"}], "default": null},
        {"name": "lower_bounds", "type": ["null", {"type": "map", "values": "bytes"}], "default": null},
        {"name": "upper_bounds", "type": ["null", {"type": "map", "values": "bytes"}], "default": null}
      ]
    }}
  ]
}"#;

/// Avro schema for Iceberg manifest-list entry
/// Based on: https://iceberg.apache.org/spec/#manifests
#[cfg(not(target_arch = "wasm32"))]
const MANIFEST_LIST_SCHEMA: &str = r#"{
  "type": "record",
  "name": "manifest_file",
  "fields": [
    {"name": "manifest_path", "type": "string"},
    {"name": "manifest_length", "type": "long"},
    {"name": "partition_spec_id", "type": "int"},
    {"name": "added_snapshot_id", "type": "long"},
    {"name": "added_data_files_count", "type": "int"},
    {"name": "added_rows_count", "type": "long"}
  ]
}"#;

/// Writes Iceberg manifest files in Avro format
#[cfg(not(target_arch = "wasm32"))]
pub struct ManifestWriter;

#[cfg(not(target_arch = "wasm32"))]
impl ManifestWriter {
    /// Write manifest file to storage
    ///
    /// # Arguments
    /// * `snapshot_id` - Snapshot ID that added these files
    /// * `data_files` - Data files to include in manifest
    /// * `storage` - OpenDAL operator for writing
    /// * `table_location` - Base location of the table
    ///
    /// # Returns
    /// Full path to the written manifest file
    pub async fn write(
        snapshot_id: i64,
        data_files: &[DataFile],
        storage: &Operator,
        table_location: &str,
    ) -> Result<String> {
        // Generate manifest filename: {table_location}/metadata/{uuid}-m0.avro
        let manifest_uuid = Uuid::new_v4();
        let manifest_filename = format!("{}-m0.avro", manifest_uuid);
        let manifest_path = format!("{}/metadata/{}", table_location, manifest_filename);

        // Parse Avro schema
        let schema = AvroSchema::parse_str(MANIFEST_ENTRY_SCHEMA)
            .context("failed to parse manifest entry schema")?;

        // Create Avro writer
        let mut writer = AvroWriter::new(&schema, Vec::new());

        // Convert each DataFile to manifest entry and write
        for data_file in data_files {
            let entry = Self::data_file_to_avro(data_file, snapshot_id)?;
            writer
                .append(entry)
                .context("failed to append manifest entry")?;
        }

        // Flush and get bytes
        let manifest_bytes = writer.into_inner().context("failed to finalize manifest")?;

        // Write to storage
        storage
            .write(&manifest_path, manifest_bytes)
            .await
            .context("failed to write manifest to storage")?;

        Ok(manifest_path)
    }

    /// Convert DataFile to Avro manifest entry
    fn data_file_to_avro(data_file: &DataFile, snapshot_id: i64) -> Result<AvroValue> {
        // Convert partition map
        let partition_map: HashMap<String, AvroValue> = data_file
            .partition
            .iter()
            .map(|(k, v)| (k.clone(), AvroValue::String(v.clone())))
            .collect();

        // Convert column_sizes (i32 -> String keys for Avro map)
        let column_sizes = if data_file.column_sizes.is_empty() {
            AvroValue::Union(0, Box::new(AvroValue::Null))
        } else {
            let map: HashMap<String, AvroValue> = data_file
                .column_sizes
                .iter()
                .map(|(k, v)| (k.to_string(), AvroValue::Long(*v as i64)))
                .collect();
            AvroValue::Union(1, Box::new(AvroValue::Map(map)))
        };

        // Convert value_counts
        let value_counts = if data_file.value_counts.is_empty() {
            AvroValue::Union(0, Box::new(AvroValue::Null))
        } else {
            let map: HashMap<String, AvroValue> = data_file
                .value_counts
                .iter()
                .map(|(k, v)| (k.to_string(), AvroValue::Long(*v as i64)))
                .collect();
            AvroValue::Union(1, Box::new(AvroValue::Map(map)))
        };

        // Convert null_value_counts
        let null_value_counts = if data_file.null_value_counts.is_empty() {
            AvroValue::Union(0, Box::new(AvroValue::Null))
        } else {
            let map: HashMap<String, AvroValue> = data_file
                .null_value_counts
                .iter()
                .map(|(k, v)| (k.to_string(), AvroValue::Long(*v as i64)))
                .collect();
            AvroValue::Union(1, Box::new(AvroValue::Map(map)))
        };

        // NAN counts - always null for Parquet (not tracked)
        let nan_value_counts = AvroValue::Union(0, Box::new(AvroValue::Null));

        // Convert lower_bounds (bytes)
        let lower_bounds = if data_file.lower_bounds.is_empty() {
            AvroValue::Union(0, Box::new(AvroValue::Null))
        } else {
            let map: HashMap<String, AvroValue> = data_file
                .lower_bounds
                .iter()
                .map(|(k, v)| (k.to_string(), AvroValue::Bytes(v.to_vec())))
                .collect();
            AvroValue::Union(1, Box::new(AvroValue::Map(map)))
        };

        // Convert upper_bounds (bytes)
        let upper_bounds = if data_file.upper_bounds.is_empty() {
            AvroValue::Union(0, Box::new(AvroValue::Null))
        } else {
            let map: HashMap<String, AvroValue> = data_file
                .upper_bounds
                .iter()
                .map(|(k, v)| (k.to_string(), AvroValue::Bytes(v.to_vec())))
                .collect();
            AvroValue::Union(1, Box::new(AvroValue::Map(map)))
        };

        // Build data_file record
        let data_file_record = AvroValue::Record(vec![
            (
                "file_path".to_string(),
                AvroValue::String(data_file.file_path.clone()),
            ),
            (
                "file_format".to_string(),
                AvroValue::String(format!("{:?}", data_file.file_format).to_uppercase()),
            ),
            ("partition".to_string(), AvroValue::Map(partition_map)),
            (
                "record_count".to_string(),
                AvroValue::Long(data_file.record_count as i64),
            ),
            (
                "file_size_in_bytes".to_string(),
                AvroValue::Long(data_file.file_size_in_bytes as i64),
            ),
            ("column_sizes".to_string(), column_sizes),
            ("value_counts".to_string(), value_counts),
            ("null_value_counts".to_string(), null_value_counts),
            ("nan_value_counts".to_string(), nan_value_counts),
            ("lower_bounds".to_string(), lower_bounds),
            ("upper_bounds".to_string(), upper_bounds),
        ]);

        // Build manifest entry record
        let entry = AvroValue::Record(vec![
            ("status".to_string(), AvroValue::Int(1)), // 1 = ADDED
            (
                "snapshot_id".to_string(),
                AvroValue::Union(1, Box::new(AvroValue::Long(snapshot_id))),
            ),
            ("data_file".to_string(), data_file_record),
        ]);

        Ok(entry)
    }
}

/// Writes Iceberg manifest-list files in Avro format
#[cfg(not(target_arch = "wasm32"))]
pub struct ManifestListWriter;

#[cfg(not(target_arch = "wasm32"))]
impl ManifestListWriter {
    /// Write manifest-list file to storage
    ///
    /// # Arguments
    /// * `snapshot_id` - Snapshot ID for this manifest list
    /// * `manifest_path` - Path to the manifest file
    /// * `manifest_length` - Size of the manifest file in bytes
    /// * `data_files` - Data files referenced by the manifest (for statistics)
    /// * `storage` - OpenDAL operator for writing
    /// * `table_location` - Base location of the table
    ///
    /// # Returns
    /// Full path to the written manifest-list file
    pub async fn write(
        snapshot_id: i64,
        manifest_path: &str,
        manifest_length: i64,
        data_files: &[DataFile],
        storage: &Operator,
        table_location: &str,
    ) -> Result<String> {
        // Generate manifest-list filename: {table_location}/metadata/snap-{snapshot_id}-1-{uuid}.avro
        let list_uuid = Uuid::new_v4();
        let manifest_list_filename = format!("snap-{}-1-{}.avro", snapshot_id, list_uuid);
        let manifest_list_path = format!("{}/metadata/{}", table_location, manifest_list_filename);

        // Parse Avro schema
        let schema = AvroSchema::parse_str(MANIFEST_LIST_SCHEMA)
            .context("failed to parse manifest-list schema")?;

        // Create Avro writer
        let mut writer = AvroWriter::new(&schema, Vec::new());

        // Calculate statistics
        let added_data_files_count = data_files.len() as i32;
        let added_rows_count: i64 = data_files.iter().map(|f| f.record_count as i64).sum();

        // Build manifest-list entry
        let entry = AvroValue::Record(vec![
            (
                "manifest_path".to_string(),
                AvroValue::String(manifest_path.to_string()),
            ),
            (
                "manifest_length".to_string(),
                AvroValue::Long(manifest_length),
            ),
            ("partition_spec_id".to_string(), AvroValue::Int(0)), // 0 for unpartitioned
            (
                "added_snapshot_id".to_string(),
                AvroValue::Long(snapshot_id),
            ),
            (
                "added_data_files_count".to_string(),
                AvroValue::Int(added_data_files_count),
            ),
            (
                "added_rows_count".to_string(),
                AvroValue::Long(added_rows_count),
            ),
        ]);

        writer
            .append(entry)
            .context("failed to append manifest-list entry")?;

        // Flush and get bytes
        let manifest_list_bytes = writer
            .into_inner()
            .context("failed to finalize manifest-list")?;

        // Write to storage
        storage
            .write(&manifest_list_path, manifest_list_bytes)
            .await
            .context("failed to write manifest-list to storage")?;

        Ok(manifest_list_path)
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::types::datafile::{DataContentType, DataFileFormat};

    #[test]
    fn test_data_file_to_avro_conversion() {
        let data_file = DataFile::builder()
            .content(DataContentType::Data)
            .file_path("s3://bucket/path/file.parquet")
            .file_format(DataFileFormat::Parquet)
            .record_count(1000)
            .file_size_in_bytes(50000)
            .build()
            .unwrap();

        let snapshot_id = 123456789;
        let avro_value = ManifestWriter::data_file_to_avro(&data_file, snapshot_id).unwrap();

        // Verify it's a record
        match avro_value {
            AvroValue::Record(fields) => {
                assert_eq!(fields.len(), 3);
                // Check status
                assert_eq!(fields[0].0, "status");
                assert_eq!(fields[0].1, AvroValue::Int(1));
                // Check snapshot_id
                assert_eq!(fields[1].0, "snapshot_id");
            }
            _ => panic!("Expected Record"),
        }
    }

    #[tokio::test]
    async fn test_manifest_writer() {
        use opendal::services::Memory;
        use opendal::Operator;

        // Create in-memory storage
        let storage = Operator::new(Memory::default()).unwrap().finish();

        let data_file = DataFile::builder()
            .content(DataContentType::Data)
            .file_path("s3://bucket/data/file.parquet")
            .file_format(DataFileFormat::Parquet)
            .record_count(500)
            .file_size_in_bytes(25000)
            .build()
            .unwrap();

        let snapshot_id = 987654321;
        let manifest_path = ManifestWriter::write(
            snapshot_id,
            &[data_file],
            &storage,
            "s3://bucket/warehouse/db.table",
        )
        .await
        .unwrap();

        // Verify manifest was written
        assert!(manifest_path.contains("/metadata/"));
        assert!(manifest_path.ends_with("-m0.avro"));

        // Verify file exists in storage
        let exists = storage.exists(&manifest_path).await.unwrap();
        assert!(exists);
    }

    #[tokio::test]
    async fn test_manifest_list_writer() {
        use opendal::services::Memory;
        use opendal::Operator;

        // Create in-memory storage
        let storage = Operator::new(Memory::default()).unwrap().finish();

        let data_file = DataFile::builder()
            .content(DataContentType::Data)
            .file_path("s3://bucket/data/file.parquet")
            .file_format(DataFileFormat::Parquet)
            .record_count(750)
            .file_size_in_bytes(37500)
            .build()
            .unwrap();

        let snapshot_id = 111222333;
        let manifest_path = "s3://bucket/warehouse/db.table/metadata/abc-m0.avro";
        let manifest_length = 5000;

        let manifest_list_path = ManifestListWriter::write(
            snapshot_id,
            manifest_path,
            manifest_length,
            &[data_file],
            &storage,
            "s3://bucket/warehouse/db.table",
        )
        .await
        .unwrap();

        // Verify manifest-list was written
        assert!(manifest_list_path.contains("/metadata/"));
        assert!(manifest_list_path.contains(&format!("snap-{}", snapshot_id)));
        assert!(manifest_list_path.ends_with(".avro"));

        // Verify file exists in storage
        let exists = storage.exists(&manifest_list_path).await.unwrap();
        assert!(exists);
    }
}
