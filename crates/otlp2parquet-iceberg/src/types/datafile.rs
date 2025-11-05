//! Iceberg DataFile types
//!
//! Minimal implementation of Iceberg DataFile for commit transactions.
//! Based on Iceberg Table Spec v2: https://iceberg.apache.org/spec/#manifests

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Iceberg data file format
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum DataFileFormat {
    /// Apache Avro format
    Avro,
    /// Apache ORC format
    Orc,
    /// Apache Parquet format
    Parquet,
}

/// Iceberg data content type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum DataContentType {
    /// Regular data records
    Data,
    /// Position delete records
    PositionDeletes,
    /// Equality delete records
    EqualityDeletes,
}

/// Iceberg DataFile descriptor
///
/// Describes a single data file committed to an Iceberg table.
/// Contains metadata and statistics extracted from Parquet files.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct DataFile {
    /// Type of content stored in the file
    pub content: DataContentType,

    /// Full path to the data file (e.g., "s3://bucket/path/file.parquet")
    pub file_path: String,

    /// File format
    pub file_format: DataFileFormat,

    /// Partition values (empty for unpartitioned tables)
    #[serde(default)]
    pub partition: HashMap<String, String>,

    /// Number of records in the file
    pub record_count: u64,

    /// Total file size in bytes
    pub file_size_in_bytes: u64,

    /// Map of field ID to column size in bytes
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub column_sizes: HashMap<i32, u64>,

    /// Map of field ID to value count
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub value_counts: HashMap<i32, u64>,

    /// Map of field ID to null value count
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub null_value_counts: HashMap<i32, u64>,

    /// Map of field ID to lower bound (serialized as base64)
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub lower_bounds: HashMap<i32, serde_bytes::ByteBuf>,

    /// Map of field ID to upper bound (serialized as base64)
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub upper_bounds: HashMap<i32, serde_bytes::ByteBuf>,

    /// Offsets of split points for the file (for distributed reads)
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub split_offsets: Vec<i64>,

    /// Partition spec ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_spec_id: Option<i32>,

    /// Sort order ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_order_id: Option<i32>,

    /// Equality field IDs (for equality deletes)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub equality_ids: Option<Vec<i32>>,
}

impl DataFile {
    /// Create a new DataFile builder
    pub fn builder() -> DataFileBuilder {
        DataFileBuilder::default()
    }
}

/// Builder for constructing DataFile instances
#[derive(Debug, Default)]
pub struct DataFileBuilder {
    content: Option<DataContentType>,
    file_path: Option<String>,
    file_format: Option<DataFileFormat>,
    partition: HashMap<String, String>,
    record_count: Option<u64>,
    file_size_in_bytes: Option<u64>,
    column_sizes: HashMap<i32, u64>,
    value_counts: HashMap<i32, u64>,
    null_value_counts: HashMap<i32, u64>,
    lower_bounds: HashMap<i32, serde_bytes::ByteBuf>,
    upper_bounds: HashMap<i32, serde_bytes::ByteBuf>,
    split_offsets: Vec<i64>,
    partition_spec_id: Option<i32>,
    sort_order_id: Option<i32>,
    equality_ids: Option<Vec<i32>>,
}

impl DataFileBuilder {
    pub fn content(mut self, content: DataContentType) -> Self {
        self.content = Some(content);
        self
    }

    pub fn file_path(mut self, path: impl Into<String>) -> Self {
        self.file_path = Some(path.into());
        self
    }

    pub fn file_format(mut self, format: DataFileFormat) -> Self {
        self.file_format = Some(format);
        self
    }

    pub fn partition(mut self, partition: HashMap<String, String>) -> Self {
        self.partition = partition;
        self
    }

    pub fn record_count(mut self, count: u64) -> Self {
        self.record_count = Some(count);
        self
    }

    pub fn file_size_in_bytes(mut self, size: u64) -> Self {
        self.file_size_in_bytes = Some(size);
        self
    }

    pub fn column_sizes(mut self, sizes: HashMap<i32, u64>) -> Self {
        self.column_sizes = sizes;
        self
    }

    pub fn value_counts(mut self, counts: HashMap<i32, u64>) -> Self {
        self.value_counts = counts;
        self
    }

    pub fn null_value_counts(mut self, counts: HashMap<i32, u64>) -> Self {
        self.null_value_counts = counts;
        self
    }

    pub fn lower_bounds(mut self, bounds: HashMap<i32, Vec<u8>>) -> Self {
        self.lower_bounds = bounds
            .into_iter()
            .map(|(k, v)| (k, serde_bytes::ByteBuf::from(v)))
            .collect();
        self
    }

    pub fn upper_bounds(mut self, bounds: HashMap<i32, Vec<u8>>) -> Self {
        self.upper_bounds = bounds
            .into_iter()
            .map(|(k, v)| (k, serde_bytes::ByteBuf::from(v)))
            .collect();
        self
    }

    pub fn split_offsets(mut self, offsets: Vec<i64>) -> Self {
        self.split_offsets = offsets;
        self
    }

    pub fn partition_spec_id(mut self, id: i32) -> Self {
        self.partition_spec_id = Some(id);
        self
    }

    pub fn sort_order_id(mut self, id: i32) -> Self {
        self.sort_order_id = Some(id);
        self
    }

    pub fn equality_ids(mut self, ids: Vec<i32>) -> Self {
        self.equality_ids = Some(ids);
        self
    }

    pub fn build(self) -> anyhow::Result<DataFile> {
        Ok(DataFile {
            content: self
                .content
                .ok_or_else(|| anyhow::anyhow!("content is required"))?,
            file_path: self
                .file_path
                .ok_or_else(|| anyhow::anyhow!("file_path is required"))?,
            file_format: self
                .file_format
                .ok_or_else(|| anyhow::anyhow!("file_format is required"))?,
            partition: self.partition,
            record_count: self
                .record_count
                .ok_or_else(|| anyhow::anyhow!("record_count is required"))?,
            file_size_in_bytes: self
                .file_size_in_bytes
                .ok_or_else(|| anyhow::anyhow!("file_size_in_bytes is required"))?,
            column_sizes: self.column_sizes,
            value_counts: self.value_counts,
            null_value_counts: self.null_value_counts,
            lower_bounds: self.lower_bounds,
            upper_bounds: self.upper_bounds,
            split_offsets: self.split_offsets,
            partition_spec_id: self.partition_spec_id,
            sort_order_id: self.sort_order_id,
            equality_ids: self.equality_ids,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_file_builder() {
        let data_file = DataFile::builder()
            .content(DataContentType::Data)
            .file_path("s3://bucket/path/file.parquet")
            .file_format(DataFileFormat::Parquet)
            .record_count(1000)
            .file_size_in_bytes(50000)
            .partition_spec_id(0)
            .build()
            .unwrap();

        assert_eq!(data_file.content, DataContentType::Data);
        assert_eq!(data_file.file_path, "s3://bucket/path/file.parquet");
        assert_eq!(data_file.file_format, DataFileFormat::Parquet);
        assert_eq!(data_file.record_count, 1000);
        assert_eq!(data_file.file_size_in_bytes, 50000);
    }

    #[test]
    fn test_data_file_serialization() {
        let mut column_sizes = HashMap::new();
        column_sizes.insert(1, 1000);
        column_sizes.insert(2, 2000);

        let data_file = DataFile::builder()
            .content(DataContentType::Data)
            .file_path("s3://bucket/file.parquet")
            .file_format(DataFileFormat::Parquet)
            .record_count(100)
            .file_size_in_bytes(5000)
            .column_sizes(column_sizes)
            .partition_spec_id(0)
            .build()
            .unwrap();

        let json = serde_json::to_string(&data_file).unwrap();
        let deserialized: DataFile = serde_json::from_str(&json).unwrap();

        assert_eq!(data_file, deserialized);
    }

    #[test]
    fn test_file_format_serialization() {
        let format = DataFileFormat::Parquet;
        let json = serde_json::to_string(&format).unwrap();
        assert_eq!(json, r#""PARQUET""#);

        let deserialized: DataFileFormat = serde_json::from_str(&json).unwrap();
        assert_eq!(format, deserialized);
    }

    #[test]
    fn test_content_type_serialization() {
        let content = DataContentType::Data;
        let json = serde_json::to_string(&content).unwrap();
        assert_eq!(json, r#""DATA""#);

        let deserialized: DataContentType = serde_json::from_str(&json).unwrap();
        assert_eq!(content, deserialized);
    }

    #[test]
    fn test_builder_missing_required_field() {
        let result = DataFile::builder()
            .file_path("s3://bucket/file.parquet")
            .file_format(DataFileFormat::Parquet)
            // Missing content and counts
            .build();

        assert!(result.is_err());
    }
}
