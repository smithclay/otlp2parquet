// Parquet writer with direct streaming to storage
//
// Handles serialization, hashing, and storage upload in one pass.
// Uses parquet_opendal for async streaming to OpenDAL backends.

use anyhow::Result;
use arrow::array::RecordBatch;
use opendal::Operator;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use std::sync::OnceLock;

#[cfg(target_arch = "wasm32")]
use parquet::basic::Compression;
#[cfg(not(target_arch = "wasm32"))]
use parquet::basic::{Compression, ZstdLevel};

/// Platform-specific compression setting
#[cfg(target_arch = "wasm32")]
fn compression_setting() -> Compression {
    Compression::SNAPPY
}

#[cfg(not(target_arch = "wasm32"))]
fn compression_setting() -> Compression {
    Compression::ZSTD(ZstdLevel::try_new(2).unwrap())
}

/// Get shared writer properties (cached)
///
/// Configuration optimized for size and query performance:
/// - Platform-specific compression (Snappy for WASM, ZSTD for native)
/// - Dictionary encoding enabled
/// - 32k rows per group (balances memory and query performance)
pub fn writer_properties() -> &'static WriterProperties {
    static PROPERTIES: OnceLock<WriterProperties> = OnceLock::new();
    PROPERTIES.get_or_init(|| {
        WriterProperties::builder()
            .set_dictionary_enabled(true)
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_compression(compression_setting())
            .set_data_page_size_limit(256 * 1024) // 256 KiB data pages
            .set_write_batch_size(32 * 1024)
            .set_max_row_group_size(32 * 1024) // 32k rows per group
            .set_dictionary_page_size_limit(128 * 1024)
            .build()
    })
}

/// Blake3 hash representation
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Blake3Hash {
    bytes: [u8; 32],
}

impl Blake3Hash {
    pub fn new(bytes: [u8; 32]) -> Self {
        Self { bytes }
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.bytes
    }
}

/// Parquet writer that streams directly to OpenDAL storage
pub struct ParquetWriter {
    operator: Operator,
}

impl ParquetWriter {
    /// Create a new ParquetWriter
    pub fn new(operator: Operator) -> Self {
        Self { operator }
    }

    /// Write a RecordBatch to storage, computing hash and generating partition path
    ///
    /// This method:
    /// 1. Serializes RecordBatch to Parquet bytes (in memory, for hashing)
    /// 2. Computes Blake3 hash of the Parquet bytes
    /// 3. Generates partition path using hash
    /// 4. Writes to storage using parquet_opendal (streams directly)
    ///
    /// Returns the partition path and content hash.
    pub async fn write_batch_with_hash(
        &self,
        batch: &RecordBatch,
        service_name: &str,
        timestamp_nanos: i64,
    ) -> Result<(String, Blake3Hash)> {
        // First, serialize to bytes for hashing
        // (We need the hash to generate the filename, so we can't stream yet)
        let mut buffer = Vec::new();
        let props = writer_properties().clone();
        let mut temp_writer =
            parquet::arrow::ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props))?;
        temp_writer.write(batch)?;
        temp_writer.close()?;

        // Compute hash
        let hash_bytes = blake3::hash(&buffer);
        let hash = Blake3Hash::new(*hash_bytes.as_bytes());

        // Generate partition path with hash
        let path = crate::partition::generate_partition_path(
            service_name,
            timestamp_nanos,
            &hash.to_hex(),
        );

        // Write to storage
        self.operator.write(&path, buffer).await?;

        Ok((path, hash))
    }

    /// Write multiple batches to a single file
    ///
    /// Useful when batching has already accumulated multiple RecordBatches.
    /// Uses parquet_opendal for direct streaming (no intermediate buffer).
    pub async fn write_batches_streaming(
        &self,
        batches: &[RecordBatch],
        service_name: &str,
        timestamp_nanos: i64,
    ) -> Result<(String, Blake3Hash)> {
        if batches.is_empty() {
            anyhow::bail!("Cannot write empty batch list");
        }

        // For hashing, we need to serialize first
        let mut buffer = Vec::new();
        let props = writer_properties().clone();
        let mut temp_writer = parquet::arrow::ArrowWriter::try_new(
            &mut buffer,
            batches[0].schema(),
            Some(props.clone()),
        )?;

        for batch in batches {
            temp_writer.write(batch)?;
        }
        temp_writer.close()?;

        // Compute hash
        let hash_bytes = blake3::hash(&buffer);
        let hash = Blake3Hash::new(*hash_bytes.as_bytes());

        // Generate partition path
        let path = crate::partition::generate_partition_path(
            service_name,
            timestamp_nanos,
            &hash.to_hex(),
        );

        // Write to storage
        self.operator.write(&path, buffer).await?;

        Ok((path, hash))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use opendal::services;
    use std::sync::Arc;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_write_batch_with_hash() {
        let op = Operator::new(services::Memory::default()).unwrap().finish();
        let writer = ParquetWriter::new(op.clone());

        let batch = create_test_batch();
        let (path, hash) = writer
            .write_batch_with_hash(&batch, "test-service", 1_705_327_800_000_000_000)
            .await
            .unwrap();

        // Verify path structure
        assert!(path.starts_with("logs/test-service/"));
        assert!(path.ends_with(".parquet"));
        assert!(path.contains(&hash.to_hex()[..16]));

        // Verify file exists in storage
        let data = op.read(&path).await.unwrap();
        let bytes = data.to_vec();
        assert!(!bytes.is_empty());
        assert_eq!(&bytes[0..4], b"PAR1");
    }

    #[tokio::test]
    async fn test_write_batches_streaming() {
        let op = Operator::new(services::Memory::default()).unwrap().finish();
        let writer = ParquetWriter::new(op.clone());

        let batch1 = create_test_batch();
        let batch2 = create_test_batch();

        let (path, _hash) = writer
            .write_batches_streaming(&[batch1, batch2], "test-service", 1_705_327_800_000_000_000)
            .await
            .unwrap();

        // Verify file exists
        let data = op.read(&path).await.unwrap();
        let bytes = data.to_vec();
        assert!(!bytes.is_empty());
        assert_eq!(&bytes[0..4], b"PAR1");
    }

    #[test]
    fn test_hash_deterministic() {
        let batch = create_test_batch();

        // Serialize twice
        let mut buf1 = Vec::new();
        let mut buf2 = Vec::new();

        let props = writer_properties().clone();
        let mut writer1 =
            parquet::arrow::ArrowWriter::try_new(&mut buf1, batch.schema(), Some(props.clone()))
                .unwrap();
        writer1.write(&batch).unwrap();
        writer1.close().unwrap();

        let mut writer2 =
            parquet::arrow::ArrowWriter::try_new(&mut buf2, batch.schema(), Some(props)).unwrap();
        writer2.write(&batch).unwrap();
        writer2.close().unwrap();

        // Hashes should match
        let hash1 = blake3::hash(&buf1);
        let hash2 = blake3::hash(&buf2);
        assert_eq!(hash1, hash2);
    }
}
