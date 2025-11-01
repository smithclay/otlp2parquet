// Parquet writer with direct serialization to storage
//
// Serializes Arrow RecordBatches, computes a Blake3 content hash while
// encoding, and uploads the resulting Parquet bytes to OpenDAL storage.

use anyhow::{bail, Result};
use arrow::array::RecordBatch;
use opendal::Operator;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use std::io::{self, Write};
use std::sync::OnceLock;

#[cfg(target_arch = "wasm32")]
use parquet::basic::Compression;
#[cfg(not(target_arch = "wasm32"))]
use parquet::basic::{Compression, ZstdLevel};

struct HashingBuffer {
    buffer: Vec<u8>,
    hasher: blake3::Hasher,
}

impl HashingBuffer {
    fn new() -> Self {
        Self {
            buffer: Vec::new(),
            hasher: blake3::Hasher::new(),
        }
    }

    fn finish(self) -> (Vec<u8>, Blake3Hash) {
        let hash = self.hasher.finalize();
        (self.buffer, Blake3Hash::new(*hash.as_bytes()))
    }
}

impl Write for HashingBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.hasher.update(buf);
        self.buffer.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Platform-specific compression setting
#[cfg(target_arch = "wasm32")]
fn compression_setting() -> Compression {
    Compression::SNAPPY
}

#[cfg(not(target_arch = "wasm32"))]
fn compression_setting() -> Compression {
    let level = ZstdLevel::try_new(2).unwrap_or_default();
    Compression::ZSTD(level)
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
    /// 1. Serializes the batches to Parquet bytes (in memory, for hashing)
    /// 2. Computes the Blake3 hash while encoding (no second pass)
    /// 3. Generates partition path using the hash
    /// 4. Writes the bytes to storage
    ///
    /// Returns the partition path and content hash.
    pub async fn write_batch_with_hash(
        &self,
        batch: &RecordBatch,
        service_name: &str,
        timestamp_nanos: i64,
    ) -> Result<(String, Blake3Hash)> {
        self.write_batches_with_hash(std::slice::from_ref(batch), service_name, timestamp_nanos)
            .await
    }

    /// Write multiple batches to a single file and compute hash during encoding.
    pub async fn write_batches_with_hash(
        &self,
        batches: &[RecordBatch],
        service_name: &str,
        timestamp_nanos: i64,
    ) -> Result<(String, Blake3Hash)> {
        if batches.is_empty() {
            bail!("Cannot write empty batch list");
        }

        let mut sink = HashingBuffer::new();
        let props = writer_properties().clone();
        {
            let mut writer =
                parquet::arrow::ArrowWriter::try_new(&mut sink, batches[0].schema(), Some(props))?;

            for batch in batches {
                writer.write(batch)?;
            }
            writer.close()?;
        }

        let (buffer, hash) = sink.finish();

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

/// Synchronous helper for benchmarking: write batches to in-memory buffer
/// without async/storage overhead
pub fn write_batches_to_parquet<W: Write + Send>(
    mut writer: W,
    batches: Vec<RecordBatch>,
) -> Result<()> {
    if batches.is_empty() {
        bail!("Cannot write empty batch list");
    }

    let props = writer_properties().clone();
    let mut arrow_writer =
        parquet::arrow::ArrowWriter::try_new(&mut writer, batches[0].schema(), Some(props))?;

    for batch in batches {
        arrow_writer.write(&batch)?;
    }
    arrow_writer.close()?;
    Ok(())
}

/// Synchronous helper for benchmarking: write batches to buffer and compute hash
pub fn write_batches_with_hash(batches: Vec<RecordBatch>) -> Result<(Vec<u8>, Blake3Hash)> {
    if batches.is_empty() {
        bail!("Cannot write empty batch list");
    }

    let mut sink = HashingBuffer::new();
    let props = writer_properties().clone();
    {
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(&mut sink, batches[0].schema(), Some(props))?;

        for batch in batches {
            writer.write(&batch)?;
        }
        writer.close()?;
    }

    Ok(sink.finish())
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
    async fn test_write_batches_with_hash() {
        let op = Operator::new(services::Memory::default()).unwrap().finish();
        let writer = ParquetWriter::new(op.clone());

        let batch1 = create_test_batch();
        let batch2 = create_test_batch();

        let (path, _hash) = writer
            .write_batches_with_hash(&[batch1, batch2], "test-service", 1_705_327_800_000_000_000)
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
