// Parquet writer with direct serialization to storage
//
// Serializes Arrow RecordBatches, computes a Blake3 content hash while
// encoding, and uploads the resulting Parquet bytes to OpenDAL storage.

use anyhow::{bail, Result};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use chrono::{DateTime, Utc};
use opendal::Operator;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::format::KeyValue;
use std::io::{self, Write};
use std::sync::{Arc, OnceLock};

#[cfg(target_arch = "wasm32")]
use parquet::basic::Compression;
#[cfg(not(target_arch = "wasm32"))]
use parquet::basic::{Compression, ZstdLevel};

const DEFAULT_ROW_GROUP_SIZE: usize = 32 * 1024;
static ROW_GROUP_SIZE: OnceLock<usize> = OnceLock::new();

/// Configure the global Parquet row group size used by Arrow writers.
///
/// Must be called before the first Parquet writer is created. Subsequent calls
/// are ignored to preserve the existing writer properties cache.
pub fn set_parquet_row_group_size(row_group_size: usize) {
    if row_group_size == 0 {
        return;
    }

    let _ = ROW_GROUP_SIZE.set(row_group_size);
}

fn configured_row_group_size() -> usize {
    ROW_GROUP_SIZE
        .get()
        .copied()
        .unwrap_or(DEFAULT_ROW_GROUP_SIZE)
}

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
/// - 32k rows per group by default (configurable)
/// - OTLP version metadata embedded in file
pub fn writer_properties() -> &'static WriterProperties {
    static PROPERTIES: OnceLock<WriterProperties> = OnceLock::new();
    PROPERTIES.get_or_init(|| {
        // Embed OTLP version and schema information in Parquet metadata
        let metadata = vec![
            KeyValue {
                key: "otlp.version".to_string(),
                value: Some("1.5.0".to_string()),
            },
            KeyValue {
                key: "otlp.protocol.version".to_string(),
                value: Some("v1".to_string()),
            },
            KeyValue {
                key: "otlp2parquet.version".to_string(),
                value: Some(env!("CARGO_PKG_VERSION").to_string()),
            },
            KeyValue {
                key: "schema.source".to_string(),
                value: Some("opentelemetry-collector-contrib/clickhouseexporter".to_string()),
            },
        ];

        WriterProperties::builder()
            .set_dictionary_enabled(true)
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_compression(compression_setting())
            .set_data_page_size_limit(256 * 1024) // 256 KiB data pages
            .set_write_batch_size(32 * 1024)
            .set_max_row_group_size(configured_row_group_size()) // Configurable row group size
            .set_dictionary_page_size_limit(128 * 1024)
            .set_key_value_metadata(Some(metadata))
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

/// Rich metadata describing a Parquet file persisted by this writer.
#[derive(Debug, Clone)]
pub struct ParquetWriteResult {
    pub path: String,
    pub hash: Blake3Hash,
    pub file_size: u64,
    pub row_count: i64,
    pub arrow_schema: SchemaRef,
    pub parquet_metadata: Arc<ParquetMetaData>,
    pub completed_at: DateTime<Utc>,
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
    ) -> Result<ParquetWriteResult> {
        self.write_batches_with_hash(std::slice::from_ref(batch), service_name, timestamp_nanos)
            .await
    }

    /// Write multiple batches to a single file and compute hash during encoding.
    pub async fn write_batches_with_hash(
        &self,
        batches: &[RecordBatch],
        service_name: &str,
        timestamp_nanos: i64,
    ) -> Result<ParquetWriteResult> {
        self.write_batches_with_signal(batches, service_name, timestamp_nanos, "logs", None)
            .await
    }

    /// Write batches with custom signal type and optional subdirectory
    ///
    /// # Arguments
    /// * `batches` - Arrow record batches to write
    /// * `service_name` - Service name for partitioning
    /// * `timestamp_nanos` - Timestamp for partitioning
    /// * `signal_type` - Signal type (logs, metrics, traces)
    /// * `subdirectory` - Optional subdirectory (e.g., metric type)
    pub async fn write_batches_with_signal(
        &self,
        batches: &[RecordBatch],
        service_name: &str,
        timestamp_nanos: i64,
        signal_type: &str,
        subdirectory: Option<&str>,
    ) -> Result<ParquetWriteResult> {
        if batches.is_empty() {
            bail!("Cannot write empty batch list");
        }

        let mut sink = HashingBuffer::new();
        let props = writer_properties().clone();
        let schema: SchemaRef = batches[0].schema();
        let metadata: ParquetMetaData = {
            let mut writer =
                parquet::arrow::ArrowWriter::try_new(&mut sink, schema.clone(), Some(props))?;

            for batch in batches {
                writer.write(batch)?;
            }
            writer.close()?
        };

        let (buffer, hash) = sink.finish();
        let file_size = buffer.len() as u64;
        let parquet_metadata = Arc::new(metadata);
        let row_count = parquet_metadata.file_metadata().num_rows();
        let completed_at = Utc::now();

        // Generate partition path with signal type
        let path = crate::partition::generate_partition_path_with_signal(
            signal_type,
            service_name,
            timestamp_nanos,
            &hash.to_hex(),
            subdirectory,
        );

        // Write to storage
        self.operator.write(&path, buffer).await?;

        Ok(ParquetWriteResult {
            path,
            hash,
            file_size,
            row_count,
            arrow_schema: schema,
            parquet_metadata,
            completed_at,
        })
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
        let result = writer
            .write_batch_with_hash(&batch, "test-service", 1_705_327_800_000_000_000)
            .await
            .unwrap();
        let path = result.path;
        let hash = result.hash;

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

        let result = writer
            .write_batches_with_hash(&[batch1, batch2], "test-service", 1_705_327_800_000_000_000)
            .await
            .unwrap();
        let path = result.path;

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

    #[test]
    fn test_metadata_embedded_in_parquet() {
        use bytes::Bytes;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let batch = create_test_batch();
        let mut buffer = Vec::new();

        // Write with our properties
        let props = writer_properties().clone();
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Read back and verify metadata
        let bytes = Bytes::from(buffer);
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        let file_metadata = reader.metadata().file_metadata();
        let kv_metadata = file_metadata.key_value_metadata();

        assert!(kv_metadata.is_some(), "Metadata should be present");
        let metadata = kv_metadata.unwrap();

        // Verify OTLP version metadata
        let otlp_version = metadata
            .iter()
            .find(|kv| kv.key == "otlp.version")
            .expect("otlp.version should be present");
        assert_eq!(
            otlp_version.value.as_ref().unwrap(),
            "1.5.0",
            "OTLP version should be 1.5.0"
        );

        // Verify protocol version
        let protocol_version = metadata
            .iter()
            .find(|kv| kv.key == "otlp.protocol.version")
            .expect("otlp.protocol.version should be present");
        assert_eq!(
            protocol_version.value.as_ref().unwrap(),
            "v1",
            "Protocol version should be v1"
        );

        // Verify schema source
        let schema_source = metadata
            .iter()
            .find(|kv| kv.key == "schema.source")
            .expect("schema.source should be present");
        assert_eq!(
            schema_source.value.as_ref().unwrap(),
            "opentelemetry-collector-contrib/clickhouseexporter",
            "Schema source should reference ClickHouse exporter"
        );

        // Verify otlp2parquet version exists
        let tool_version = metadata
            .iter()
            .find(|kv| kv.key == "otlp2parquet.version")
            .expect("otlp2parquet.version should be present");
        assert!(
            tool_version.value.is_some(),
            "Tool version should have a value"
        );
    }
}
