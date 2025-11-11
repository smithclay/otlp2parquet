// Parquet writer with direct serialization to storage
//
// Serializes Arrow RecordBatches, computes a Blake3 content hash while
// encoding, and uploads the resulting Parquet bytes to OpenDAL storage.

use anyhow::{bail, Context, Result};
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use opendal::Operator;
use otlp2parquet_core::parquet::{encode_record_batches, writer_properties};
use otlp2parquet_core::{Blake3Hash, ParquetWriteResult};
use std::io::Write;

/// Parquet writer that streams directly to OpenDAL storage
pub struct ParquetWriter {
    operator: Operator,
}

impl ParquetWriter {
    /// Create a new ParquetWriter
    pub fn new(operator: Operator) -> Self {
        Self { operator }
    }

    /// Get reference to the underlying OpenDAL operator
    ///
    /// Useful for Iceberg manifest operations that need direct storage access
    pub fn operator(&self) -> &Operator {
        &self.operator
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

        let encoded = encode_record_batches(batches, writer_properties())
            .context("failed to encode record batches to parquet")?;
        let otlp2parquet_core::parquet::EncodedParquet {
            bytes,
            hash,
            schema,
            parquet_metadata,
            row_count,
        } = encoded;
        let file_size = bytes.len() as u64;
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
        self.operator.write(&path, bytes).await?;

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
    let _ = arrow_writer.close()?;
    Ok(())
}

/// Synchronous helper for benchmarking: write batches to buffer and compute hash
pub fn write_batches_with_hash(batches: Vec<RecordBatch>) -> Result<(Vec<u8>, Blake3Hash)> {
    if batches.is_empty() {
        bail!("Cannot write empty batch list");
    }

    let encoded = encode_record_batches(&batches, writer_properties())
        .context("failed to encode record batches to parquet")?;

    Ok((encoded.bytes, encoded.hash))
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
