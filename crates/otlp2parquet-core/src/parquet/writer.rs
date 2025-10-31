// Minimal Parquet writer with size-optimized configuration
//
// Uses Snappy compression and dictionary encoding to minimize size
// while maintaining reasonable write performance.

use anyhow::Result;
use arrow::array::RecordBatch;
use parquet::arrow::ArrowWriter;
#[cfg(target_arch = "wasm32")]
use parquet::basic::Compression;
#[cfg(not(target_arch = "wasm32"))]
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use std::io::Write;
use std::sync::OnceLock;

#[cfg(target_arch = "wasm32")]
fn compression_setting() -> Compression {
    Compression::SNAPPY
}

#[cfg(not(target_arch = "wasm32"))]
fn compression_setting() -> Compression {
    Compression::ZSTD(ZstdLevel::try_new(2).unwrap())
}

pub fn writer_properties() -> &'static WriterProperties {
    static PROPERTIES: OnceLock<WriterProperties> = OnceLock::new();
    PROPERTIES.get_or_init(|| {
        let builder = WriterProperties::builder()
            .set_dictionary_enabled(true)
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_compression(compression_setting())
            .set_data_page_size_limit(256 * 1024) // 256 KiB data pages balance CPU vs. IO
            .set_write_batch_size(32 * 1024)
            .set_max_row_group_size(32 * 1024) // 32k rows per group keeps query engines happy
            .set_dictionary_page_size_limit(128 * 1024);

        builder.build()
    })
}

/// Write Arrow `RecordBatch` into an arbitrary `Write` sink.
///
/// This allows callers to stream Parquet bytes directly into their preferred
/// storage backend without forcing an intermediate buffer allocation.
pub fn write_parquet_into<W>(batch: &RecordBatch, writer: &mut W) -> Result<()>
where
    W: Write + Send,
{
    let props = writer_properties().clone();
    let mut arrow_writer = ArrowWriter::try_new(writer, batch.schema(), Some(props))?;

    arrow_writer.write(batch)?;
    arrow_writer.close()?;

    Ok(())
}

/// Write Arrow RecordBatch to Parquet format (in-memory buffer)
///
/// Configuration optimized for size:
/// - Snappy compression (smallest compressor in our allowed features)
/// - Dictionary encoding enabled
pub fn write_parquet(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut buffer = Vec::new();
    write_parquet_into(batch, &mut buffer)?;
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_write_parquet() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        let result = write_parquet(&batch);
        assert!(result.is_ok());

        let parquet_bytes = result.unwrap();
        assert!(!parquet_bytes.is_empty());
        // Parquet files start with "PAR1" magic bytes
        assert_eq!(&parquet_bytes[0..4], b"PAR1");
    }

    #[test]
    fn test_write_parquet_into_vec() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        let mut buffer = Vec::new();
        write_parquet_into(&batch, &mut buffer).unwrap();
        assert!(!buffer.is_empty());
        assert_eq!(&buffer[0..4], b"PAR1");
    }
}
