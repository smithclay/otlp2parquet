// Minimal Parquet writer with size-optimized configuration
//
// Uses Snappy compression and dictionary encoding to minimize size
// while maintaining reasonable write performance.

use anyhow::Result;
use arrow::array::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

/// Write Arrow RecordBatch to Parquet format (in-memory buffer)
///
/// Configuration optimized for size:
/// - Snappy compression (smallest compressor in our allowed features)
/// - Dictionary encoding enabled
pub fn write_parquet(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut buffer = Vec::new();

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_dictionary_enabled(true)
        .build();

    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props))?;

    writer.write(batch)?;
    writer.close()?;

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
}
