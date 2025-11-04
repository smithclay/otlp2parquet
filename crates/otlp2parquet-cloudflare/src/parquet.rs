// WASM-specific Parquet writer
//
// Why synchronous approach for WASM:
// - parquet_opendal uses tokio::spawn which requires Send trait
// - Send is not available in single-threaded wasm32-unknown-unknown target
// - OpenDAL S3 service DOES work in WASM for R2 uploads
// - Solution: Write Parquet synchronously to `Vec<u8>`, then upload via OpenDAL
//
// This is platform-specific "accidental complexity" (storage format + I/O)
// and belongs in the platform layer, not core.

use anyhow::{bail, Result};
use arrow::array::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

/// Write one or more RecordBatches to Parquet bytes synchronously (WASM-compatible)
pub fn write_batches_to_parquet(batches: &[RecordBatch]) -> Result<Vec<u8>> {
    if batches.is_empty() {
        bail!("cannot write empty batch list");
    }

    let mut buffer = Vec::new();
    let props = writer_properties();
    let mut writer = ArrowWriter::try_new(&mut buffer, batches[0].schema(), Some(props))?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.close()?;
    Ok(buffer)
}

/// Platform-specific writer properties for WASM (Snappy compression only)
fn writer_properties() -> WriterProperties {
    use parquet::basic::Compression;
    use parquet::file::properties::EnabledStatistics;

    WriterProperties::builder()
        .set_dictionary_enabled(true)
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_compression(Compression::SNAPPY)
        .set_data_page_size_limit(256 * 1024)
        .set_write_batch_size(32 * 1024)
        .set_max_row_group_size(32 * 1024)
        .set_dictionary_page_size_limit(128 * 1024)
        .build()
}
