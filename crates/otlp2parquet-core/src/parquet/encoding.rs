use anyhow::{anyhow, Result};
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use parquet::arrow::ArrowWriter;
use parquet::basic::ColumnOrder;
use parquet::file::metadata::{
    FileMetaData as PhysicalFileMetaData, ParquetMetaData, RowGroupMetaData,
};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::format::{ColumnOrder as TColumnOrder, FileMetaData as ThriftFileMetaData, KeyValue};
use parquet::schema::types::{self, SchemaDescriptor};
use std::io::{self, Write};
use std::sync::{Arc, OnceLock};

use crate::types::Blake3Hash;

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
            .set_data_page_size_limit(256 * 1024)
            .set_write_batch_size(32 * 1024)
            .set_max_row_group_size(configured_row_group_size())
            .set_dictionary_page_size_limit(128 * 1024)
            .set_key_value_metadata(Some(metadata))
            .build()
    })
}

/// Result of encoding Arrow record batches into Parquet bytes.
pub struct EncodedParquet {
    pub bytes: Vec<u8>,
    pub hash: Blake3Hash,
    pub schema: SchemaRef,
    pub parquet_metadata: Arc<ParquetMetaData>,
    pub row_count: i64,
}

/// Encode one or more record batches into Parquet bytes using the provided writer properties.
pub fn encode_record_batches(
    batches: &[RecordBatch],
    properties: &WriterProperties,
) -> Result<EncodedParquet> {
    if batches.is_empty() {
        return Err(anyhow!("cannot encode empty batch list"));
    }

    let mut sink = HashingBuffer::new();
    let schema: SchemaRef = batches[0].schema();

    let file_metadata = {
        let mut writer = ArrowWriter::try_new(&mut sink, schema.clone(), Some(properties.clone()))
            .map_err(|e| anyhow!("failed to create Arrow writer: {}", e))?;

        for batch in batches {
            if batch.schema() != schema {
                return Err(anyhow!("all batches must share the same schema"));
            }
            writer
                .write(batch)
                .map_err(|e| anyhow!("failed to write batch: {}", e))?;
        }

        writer
            .close()
            .map_err(|e| anyhow!("failed to close writer: {}", e))?
    };

    let (buffer, hash) = sink.finish();

    let parquet_metadata = Arc::new(
        build_parquet_metadata(file_metadata)
            .map_err(|e| anyhow!("failed to reconstruct parquet metadata: {}", e))?,
    );
    let row_count = parquet_metadata.file_metadata().num_rows();

    Ok(EncodedParquet {
        bytes: buffer,
        hash,
        schema,
        parquet_metadata,
        row_count,
    })
}

fn build_parquet_metadata(file_metadata: ThriftFileMetaData) -> Result<ParquetMetaData> {
    let schema = types::from_thrift(&file_metadata.schema)
        .map_err(|e| anyhow!("failed to decode parquet schema: {}", e))?;
    let schema_descr = Arc::new(SchemaDescriptor::new(schema));

    let column_orders = parse_column_orders(file_metadata.column_orders, &schema_descr)?;

    let mut row_groups = Vec::with_capacity(file_metadata.row_groups.len());
    for row_group in file_metadata.row_groups {
        let metadata = RowGroupMetaData::from_thrift(schema_descr.clone(), row_group)
            .map_err(|e| anyhow!("failed to decode row group metadata: {}", e))?;
        row_groups.push(metadata);
    }

    let physical_metadata = PhysicalFileMetaData::new(
        file_metadata.version,
        file_metadata.num_rows,
        file_metadata.created_by,
        file_metadata.key_value_metadata,
        schema_descr,
        column_orders,
    );

    Ok(ParquetMetaData::new(physical_metadata, row_groups))
}

fn parse_column_orders(
    orders: Option<Vec<TColumnOrder>>,
    schema_descr: &SchemaDescriptor,
) -> Result<Option<Vec<ColumnOrder>>> {
    match orders {
        Some(order_defs) => {
            if order_defs.len() != schema_descr.num_columns() {
                return Err(anyhow!("column order length mismatch"));
            }

            let mut parsed = Vec::with_capacity(order_defs.len());
            for (idx, column) in schema_descr.columns().iter().enumerate() {
                match &order_defs[idx] {
                    TColumnOrder::TYPEORDER(_) => {
                        let sort_order = ColumnOrder::get_sort_order(
                            column.logical_type(),
                            column.converted_type(),
                            column.physical_type(),
                        );
                        parsed.push(ColumnOrder::TYPE_DEFINED_ORDER(sort_order));
                    }
                }
            }
            Ok(Some(parsed))
        }
        None => Ok(None),
    }
}
