use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::format::KeyValue;
use std::sync::OnceLock;

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

fn compression_setting() -> Compression {
    Compression::SNAPPY
}

/// Get shared writer properties (cached)
///
/// Configuration optimized for size and query performance:
/// - Snappy compression
/// - Dictionary encoding enabled
/// - 32k rows per group by default (configurable)
/// - OTLP version metadata embedded in file
pub(crate) fn writer_properties() -> &'static WriterProperties {
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
