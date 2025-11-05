// otlp2parquet-iceberg - Apache Iceberg catalog integration
//
// This crate provides Iceberg table catalog integration for S3 Tables.
// It builds DataFile descriptors from Parquet metadata and commits them
// to Iceberg tables via the S3 Tables catalog.
//
// Only used by Lambda and Server runtimes (not Cloudflare Workers).

use std::collections::HashMap;
use std::env;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, Datum, Schema, Struct,
    UnboundPartitionSpec,
};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{
    RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE,
};
use otlp2parquet_storage::ParquetWriteResult;
use parquet::file::metadata::ParquetMetaData;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, instrument, warn};

/// Configuration for Iceberg REST catalog integration.
///
/// Works with any Iceberg REST catalog including:
/// - AWS S3 Tables (via Iceberg REST endpoint)
/// - AWS Glue Data Catalog
/// - Tabular
/// - Other REST-compatible catalogs
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IcebergRestConfig {
    /// REST catalog endpoint URI
    /// Examples:
    /// - S3 Tables: https://s3tables.<region>.amazonaws.com/iceberg
    /// - Glue: https://glue.<region>.amazonaws.com/iceberg
    pub rest_uri: String,
    /// Warehouse location (optional, depends on catalog)
    pub warehouse: Option<String>,
    /// Namespace for the table (dot-separated, e.g., "otel.logs")
    pub namespace: Vec<String>,
    /// Table name
    pub table: String,
    /// Catalog name (defaults to "rest")
    #[serde(default = "default_catalog_name")]
    pub catalog_name: String,
    #[serde(default = "default_format_version")]
    pub format_version: i32,
    #[serde(default = "default_target_file_size_bytes")]
    pub target_file_size_bytes: u64,
    #[serde(default = "default_staging_prefix")]
    pub staging_prefix: String,
}

const DEFAULT_CATALOG_NAME: &str = "rest";
const DEFAULT_STAGING_PREFIX: &str = "data/incoming";
const DEFAULT_TARGET_FILE_SIZE: u64 = 512 * 1024 * 1024;

fn default_catalog_name() -> String {
    DEFAULT_CATALOG_NAME.to_string()
}

fn default_format_version() -> i32 {
    2
}

fn default_target_file_size_bytes() -> u64 {
    DEFAULT_TARGET_FILE_SIZE
}

fn default_staging_prefix() -> String {
    DEFAULT_STAGING_PREFIX.to_string()
}

impl Default for IcebergRestConfig {
    fn default() -> Self {
        Self {
            rest_uri: String::new(),
            warehouse: None,
            namespace: Vec::new(),
            table: String::new(),
            catalog_name: default_catalog_name(),
            format_version: default_format_version(),
            target_file_size_bytes: default_target_file_size_bytes(),
            staging_prefix: default_staging_prefix(),
        }
    }
}

impl IcebergRestConfig {
    /// Load configuration from environment variables.
    ///
    /// Required:
    /// - `ICEBERG_REST_URI`: REST catalog endpoint
    /// - `ICEBERG_TABLE`: Table name
    ///
    /// Optional:
    /// - `ICEBERG_WAREHOUSE`: Warehouse location
    /// - `ICEBERG_NAMESPACE`: Dot-separated namespace (e.g., "otel.logs")
    /// - `ICEBERG_CATALOG_NAME`: Catalog name (default: "rest")
    /// - `ICEBERG_STAGING_PREFIX`: Staging prefix for data files
    /// - `ICEBERG_TARGET_FILE_SIZE_BYTES`: Target file size
    pub fn from_env() -> Result<Self> {
        let rest_uri = env::var("ICEBERG_REST_URI").context(
            "ICEBERG_REST_URI must be set (e.g., https://s3tables.us-east-1.amazonaws.com/iceberg)",
        )?;

        let warehouse = env::var("ICEBERG_WAREHOUSE").ok();

        let namespace = env::var("ICEBERG_NAMESPACE")
            .unwrap_or_default()
            .split('.')
            .filter(|segment| !segment.trim().is_empty())
            .map(|segment| segment.trim().to_string())
            .collect::<Vec<_>>();

        let table = env::var("ICEBERG_TABLE")
            .context("ICEBERG_TABLE must be set for Iceberg catalog access")?;

        let catalog_name =
            env::var("ICEBERG_CATALOG_NAME").unwrap_or_else(|_| DEFAULT_CATALOG_NAME.to_string());

        let staging_prefix = env::var("ICEBERG_STAGING_PREFIX")
            .unwrap_or_else(|_| DEFAULT_STAGING_PREFIX.to_string());

        let target_file_size_bytes = env::var("ICEBERG_TARGET_FILE_SIZE_BYTES")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(DEFAULT_TARGET_FILE_SIZE);

        Ok(Self {
            rest_uri,
            warehouse,
            namespace,
            table,
            catalog_name,
            format_version: default_format_version(),
            target_file_size_bytes,
            staging_prefix,
        })
    }

    pub fn namespace_ident(&self) -> Result<NamespaceIdent> {
        if self.namespace.is_empty() {
            bail!("Iceberg namespace must contain at least one element");
        }

        NamespaceIdent::from_strs(self.namespace.clone())
            .context("failed to parse namespace for Iceberg table")
    }
}

/// Thin wrapper describing a fully-qualified Iceberg table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IcebergTableIdentifier {
    pub namespace: NamespaceIdent,
    pub name: String,
}

impl IcebergTableIdentifier {
    pub fn new(namespace: NamespaceIdent, name: impl Into<String>) -> Self {
        Self {
            namespace,
            name: name.into(),
        }
    }

    /// Convert to iceberg's TableIdent type.
    pub fn to_table_ident(&self) -> TableIdent {
        TableIdent::new(self.namespace.clone(), self.name.clone())
    }
}

/// Abstract catalog interactions required by the storage layer.
#[async_trait]
pub trait CatalogAdapter: Send + Sync {
    async fn ensure_namespace(&self, namespace: &NamespaceIdent) -> Result<()>;
    async fn ensure_table(
        &self,
        ident: &IcebergTableIdentifier,
        schema: Arc<Schema>,
        partition_spec: Arc<UnboundPartitionSpec>,
    ) -> Result<()>;
    async fn append_files(
        &self,
        ident: &IcebergTableIdentifier,
        files: Vec<DataFile>,
        committed_at: DateTime<Utc>,
    ) -> Result<()>;
    async fn expire_snapshots(
        &self,
        ident: &IcebergTableIdentifier,
        older_than: DateTime<Utc>,
        retain_last: usize,
    ) -> Result<()>;
}

/// Create an Iceberg REST catalog from configuration.
///
/// Works with any Iceberg REST catalog including:
/// - AWS S3 Tables
/// - AWS Glue Data Catalog
/// - Tabular
/// - Other REST-compatible catalogs
pub async fn create_rest_catalog(config: &IcebergRestConfig) -> Result<Arc<dyn Catalog>> {
    let mut properties = HashMap::new();
    properties.insert(REST_CATALOG_PROP_URI.to_string(), config.rest_uri.clone());

    if let Some(ref warehouse) = config.warehouse {
        properties.insert(REST_CATALOG_PROP_WAREHOUSE.to_string(), warehouse.clone());
    }

    let catalog = RestCatalogBuilder::default()
        .load(&config.catalog_name, properties)
        .await
        .context("failed to create REST catalog")?;

    Ok(Arc::new(catalog))
}

/// Convert a written Parquet file into an Iceberg DataFile descriptor.
pub fn build_data_file(
    result: &ParquetWriteResult,
    _table_config: &IcebergRestConfig,
    _schema: &Schema,
    _partition_spec: &Arc<iceberg::spec::PartitionSpec>,
) -> Result<DataFile> {
    let parquet_metadata: &ParquetMetaData = &result.parquet_metadata;

    debug!(
        path = %result.path,
        row_count = result.row_count,
        file_size = result.file_size,
        num_row_groups = parquet_metadata.num_row_groups(),
        "constructing iceberg DataFile from Parquet metadata"
    );

    // Extract column statistics from Parquet metadata
    let mut column_sizes = HashMap::new();
    let mut value_counts = HashMap::new();
    let mut null_value_counts = HashMap::new();
    let mut lower_bounds = HashMap::new();
    let mut upper_bounds = HashMap::new();

    // Iceberg uses field IDs, we'll use positional mapping for now
    // TODO: Proper field ID mapping when schema evolution is implemented
    let num_columns = parquet_metadata
        .file_metadata()
        .schema_descr()
        .num_columns();

    for field_idx in 0..num_columns {
        let mut total_size = 0u64;
        let mut total_values = 0u64;
        let mut total_nulls = 0u64;
        let mut col_min: Option<Vec<u8>> = None;
        let mut col_max: Option<Vec<u8>> = None;

        // Aggregate statistics across all row groups
        for rg_idx in 0..parquet_metadata.num_row_groups() {
            let row_group = parquet_metadata.row_group(rg_idx);
            if field_idx < row_group.num_columns() {
                let column_chunk = row_group.column(field_idx);

                // Column size
                total_size += column_chunk.compressed_size() as u64;

                // Value and null counts
                if let Some(stats) = column_chunk.statistics() {
                    total_values += row_group.num_rows() as u64;
                    if let Some(null_count) = stats.null_count_opt() {
                        total_nulls += null_count;
                    }

                    // Min/max bounds (only if we haven't set them or need to update)
                    if let Some(min_bytes) = stats.min_bytes_opt() {
                        if col_min.is_none() || min_bytes < col_min.as_ref().unwrap().as_slice() {
                            col_min = Some(min_bytes.to_vec());
                        }
                    }
                    if let Some(max_bytes) = stats.max_bytes_opt() {
                        if col_max.is_none() || max_bytes > col_max.as_ref().unwrap().as_slice() {
                            col_max = Some(max_bytes.to_vec());
                        }
                    }
                }
            }
        }

        // Store per-field statistics
        // Iceberg uses field_id -> value maps; we use sequential IDs starting from schema field index
        let field_id = (field_idx + 1) as i32; // Iceberg field IDs typically start at 1

        if total_size > 0 {
            column_sizes.insert(field_id, total_size);
        }
        if total_values > 0 {
            value_counts.insert(field_id, total_values);
        }
        if total_nulls > 0 {
            null_value_counts.insert(field_id, total_nulls);
        }
        if let Some(min) = col_min {
            lower_bounds.insert(field_id, Datum::binary(min));
        }
        if let Some(max) = col_max {
            upper_bounds.insert(field_id, Datum::binary(max));
        }
    }

    // Calculate split offsets from row group positions
    let mut split_offsets = Vec::new();
    let mut current_offset: i64 = 4; // Parquet magic number size
    for rg_idx in 0..parquet_metadata.num_row_groups() {
        let row_group = parquet_metadata.row_group(rg_idx);
        if let Some(dict_page_offset) = row_group.column(0).dictionary_page_offset() {
            split_offsets.push(dict_page_offset);
        } else {
            let data_page_offset = row_group.column(0).data_page_offset();
            if data_page_offset > 0 {
                split_offsets.push(data_page_offset);
            } else {
                // Fallback: estimate based on cumulative size
                current_offset += row_group.compressed_size();
                split_offsets.push(current_offset);
            }
        }
    }

    let mut builder = DataFileBuilder::default();
    builder
        .file_path(result.path.clone())
        .file_format(DataFileFormat::Parquet)
        .record_count(result.row_count as u64)
        .file_size_in_bytes(result.file_size)
        .partition(Struct::empty()) // No partitioning for now
        .content(DataContentType::Data)
        .partition_spec_id(0)
        .column_sizes(column_sizes)
        .value_counts(value_counts)
        .null_value_counts(null_value_counts)
        .lower_bounds(lower_bounds)
        .upper_bounds(upper_bounds)
        .split_offsets(split_offsets);

    // Set sort order ID if schema metadata indicates sorted data
    // TODO: Extract from Arrow schema metadata when available
    // builder.sort_order_id(Some(1));

    builder
        .build()
        .context("failed to build Iceberg DataFile from Parquet metadata")
}

/// Committer that appends Parquet files to an Iceberg table.
///
/// Uses the official Iceberg Catalog trait to:
/// 1. Load the table
/// 2. Create a transaction
/// 3. Append DataFile descriptors
/// 4. Commit the transaction
pub struct IcebergCommitter {
    catalog: Arc<dyn Catalog>,
    table_ident: IcebergTableIdentifier,
    config: IcebergRestConfig,
}

impl IcebergCommitter {
    pub fn new(
        catalog: Arc<dyn Catalog>,
        table_ident: IcebergTableIdentifier,
        config: IcebergRestConfig,
    ) -> Self {
        Self {
            catalog,
            table_ident,
            config,
        }
    }

    /// Commit Parquet files to the Iceberg table.
    ///
    /// This:
    /// 1. Loads the current table state
    /// 2. Builds DataFile descriptors from Parquet metadata
    /// 3. Creates a transaction with append operation
    /// 4. Commits atomically
    #[instrument(skip(self, parquet_results), fields(num_files = parquet_results.len()))]
    pub async fn commit(&self, parquet_results: &[ParquetWriteResult]) -> Result<()> {
        if parquet_results.is_empty() {
            debug!("No files to commit");
            return Ok(());
        }

        // Load the table
        let table_ident = self.table_ident.to_table_ident();
        let table = self
            .catalog
            .load_table(&table_ident)
            .await
            .context("failed to load Iceberg table")?;

        let table_metadata = table.metadata();
        let schema = table_metadata.current_schema();
        let partition_spec = table_metadata.default_partition_spec();

        // Build DataFile descriptors from Parquet results
        let mut data_files = Vec::with_capacity(parquet_results.len());
        for result in parquet_results {
            let data_file = build_data_file(result, &self.config, schema.as_ref(), partition_spec)?;
            data_files.push(data_file);
        }

        let total_rows = data_files.iter().map(|f| f.record_count()).sum::<u64>();
        let total_bytes = data_files
            .iter()
            .map(|f| f.file_size_in_bytes())
            .sum::<u64>();

        debug!(
            table = %self.table_ident.name,
            namespace = ?self.table_ident.namespace,
            num_files = data_files.len(),
            total_rows = total_rows,
            total_bytes = total_bytes,
            "committing files to Iceberg table"
        );

        // Create transaction and append data files
        let txn = Transaction::new(&table);
        let append = txn.fast_append().add_data_files(data_files.into_iter());

        // Apply action to transaction
        let txn = match append.apply(txn) {
            Ok(txn) => txn,
            Err(e) => {
                warn!(
                    table = %self.table_ident.name,
                    error = %e,
                    "failed to apply append action to transaction - files written to storage but not cataloged"
                );
                return Ok(()); // Warn and succeed
            }
        };

        // Commit transaction
        if let Err(e) = txn.commit(&*self.catalog).await {
            warn!(
                table = %self.table_ident.name,
                error = %e,
                "failed to commit transaction to catalog - files written to storage but not cataloged"
            );
            return Ok(()); // Warn and succeed
        }

        info!(
            table = %self.table_ident.name,
            num_files = parquet_results.len(),
            total_rows = total_rows,
            total_bytes = total_bytes,
            "successfully committed files to Iceberg table"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_from_env() {
        // Set required environment variables
        std::env::set_var("ICEBERG_REST_URI", "https://test.example.com/iceberg");
        std::env::set_var("ICEBERG_TABLE", "test_table");
        std::env::set_var("ICEBERG_NAMESPACE", "otel.logs");

        let config = IcebergRestConfig::from_env().unwrap();

        assert_eq!(config.rest_uri, "https://test.example.com/iceberg");
        assert_eq!(config.table, "test_table");
        assert_eq!(config.namespace, vec!["otel", "logs"]);
        assert_eq!(config.catalog_name, DEFAULT_CATALOG_NAME);
        assert_eq!(config.staging_prefix, DEFAULT_STAGING_PREFIX);
        assert_eq!(config.target_file_size_bytes, DEFAULT_TARGET_FILE_SIZE);

        // Clean up
        std::env::remove_var("ICEBERG_REST_URI");
        std::env::remove_var("ICEBERG_TABLE");
        std::env::remove_var("ICEBERG_NAMESPACE");
    }

    #[test]
    fn test_config_optional_warehouse() {
        std::env::set_var("ICEBERG_REST_URI", "https://test.example.com/iceberg");
        std::env::set_var("ICEBERG_TABLE", "test_table");
        std::env::set_var("ICEBERG_WAREHOUSE", "s3://my-bucket/warehouse");

        let config = IcebergRestConfig::from_env().unwrap();

        assert_eq!(
            config.warehouse,
            Some("s3://my-bucket/warehouse".to_string())
        );

        // Clean up
        std::env::remove_var("ICEBERG_REST_URI");
        std::env::remove_var("ICEBERG_TABLE");
        std::env::remove_var("ICEBERG_WAREHOUSE");
    }

    #[test]
    fn test_namespace_ident_parsing() {
        let config = IcebergRestConfig {
            rest_uri: "https://test.example.com/iceberg".to_string(),
            warehouse: None,
            namespace: vec!["otel".to_string(), "logs".to_string()],
            table: "test_table".to_string(),
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            format_version: 2,
            target_file_size_bytes: DEFAULT_TARGET_FILE_SIZE,
            staging_prefix: DEFAULT_STAGING_PREFIX.to_string(),
        };

        let ident = config.namespace_ident().unwrap();
        assert_eq!(ident.to_string(), "otel.logs");
    }

    #[test]
    fn test_namespace_empty_error() {
        let config = IcebergRestConfig {
            namespace: vec![],
            ..Default::default()
        };

        assert!(config.namespace_ident().is_err());
    }

    #[test]
    fn test_table_identifier_conversion() {
        let namespace = NamespaceIdent::from_strs(vec!["otel", "logs"]).unwrap();
        let ident = IcebergTableIdentifier::new(namespace.clone(), "test_table");

        assert_eq!(ident.name, "test_table");
        assert_eq!(ident.namespace, namespace);

        let table_ident = ident.to_table_ident();
        assert_eq!(table_ident.name(), "test_table");
        assert_eq!(table_ident.namespace(), &namespace);
    }

    #[test]
    fn test_default_values() {
        let config = IcebergRestConfig::default();

        assert_eq!(config.catalog_name, DEFAULT_CATALOG_NAME);
        assert_eq!(config.format_version, 2);
        assert_eq!(config.target_file_size_bytes, DEFAULT_TARGET_FILE_SIZE);
        assert_eq!(config.staging_prefix, DEFAULT_STAGING_PREFIX);
    }
}
