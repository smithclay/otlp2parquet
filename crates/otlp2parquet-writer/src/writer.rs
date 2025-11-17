//! Core writer types and implementations

use anyhow::Result;
use arrow::array::RecordBatch;
use async_trait::async_trait;
use icepick::catalog::Catalog;
use icepick::{DataFile, FileIO, Table};
use otlp2parquet_core::SignalType;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::partition::generate_partition_path_with_signal;
use crate::table_mapping::{signal_type_for_partition, table_name_for_signal};

/// Result of a write operation
#[derive(Clone, Debug)]
pub struct WriteResult {
    /// Path where the file was written
    pub path: String,
    /// File size in bytes
    pub file_size: u64,
    /// Number of rows written
    pub row_count: i64,
    /// Signal type
    pub signal_type: SignalType,
    /// Metric type (if applicable)
    pub metric_type: Option<String>,
    /// Timestamp when write completed
    pub completed_at: chrono::DateTime<chrono::Utc>,
}

/// Configuration for the writer
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct WriterConfig {
    /// Base path for writing files
    pub base_path: String,
    /// Optional catalog configuration
    pub catalog_config: Option<CatalogConfig>,
    /// Whether to enable table caching
    pub enable_table_cache: bool,
}

/// Catalog configuration
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct CatalogConfig {
    /// Catalog type (S3Tables, R2Catalog, etc.)
    pub catalog_type: String,
    /// Catalog-specific settings
    pub settings: std::collections::HashMap<String, String>,
}

/// Trait for writing OTLP data to Parquet files
#[async_trait]
pub trait OtlpWriter: Send + Sync {
    /// Write a single RecordBatch to storage with optional catalog commit
    ///
    /// # Arguments
    /// * `batch` - Arrow RecordBatch to write
    /// * `signal_type` - Type of OTLP signal (logs, traces, metrics)
    /// * `metric_type` - Metric type if signal_type is Metrics (gauge, sum, etc.)
    /// * `service_name` - Service name for partitioning
    /// * `timestamp_nanos` - Timestamp for partitioning
    async fn write_batch(
        &self,
        batch: &RecordBatch,
        signal_type: SignalType,
        metric_type: Option<&str>,
        service_name: &str,
        timestamp_nanos: i64,
    ) -> Result<WriteResult>;
}

/// Icepick-based writer implementation
pub struct IcepickWriter {
    /// FileIO for reading/writing Parquet files
    file_io: FileIO,
    /// Optional catalog for Iceberg integration
    catalog: Option<Arc<dyn Catalog>>,
    /// Table cache (table_name -> Table)
    table_cache: Arc<RwLock<HashMap<String, Table>>>,
    /// Base path prefix for file writes
    base_path: String,
}

impl IcepickWriter {
    /// Create a new IcepickWriter with FileIO and optional catalog
    pub fn new(
        file_io: FileIO,
        catalog: Option<Arc<dyn Catalog>>,
        base_path: String,
    ) -> Result<Self> {
        Ok(Self {
            file_io,
            catalog,
            table_cache: Arc::new(RwLock::new(HashMap::new())),
            base_path,
        })
    }

    /// Get or create an Iceberg table
    async fn get_or_create_table(
        &self,
        table_name: &str,
        signal_type: SignalType,
        metric_type: Option<&str>,
    ) -> Result<Table> {
        // Check cache first
        {
            let cache = self.table_cache.read().await;
            if let Some(table) = cache.get(table_name) {
                return Ok(table.clone());
            }
        }

        // If no catalog, can't create tables
        let catalog = match &self.catalog {
            Some(c) => c,
            None => anyhow::bail!("No catalog configured for table operations"),
        };

        // Get Iceberg schema for this signal type
        let iceberg_schema =
            otlp2parquet_core::iceberg_schemas::schema_for_signal(signal_type, metric_type);

        // Load or create table
        let namespace = icepick::NamespaceIdent::new(vec!["otel".to_string()]);
        let table_ident = icepick::TableIdent::new(namespace.clone(), table_name.to_string());

        let table = match catalog.load_table(&table_ident).await {
            Ok(t) => {
                tracing::debug!("Loaded existing table: {}", table_name);
                t
            }
            Err(_) => {
                tracing::info!("Creating new table: {}", table_name);
                // Table doesn't exist, create it
                let creation = icepick::spec::TableCreation::builder()
                    .with_name(table_name)
                    .with_schema(iceberg_schema)
                    .build()
                    .map_err(|e| anyhow::anyhow!("Failed to build table creation: {}", e))?;

                catalog
                    .create_table(&namespace, creation)
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to create table {}: {}", table_name, e))?
            }
        };

        // Cache the table
        {
            let mut cache = self.table_cache.write().await;
            cache.insert(table_name.to_string(), table.clone());
        }

        Ok(table)
    }

    /// Compute Blake3 hash of RecordBatch for deduplication
    fn compute_batch_hash(batch: &RecordBatch) -> String {
        let mut hasher = blake3::Hasher::new();

        // Hash schema - use debug format since Schema doesn't implement Serialize
        let schema_repr = format!("{:?}", batch.schema());
        hasher.update(schema_repr.as_bytes());

        // Hash each column's data
        for column in batch.columns() {
            // Use array's memory representation for hashing
            for buffer in column.to_data().buffers() {
                hasher.update(buffer.as_slice());
            }
        }

        hasher.finalize().to_hex().to_string()
    }
}

#[async_trait]
impl OtlpWriter for IcepickWriter {
    async fn write_batch(
        &self,
        batch: &RecordBatch,
        signal_type: SignalType,
        metric_type: Option<&str>,
        service_name: &str,
        timestamp_nanos: i64,
    ) -> Result<WriteResult> {
        // Compute content hash for idempotent filenames
        let hash_hex = Self::compute_batch_hash(batch);

        // Generate partition path
        let signal_str = signal_type_for_partition(signal_type);
        let subdirectory = metric_type; // For metrics, this is Some("gauge"), Some("sum"), etc.
        let partition_path = generate_partition_path_with_signal(
            signal_str,
            service_name,
            timestamp_nanos,
            &hash_hex,
            subdirectory,
        );

        // Full path includes base prefix
        let full_path = if self.base_path.is_empty() {
            partition_path.clone()
        } else {
            format!(
                "{}/{}",
                self.base_path.trim_end_matches('/'),
                partition_path
            )
        };

        // Write Parquet file using icepick
        icepick::arrow_to_parquet(batch, &full_path, &self.file_io)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to write Parquet: {}", e))?;

        let row_count = batch.num_rows() as i64;
        let completed_at = chrono::Utc::now();

        // If catalog is configured, commit to Iceberg
        if self.catalog.is_some() {
            let table_name = table_name_for_signal(signal_type, metric_type);

            match self
                .get_or_create_table(&table_name, signal_type, metric_type)
                .await
            {
                Ok(table) => {
                    // Create DataFile entry
                    let data_file = DataFile::builder()
                        .with_file_path(&partition_path)
                        .with_file_format("PARQUET")
                        .with_record_count(row_count)
                        .with_file_size_in_bytes(0) // TODO: Get actual file size
                        .build()
                        .map_err(|e| anyhow::anyhow!("Failed to build DataFile: {}", e))?;

                    // Create transaction and commit
                    let transaction = table.transaction().append(vec![data_file]);

                    if let Err(e) = transaction
                        .commit(self.catalog.as_ref().unwrap().as_ref())
                        .await
                    {
                        tracing::warn!(
                            "Failed to commit {} to Iceberg catalog (file written successfully): {}",
                            table_name,
                            e
                        );
                    } else {
                        tracing::debug!("Successfully committed {} to Iceberg catalog", table_name);
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to get/create table for catalog commit (file written successfully): {}",
                        e
                    );
                }
            }
        }

        Ok(WriteResult {
            path: full_path,
            file_size: 0, // TODO: Get actual file size from write operation
            row_count,
            signal_type,
            metric_type: metric_type.map(String::from),
            completed_at,
        })
    }
}
