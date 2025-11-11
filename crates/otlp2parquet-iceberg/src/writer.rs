use crate::catalog::IcebergCatalog;
use crate::http::HttpClient;
use crate::path::{catalog_path, storage_key_from_path};
use crate::types::table::TableMetadata;
use anyhow::{anyhow, Context, Result};
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use otlp2parquet_core::parquet::{encode_record_batches, writer_properties};
use otlp2parquet_core::ParquetWriteResult;
use std::sync::Arc;
use tracing::{debug, info, warn};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct IcebergConfig {
    pub rest_uri: String,
    pub warehouse: String,
    pub namespace: String,
}

pub struct IcebergWriter<C: HttpClient> {
    catalog: Arc<IcebergCatalog<C>>,
    #[allow(dead_code)] // Will be used in future tasks for Parquet write
    storage: opendal::Operator,
    pub config: IcebergConfig,
}

impl<C: HttpClient> IcebergWriter<C> {
    pub fn new(
        catalog: Arc<IcebergCatalog<C>>,
        storage: opendal::Operator,
        config: IcebergConfig,
    ) -> Self {
        Self {
            catalog,
            storage,
            config,
        }
    }

    /// Generate warehouse path for Parquet file
    pub fn generate_warehouse_path(base_location: &str, _signal_type: &str) -> String {
        let timestamp = Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let uuid = Uuid::new_v4();
        let filename = format!("{}-{}.parquet", timestamp, uuid);

        let data_suffix = format!("data/{}", filename);
        catalog_path(base_location, &data_suffix)
    }

    /// Write Arrow batch to S3 Tables warehouse and commit to catalog
    pub async fn write_and_commit(
        &self,
        signal_type: &str,
        metric_type: Option<&str>,
        arrow_batch: &RecordBatch,
    ) -> Result<ParquetWriteResult> {
        // 1. Get or create table
        let table = self.get_or_create_table(signal_type, metric_type).await?;

        // 2. Generate warehouse path
        let file_path = Self::generate_warehouse_path(&table.location, signal_type);
        let storage_path = storage_key_from_path(&file_path);

        // 3. Write Parquet file
        let write_result = self
            .write_parquet(&file_path, &storage_path, arrow_batch)
            .await?;

        // 4. Commit to catalog using manifest-based flow (non-WASM only)
        // Convert ParquetWriteResult to DataFile
        #[cfg(not(target_arch = "wasm32"))]
        let iceberg_schema =
            crate::arrow_convert::arrow_to_iceberg_schema(write_result.arrow_schema.as_ref())?;

        // Build data file metadata (only needed for non-WASM catalog commits)
        #[cfg(not(target_arch = "wasm32"))]
        let data_file = crate::datafile_convert::build_data_file(&write_result, &iceberg_schema)?;

        // Call catalog.commit_with_signal (warn and succeed on error)
        // Note: commit_with_signal is only available for non-WASM targets
        #[cfg(not(target_arch = "wasm32"))]
        if let Err(e) = self
            .catalog
            .commit_with_signal(signal_type, metric_type, vec![data_file], &self.storage)
            .await
        {
            warn!(
                "Failed to commit to catalog: {} - Parquet file written but not cataloged at: {}",
                e, write_result.path
            );
        }

        Ok(write_result)
    }

    /// Get or create table by signal type
    async fn get_or_create_table(
        &self,
        signal_type: &str,
        metric_type: Option<&str>,
    ) -> Result<TableMetadata> {
        // Build table name
        let table_name = self.build_table_name(signal_type, metric_type);

        info!("Getting or creating table: {}", table_name);

        if let Ok(metadata) = self.catalog.load_table(&table_name).await {
            info!("Table '{}' is ready (loaded existing metadata)", table_name);
            return Ok(metadata);
        }

        info!("Table '{}' not found, attempting to create", table_name);

        if let Err(e) = self
            .create_table_from_signal(signal_type, metric_type, &table_name)
            .await
        {
            warn!(
                "Table creation returned error (may already exist): {}. Attempting to load metadata",
                e
            );
        }

        self.catalog
            .load_table(&table_name)
            .await
            .with_context(|| format!("failed to load table metadata for '{}'", table_name))
    }

    /// Build table name from signal and metric type
    fn build_table_name(&self, signal_type: &str, metric_type: Option<&str>) -> String {
        match metric_type {
            Some(mt) => format!("metrics_{}", mt),
            None => signal_type.to_string(),
        }
    }

    /// Create table from signal type using Arrow schema
    async fn create_table_from_signal(
        &self,
        signal_type: &str,
        metric_type: Option<&str>,
        table_name: &str,
    ) -> Result<()> {
        info!("Creating table '{}' from signal type", table_name);

        // Get Arrow schema for signal type
        let arrow_schema = self.get_schema_for_signal(signal_type, metric_type)?;
        debug!(
            "Got Arrow schema with {} fields",
            arrow_schema.fields().len()
        );

        // Convert Arrow schema to Iceberg schema
        let iceberg_schema = crate::arrow_convert::arrow_to_iceberg_schema(arrow_schema.as_ref())?;
        debug!(
            "Converted to Iceberg schema with {} fields",
            iceberg_schema.fields.len()
        );

        // Create table via catalog
        info!("Calling catalog.create_table for '{}'", table_name);
        self.catalog
            .create_table(table_name, arrow_schema.as_ref().clone())
            .await?;
        info!(
            "catalog.create_table returned successfully for '{}'",
            table_name
        );

        Ok(())
    }

    /// Get Arrow schema for signal type
    fn get_schema_for_signal(
        &self,
        signal_type: &str,
        metric_type: Option<&str>,
    ) -> Result<Arc<arrow::datatypes::Schema>> {
        use otlp2parquet_core::schema;

        match signal_type {
            "logs" => Ok(schema::otel_logs_schema_arc()),
            "traces" => Ok(schema::otel_traces_schema_arc()),
            "metrics" => {
                let mt = metric_type.ok_or_else(|| anyhow!("metric_type required for metrics"))?;
                match mt {
                    "gauge" => Ok(schema::otel_metrics_gauge_schema_arc()),
                    "sum" => Ok(schema::otel_metrics_sum_schema_arc()),
                    "histogram" => Ok(schema::otel_metrics_histogram_schema_arc()),
                    "exponential_histogram" => {
                        Ok(schema::otel_metrics_exponential_histogram_schema_arc())
                    }
                    "summary" => Ok(schema::otel_metrics_summary_schema_arc()),
                    _ => Err(anyhow!("Unknown metric type: {}", mt)),
                }
            }
            _ => Err(anyhow!("Unknown signal type: {}", signal_type)),
        }
    }

    async fn write_parquet(
        &self,
        catalog_path: &str,
        storage_path: &str,
        arrow_batch: &RecordBatch,
    ) -> Result<ParquetWriteResult> {
        let encoded = encode_record_batches(std::slice::from_ref(arrow_batch), writer_properties())
            .context("failed to encode record batch to parquet")?;
        let file_size = encoded.bytes.len() as u64;
        let completed_at = Utc::now();

        self.storage
            .write(storage_path, encoded.bytes)
            .await
            .map_err(|e| anyhow!("Failed to write to storage: {}", e))?;

        Ok(ParquetWriteResult {
            path: catalog_path.to_string(),
            hash: encoded.hash,
            file_size,
            row_count: encoded.row_count,
            arrow_schema: encoded.schema,
            parquet_metadata: encoded.parquet_metadata,
            completed_at,
        })
    }
}

#[cfg(test)]
#[path = "writer_test.rs"]
mod writer_test;
