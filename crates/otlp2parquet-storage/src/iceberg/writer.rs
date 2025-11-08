use crate::iceberg::catalog::IcebergCatalog;
use crate::iceberg::http::ReqwestHttpClient;
use crate::iceberg::types::table::TableMetadata;
use crate::opendal_storage::OpenDalStorage;
use crate::parquet_writer::ParquetWriteResult;
use anyhow::{anyhow, Result};
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use parquet::arrow::ArrowWriter;
use parquet::basic::ColumnOrder;
use parquet::file::metadata::{
    FileMetaData as PhysicalFileMetaData, ParquetMetaData, RowGroupMetaData,
};
use parquet::format::ColumnOrder as TColumnOrder;
use parquet::schema::types::{self, SchemaDescriptor};
use std::io::Write;
use std::sync::Arc;
use tracing::{debug, info, warn};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct IcebergConfig {
    pub rest_uri: String,
    pub warehouse: String,
    pub namespace: String,
}

pub struct IcebergWriter {
    catalog: Arc<IcebergCatalog<ReqwestHttpClient>>,
    #[allow(dead_code)] // Will be used in future tasks for Parquet write
    storage: Arc<OpenDalStorage>,
    pub config: IcebergConfig,
}

impl IcebergWriter {
    pub fn new(
        catalog: Arc<IcebergCatalog<ReqwestHttpClient>>,
        storage: Arc<OpenDalStorage>,
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

        // Path format: {base}/data/{filename}
        format!("{}/data/{}", base_location.trim_end_matches('/'), filename)
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

        // 3. Write Parquet file
        let write_result = self.write_parquet(&file_path, arrow_batch).await?;

        // 4. Commit to catalog using manifest-based flow (non-WASM only)
        // Convert ParquetWriteResult to DataFile
        #[cfg(not(target_arch = "wasm32"))]
        let iceberg_schema = crate::iceberg::arrow_convert::arrow_to_iceberg_schema(
            write_result.arrow_schema.as_ref(),
        )?;

        // Build data file metadata (only needed for non-WASM catalog commits)
        #[cfg(not(target_arch = "wasm32"))]
        let data_file =
            crate::iceberg::datafile_convert::build_data_file(&write_result, &iceberg_schema)?;

        // Call catalog.commit_with_signal (warn and succeed on error)
        // Note: commit_with_signal is only available for non-WASM targets
        #[cfg(not(target_arch = "wasm32"))]
        if let Err(e) = self
            .catalog
            .commit_with_signal(
                signal_type,
                metric_type,
                vec![data_file],
                self.storage.operator(),
            )
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

        // Try to load existing table using reflection API
        // Since load_table is private in catalog, we'll attempt create_table which
        // handles "already exists" gracefully (returns 200 or 409)
        match self
            .create_table_from_signal(signal_type, metric_type, &table_name)
            .await
        {
            Ok(metadata) => {
                info!("Table '{}' is ready (created or exists)", table_name);
                Ok(metadata)
            }
            Err(e) => {
                // If table creation fails (likely already exists), construct metadata manually
                warn!(
                    "Table creation returned error (may already exist), using fallback: {}",
                    e
                );

                // Get Arrow schema and convert to Iceberg schema
                let arrow_schema = self.get_schema_for_signal(signal_type, metric_type)?;
                let iceberg_schema =
                    crate::iceberg::arrow_convert::arrow_to_iceberg_schema(arrow_schema.as_ref())?;

                let location = format!(
                    "{}/{}/{}",
                    self.config.warehouse, self.config.namespace, table_name
                );
                Ok(TableMetadata {
                    format_version: 2,
                    table_uuid: String::new(),
                    location,
                    current_schema_id: 0,
                    schemas: vec![iceberg_schema],
                    default_spec_id: None,
                    partition_specs: None,
                    current_snapshot_id: None,
                    snapshots: None,
                    last_updated_ms: None,
                    properties: None,
                    default_sort_order_id: None,
                    sort_orders: None,
                    last_column_id: None,
                    last_partition_id: None,
                    last_sequence_number: None,
                    metadata_log: None,
                    partition_statistics: None,
                    refs: None,
                    snapshot_log: None,
                    statistics: None,
                })
            }
        }
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
    ) -> Result<TableMetadata> {
        info!("Creating table '{}' from signal type", table_name);

        // Get Arrow schema for signal type
        let arrow_schema = self.get_schema_for_signal(signal_type, metric_type)?;
        debug!(
            "Got Arrow schema with {} fields",
            arrow_schema.fields().len()
        );

        // Convert Arrow schema to Iceberg schema
        let iceberg_schema =
            crate::iceberg::arrow_convert::arrow_to_iceberg_schema(arrow_schema.as_ref())?;
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

        // Return metadata with schema information
        let location = format!(
            "{}/{}/{}",
            self.config.warehouse, self.config.namespace, table_name
        );
        Ok(TableMetadata {
            format_version: 2,
            table_uuid: String::new(),
            location,
            current_schema_id: 0,
            schemas: vec![iceberg_schema],
            default_spec_id: None,
            partition_specs: None,
            current_snapshot_id: None,
            snapshots: None,
            last_updated_ms: None,
            properties: None,
            default_sort_order_id: None,
            sort_orders: None,
            last_column_id: None,
            last_partition_id: None,
            last_sequence_number: None,
            metadata_log: None,
            partition_statistics: None,
            refs: None,
            snapshot_log: None,
            statistics: None,
        })
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
        file_path: &str,
        arrow_batch: &RecordBatch,
    ) -> Result<ParquetWriteResult> {
        use crate::parquet_writer::{writer_properties, Blake3Hash};

        // Serialize to in-memory buffer with hashing
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
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.hasher.update(buf);
                self.buffer.extend_from_slice(buf);
                Ok(buf.len())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        // Write to buffer
        let mut sink = HashingBuffer::new();
        let props = writer_properties().clone();
        let schema = arrow_batch.schema();
        let file_metadata = {
            let mut writer = ArrowWriter::try_new(&mut sink, schema.clone(), Some(props))
                .map_err(|e| anyhow!("Failed to create Arrow writer: {}", e))?;
            writer
                .write(arrow_batch)
                .map_err(|e| anyhow!("Failed to write batch: {}", e))?;
            writer
                .close()
                .map_err(|e| anyhow!("Failed to close writer: {}", e))?
        };

        let (buffer, hash) = sink.finish();
        let file_size = buffer.len() as u64;

        // Build metadata
        let parquet_metadata = Arc::new(
            self.build_parquet_metadata(file_metadata)
                .map_err(|e| anyhow!("Failed to reconstruct parquet metadata: {}", e))?,
        );
        let row_count = parquet_metadata.file_metadata().num_rows();
        let completed_at = Utc::now();

        // Write to storage at exact path
        self.storage
            .operator()
            .write(file_path, buffer)
            .await
            .map_err(|e| anyhow!("Failed to write to storage: {}", e))?;

        Ok(ParquetWriteResult {
            path: file_path.to_string(),
            hash,
            file_size,
            row_count,
            arrow_schema: schema,
            parquet_metadata,
            completed_at,
        })
    }

    fn build_parquet_metadata(
        &self,
        file_metadata: parquet::format::FileMetaData,
    ) -> Result<ParquetMetaData> {
        let schema = types::from_thrift(&file_metadata.schema)
            .map_err(|e| anyhow!("Failed to decode parquet schema: {}", e))?;
        let schema_descr = Arc::new(SchemaDescriptor::new(schema));

        let column_orders =
            self.parse_column_orders(file_metadata.column_orders.as_ref(), &schema_descr)?;

        let mut row_groups = Vec::with_capacity(file_metadata.row_groups.len());
        for row_group in file_metadata.row_groups {
            let metadata = RowGroupMetaData::from_thrift(schema_descr.clone(), row_group)
                .map_err(|e| anyhow!("Failed to decode row group metadata: {}", e))?;
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
        &self,
        orders: Option<&Vec<TColumnOrder>>,
        schema_descr: &SchemaDescriptor,
    ) -> Result<Option<Vec<ColumnOrder>>> {
        match orders {
            Some(order_defs) => {
                if order_defs.len() != schema_descr.num_columns() {
                    return Err(anyhow!("Column order length mismatch"));
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
}

#[cfg(test)]
#[path = "writer_test.rs"]
mod writer_test;
