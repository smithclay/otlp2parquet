use super::*;

#[test]
fn test_iceberg_config_creation() {
    // Arrange & Act
    let config = IcebergConfig {
        rest_uri: "https://s3tables.us-west-2.amazonaws.com/iceberg".to_string(),
        warehouse: "arn:aws:s3tables:us-west-2:123456789012:bucket/test".to_string(),
        namespace: "otel".to_string(),
    };

    // Assert
    assert!(config.rest_uri.contains("s3tables"));
    assert!(config.warehouse.contains("bucket/test"));
    assert_eq!(config.namespace, "otel");
}

#[test]
fn test_generate_warehouse_path() {
    // Arrange
    let base_location = "s3://bucket/warehouse/otel/logs";
    let signal_type = "logs";

    // Act
    let path = IcebergWriter::generate_warehouse_path(base_location, signal_type);

    // Assert
    assert!(path.starts_with("s3://bucket/warehouse/otel/logs/data/"));
    assert!(path.ends_with(".parquet"));
    // Check that the path contains a timestamp+UUID component
    // Format: {base}/data/{timestamp}-{uuid}.parquet
    assert!(path.len() > "s3://bucket/warehouse/otel/logs/data/".len() + 20);
    // Verify it contains a hyphen between timestamp and UUID
    let data_part = path
        .strip_prefix("s3://bucket/warehouse/otel/logs/data/")
        .unwrap();
    assert!(data_part.contains('-'));
}

#[tokio::test]
async fn test_write_and_commit_integration() {
    use crate::catalog::{IcebergCatalog, NamespaceIdent};
    use crate::http::ReqwestHttpClient;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::collections::HashMap;

    // Arrange - Create test Arrow batch
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("message", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1699564800000000000])),
            Arc::new(StringArray::from(vec!["test message"])),
        ],
    )
    .unwrap();

    // Mock catalog and storage
    let config = IcebergConfig {
        rest_uri: "https://test.com/iceberg".to_string(),
        warehouse: "s3://test-bucket/warehouse".to_string(),
        namespace: "otel".to_string(),
    };

    let http_client = ReqwestHttpClient::new().unwrap();
    let namespace = NamespaceIdent::from_vec(vec!["otel".to_string()]).unwrap();
    let catalog = Arc::new(IcebergCatalog::new(
        http_client,
        config.rest_uri.clone(),
        String::new(),
        namespace,
        HashMap::new(),
    ));

    let temp_dir = std::env::temp_dir();
    let test_root = temp_dir.join("iceberg_writer_test");
    std::fs::create_dir_all(&test_root).unwrap();
    let storage =
        opendal::Operator::new(opendal::services::Fs::default().root(test_root.to_str().unwrap()))
            .unwrap()
            .finish();
    let writer = IcebergWriter::new(catalog, storage, config);

    // Act
    let result = writer.write_and_commit("logs", None, &batch).await;

    // Assert - For now, expect error since catalog is not mocked
    assert!(result.is_err());
}

#[tokio::test]
async fn test_get_or_create_table_loads_existing() {
    // This test documents expected behavior:
    // - Try load_table first
    // - If success, return table metadata with location
    // - If failure, fall through to create
    //
    // Actual implementation requires mocking the catalog response
}

#[tokio::test]
async fn test_get_or_create_table_creates_new() {
    // This test documents expected behavior:
    // - If load_table fails, call create_table
    // - Returns table metadata with warehouse location
    //
    // Actual implementation requires mocking the catalog response
}

#[tokio::test]
async fn test_write_parquet_to_memory() {
    use crate::catalog::{IcebergCatalog, NamespaceIdent};
    use crate::http::ReqwestHttpClient;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use opendal::services;
    use std::collections::HashMap;

    // Arrange
    let storage = opendal::Operator::new(services::Memory::default())
        .unwrap()
        .finish();

    let config = IcebergConfig {
        rest_uri: "https://test.com".to_string(),
        warehouse: "memory://".to_string(),
        namespace: "test".to_string(),
    };

    let http_client = ReqwestHttpClient::new().unwrap();
    let namespace = NamespaceIdent::from_vec(vec!["test".to_string()]).unwrap();
    let catalog = Arc::new(IcebergCatalog::new(
        http_client,
        config.rest_uri.clone(),
        String::new(),
        namespace,
        HashMap::new(),
    ));
    let writer = IcebergWriter::new(catalog, storage.clone(), config);

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();

    // Act
    let result = writer.write_parquet("test/file.parquet", &batch).await;

    // Assert
    assert!(result.is_ok());
    let write_result = result.unwrap();
    assert_eq!(write_result.path, "test/file.parquet");
    assert_eq!(write_result.row_count, 3);
    assert!(write_result.file_size > 0);

    // Verify file exists in storage
    let file_exists = storage.exists("test/file.parquet").await.unwrap();
    assert!(file_exists, "File should exist in storage");

    // Verify file content is valid Parquet
    let file_data = storage.read("test/file.parquet").await.unwrap();
    let bytes = file_data.to_vec();
    assert!(!bytes.is_empty());
    assert_eq!(
        &bytes[0..4],
        b"PAR1",
        "File should have Parquet magic bytes"
    );
}

#[tokio::test]
async fn test_build_data_file() {
    use crate::arrow_convert::arrow_to_iceberg_schema;
    use crate::catalog::{IcebergCatalog, NamespaceIdent};
    use crate::http::ReqwestHttpClient;
    use std::collections::HashMap;

    // Create a simple test setup
    // We'll use the real logs schema from otlp2parquet_core for a realistic test
    let arrow_schema = otlp2parquet_core::schema::otel_logs_schema_arc();

    // Create table metadata with Iceberg schema
    let iceberg_schema = arrow_to_iceberg_schema(&arrow_schema).unwrap();
    let table = TableMetadata {
        format_version: 2,
        table_uuid: "test-uuid".to_string(),
        location: "s3://bucket/warehouse/otel/logs".to_string(),
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
    };

    let config = IcebergConfig {
        rest_uri: "https://test.com".to_string(),
        warehouse: "s3://bucket/warehouse".to_string(),
        namespace: "otel".to_string(),
    };

    let http_client = ReqwestHttpClient::new().unwrap();
    let namespace = NamespaceIdent::from_vec(vec!["otel".to_string()]).unwrap();
    let catalog = Arc::new(IcebergCatalog::new(
        http_client,
        config.rest_uri.clone(),
        String::new(),
        namespace,
        HashMap::new(),
    ));

    let storage = opendal::Operator::new(opendal::services::Memory::default())
        .unwrap()
        .finish();
    let writer = IcebergWriter::new(catalog, storage.clone(), config);

    // Create a real record batch and write it to get a ParquetWriteResult
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;

    let test_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("message", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        test_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1699564800000000000])),
            Arc::new(StringArray::from(vec!["test message"])),
        ],
    )
    .unwrap();

    // Write the batch to get a ParquetWriteResult
    let write_result = writer
        .write_parquet("test/file.parquet", &batch)
        .await
        .unwrap();

    // Act - build the DataFile from the write result
    let iceberg_schema = table.schemas.first().unwrap();
    let data_file =
        crate::datafile_convert::build_data_file(&write_result, iceberg_schema).unwrap();

    // Assert
    assert_eq!(data_file.file_path, write_result.path);
    assert_eq!(data_file.record_count as i64, write_result.row_count);
    assert_eq!(data_file.file_size_in_bytes, write_result.file_size);
}

#[tokio::test]
async fn test_commit_data_file() {
    // This test documents expected catalog commit behavior:
    // - Calls catalog.commit_transaction with table name and data_file
    // - Returns Ok(()) on success
    // - Logs warning but doesn't fail on catalog errors (warn-and-succeed pattern)
    //
    // The actual catalog API signature is:
    // async fn commit_transaction(&self, table_name: &str, data_files: Vec<DataFile>) -> Result<()>
    //
    // Since commit_transaction is private, the implementation will need to either:
    // 1. Make it public, or
    // 2. Use a different approach
    //
    // For now, this test documents the expected behavior.
    // Actual implementation will determine if we need to refactor catalog API.
}
