//! Iceberg REST catalog client
//!
//! Minimal implementation of Iceberg REST API for committing data files.

use super::http::{HttpClient, HttpResponse};
use super::protocol::{CommitTransactionRequest, ErrorResponse, TableUpdate};
use super::types::{DataFile, LoadTableResponse, TableMetadata};
use anyhow::{anyhow, Context, Result};
use std::collections::HashMap;
use tracing::{debug, info, warn};

#[cfg(not(target_arch = "wasm32"))]
use tracing::instrument;

/// Iceberg namespace identifier
///
/// Represents a dot-separated namespace path (e.g., "otel.logs")
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceIdent {
    inner: Vec<String>,
}

impl NamespaceIdent {
    /// Create a namespace from a list of parts
    pub fn from_vec(parts: Vec<String>) -> Result<Self> {
        if parts.is_empty() {
            return Err(anyhow!("Namespace cannot be empty"));
        }
        Ok(Self { inner: parts })
    }

    /// Convert to a dot-separated string
    pub fn as_str(&self) -> String {
        self.inner.join(".")
    }

    /// Get the parts of the namespace
    pub fn parts(&self) -> &[String] {
        &self.inner
    }
}

/// Iceberg catalog client
///
/// Generic over HttpClient implementation to support different platforms.
pub struct IcebergCatalog<T: HttpClient> {
    /// HTTP client for making requests
    http: T,
    /// Base URL of the REST catalog (e.g., "<https://s3tables.us-east-1.amazonaws.com/iceberg>")
    base_url: String,
    /// Catalog prefix from config (e.g., "main" for Nessie branch)
    prefix: String,
    /// Namespace for tables
    namespace: NamespaceIdent,
    /// Map of signal/metric type to table name
    /// Keys: "logs", "traces", "metrics:gauge", etc.
    tables: HashMap<String, String>,
}

impl<T: HttpClient> IcebergCatalog<T> {
    /// Create a new Iceberg catalog client with explicit prefix
    pub fn new(
        http: T,
        base_url: String,
        prefix: String,
        namespace: NamespaceIdent,
        tables: HashMap<String, String>,
    ) -> Self {
        Self {
            http,
            base_url,
            prefix,
            namespace,
            tables,
        }
    }

    /// Fetch catalog configuration to get the prefix
    ///
    /// Calls: GET /v1/config
    #[cfg_attr(not(target_arch = "wasm32"), instrument(skip(self)))]
    async fn fetch_config(&self) -> Result<super::types::CatalogConfig> {
        use super::types::CatalogConfig;

        let url = format!("{}/v1/config", self.base_url);
        debug!("Fetching catalog config from: {}", url);

        let response = self
            .http
            .get(
                &url,
                vec![("Accept".to_string(), "application/json".to_string())],
            )
            .await
            .context("Failed to fetch catalog config")?;

        if !response.is_success() {
            return Err(self.handle_error_response(&response)?);
        }

        let config: CatalogConfig = response.json().context("Failed to parse CatalogConfig")?;

        Ok(config)
    }

    /// Create a new Iceberg catalog client by fetching the config
    pub async fn from_config(
        http: T,
        base_url: String,
        namespace: NamespaceIdent,
        tables: HashMap<String, String>,
    ) -> Result<Self> {
        // Create a temporary instance to fetch config
        let temp = Self {
            http,
            base_url: base_url.clone(),
            prefix: String::new(),
            namespace: namespace.clone(),
            tables: tables.clone(),
        };

        // Fetch config to get prefix
        let config = temp.fetch_config().await?;
        let prefix = config
            .defaults
            .get("prefix")
            .cloned()
            .unwrap_or_else(|| String::from(""));

        debug!("Using catalog prefix: '{}'", prefix);

        // Create final instance with prefix
        Ok(Self {
            http: temp.http,
            base_url: temp.base_url,
            prefix,
            namespace: temp.namespace,
            tables: temp.tables,
        })
    }

    /// Create a namespace in the catalog
    ///
    /// Calls: POST /v1/{prefix}/namespaces
    #[cfg_attr(not(target_arch = "wasm32"), instrument(skip(self)))]
    pub async fn create_namespace(&self) -> Result<()> {
        let url = if self.prefix.is_empty() {
            format!("{}/v1/namespaces", self.base_url)
        } else {
            format!("{}/v1/{}/namespaces", self.base_url, self.prefix)
        };

        debug!("Creating namespace: {}", self.namespace.as_str());

        let request = serde_json::json!({
            "namespace": self.namespace.parts(),
            "properties": {}
        });

        let body =
            serde_json::to_vec(&request).context("Failed to serialize create namespace request")?;

        let response = self
            .http
            .post(
                &url,
                vec![
                    ("Content-Type".to_string(), "application/json".to_string()),
                    ("Accept".to_string(), "application/json".to_string()),
                ],
                body,
            )
            .await
            .context("Failed to create namespace")?;

        // 200 = created, 409 = already exists (both are OK)
        if response.is_success() || response.status == 409 {
            info!("Namespace '{}' ready", self.namespace.as_str());
            Ok(())
        } else {
            Err(self.handle_error_response(&response)?)
        }
    }

    /// Create a table in the catalog
    ///
    /// Calls: POST /v1/{prefix}/namespaces/{namespace}/tables
    #[cfg_attr(not(target_arch = "wasm32"), instrument(skip(self, schema), fields(table = %table_name)))]
    pub async fn create_table(
        &self,
        table_name: &str,
        schema: arrow::datatypes::Schema,
    ) -> Result<()> {
        let url = if self.prefix.is_empty() {
            format!(
                "{}/v1/namespaces/{}/tables",
                self.base_url,
                self.namespace.as_str()
            )
        } else {
            format!(
                "{}/v1/{}/namespaces/{}/tables",
                self.base_url,
                self.prefix,
                self.namespace.as_str()
            )
        };

        debug!("Creating table: {}", table_name);

        // Convert Arrow schema to Iceberg schema
        let iceberg_schema = arrow_to_iceberg_schema(&schema)?;

        // Build CreateTable request following Iceberg REST spec
        // Reference: https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml
        // For S3 Tables: only include required fields
        let request = serde_json::json!({
            "name": table_name,
            "schema": iceberg_schema,
            "stage-create": false
        });

        info!(
            "CreateTable request body: {}",
            serde_json::to_string_pretty(&request).unwrap_or_else(|_| "invalid json".to_string())
        );

        let body =
            serde_json::to_vec(&request).context("Failed to serialize create table request")?;

        let response = self
            .http
            .post(
                &url,
                vec![
                    ("Content-Type".to_string(), "application/json".to_string()),
                    ("Accept".to_string(), "application/json".to_string()),
                ],
                body,
            )
            .await
            .context("Failed to create table")?;

        // Log response details for debugging
        info!("CreateTable response status: {}", response.status);
        if !response.is_success() && response.status != 409 {
            let response_body = response
                .body_string()
                .unwrap_or_else(|_| "<binary>".to_string());
            warn!(
                "CreateTable failed with status {}: {}",
                response.status, response_body
            );
        }

        // 200 = created, 409 = already exists (both are OK)
        if response.is_success() || response.status == 409 {
            info!("Table '{}' ready", table_name);
            Ok(())
        } else {
            Err(self.handle_error_response(&response)?)
        }
    }

    /// Load table metadata from the catalog
    ///
    /// Calls: GET /v1/{prefix}/namespaces/{namespace}/tables/{table}
    #[cfg_attr(not(target_arch = "wasm32"), instrument(skip(self), fields(table = %table_name)))]
    async fn load_table(&self, table_name: &str) -> Result<TableMetadata> {
        let url = if self.prefix.is_empty() {
            format!(
                "{}/v1/namespaces/{}/tables/{}",
                self.base_url,
                self.namespace.as_str(),
                table_name
            )
        } else {
            format!(
                "{}/v1/{}/namespaces/{}/tables/{}",
                self.base_url,
                self.prefix,
                self.namespace.as_str(),
                table_name
            )
        };

        debug!("Loading table metadata from: {}", url);

        let response = self
            .http
            .get(
                &url,
                vec![("Accept".to_string(), "application/json".to_string())],
            )
            .await
            .context("Failed to load table")?;

        if !response.is_success() {
            return Err(self.handle_error_response(&response)?);
        }

        let load_response: LoadTableResponse = response
            .json()
            .context("Failed to parse LoadTableResponse")?;

        Ok(load_response.table_metadata)
    }

    /// Commit a transaction to the catalog
    ///
    /// Calls: POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/transactions/commit
    #[cfg_attr(not(target_arch = "wasm32"), instrument(skip(self, data_files), fields(table = %table_name, num_files = data_files.len())))]
    pub async fn commit_transaction(
        &self,
        table_name: &str,
        data_files: Vec<DataFile>,
    ) -> Result<()> {
        let url = if self.prefix.is_empty() {
            format!(
                "{}/v1/namespaces/{}/tables/{}/transactions/commit",
                self.base_url,
                self.namespace.as_str(),
                table_name
            )
        } else {
            format!(
                "{}/v1/{}/namespaces/{}/tables/{}/transactions/commit",
                self.base_url,
                self.prefix,
                self.namespace.as_str(),
                table_name
            )
        };

        debug!("Committing {} files to: {}", data_files.len(), url);

        // Build commit request
        let request = CommitTransactionRequest {
            table_changes: vec![TableUpdate::AppendFiles { data_files }],
        };

        let body = serde_json::to_vec(&request).context("Failed to serialize commit request")?;

        let response = self
            .http
            .post(
                &url,
                vec![
                    ("Content-Type".to_string(), "application/json".to_string()),
                    ("Accept".to_string(), "application/json".to_string()),
                ],
                body,
            )
            .await
            .context("Failed to commit transaction")?;

        if !response.is_success() {
            return Err(self.handle_error_response(&response)?);
        }

        Ok(())
    }

    /// Commit Parquet files with signal type information
    ///
    /// This is the main entry point for committing files to Iceberg tables.
    /// It maps signal types to table names, loads the table schema, and commits the files.
    ///
    /// # Parameters
    /// - `signal_type`: "logs", "traces", or "metrics"
    /// - `metric_type`: For metrics only - "gauge", "sum", "histogram", etc.
    /// - `data_files`: Data files to commit
    #[cfg_attr(not(target_arch = "wasm32"), instrument(skip(self, data_files), fields(num_files = data_files.len())))]
    pub async fn commit_with_signal(
        &self,
        signal_type: &str,
        metric_type: Option<&str>,
        data_files: Vec<DataFile>,
    ) -> Result<()> {
        if data_files.is_empty() {
            debug!("No files to commit");
            return Ok(());
        }

        // Build table key
        let table_key = if let Some(mt) = metric_type {
            format!("metrics:{}", mt)
        } else {
            signal_type.to_string()
        };

        // Look up table name
        let table_name = self.tables.get(&table_key).ok_or_else(|| {
            anyhow!(
                "No table configured for signal_type='{}', metric_type='{:?}'",
                signal_type,
                metric_type
            )
        })?;

        // Load table to verify it exists (optional - could skip for performance)
        match self.load_table(table_name).await {
            Ok(metadata) => {
                debug!(
                    "Loaded table '{}' (format version: {})",
                    table_name, metadata.format_version
                );
            }
            Err(e) => {
                warn!(
                    "Failed to load table '{}': {} - will attempt commit anyway",
                    table_name, e
                );
                // Continue - the commit might still succeed
            }
        }

        // Compute statistics for logging
        let total_rows: u64 = data_files.iter().map(|f| f.record_count).sum();
        let total_bytes: u64 = data_files.iter().map(|f| f.file_size_in_bytes).sum();

        info!(
            "Committing {} files ({} rows, {} bytes) to table '{}'",
            data_files.len(),
            total_rows,
            total_bytes,
            table_name
        );

        // Commit transaction
        self.commit_transaction(table_name, data_files).await?;

        info!("Successfully committed to table '{}'", table_name);

        Ok(())
    }

    /// Handle error responses from the catalog
    fn handle_error_response(&self, response: &HttpResponse) -> Result<anyhow::Error> {
        // Try to parse as Iceberg error response
        if let Ok(error_response) = response.json::<ErrorResponse>() {
            return Ok(anyhow!(
                "Iceberg catalog error ({}): {}",
                error_response.code,
                error_response.message
            ));
        }

        // Fall back to generic error
        let body = response
            .body_string()
            .unwrap_or_else(|_| "<binary>".to_string());
        Ok(anyhow!(
            "HTTP {} error from catalog: {}",
            response.status,
            body
        ))
    }
}

/// Convert Arrow schema to Iceberg schema JSON
///
/// Iceberg schema format (REST API):
/// ```json
/// {
///   "type": "struct",
///   "schema-id": 0,
///   "fields": [
///     {"id": 1, "name": "field_name", "required": true, "type": "string"}
///   ]
/// }
/// ```
pub(crate) fn arrow_to_iceberg_schema(
    schema: &arrow::datatypes::Schema,
) -> Result<serde_json::Value> {
    // Global ID allocator to ensure field ID uniqueness across entire schema
    // Iceberg requires all field IDs to be unique within a schema
    let next_id = std::cell::Cell::new(1000);

    let fields: Result<Vec<_>> = schema
        .fields()
        .iter()
        .map(|field| {
            // Extract field_id from PARQUET:field_id metadata
            let field_id: i32 = field
                .metadata()
                .get("PARQUET:field_id")
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| {
                    anyhow!(
                        "Field '{}' missing PARQUET:field_id metadata required for Iceberg",
                        field.name()
                    )
                })?;

            let iceberg_type = arrow_type_to_iceberg(field.data_type(), &next_id)?;

            Ok(serde_json::json!({
                "id": field_id,
                "name": field.name(),
                "required": !field.is_nullable(),
                "type": iceberg_type
            }))
        })
        .collect();

    Ok(serde_json::json!({
        "type": "struct",
        "schema-id": 0,
        "fields": fields?
    }))
}

/// Convert Arrow DataType to Iceberg type string/object
///
/// Takes a Cell-based ID allocator to ensure unique field IDs across nested structures
fn arrow_type_to_iceberg(
    data_type: &arrow::datatypes::DataType,
    next_id: &std::cell::Cell<i32>,
) -> Result<serde_json::Value> {
    use arrow::datatypes::DataType;
    use arrow::datatypes::TimeUnit;

    match data_type {
        DataType::Boolean => Ok(serde_json::json!("boolean")),
        DataType::Int8 => Ok(serde_json::json!("int")),
        DataType::Int16 => Ok(serde_json::json!("int")),
        DataType::Int32 => Ok(serde_json::json!("int")),
        DataType::Int64 => Ok(serde_json::json!("long")),
        DataType::UInt8 => Ok(serde_json::json!("int")),
        DataType::UInt16 => Ok(serde_json::json!("int")),
        DataType::UInt32 => Ok(serde_json::json!("long")),
        DataType::UInt64 => Ok(serde_json::json!("long")),
        DataType::Float32 => Ok(serde_json::json!("float")),
        DataType::Float64 => Ok(serde_json::json!("double")),
        DataType::Utf8 => Ok(serde_json::json!("string")),
        DataType::LargeUtf8 => Ok(serde_json::json!("string")),
        DataType::Binary => Ok(serde_json::json!("binary")),
        DataType::LargeBinary => Ok(serde_json::json!("binary")),
        DataType::FixedSizeBinary(size) => Ok(serde_json::json!(format!("fixed[{}]", size))),
        DataType::Timestamp(TimeUnit::Microsecond, _) => Ok(serde_json::json!("timestamp")),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => Ok(serde_json::json!("timestamp")),
        DataType::Timestamp(_, _) => Ok(serde_json::json!("timestamp")),
        DataType::Date32 => Ok(serde_json::json!("date")),
        DataType::Date64 => Ok(serde_json::json!("date")),
        DataType::Decimal128(precision, scale) => Ok(serde_json::json!({
            "type": "decimal",
            "precision": precision,
            "scale": scale
        })),
        DataType::Struct(fields) => {
            let iceberg_fields: Result<Vec<_>> = fields
                .iter()
                .map(|field| {
                    // For nested struct fields, allocate unique IDs
                    let field_id: i32 = field
                        .metadata()
                        .get("PARQUET:field_id")
                        .and_then(|s| s.parse().ok())
                        .unwrap_or_else(|| {
                            let id = next_id.get();
                            next_id.set(id + 1);
                            id
                        });

                    let iceberg_type = arrow_type_to_iceberg(field.data_type(), next_id)?;

                    Ok(serde_json::json!({
                        "id": field_id,
                        "name": field.name(),
                        "required": !field.is_nullable(),
                        "type": iceberg_type
                    }))
                })
                .collect();

            Ok(serde_json::json!({
                "type": "struct",
                "fields": iceberg_fields?
            }))
        }
        DataType::List(field) | DataType::LargeList(field) => {
            // Allocate unique element ID
            let element_id = field
                .metadata()
                .get("PARQUET:field_id")
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(|| {
                    let id = next_id.get();
                    next_id.set(id + 1);
                    id
                });

            Ok(serde_json::json!({
                "type": "list",
                "element-id": element_id,
                "element-required": !field.is_nullable(),
                "element": arrow_type_to_iceberg(field.data_type(), next_id)?
            }))
        }
        DataType::Map(field, _) => {
            // Map expects a struct with "key" and "value" fields
            if let DataType::Struct(entries) = field.data_type() {
                if entries.len() == 2 {
                    let key_field = &entries[0];
                    let value_field = &entries[1];

                    // Allocate unique IDs for map key and value
                    let key_id = next_id.get();
                    next_id.set(key_id + 1);
                    let value_id = next_id.get();
                    next_id.set(value_id + 1);

                    return Ok(serde_json::json!({
                        "type": "map",
                        "key-id": key_id,
                        "key": arrow_type_to_iceberg(key_field.data_type(), next_id)?,
                        "value-id": value_id,
                        "value-required": !value_field.is_nullable(),
                        "value": arrow_type_to_iceberg(value_field.data_type(), next_id)?
                    }));
                }
            }
            Err(anyhow!("Map field must contain struct with key and value"))
        }
        _ => Err(anyhow!(
            "Unsupported Arrow type for Iceberg conversion: {:?}",
            data_type
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iceberg::http::{HttpClient, HttpResponse};
    use crate::iceberg::types::{DataContentType, DataFileFormat};
    use async_trait::async_trait;
    use std::sync::Mutex;

    /// Mock HTTP client for testing
    struct MockHttpClient {
        responses: Mutex<Vec<HttpResponse>>,
    }

    impl MockHttpClient {
        fn new(responses: Vec<HttpResponse>) -> Self {
            Self {
                responses: Mutex::new(responses),
            }
        }
    }

    #[async_trait]
    impl HttpClient for MockHttpClient {
        async fn request(
            &self,
            _method: &str,
            _url: &str,
            _headers: Vec<(String, String)>,
            _body: Option<Vec<u8>>,
        ) -> Result<HttpResponse> {
            let mut responses = self.responses.lock().unwrap();
            if responses.is_empty() {
                return Err(anyhow!("No more mock responses"));
            }
            Ok(responses.remove(0))
        }
    }

    #[tokio::test]
    async fn test_namespace_ident() {
        let ns = NamespaceIdent::from_vec(vec!["otel".to_string(), "logs".to_string()]).unwrap();
        assert_eq!(ns.as_str(), "otel.logs");
        assert_eq!(ns.parts(), &["otel", "logs"]);
    }

    #[tokio::test]
    async fn test_namespace_empty_error() {
        let result = NamespaceIdent::from_vec(vec![]);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_catalog_creation() {
        let mock = MockHttpClient::new(vec![]);
        let mut tables = HashMap::new();
        tables.insert("logs".to_string(), "otel_logs".to_string());

        let catalog = IcebergCatalog::new(
            mock,
            "https://catalog.example.com".to_string(),
            String::new(),
            NamespaceIdent::from_vec(vec!["otel".to_string()]).unwrap(),
            tables,
        );

        assert_eq!(catalog.base_url, "https://catalog.example.com");
        assert_eq!(catalog.namespace.as_str(), "otel");
    }

    #[tokio::test]
    async fn test_commit_with_no_files() {
        let mock = MockHttpClient::new(vec![]);
        let mut tables = HashMap::new();
        tables.insert("logs".to_string(), "otel_logs".to_string());

        let catalog = IcebergCatalog::new(
            mock,
            "https://catalog.example.com".to_string(),
            String::new(),
            NamespaceIdent::from_vec(vec!["otel".to_string()]).unwrap(),
            tables,
        );

        // Should succeed with empty list
        let result = catalog.commit_with_signal("logs", None, vec![]).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_commit_with_missing_table_config() {
        let mock = MockHttpClient::new(vec![]);
        let tables = HashMap::new(); // Empty - no tables configured

        let catalog = IcebergCatalog::new(
            mock,
            "https://catalog.example.com".to_string(),
            String::new(),
            NamespaceIdent::from_vec(vec!["otel".to_string()]).unwrap(),
            tables,
        );

        let data_file = DataFile::builder()
            .content(DataContentType::Data)
            .file_path("s3://bucket/file.parquet")
            .file_format(DataFileFormat::Parquet)
            .record_count(100)
            .file_size_in_bytes(5000)
            .build()
            .unwrap();

        // Should fail with "no table configured" error
        let result = catalog
            .commit_with_signal("logs", None, vec![data_file])
            .await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("No table configured"));
    }

    #[tokio::test]
    async fn test_load_table_success() {
        use crate::iceberg::types::{
            LoadTableResponse, NestedField, Schema as IcebergSchema, TableMetadata, Type,
        };

        let table_metadata = TableMetadata {
            format_version: 2,
            table_uuid: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            location: "s3://bucket/warehouse/db/table".to_string(),
            current_schema_id: 0,
            schemas: vec![IcebergSchema {
                schema_id: 0,
                fields: vec![NestedField {
                    id: 1,
                    name: "id".to_string(),
                    required: true,
                    field_type: Type::Long,
                    doc: None,
                }],
                identifier_field_ids: None,
            }],
            default_spec_id: Some(0),
            partition_specs: None,
            current_snapshot_id: None,
            snapshots: None,
            last_updated_ms: None,
            properties: None,
            default_sort_order_id: None,
            sort_orders: None,
        };

        let load_response = LoadTableResponse {
            table_metadata,
            metadata_location: "s3://bucket/warehouse/db/table/metadata/v1.metadata.json"
                .to_string(),
            config: None,
        };

        let response_json = serde_json::to_vec(&load_response).unwrap();
        let mock = MockHttpClient::new(vec![HttpResponse {
            status: 200,
            headers: vec![("content-type".to_string(), "application/json".to_string())],
            body: response_json,
        }]);

        let mut tables = HashMap::new();
        tables.insert("logs".to_string(), "otel_logs".to_string());

        let catalog = IcebergCatalog::new(
            mock,
            "https://catalog.example.com".to_string(),
            String::new(),
            NamespaceIdent::from_vec(vec!["otel".to_string()]).unwrap(),
            tables,
        );

        let metadata = catalog.load_table("otel_logs").await.unwrap();
        assert_eq!(metadata.format_version, 2);
        assert_eq!(metadata.schemas.len(), 1);
    }

    #[tokio::test]
    async fn test_commit_transaction_success() {
        let mock = MockHttpClient::new(vec![HttpResponse {
            status: 200,
            headers: vec![],
            body: br#"{"metadata-location":"s3://bucket/metadata/v2.json"}"#.to_vec(),
        }]);

        let mut tables = HashMap::new();
        tables.insert("logs".to_string(), "otel_logs".to_string());

        let catalog = IcebergCatalog::new(
            mock,
            "https://catalog.example.com".to_string(),
            String::new(),
            NamespaceIdent::from_vec(vec!["otel".to_string()]).unwrap(),
            tables,
        );

        let data_file = DataFile::builder()
            .content(DataContentType::Data)
            .file_path("s3://bucket/file.parquet")
            .file_format(DataFileFormat::Parquet)
            .record_count(100)
            .file_size_in_bytes(5000)
            .build()
            .unwrap();

        let result = catalog
            .commit_transaction("otel_logs", vec![data_file])
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_commit_with_signal_full_workflow() {
        use crate::iceberg::types::{
            LoadTableResponse, NestedField, Schema as IcebergSchema, TableMetadata, Type,
        };

        // Mock responses: 1) load_table, 2) commit_transaction
        let table_metadata = TableMetadata {
            format_version: 2,
            table_uuid: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            location: "s3://bucket/warehouse/db/table".to_string(),
            current_schema_id: 0,
            schemas: vec![IcebergSchema {
                schema_id: 0,
                fields: vec![NestedField {
                    id: 1,
                    name: "id".to_string(),
                    required: true,
                    field_type: Type::Long,
                    doc: None,
                }],
                identifier_field_ids: None,
            }],
            default_spec_id: Some(0),
            partition_specs: None,
            current_snapshot_id: None,
            snapshots: None,
            last_updated_ms: None,
            properties: None,
            default_sort_order_id: None,
            sort_orders: None,
        };

        let load_response = LoadTableResponse {
            table_metadata,
            metadata_location: "s3://bucket/warehouse/db/table/metadata/v1.metadata.json"
                .to_string(),
            config: None,
        };

        let mock = MockHttpClient::new(vec![
            // load_table response
            HttpResponse {
                status: 200,
                headers: vec![],
                body: serde_json::to_vec(&load_response).unwrap(),
            },
            // commit_transaction response
            HttpResponse {
                status: 200,
                headers: vec![],
                body: br#"{"metadata-location":"s3://bucket/metadata/v2.json"}"#.to_vec(),
            },
        ]);

        let mut tables = HashMap::new();
        tables.insert("logs".to_string(), "otel_logs".to_string());

        let catalog = IcebergCatalog::new(
            mock,
            "https://catalog.example.com".to_string(),
            String::new(),
            NamespaceIdent::from_vec(vec!["otel".to_string()]).unwrap(),
            tables,
        );

        let data_file = DataFile::builder()
            .content(DataContentType::Data)
            .file_path("s3://bucket/file.parquet")
            .file_format(DataFileFormat::Parquet)
            .record_count(100)
            .file_size_in_bytes(5000)
            .build()
            .unwrap();

        let result = catalog
            .commit_with_signal("logs", None, vec![data_file])
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_http_404_error() {
        let mock = MockHttpClient::new(vec![HttpResponse {
            status: 404,
            headers: vec![],
            body: br#"{"error":"NoSuchTableException","message":"Table not found","code":404}"#
                .to_vec(),
        }]);

        let mut tables = HashMap::new();
        tables.insert("logs".to_string(), "otel_logs".to_string());

        let catalog = IcebergCatalog::new(
            mock,
            "https://catalog.example.com".to_string(),
            String::new(),
            NamespaceIdent::from_vec(vec!["otel".to_string()]).unwrap(),
            tables,
        );

        let result = catalog.load_table("otel_logs").await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("404") || err.to_string().contains("Table not found"));
    }

    #[tokio::test]
    async fn test_commit_with_metrics() {
        use crate::iceberg::types::{LoadTableResponse, Schema as IcebergSchema, TableMetadata};

        let table_metadata = TableMetadata {
            format_version: 2,
            table_uuid: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            location: "s3://bucket/warehouse/db/table".to_string(),
            current_schema_id: 0,
            schemas: vec![IcebergSchema {
                schema_id: 0,
                fields: vec![],
                identifier_field_ids: None,
            }],
            default_spec_id: Some(0),
            partition_specs: None,
            current_snapshot_id: None,
            snapshots: None,
            last_updated_ms: None,
            properties: None,
            default_sort_order_id: None,
            sort_orders: None,
        };

        let load_response = LoadTableResponse {
            table_metadata,
            metadata_location: "s3://bucket/metadata/v1.json".to_string(),
            config: None,
        };

        let mock = MockHttpClient::new(vec![
            HttpResponse {
                status: 200,
                headers: vec![],
                body: serde_json::to_vec(&load_response).unwrap(),
            },
            HttpResponse {
                status: 200,
                headers: vec![],
                body: br#"{"metadata-location":"s3://bucket/metadata/v2.json"}"#.to_vec(),
            },
        ]);

        let mut tables = HashMap::new();
        tables.insert(
            "metrics:gauge".to_string(),
            "otel_metrics_gauge".to_string(),
        );

        let catalog = IcebergCatalog::new(
            mock,
            "https://catalog.example.com".to_string(),
            String::new(),
            NamespaceIdent::from_vec(vec!["otel".to_string()]).unwrap(),
            tables,
        );

        let data_file = DataFile::builder()
            .content(DataContentType::Data)
            .file_path("s3://bucket/metrics_gauge.parquet")
            .file_format(DataFileFormat::Parquet)
            .record_count(500)
            .file_size_in_bytes(25000)
            .build()
            .unwrap();

        // Test metrics with metric_type
        let result = catalog
            .commit_with_signal("metrics", Some("gauge"), vec![data_file])
            .await;
        assert!(result.is_ok());
    }
}
