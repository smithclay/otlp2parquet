//! Iceberg REST catalog client
//!
//! Minimal implementation of Iceberg REST API for committing data files.

use crate::http::{HttpClient, HttpResponse};
use crate::protocol::{CommitTransactionRequest, ErrorResponse, TableUpdate};
use crate::types::{DataFile, LoadTableResponse, TableMetadata};
use anyhow::{anyhow, Context, Result};
use std::collections::HashMap;
use tracing::{debug, info, instrument, warn};

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
    /// Base URL of the REST catalog (e.g., "https://s3tables.us-east-1.amazonaws.com/iceberg")
    base_url: String,
    /// Namespace for tables
    namespace: NamespaceIdent,
    /// Map of signal/metric type to table name
    /// Keys: "logs", "traces", "metrics:gauge", etc.
    tables: HashMap<String, String>,
}

impl<T: HttpClient> IcebergCatalog<T> {
    /// Create a new Iceberg catalog client
    pub fn new(
        http: T,
        base_url: String,
        namespace: NamespaceIdent,
        tables: HashMap<String, String>,
    ) -> Self {
        Self {
            http,
            base_url,
            namespace,
            tables,
        }
    }

    /// Load table metadata from the catalog
    ///
    /// Calls: GET /v1/{namespace}/tables/{table}
    #[instrument(skip(self), fields(table = %table_name))]
    async fn load_table(&self, table_name: &str) -> Result<TableMetadata> {
        let url = format!(
            "{}/v1/namespaces/{}/tables/{}",
            self.base_url,
            self.namespace.as_str(),
            table_name
        );

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
    /// Calls: POST /v1/{namespace}/tables/{table}/transactions/commit
    #[instrument(skip(self, data_files), fields(table = %table_name, num_files = data_files.len()))]
    async fn commit_transaction(&self, table_name: &str, data_files: Vec<DataFile>) -> Result<()> {
        let url = format!(
            "{}/v1/namespaces/{}/tables/{}/transactions/commit",
            self.base_url,
            self.namespace.as_str(),
            table_name
        );

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
    #[instrument(skip(self, data_files), fields(num_files = data_files.len()))]
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::{HttpClient, HttpResponse};
    use crate::types::{DataContentType, DataFileFormat};
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
        use crate::types::{
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
        use crate::types::{
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
        use crate::types::{LoadTableResponse, Schema as IcebergSchema, TableMetadata};

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
