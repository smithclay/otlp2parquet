//! Initialization utilities for Iceberg catalog integration.
//!
//! This module provides shared initialization logic used by both Lambda and Server runtimes
//! to avoid code duplication.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use tracing::info;

use super::{
    catalog::{IcebergCatalog, NamespaceIdent},
    http::ReqwestHttpClient,
    IcebergCommitter, IcebergRestConfig,
};

/// Result of Iceberg initialization.
///
/// This enum distinguishes between different failure modes to allow callers
/// to provide appropriate logging messages.
pub enum InitResult {
    /// Successfully initialized with number of tables configured
    Success {
        committer: Arc<IcebergCommitter>,
        table_count: usize,
    },
    /// Iceberg not configured (no REST_URI env var)
    NotConfigured(String),
    /// Failed to create catalog
    CatalogError(String),
    /// Failed to parse namespace
    NamespaceError(String),
}

/// Initialize Iceberg committer from environment variables.
///
/// This function encapsulates the full initialization logic including:
/// - Loading configuration from environment
/// - Creating HTTP client
/// - Creating IcebergCatalog with REST endpoint
/// - Registering tables from configuration
/// - Constructing the committer
///
/// # Returns
///
/// Returns `InitResult` which the caller can pattern match to provide
/// appropriate logging (println! for Lambda, tracing::info! for Server).
///
/// # Example
///
/// ```no_run
/// use otlp2parquet_storage::iceberg::init::{initialize_committer, InitResult};
///
/// # async fn example() {
/// match initialize_committer().await {
///     InitResult::Success { committer, table_count } => {
///         println!("Iceberg catalog enabled with {} tables", table_count);
///     }
///     InitResult::NotConfigured(msg) => {
///         println!("Iceberg not configured: {}", msg);
///     }
///     InitResult::CatalogError(msg) => {
///         println!("Failed to create catalog: {} - continuing without Iceberg", msg);
///     }
///     InitResult::NamespaceError(msg) => {
///         println!("Failed to parse namespace: {} - continuing without Iceberg", msg);
///     }
/// }
/// # }
/// ```
pub async fn initialize_committer() -> InitResult {
    // Load config from environment
    let config = match IcebergRestConfig::from_env() {
        Ok(c) => c,
        Err(e) => {
            return InitResult::NotConfigured(format!(
                "OTLP2PARQUET_ICEBERG_REST_URI not set: {}",
                e
            ))
        }
    };

    // Parse namespace (convert Vec<String> to NamespaceIdent)
    let namespace = match NamespaceIdent::from_vec(config.namespace.clone()) {
        Ok(ns) => ns,
        Err(e) => return InitResult::NamespaceError(format!("Failed to parse namespace: {}", e)),
    };

    // Create HTTP client
    let http_client = match ReqwestHttpClient::new(&config.rest_uri).await {
        Ok(client) => client,
        Err(e) => return InitResult::CatalogError(format!("Failed to create HTTP client: {}", e)),
    };

    // Create IcebergCatalog with tables from config (fetches config to get prefix)
    let table_count = config.tables.len();
    let catalog = match IcebergCatalog::from_config(
        http_client,
        config.rest_uri.clone(),
        namespace,
        config.tables.clone(),
    )
    .await
    {
        Ok(cat) => cat,
        Err(e) => return InitResult::CatalogError(format!("Failed to initialize catalog: {}", e)),
    };

    // Create namespace and tables
    if let Err(e) = initialize_tables(&catalog, &config.tables).await {
        return InitResult::CatalogError(format!("Failed to create tables: {}", e));
    }

    let committer = IcebergCommitter::new(Arc::new(catalog));

    InitResult::Success {
        committer: Arc::new(committer),
        table_count,
    }
}

/// Initialize tables in the catalog by creating namespace and all configured tables
///
/// This function:
/// 1. Creates the namespace (if it doesn't exist)
/// 2. Creates each table with the appropriate schema based on signal type
///
/// # Arguments
///
/// * `catalog` - The Iceberg catalog client
/// * `tables` - Map of signal/metric types to table names
async fn initialize_tables<T: super::http::HttpClient>(
    catalog: &IcebergCatalog<T>,
    tables: &HashMap<String, String>,
) -> Result<()> {
    use otlp2parquet_core::schema;

    // Create namespace first (idempotent - OK if already exists)
    catalog.create_namespace().await?;

    // Create each table based on signal type
    for (signal_key, table_name) in tables {
        let schema = match signal_key.as_str() {
            "logs" => schema::otel_logs_schema(),
            "traces" => schema::otel_traces_schema(),
            "metrics:gauge" => schema::otel_metrics_gauge_schema(),
            "metrics:sum" => schema::otel_metrics_sum_schema(),
            "metrics:histogram" => schema::otel_metrics_histogram_schema(),
            "metrics:exponential_histogram" => schema::otel_metrics_exponential_histogram_schema(),
            "metrics:summary" => schema::otel_metrics_summary_schema(),
            _ => {
                tracing::warn!(
                    "Unknown signal type '{}', skipping table creation",
                    signal_key
                );
                continue;
            }
        };

        info!(
            "Creating table '{}' for signal '{}'",
            table_name, signal_key
        );
        catalog.create_table(table_name, schema).await?;
    }

    Ok(())
}
