//! Initialization utilities for Iceberg catalog integration.
//!
//! This module provides shared initialization logic used by both Lambda and Server runtimes
//! to avoid code duplication.

use std::sync::Arc;

use crate::{
    catalog::NamespaceIdent, IcebergCatalog, IcebergCommitter, IcebergRestConfig, ReqwestHttpClient,
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
/// use otlp2parquet_iceberg::init::{initialize_committer, InitResult};
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

    // Create IcebergCatalog with tables from config
    let table_count = config.tables.len();
    let catalog = IcebergCatalog::new(
        http_client,
        config.rest_uri.clone(),
        namespace,
        config.tables.clone(),
    );

    let committer = IcebergCommitter::new(Arc::new(catalog));

    InitResult::Success {
        committer: Arc::new(committer),
        table_count,
    }
}
