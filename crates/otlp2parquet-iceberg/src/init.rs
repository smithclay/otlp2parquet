//! Initialization utilities for Iceberg catalog integration.
//!
//! This module provides shared initialization logic used by both Lambda and Server runtimes
//! to avoid code duplication.

use std::collections::HashMap;
use std::sync::Arc;

use crate::{create_rest_catalog, IcebergCommitter, IcebergRestConfig, IcebergTableIdentifier};

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
/// - Creating REST catalog client
/// - Building table identifier map
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

    // Create REST catalog
    let catalog = match create_rest_catalog(&config).await {
        Ok(c) => c,
        Err(e) => return InitResult::CatalogError(e.to_string()),
    };

    // Parse namespace
    let namespace = match config.namespace_ident() {
        Ok(n) => n,
        Err(e) => return InitResult::NamespaceError(e.to_string()),
    };

    // Build table map from config
    let mut tables = HashMap::new();
    for (signal_key, table_name) in &config.tables {
        let table_ident = IcebergTableIdentifier::new(namespace.clone(), table_name.clone());
        tables.insert(signal_key.clone(), table_ident);
    }

    let table_count = tables.len();
    let committer = IcebergCommitter::new(catalog, tables);

    InitResult::Success {
        committer: Arc::new(committer),
        table_count,
    }
}
