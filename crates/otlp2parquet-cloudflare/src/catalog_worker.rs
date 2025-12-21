//! Cron-triggered Catalog Worker for batch Iceberg commits.
//!
//! Runs every 5 minutes to:
//! 1. Read pending file receipts from KV
//! 2. Group files by Iceberg table
//! 3. Batch-commit to R2 Data Catalog
//! 4. Delete processed KV keys

use crate::do_config::{ensure_storage_initialized, WorkerEnvSource};
use crate::errors::{ConfigValidationError, OtlpErrorKind};
use async_trait::async_trait;
use futures::stream::{self, StreamExt};
use otlp2parquet_core::config::{EnvSource, RuntimeConfig};
use otlp2parquet_writer::{
    ensure_namespace,
    icepick::{
        catalog::register::{introspect_parquet_file, register_data_files, RegisterOptions},
        catalog::Catalog,
        spec::{NamespaceIdent, PartitionField, PartitionSpec, TableIdent},
        FileIO, Schema, SchemaEvolutionPolicy,
    },
    initialize_catalog, CatalogConfig, CatalogType,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use worker::{kv::KvStore, Env, Result};

/// Maximum number of catalog registration attempts before moving to dead-letter queue.
const MAX_CATALOG_RETRIES: u32 = 5;
/// 16 concurrent KV fetches per page.
/// Balances parallelism vs. Workers CPU time limits (~50ms wall clock).
/// Higher values risk hitting execution time limits on large pages.
const MAX_KV_FETCH_CONCURRENCY: usize = 16;

/// Maximum concurrent KV delete operations.
/// Matches fetch concurrency to stay within Workers CPU time limits.
const MAX_KV_DELETE_CONCURRENCY: usize = 16;

/// Pending file receipt stored in KV
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PendingFile {
    /// R2 path to the Parquet file
    pub path: String,
    /// Iceberg table name (e.g., "otel_logs", "otel_metrics_gauge")
    pub table: String,
    /// Number of rows in the file
    pub rows: usize,
    /// Timestamp when file was written
    pub timestamp_ms: i64,
    /// Number of failed catalog registration attempts (0 = first attempt)
    #[serde(default)]
    pub retry_count: u32,
    /// KV key (populated during list)
    #[serde(skip)]
    pub key: String,
}

/// Catalog sync summary for observability/test hooks.
#[derive(Debug, Clone, Serialize)]
pub struct CatalogSyncReport {
    pub namespace: String,
    pub tables: Vec<TableCommitResult>,
}

impl CatalogSyncReport {
    fn skipped() -> Self {
        Self {
            namespace: "unknown".to_string(),
            tables: Vec::new(),
        }
    }

    fn empty() -> Self {
        Self {
            namespace: "unknown".to_string(),
            tables: Vec::new(),
        }
    }
}

/// Per-table commit result.
#[derive(Debug, Clone, Serialize)]
pub struct TableCommitResult {
    pub table: String,
    pub files: usize,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Check if an error indicates a permanent failure that should not be retried.
/// NotFound errors mean the file doesn't exist in R2 and never will.
fn is_permanent_failure(error: &str) -> bool {
    error.contains("NotFound")
}

/// Move a file to the dead-letter queue after exhausting retries.
async fn move_to_dead_letter(kv: &KvStore, file: &PendingFile, reason: &str) {
    tracing::error!(
        path = %file.path,
        table = %file.table,
        retries = file.retry_count + 1,
        reason = %reason,
        "Moving to dead-letter queue"
    );

    let dead_key = file.key.replace("pending:", "dead:");
    let dead_file = PendingFile {
        retry_count: file.retry_count + 1,
        ..file.clone()
    };

    if let Ok(json) = serde_json::to_string(&dead_file) {
        match kv.put(&dead_key, json) {
            Ok(builder) => {
                if let Err(exec_err) = builder.execute().await {
                    tracing::warn!(
                        error = %exec_err,
                        dead_key = %dead_key,
                        "Failed to execute dead-letter write"
                    );
                }
            }
            Err(kv_err) => {
                tracing::warn!(
                    error = %kv_err,
                    dead_key = %dead_key,
                    "Failed to prepare dead-letter entry"
                );
            }
        }
    }

    // Delete the original pending key
    if let Err(del_err) = kv.delete(&file.key).await {
        tracing::warn!(
            error = %del_err,
            key = %file.key,
            "Failed to delete pending key after dead-letter move"
        );
    }
}

/// Increment retry count for a file in KV.
async fn increment_retry_count(kv: &KvStore, file: &PendingFile) {
    let updated_file = PendingFile {
        retry_count: file.retry_count + 1,
        ..file.clone()
    };

    if let Ok(json) = serde_json::to_string(&updated_file) {
        match kv.put(&file.key, json) {
            Ok(builder) => {
                if let Err(exec_err) = builder.execute().await {
                    tracing::warn!(
                        error = %exec_err,
                        key = %file.key,
                        "Failed to update retry count"
                    );
                } else {
                    tracing::debug!(
                        key = %file.key,
                        retry_count = updated_file.retry_count,
                        "Updated retry count for next attempt"
                    );
                }
            }
            Err(kv_err) => {
                tracing::warn!(
                    error = %kv_err,
                    key = %file.key,
                    "Failed to prepare retry count update"
                );
            }
        }
    }
}

/// Handle commit failure for a batch of files.
/// Either moves to dead-letter queue or increments retry count.
/// Permanent failures (like NotFound) bypass retry logic entirely.
async fn handle_commit_failure(kv: &KvStore, files: &[&PendingFile], error: &str) {
    let is_permanent = is_permanent_failure(error);

    for file in files {
        if is_permanent {
            // Permanent failure - move directly to DLQ without wasting retries
            move_to_dead_letter(kv, file, "file not found in storage (permanent)").await;
        } else if file.retry_count + 1 >= MAX_CATALOG_RETRIES {
            move_to_dead_letter(kv, file, "max retries exceeded").await;
        } else {
            increment_retry_count(kv, file).await;
        }
    }
}

/// Delete committed keys from KV.
async fn delete_committed_keys(kv: &KvStore, keys: Vec<String>) {
    stream::iter(keys.into_iter().map(|key| async move {
        if let Err(e) = kv.delete(&key).await {
            tracing::warn!(key = %key, error = %e, "Failed to delete KV key");
        }
    }))
    .buffer_unordered(MAX_KV_DELETE_CONCURRENCY)
    .collect::<Vec<_>>()
    .await;
}

/// Sync catalog with pending files from KV.
/// Called by #[event(scheduled)] handler.
pub async fn sync_catalog(env: &Env) -> Result<()> {
    sync_catalog_with_report(env).await.map(|_| ())
}

/// Sync catalog with pending files and return a report (used by test hook).
#[tracing::instrument(
    name = "catalog.sync",
    skip(env),
    fields(
        namespace = tracing::field::Empty,
        pending_files = tracing::field::Empty,
        tables_processed = tracing::field::Empty,
        error = tracing::field::Empty,
    )
)]
pub async fn sync_catalog_with_report(env: &Env) -> Result<CatalogSyncReport> {
    // Get KV namespace
    let kv = match env.kv("PENDING_FILES") {
        Ok(kv) => kv,
        Err(_) => {
            tracing::debug!("PENDING_FILES KV not bound, skipping catalog sync");
            return Ok(CatalogSyncReport::skipped());
        }
    };

    // List pending files
    let pending = get_pending_files(&kv).await?;
    if pending.is_empty() {
        tracing::debug!("No pending files to commit");
        return Ok(CatalogSyncReport::empty());
    }

    tracing::Span::current().record("pending_files", pending.len());

    tracing::info!(
        count = pending.len(),
        "Found pending files for catalog commit"
    );

    // Initialize storage and catalog
    let config = ensure_storage_initialized(env)?;
    let namespace = resolve_namespace(&config, env);
    tracing::Span::current().record("namespace", namespace.as_str());
    let catalog = init_catalog_from_env(env, &config, &namespace).await?;

    // Get bucket name for constructing absolute S3 URIs (required by Iceberg spec)
    let bucket = config
        .storage
        .r2
        .as_ref()
        .map(|r2| r2.bucket.clone())
        .unwrap_or_default();

    // Group files by table, deduplicating by Parquet path to avoid double registration
    let deduped = dedup_by_path(pending);
    let by_table = group_by_table(&deduped);

    tracing::info!(
        namespace = %namespace,
        tables = %by_table.len(),
        total_files = %deduped.len(),
        "Starting catalog sync with resolved namespace"
    );

    let mut report = CatalogSyncReport {
        namespace,
        tables: Vec::new(),
    };

    // Commit each table's files
    let mut committed_keys = Vec::new();
    for (table_name, files) in by_table {
        tracing::info!(
            table = %table_name,
            file_count = files.len(),
            "Committing files to catalog table"
        );

        let result = commit_files_to_table(
            catalog.as_ref(),
            &table_name,
            &files,
            report.namespace.clone(),
            &bucket,
        )
        .await;

        match result {
            Ok(_) => {
                tracing::info!(table = %table_name, file_count = files.len(), "Committed files to catalog");
                committed_keys.extend(files.iter().map(|f| f.key.clone()));
                report.tables.push(TableCommitResult {
                    table: table_name,
                    files: files.len(),
                    success: true,
                    error: None,
                });
            }
            Err(e) => {
                let error_str = e.to_string();
                tracing::error!(table = %table_name, error = %error_str, "Failed to commit files");
                handle_commit_failure(&kv, &files, &error_str).await;
                report.tables.push(TableCommitResult {
                    table: table_name,
                    files: files.len(),
                    success: false,
                    error: Some(error_str),
                });
            }
        }
    }

    delete_committed_keys(&kv, committed_keys).await;

    tracing::Span::current().record("tables_processed", report.tables.len());

    Ok(report)
}

/// List all pending files from KV namespace.
///
/// Parallelizes KV GET requests within each page to reduce latency.
async fn get_pending_files(kv: &KvStore) -> Result<Vec<PendingFile>> {
    let mut files = Vec::new();
    let mut cursor: Option<String> = None;

    loop {
        let mut builder = kv.list().prefix("pending:".to_string());
        if let Some(ref c) = cursor {
            builder = builder.cursor(c.clone());
        }

        let page = builder.execute().await?;

        // Collect key names for parallel fetch
        let key_names: Vec<String> = page.keys.iter().map(|k| k.name.clone()).collect();

        // Fetch values with bounded concurrency to avoid OOM on large pages.
        let results = stream::iter(key_names.into_iter().map(|name| async {
            let value = kv.get(&name).text().await;
            (name, value)
        }))
        .buffer_unordered(MAX_KV_FETCH_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;

        for (key_name, value_result) in results {
            match value_result {
                Ok(Some(value)) => match serde_json::from_str::<PendingFile>(&value) {
                    Ok(mut file) => {
                        file.key = key_name;
                        files.push(file);
                    }
                    Err(e) => {
                        tracing::warn!(key = %key_name, error = %e, "Failed to parse KV value");
                    }
                },
                Ok(None) => {
                    tracing::debug!(key = %key_name, "KV key listed but value missing (deleted?)");
                }
                Err(e) => {
                    tracing::warn!(key = %key_name, error = %e, "Failed to fetch KV value");
                }
            }
        }

        if page.list_complete {
            break;
        }

        cursor = page.cursor;
        if cursor.is_none() {
            break;
        }
    }

    Ok(files)
}

/// Group pending files by Iceberg table name.
fn group_by_table(files: &[PendingFile]) -> HashMap<String, Vec<&PendingFile>> {
    let mut grouped: HashMap<String, Vec<&PendingFile>> = HashMap::new();
    for file in files {
        grouped.entry(file.table.clone()).or_default().push(file);
    }
    grouped
}

/// Deduplicate pending files by Parquet path, keeping the first entry.
fn dedup_by_path(files: Vec<PendingFile>) -> Vec<PendingFile> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut deduped = Vec::with_capacity(files.len());

    for file in files {
        if seen.insert(file.path.clone()) {
            deduped.push(file);
        } else {
            tracing::debug!(
                path = %file.path,
                key = %file.key,
                "Dropping duplicate pending file path"
            );
        }
    }

    deduped
}

/// Build the default partition spec (year/month/day/hour on Timestamp) when a table has none.
fn build_partition_spec(schema: &Schema) -> Option<PartitionSpec> {
    // Timestamp field is shared across schemas and is the source for calendar transforms.
    let timestamp_field = schema
        .fields()
        .iter()
        .find(|f| f.name().eq_ignore_ascii_case("Timestamp"))?;

    let ts_id = timestamp_field.id();
    let fields = vec![
        PartitionField::new(1, ts_id, "year", "year"),
        PartitionField::new(2, ts_id, "month", "month"),
        PartitionField::new(3, ts_id, "day", "day"),
        PartitionField::new(4, ts_id, "hour", "hour"),
    ];

    Some(PartitionSpec::new(0, fields))
}

/// Resolve Iceberg namespace from config or direct env lookup.
pub fn resolve_namespace(config: &RuntimeConfig, env: &Env) -> String {
    if let Some(ns) = config.iceberg.as_ref().and_then(|c| c.namespace.clone()) {
        return ns;
    }

    // Fallback: read directly from env in case overrides weren't applied
    let provider = WorkerEnvSource { env };
    if let Some(ns) = provider.get("ICEBERG_NAMESPACE") {
        tracing::warn!(
            namespace = %ns,
            "Iceberg namespace missing from config; using env fallback"
        );
        return ns;
    }

    "otlp".to_string()
}

/// Initialize R2 Data Catalog from environment.
pub async fn init_catalog_from_env(
    env: &Env,
    config: &RuntimeConfig,
    namespace: &str,
) -> Result<Arc<dyn Catalog>> {
    let provider = WorkerEnvSource { env };

    // Collect present and missing variables to provide helpful error messages
    let account_id = provider.get_raw("CLOUDFLARE_ACCOUNT_ID");
    let bucket_name = provider.get_raw("CLOUDFLARE_BUCKET_NAME");
    let api_token = provider.get_raw("CLOUDFLARE_API_TOKEN");

    let mut present_vars = Vec::new();
    let mut missing_vars = Vec::new();

    if account_id.is_some() {
        present_vars.push("CLOUDFLARE_ACCOUNT_ID");
    } else {
        missing_vars.push("CLOUDFLARE_ACCOUNT_ID");
    }
    if bucket_name.is_some() {
        present_vars.push("CLOUDFLARE_BUCKET_NAME");
    } else {
        missing_vars.push("CLOUDFLARE_BUCKET_NAME");
    }
    if api_token.is_some() {
        present_vars.push("CLOUDFLARE_API_TOKEN");
    } else {
        missing_vars.push("CLOUDFLARE_API_TOKEN");
    }

    // Provide more specific error for partial vs complete missing config
    if !missing_vars.is_empty() {
        let error = if present_vars.is_empty() {
            // All missing - generic missing required error
            OtlpErrorKind::ConfigError(ConfigValidationError::MissingRequired {
                component: "R2 Data Catalog",
                missing_vars,
                hint: "These variables are required for Iceberg catalog sync. Set them in wrangler.toml or as secrets.".to_string(),
            })
        } else {
            // Partial config - more helpful error
            OtlpErrorKind::ConfigError(ConfigValidationError::PartialCatalogConfig {
                catalog_type: "R2 Data Catalog",
                present: present_vars,
                missing: missing_vars,
                hint: "Either set all R2 Data Catalog variables or remove them all to disable catalog sync.".to_string(),
            })
        };
        return Err(worker::Error::RustError(format!(
            "{}: {}",
            error.message(),
            error.details().unwrap_or_default()
        )));
    }

    let account_id = account_id.unwrap();
    let bucket_name = bucket_name.unwrap();
    let api_token = api_token.unwrap();

    // Get R2 credentials and namespace from config
    let r2 = config.storage.r2.as_ref().ok_or_else(|| {
        let error = OtlpErrorKind::ConfigError(ConfigValidationError::MissingRequired {
            component: "R2 Storage",
            missing_vars: vec!["R2_BUCKET", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"],
            hint: "R2 storage configuration is required for catalog sync.".to_string(),
        });
        worker::Error::RustError(error.message())
    })?;

    let catalog_config = CatalogConfig {
        namespace: namespace.to_string(),
        catalog_type: CatalogType::R2DataCatalog {
            account_id,
            bucket_name,
            api_token,
            access_key_id: r2.access_key_id.clone(),
            secret_access_key: r2.secret_access_key.clone(),
        },
    };

    let catalog = initialize_catalog(catalog_config)
        .await
        .map_err(|e| worker::Error::RustError(format!("Catalog init failed: {}", e)))?;

    // Ensure namespace exists
    ensure_namespace(catalog.as_ref(), namespace)
        .await
        .map_err(|e| worker::Error::RustError(format!("Namespace creation failed: {}", e)))?;

    Ok(catalog)
}

/// Commit files to Iceberg table using icepick's register API.
#[tracing::instrument(
    name = "catalog.commit_table",
    skip(catalog, files),
    fields(
        table = %table_name,
        namespace = %namespace,
        file_count = files.len(),
        error = tracing::field::Empty,
    )
)]
async fn commit_files_to_table(
    catalog: &dyn Catalog,
    table_name: &str,
    files: &[&PendingFile],
    namespace: String,
    bucket: &str,
) -> Result<()> {
    let operator = otlp2parquet_writer::get_operator_clone().ok_or_else(|| {
        let error = "Storage operator not initialized; call initialize_storage first";
        tracing::Span::current().record("error", error);
        worker::Error::RustError(error.to_string())
    })?;
    let registrar = RealRegistrar {
        catalog,
        file_io: FileIO::new(operator),
        bucket: bucket.to_string(),
    };

    match commit_files_with_registrar(&registrar, table_name, files, namespace).await {
        Ok(result) => Ok(result),
        Err(e) => {
            tracing::Span::current().record("error", tracing::field::display(&e));
            Err(e)
        }
    }
}

/// Trait for registering pending Parquet files to Iceberg catalog.
///
/// # Why `?Send`
///
/// This trait uses `#[async_trait(?Send)]` because:
///
/// 1. **WASM is single-threaded**: Cloudflare Workers run in single-threaded WASM
///    isolates where `Send` bounds are unnecessary and overly restrictive.
///
/// 2. **Non-Send dependencies**: The `Catalog` trait from icepick and various
///    Workers SDK types (like `Env`) are not `Send` in WASM context.
///
/// 3. **No cross-thread usage**: Scheduled events (`#[event(scheduled)]`) execute
///    entirely within a single isolate—no thread spawning or work stealing.
///
/// This trait exists primarily to enable unit testing with mock implementations
/// that don't require actual catalog connections.
#[async_trait(?Send)]
trait PendingRegistrar {
    async fn register_pending(
        &self,
        table_name: &str,
        files: &[&PendingFile],
        namespace: String,
    ) -> Result<()>;
}

struct RealRegistrar<'a> {
    catalog: &'a dyn Catalog,
    file_io: FileIO,
    /// S3 bucket name for constructing absolute URIs (required by Iceberg spec)
    bucket: String,
}

#[async_trait(?Send)]
impl PendingRegistrar for RealRegistrar<'_> {
    async fn register_pending(
        &self,
        table_name: &str,
        files: &[&PendingFile],
        namespace: String,
    ) -> Result<()> {
        let namespace_ident = NamespaceIdent::new(vec![namespace.clone()]);
        let table_ident = TableIdent::new(namespace_ident.clone(), table_name.to_string());
        let mut partition_spec = None;
        let mut table_schema_for_creation = None;

        // Try to load existing table to pick up partition spec; if missing, build a default
        // spec that matches our Hive-style layout.
        match self.catalog.load_table(&table_ident).await {
            Ok(table) => {
                partition_spec = table.metadata().partition_specs().first().cloned();
                if partition_spec.is_none() {
                    tracing::warn!(
                        table = %table_name,
                        "Table has no partition spec - partition values will not be extracted"
                    );
                }
            }
            Err(err) => {
                tracing::info!(
                    table = %table_name,
                    error = %err,
                    "Table not found; will attempt creation during registration"
                );
            }
        }

        // If table is new (no partition spec yet), introspect first file to get schema
        // and build partition spec before processing all files.
        if partition_spec.is_none() && !files.is_empty() {
            // Convert relative path to absolute S3 URI (required by Iceberg spec)
            let first_absolute_path = format!("s3://{}/{}", self.bucket, files[0].path);
            let first_inspection =
                introspect_parquet_file(&self.file_io, &first_absolute_path, None)
                    .await
                    .map_err(|e| {
                        worker::Error::RustError(format!(
                            "Failed to introspect Parquet file {}: {}",
                            first_absolute_path, e
                        ))
                    })?;

            table_schema_for_creation = Some(first_inspection.schema.clone());
            partition_spec = build_partition_spec(&first_inspection.schema);

            if partition_spec.is_none() {
                tracing::warn!(
                    table = %table_name,
                    "Timestamp field missing; proceeding without partition spec"
                );
            }
        }

        // Now introspect all files with the partition spec to extract partition values
        let mut data_files = Vec::with_capacity(files.len());
        for file in files {
            // Convert relative path to absolute S3 URI (required by Iceberg spec)
            let absolute_path = format!("s3://{}/{}", self.bucket, file.path);
            let inspection =
                introspect_parquet_file(&self.file_io, &absolute_path, partition_spec.as_ref())
                    .await
                    .map_err(|e| {
                        worker::Error::RustError(format!(
                            "Failed to introspect Parquet file {}: {}",
                            absolute_path, e
                        ))
                    })?;

            // Capture schema from first file if we haven't already (existing table case)
            if table_schema_for_creation.is_none() {
                table_schema_for_creation = Some(inspection.schema.clone());
            }

            data_files.push(inspection.data_file);
        }

        // Always use current time for snapshot timestamp to ensure monotonic ordering.
        // Using file timestamps could violate Iceberg's requirement that snapshot timestamps
        // never go backwards if older files are registered after newer ones.
        let snapshot_ts = worker::Date::now().as_millis() as i64;

        let mut options = RegisterOptions::new()
            .with_timestamp_ms(snapshot_ts)
            .with_fail_if_missing(false)
            .allow_noop(true)
            .with_schema_evolution(SchemaEvolutionPolicy::AddFields);

        if let Some(schema) = table_schema_for_creation {
            options = options.allow_create_with_schema(schema);
        }
        if let Some(spec) = partition_spec.clone() {
            options = options.with_partition_spec(spec);
        }

        // Attempt registration; if partition spec causes a redundant-partition error,
        // retry without the spec to avoid hard failure on duplicate transforms.
        let result = match register_data_files(
            self.catalog,
            namespace_ident.clone(),
            table_ident.clone(),
            data_files.clone(),
            options.clone(),
        )
        .await
        {
            Ok(res) => Ok(res),
            Err(e) => {
                let msg = e.to_string();
                if partition_spec.is_some() && msg.contains("redundant partition") {
                    tracing::warn!(
                        table = %table_name,
                        namespace = %namespace_ident,
                        error = %msg,
                        "Partition spec rejected; retrying registration without partition spec"
                    );
                    let mut retry_opts = options.clone();
                    retry_opts.partition_spec = None;

                    register_data_files(
                        self.catalog,
                        namespace_ident.clone(),
                        table_ident,
                        data_files,
                        retry_opts,
                    )
                    .await
                } else {
                    Err(e)
                }
            }
        }
        .map_err(|e| {
            worker::Error::RustError(format!(
                "Catalog registration failed for table {}: {}",
                table_name, e
            ))
        })?;

        tracing::info!(
            table = %table_name,
            added_files = result.added_files,
            added_records = result.added_records,
            skipped = result.skipped_files.len(),
            "Registered pending Parquet files to Iceberg"
        );

        Ok(())
    }
}

async fn commit_files_with_registrar(
    registrar: &dyn PendingRegistrar,
    table_name: &str,
    files: &[&PendingFile],
    namespace: String,
) -> Result<()> {
    registrar
        .register_pending(table_name, files, namespace)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    struct MockRegistrar {
        calls: RefCell<Vec<(String, Vec<String>)>>,
    }

    #[async_trait(?Send)]
    impl PendingRegistrar for MockRegistrar {
        async fn register_pending(
            &self,
            table_name: &str,
            files: &[&PendingFile],
            namespace: String,
        ) -> Result<()> {
            let paths = files.iter().map(|f| f.path.clone()).collect();
            self.calls
                .borrow_mut()
                .push((format!("{}/{}", namespace, table_name), paths));
            Ok(())
        }
    }

    #[test]
    fn commit_files_with_registrar_invokes_register() {
        let mock = MockRegistrar {
            calls: RefCell::new(Vec::new()),
        };

        let pf1 = PendingFile {
            path: "metrics/gauge/foo".to_string(),
            table: "otel_metrics_gauge".to_string(),
            rows: 10,
            timestamp_ms: 1,
            retry_count: 0,
            key: "pending:1:a".to_string(),
        };
        let pf2 = PendingFile {
            path: "metrics/gauge/bar".to_string(),
            table: "otel_metrics_gauge".to_string(),
            rows: 20,
            timestamp_ms: 2,
            retry_count: 0,
            key: "pending:2:b".to_string(),
        };
        let files: Vec<&PendingFile> = vec![&pf1, &pf2];

        // This test is synchronous; drive the async future manually.
        futures::executor::block_on(commit_files_with_registrar(
            &mock,
            "otel_metrics_gauge",
            &files,
            "custom".to_string(),
        ))
        .expect("registration should succeed");

        let calls = mock.calls.borrow();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "custom/otel_metrics_gauge");
        assert_eq!(calls[0].1, vec!["metrics/gauge/foo", "metrics/gauge/bar"]);
    }

    #[test]
    fn dedup_by_path_removes_duplicates() {
        let files = vec![
            PendingFile {
                path: "p1".to_string(),
                table: "t1".to_string(),
                rows: 1,
                timestamp_ms: 1,
                retry_count: 0,
                key: "pending:1".to_string(),
            },
            PendingFile {
                path: "p1".to_string(),
                table: "t1".to_string(),
                rows: 2,
                timestamp_ms: 2,
                retry_count: 0,
                key: "pending:2".to_string(),
            },
            PendingFile {
                path: "p2".to_string(),
                table: "t1".to_string(),
                rows: 3,
                timestamp_ms: 3,
                retry_count: 1,
                key: "pending:3".to_string(),
            },
        ];

        let deduped = dedup_by_path(files);
        assert_eq!(deduped.len(), 2);
        assert!(deduped.iter().any(|f| f.key == "pending:1"));
        assert!(deduped.iter().any(|f| f.key == "pending:3"));
    }
}
