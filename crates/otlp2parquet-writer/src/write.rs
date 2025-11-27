//! Core write operations using icepick
//!
//! Uses icepick's AppendOnlyTableWriter for high-level catalog integration.
//! Arrow schemas with field_id metadata are passed to icepick, which:
//! - Derives Iceberg schemas automatically
//! - Creates tables if they don't exist
//! - Writes Parquet files with statistics
//! - Commits to catalog atomically

use crate::error::{Result, WriterError};
use crate::table_mapping::table_name_for_signal;
use arrow::array::RecordBatch;
#[cfg(not(target_family = "wasm"))]
#[cfg(not(target_family = "wasm"))]
use bytes::Bytes;
#[cfg(not(target_family = "wasm"))]
use futures::future::BoxFuture;
use icepick::catalog::Catalog;
use icepick::error::Error as CatalogError;
use icepick::spec::NamespaceIdent;
use icepick::{AppendOnlyTableWriter, AppendResult, TableWriterOptions};
use otlp2parquet_core::SignalType;
#[cfg(target_family = "wasm")]
use parquet::arrow::ArrowWriter;
#[cfg(not(target_family = "wasm"))]
use parquet::arrow::{async_writer::AsyncFileWriter, AsyncArrowWriter};
#[cfg(not(target_family = "wasm"))]
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::properties::WriterProperties;
use std::borrow::Cow;
use time::OffsetDateTime;
use uuid::Uuid;

#[cfg(not(target_family = "wasm"))]
struct OpendalAsyncWriter(opendal::Writer);

#[cfg(not(target_family = "wasm"))]
impl OpendalAsyncWriter {
    fn new(inner: opendal::Writer) -> Self {
        Self(inner)
    }
}

#[cfg(not(target_family = "wasm"))]
impl AsyncFileWriter for OpendalAsyncWriter {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, ParquetResult<()>> {
        Box::pin(async move {
            self.0
                .write(bs)
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))?;
            Ok(())
        })
    }

    fn complete(&mut self) -> BoxFuture<'_, ParquetResult<()>> {
        Box::pin(async move {
            self.0
                .close()
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))?;
            Ok(())
        })
    }
}

/// Request parameters for writing a batch to storage
pub struct WriteBatchRequest<'a> {
    /// Optional catalog instance for Iceberg table operations
    pub catalog: Option<&'a dyn Catalog>,
    /// Namespace for tables (e.g., "otlp")
    pub namespace: &'a str,
    /// Arrow RecordBatch to write
    pub batch: &'a RecordBatch,
    /// Type of OTLP signal (logs, traces, metrics)
    pub signal_type: SignalType,
    /// Metric type if signal_type is Metrics (gauge, sum, etc.)
    pub metric_type: Option<&'a str>,
    /// Service name for logging (not used for partitioning)
    pub service_name: &'a str,
    /// Timestamp in microseconds (from OTLP-to-Arrow nanos_to_micros conversion)
    pub timestamp_micros: i64,
    /// Optional Iceberg snapshot timestamp in milliseconds
    ///
    /// If provided, used for catalog commit timestamp. Should represent when the write
    /// occurs (current time), not when the data was generated. Use this on WASM platforms
    /// (Cloudflare Workers) where system time is unavailable.
    pub snapshot_timestamp_ms: Option<i64>,
    /// Retry policy for optimistic Iceberg commits. Use [`RetryPolicy::disabled()`]
    /// to turn retries off entirely.
    pub retry_policy: RetryPolicy,
}

/// Controls optimistic retry behavior when committing to Iceberg catalogs.
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    /// Whether retries are enabled.
    pub enabled: bool,
    /// Maximum number of attempts (including the initial write).
    pub max_attempts: u32,
    /// Initial delay in milliseconds before retrying (applies to the first retry).
    pub initial_delay_ms: u32,
    /// Maximum backoff delay in milliseconds.
    pub max_delay_ms: u32,
}

impl RetryPolicy {
    /// Disable optimistic retries entirely.
    pub const fn disabled() -> Self {
        Self {
            enabled: false,
            max_attempts: 1,
            initial_delay_ms: 0,
            max_delay_ms: 0,
        }
    }

    /// Create an enabled retry policy with the provided parameters.
    pub const fn enabled(max_attempts: u32, initial_delay_ms: u32, max_delay_ms: u32) -> Self {
        Self {
            enabled: true,
            max_attempts,
            initial_delay_ms,
            max_delay_ms,
        }
    }

    /// Number of attempts the writer will make (always >= 1).
    pub fn effective_max_attempts(&self) -> u32 {
        self.max_attempts.max(1)
    }

    /// Determine whether the failure should be retried.
    pub fn should_retry(&self, attempt: u32, err: &CatalogError) -> bool {
        self.enabled && attempt < self.effective_max_attempts() && is_retryable_error(err)
    }

    /// Compute the delay applied before the next retry (in milliseconds).
    pub fn delay_for_attempt(&self, attempt: u32) -> u32 {
        if !self.enabled || self.initial_delay_ms == 0 {
            return 0;
        }

        let exp = attempt.saturating_sub(1).min(10); // prevent overflow
        let mut delay = (self.initial_delay_ms as u64) << exp;
        let max_allowed = self.max_delay_ms.max(self.initial_delay_ms) as u64;
        if delay > max_allowed {
            delay = max_allowed;
        }
        delay as u32
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        #[cfg(target_family = "wasm")]
        {
            Self {
                enabled: true,
                max_attempts: 3,
                initial_delay_ms: 0,
                max_delay_ms: 0,
            }
        }

        #[cfg(not(target_family = "wasm"))]
        {
            Self {
                enabled: true,
                max_attempts: 4,
                initial_delay_ms: 25,
                max_delay_ms: 250,
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct SnapshotClock {
    base_ms: Option<i64>,
}

impl SnapshotClock {
    fn new(snapshot_timestamp_ms: Option<i64>) -> Self {
        #[cfg(target_family = "wasm")]
        {
            let base = snapshot_timestamp_ms.or_else(|| Some(js_sys::Date::now() as i64));
            Self { base_ms: base }
        }

        #[cfg(not(target_family = "wasm"))]
        {
            Self {
                base_ms: snapshot_timestamp_ms,
            }
        }
    }

    fn timestamp_for_attempt(&self, attempt: u32) -> Option<i64> {
        self.base_ms
            .map(|base| base + i64::from(attempt.saturating_sub(1)))
    }
}

/// Write a RecordBatch to an Iceberg table via catalog
///
/// This function delegates all Parquet writing, schema management, and catalog operations
/// to icepick's `AppendOnlyTableWriter`. It focuses on table name resolution and error
/// translation.
///
/// # Behavior
///
/// 1. **Table creation**: If the table doesn't exist, icepick creates it automatically by
///    deriving the Iceberg schema from Arrow field_id metadata in the RecordBatch
/// 2. **Schema evolution**: Not currently supported - schema changes will fail the write
/// 3. **Atomicity**: Write and catalog commit are atomic via icepick
/// 4. **Optimistic retries**: Controlled via `RetryPolicy`. Enabled by default to smooth out
///    thundering herd commits and can be disabled with `RetryPolicy::disabled()`
///
/// # Resilience
///
/// Write operations include a lightweight optimistic retry loop that only retries retryable
/// catalog failures (concurrent modifications, transient 5xxs). When disabled, failures propagate
/// immediately as `WriterError::WriteFailure`. Typical failure modes:
/// - Network errors (catalog unreachable)
/// - Schema mismatches (field type changes)
/// - Storage errors (quota exceeded, permissions)
/// - Catalog errors (transaction conflicts)
///
/// Application-level retry logic should handle transient failures based on error type.
///
/// # Returns
/// Table path in format "{namespace}/{table_name}" for logging purposes only
///
/// # Errors
/// Returns `WriterError::InvalidTableName` if signal/metric type combination is invalid
/// Returns `WriterError::WriteFailure` if icepick append operation fails or direct write fails
/// Returns `WriterError::WriteFailure` if catalog is None and storage operator is not initialized
/// Write a batch to Iceberg catalog with optimistic retries.
async fn write_with_catalog(
    catalog: &dyn Catalog,
    namespace: &str,
    table_name: &str,
    batch: &RecordBatch,
    snapshot_timestamp_ms: Option<i64>,
    retry_policy: RetryPolicy,
) -> Result<()> {
    let namespace_ident = NamespaceIdent::new(vec![namespace.to_string()]);
    let table_name_owned = table_name.to_string();
    let mut attempt: u32 = 1;
    let max_attempts = retry_policy.effective_max_attempts();
    let snapshot_clock = SnapshotClock::new(snapshot_timestamp_ms);

    loop {
        let mut writer =
            AppendOnlyTableWriter::new(catalog, namespace_ident.clone(), table_name_owned.clone());

        if let Some(timestamp_ms) = snapshot_clock.timestamp_for_attempt(attempt) {
            let options = TableWriterOptions::new().with_timestamp_ms(timestamp_ms);
            writer = writer.with_options(options);
        }

        match writer.append_batch(batch.clone()).await {
            Ok(result) => {
                log_append_result(table_name, &result);
                return Ok(());
            }
            Err(err) => {
                if !retry_policy.should_retry(attempt, &err) {
                    return Err(WriterError::write_failure(format!(
                        "table '{}': {} (attempt {}/{})",
                        table_name, err, attempt, max_attempts
                    )));
                }

                let delay_ms = retry_policy.delay_for_attempt(attempt);
                tracing::warn!(
                    table = %table_name,
                    attempt,
                    max_attempts,
                    delay_ms,
                    error = %err,
                    "Iceberg append conflict detected; retrying"
                );
                wait_for_retry(delay_ms).await;
                attempt += 1;
            }
        }
    }
}

fn log_append_result(table_name: &str, result: &AppendResult) {
    match result {
        AppendResult::TableCreated {
            ref data_file,
            ref schema,
        } => {
            tracing::info!(
                "✓ Created table '{}' with {} fields and wrote {} rows",
                table_name,
                schema.fields().len(),
                data_file.record_count()
            );
        }
        AppendResult::SchemaEvolved {
            ref data_file,
            ref old_schema,
            ref new_schema,
        } => {
            tracing::warn!(
                "✓ Schema evolved for '{}' from {} to {} fields, wrote {} rows",
                table_name,
                old_schema.fields().len(),
                new_schema.fields().len(),
                data_file.record_count()
            );
        }
        AppendResult::Appended { ref data_file } => {
            tracing::info!(
                "✓ Wrote {} rows to '{}' via AppendOnlyTableWriter",
                data_file.record_count(),
                table_name
            );
        }
    }
}

#[cfg(not(target_family = "wasm"))]
async fn wait_for_retry(delay_ms: u32) {
    if delay_ms == 0 {
        return;
    }
    tokio::time::sleep(std::time::Duration::from_millis(delay_ms as u64)).await;
}

#[cfg(target_family = "wasm")]
async fn wait_for_retry(delay_ms: u32) {
    if delay_ms == 0 {
        return;
    }
    gloo_timers::future::TimeoutFuture::new(delay_ms).await;
}

fn is_retryable_error(err: &CatalogError) -> bool {
    matches!(
        err,
        CatalogError::ConcurrentModification { .. }
            | CatalogError::Conflict { .. }
            | CatalogError::ServerError { .. }
            | CatalogError::NetworkError { .. }
    )
}

/// Write a batch as plain Parquet file (no catalog)
#[cfg(not(target_family = "wasm"))]
async fn write_plain_parquet(
    signal_type: SignalType,
    metric_type: Option<&str>,
    service_name: &str,
    timestamp_micros: i64,
    batch: &RecordBatch,
) -> Result<String> {
    // Get global storage operator
    let op = crate::storage::get_operator().ok_or_else(|| {
        WriterError::write_failure(
            "Storage operator not initialized. Call initialize_storage() with RuntimeConfig before writing. \
             For catalog mode, ensure catalog is provided in WriteBatchRequest.".to_string(),
        )
    })?;

    // Generate timestamped file path with partitioning
    // Format: {signal}/{service}/year={year}/month={month}/day={day}/hour={hour}/{uuid}.parquet
    let file_path =
        generate_parquet_path(signal_type, metric_type, service_name, timestamp_micros)?;

    tracing::debug!("Writing plain Parquet to path: {}", file_path);

    // Initialize streaming writer
    let op_writer = op.writer(&file_path).await.map_err(|e| {
        WriterError::write_failure(format!("Failed to open writer for '{}': {}", file_path, e))
    })?;
    let async_writer = OpendalAsyncWriter::new(op_writer);

    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();

    let mut parquet_writer = AsyncArrowWriter::try_new(async_writer, batch.schema(), Some(props))
        .map_err(|e| {
        WriterError::write_failure(format!("Failed to create Parquet writer: {}", e))
    })?;

    parquet_writer
        .write(batch)
        .await
        .map_err(|e| WriterError::write_failure(format!("Failed to write RecordBatch: {}", e)))?;

    parquet_writer.finish().await.map_err(|e| {
        WriterError::write_failure(format!("Failed to close Parquet writer: {}", e))
    })?;

    let bytes_written = parquet_writer.bytes_written();

    let row_count = batch.num_rows();
    tracing::info!(
        "✓ Wrote {} rows to '{}' (plain Parquet, no catalog, {} bytes)",
        row_count,
        file_path,
        bytes_written
    );

    Ok(file_path)
}

/// WASM plain Parquet path: write into an in-memory buffer synchronously to avoid Send bounds.
#[cfg(target_family = "wasm")]
async fn write_plain_parquet(
    signal_type: SignalType,
    metric_type: Option<&str>,
    service_name: &str,
    timestamp_micros: i64,
    batch: &RecordBatch,
) -> Result<String> {
    let op = crate::storage::get_operator().ok_or_else(|| {
        WriterError::write_failure(
            "Storage operator not initialized. Call initialize_storage() with RuntimeConfig before writing. \
             For catalog mode, ensure catalog is provided in WriteBatchRequest.".to_string(),
        )
    })?;

    let file_path =
        generate_parquet_path(signal_type, metric_type, service_name, timestamp_micros)?;

    tracing::debug!("Writing plain Parquet (WASM) to path: {}", file_path);

    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();

    let mut buffer = Vec::new();
    {
        let mut writer =
            ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props)).map_err(|e| {
                WriterError::write_failure(format!("Failed to create Parquet writer: {}", e))
            })?;
        writer.write(batch).map_err(|e| {
            WriterError::write_failure(format!("Failed to write RecordBatch: {}", e))
        })?;
        writer.close().map_err(|e| {
            WriterError::write_failure(format!("Failed to close Parquet writer: {}", e))
        })?;
    }

    let bytes_written = buffer.len();
    op.write(&file_path, buffer).await.map_err(|e| {
        WriterError::write_failure(format!(
            "Failed to upload Parquet to '{}': {}",
            file_path, e
        ))
    })?;

    let row_count = batch.num_rows();
    tracing::info!(
        "✓ Wrote {} rows to '{}' (plain Parquet, no catalog, {} bytes, wasm buffer)",
        row_count,
        file_path,
        bytes_written
    );

    Ok(file_path)
}

pub async fn write_batch(req: WriteBatchRequest<'_>) -> Result<String> {
    let row_count = req.batch.num_rows();

    // Get table name based on signal type
    // e.g., "otel_logs", "otel_traces", "otel_metrics_gauge"
    let table_name = table_name_for_signal(req.signal_type, req.metric_type)?;

    tracing::debug!(
        "Writing {} rows to table '{}' (service: {}, signal: {:?}, catalog: {})",
        row_count,
        table_name,
        req.service_name,
        req.signal_type,
        if req.catalog.is_some() {
            "enabled"
        } else {
            "disabled"
        }
    );

    // If catalog is provided, use Iceberg. Otherwise, write plain Parquet to storage
    let path = match req.catalog {
        Some(cat) => {
            write_with_catalog(
                cat,
                req.namespace,
                &table_name,
                req.batch,
                req.snapshot_timestamp_ms,
                req.retry_policy,
            )
            .await?;

            // Catalog mode returns logical table path
            format!("{}/{}", req.namespace, table_name)
        }
        None => {
            // Plain Parquet mode returns physical object path
            write_plain_parquet(
                req.signal_type,
                req.metric_type,
                req.service_name,
                req.timestamp_micros,
                req.batch,
            )
            .await?
        }
    };

    Ok(path)
}

/// Generate a partitioned file path for plain Parquet files
///
/// Format: {signal_type}/{service}/year={year}/month={month}/day={day}/hour={hour}/{timestamp}-{uuid}.parquet
/// Example: logs/my-service/year=2025/month=01/day=15/hour=10/1736938800000000-<uuid>.parquet
fn generate_parquet_path(
    signal_type: SignalType,
    metric_type: Option<&str>,
    service_name: &str,
    timestamp_micros: i64,
) -> Result<String> {
    let (year, month, day, hour) = partition_from_timestamp(timestamp_micros);

    let signal_prefix: Cow<'_, str> = match signal_type {
        SignalType::Logs => Cow::Borrowed("logs"),
        SignalType::Traces => Cow::Borrowed("traces"),
        SignalType::Metrics => {
            if let Some(mtype) = metric_type {
                Cow::Owned(format!("metrics/{}", mtype))
            } else {
                Cow::Borrowed("metrics")
            }
        }
    };

    let safe_service = sanitize_service_name(service_name);
    let suffix = Uuid::new_v4().simple();

    Ok(format!(
        "{}/{}/year={}/month={:02}/day={:02}/hour={:02}/{}-{}.parquet",
        signal_prefix, safe_service, year, month, day, hour, timestamp_micros, suffix
    ))
}

fn sanitize_service_name(service_name: &str) -> Cow<'_, str> {
    const INVALID: [char; 3] = ['/', '\\', ' '];

    if service_name.is_empty() {
        return Cow::Borrowed("unknown-service");
    }

    if service_name.chars().any(|c| INVALID.contains(&c)) {
        let sanitized = service_name
            .chars()
            .map(|c| if INVALID.contains(&c) { '_' } else { c })
            .collect::<String>();
        Cow::Owned(sanitized)
    } else {
        Cow::Borrowed(service_name)
    }
}

/// Get fallback timestamp partition values when timestamp_micros is invalid.
/// Uses platform-appropriate time source (std::time on native, js_sys on WASM).
#[cfg(not(target_family = "wasm"))]
fn fallback_partition() -> (i32, u8, u8, u8) {
    let now = OffsetDateTime::now_utc();
    (now.year(), u8::from(now.month()), now.day(), now.hour())
}

#[cfg(target_family = "wasm")]
fn fallback_partition() -> (i32, u8, u8, u8) {
    // js_sys::Date::now() returns milliseconds since epoch
    let now_ms = js_sys::Date::now() as i64;
    let nanos = i128::from(now_ms).saturating_mul(1_000_000);
    match OffsetDateTime::from_unix_timestamp_nanos(nanos) {
        Ok(dt) => (dt.year(), u8::from(dt.month()), dt.day(), dt.hour()),
        Err(_) => {
            // Absolute fallback if even JS time conversion fails
            (2025, 1, 1, 0)
        }
    }
}

fn partition_from_timestamp(timestamp_micros: i64) -> (i32, u8, u8, u8) {
    if timestamp_micros <= 0 {
        return fallback_partition();
    }

    let nanos = i128::from(timestamp_micros).saturating_mul(1_000);
    match OffsetDateTime::from_unix_timestamp_nanos(nanos) {
        Ok(dt) => (dt.year(), u8::from(dt.month()), dt.day(), dt.hour()),
        Err(_) => fallback_partition(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_policy_can_be_disabled() {
        let policy = RetryPolicy::disabled();
        assert_eq!(policy.effective_max_attempts(), 1);
        let err = CatalogError::concurrent_modification("conflict");
        assert!(!policy.should_retry(1, &err));
        assert_eq!(policy.delay_for_attempt(1), 0);
    }

    #[test]
    fn snapshot_clock_increments_monotonically() {
        let clock = SnapshotClock::new(Some(1_000));
        assert_eq!(clock.timestamp_for_attempt(1), Some(1_000));
        assert_eq!(clock.timestamp_for_attempt(2), Some(1_001));
        assert_eq!(clock.timestamp_for_attempt(3), Some(1_002));
    }

    #[test]
    fn test_timestamp_conversion_from_nanos_to_millis() {
        // Test typical OTLP timestamp (nanoseconds since Unix epoch)
        // Example: 2025-01-15 10:00:00 UTC = 1736938800 seconds
        let timestamp_nanos = 1736938800000000000i64; // 19 digits (nanoseconds)
        let timestamp_ms = timestamp_nanos / 1_000_000;

        // Should produce 13-digit milliseconds timestamp
        assert_eq!(timestamp_ms, 1736938800000);
        assert_eq!(
            timestamp_ms.to_string().len(),
            13,
            "Millisecond timestamp should have 13 digits"
        );
    }

    #[test]
    fn test_timestamp_from_error_message() {
        // These are the actual timestamps from the error message:
        // "Invalid snapshot timestamp 1760741572: before last updated timestamp 1763790139618"

        let invalid_timestamp = 1760741572i64; // 10 digits
        let last_updated = 1763790139618i64; // 13 digits

        println!(
            "Invalid timestamp: {} ({} digits)",
            invalid_timestamp,
            invalid_timestamp.to_string().len()
        );
        println!(
            "Last updated: {} ({} digits)",
            last_updated,
            last_updated.to_string().len()
        );

        // If this was supposed to be milliseconds, what was the original nanoseconds value?
        let reconstructed_nanos_from_invalid = invalid_timestamp * 1_000_000;
        println!(
            "If {} was the result of nanos/1_000_000, original nanos would be: {}",
            invalid_timestamp, reconstructed_nanos_from_invalid
        );

        // Check: 10 digits suggests this might be seconds, not milliseconds
        // If it was seconds, the original nanoseconds would be:
        let nanos_if_seconds = invalid_timestamp * 1_000_000_000;
        println!(
            "If {} was seconds, nanos would be: {} ({} digits)",
            invalid_timestamp,
            nanos_if_seconds,
            nanos_if_seconds.to_string().len()
        );

        // The issue: 1760741572 is 10 digits (seconds or short milliseconds)
        // Expected: 13 digits for milliseconds (like 1763790139618)
        assert_eq!(invalid_timestamp.to_string().len(), 10);
        assert_eq!(last_updated.to_string().len(), 13);
    }

    #[test]
    fn test_timestamp_scenarios() {
        // Scenario 1: Correct nanosecond input
        let correct_nanos = 1763790139618000000i64; // 19 digits
        let correct_ms = correct_nanos / 1_000_000;
        assert_eq!(correct_ms, 1763790139618); // 13 digits
        assert_eq!(correct_ms.to_string().len(), 13);

        // Scenario 2: What if input was already in milliseconds?
        let already_ms = 1763790139618i64; // 13 digits
        let double_converted = already_ms / 1_000_000;
        assert_eq!(double_converted, 1763790); // Oops! Now only 7 digits
        println!(
            "If input was already milliseconds ({}), dividing by 1_000_000 gives: {} ({} digits)",
            already_ms,
            double_converted,
            double_converted.to_string().len()
        );

        // Scenario 3: What if input was in seconds?
        let seconds = 1763790139i64; // 10 digits
        let seconds_to_ms_wrong = seconds / 1_000_000;
        println!(
            "If input was seconds ({}), dividing by 1_000_000 gives: {} ({} digits)",
            seconds,
            seconds_to_ms_wrong,
            seconds_to_ms_wrong.to_string().len()
        );

        // Scenario 4: What if input was in microseconds?
        let micros = 1763790139618000i64; // 16 digits
        let micros_to_ms = micros / 1_000_000;
        assert_eq!(micros_to_ms, 1763790139); // 10 digits - matches the error!
        println!(
            "If input was microseconds ({}), dividing by 1_000_000 gives: {} ({} digits)",
            micros,
            micros_to_ms,
            micros_to_ms.to_string().len()
        );
    }

    #[test]
    fn test_extract_timestamp_from_arrow_batch() {
        // This test would verify the actual extraction from RecordBatch
        // We need to check if extract_first_timestamp returns nanoseconds as expected
        use arrow::array::{ArrayRef, TimestampNanosecondArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        // Create a test RecordBatch with timestamp in nanoseconds
        let timestamp_nanos = 1736938800000000000i64; // 19 digits (nanoseconds)
        let timestamp_array = TimestampNanosecondArray::from(vec![timestamp_nanos]);
        let dummy_array = arrow::array::StringArray::from(vec!["test"]);

        let schema = Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("service", DataType::Utf8, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(timestamp_array) as ArrayRef,
                Arc::new(dummy_array) as ArrayRef,
            ],
        )
        .unwrap();

        // Extract timestamp using the same logic as handlers.rs
        if let Some(array) = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
        {
            let value = array.value(0);
            assert_eq!(value, timestamp_nanos);
            assert_eq!(
                value.to_string().len(),
                19,
                "Extracted timestamp should be 19 digits (nanoseconds)"
            );

            // Now test the conversion
            let timestamp_ms = value / 1_000_000;
            assert_eq!(timestamp_ms, 1736938800000);
            assert_eq!(
                timestamp_ms.to_string().len(),
                13,
                "Converted timestamp should be 13 digits (milliseconds)"
            );
        } else {
            panic!("Failed to extract timestamp from batch");
        }
    }

    #[test]
    fn test_real_otlp_timestamp_from_testdata() {
        // Parse actual OTLP logs from testdata to see what timestamp values we get
        use otlp2parquet_core::otlp;

        // Read the test data file
        let test_data =
            std::fs::read("../../testdata/logs.pb").expect("Failed to read testdata/logs.pb");

        // Parse OTLP request
        let request =
            otlp::parse_otlp_request(&test_data, otlp2parquet_core::InputFormat::Protobuf)
                .expect("Failed to parse OTLP request");

        println!("\n=== Real OTLP Test Data Analysis ===");
        println!("Resource logs count: {}", request.resource_logs.len());

        // Get first resource logs
        if let Some(resource_logs) = request.resource_logs.first() {
            if let Some(scope_logs) = resource_logs.scope_logs.first() {
                if let Some(log_record) = scope_logs.log_records.first() {
                    let time_unix_nano = log_record.time_unix_nano;

                    println!("Raw OTLP time_unix_nano: {}", time_unix_nano);
                    println!("Timestamp digits: {}", time_unix_nano.to_string().len());

                    // Test all three conversion scenarios
                    println!("\n--- Conversion Analysis ---");

                    // Scenario 1: If this is nanoseconds (expected)
                    let ms_from_nano = time_unix_nano / 1_000_000;
                    println!(
                        "If nanoseconds -> milliseconds (/ 1_000_000): {} ({} digits)",
                        ms_from_nano,
                        ms_from_nano.to_string().len()
                    );

                    // Scenario 2: If this is microseconds (hypothesis)
                    let ms_from_micro = time_unix_nano / 1_000;
                    println!(
                        "If microseconds -> milliseconds (/ 1_000): {} ({} digits)",
                        ms_from_micro,
                        ms_from_micro.to_string().len()
                    );

                    // Scenario 3: If this is already milliseconds
                    println!(
                        "If already milliseconds (/ 1): {} ({} digits)",
                        time_unix_nano,
                        time_unix_nano.to_string().len()
                    );

                    // Check if this matches the error pattern
                    if ms_from_nano.to_string().len() == 10 {
                        println!("\n⚠️  FOUND THE BUG!");
                        println!(
                            "   Current conversion (nanos / 1_000_000) produces 10 digits: {}",
                            ms_from_nano
                        );
                        println!(
                            "   This suggests the input '{}' is in MICROSECONDS, not nanoseconds",
                            time_unix_nano
                        );
                        println!(
                            "   Correct conversion should be: {} / 1_000 = {} (13 digits)",
                            time_unix_nano, ms_from_micro
                        );
                    } else if ms_from_nano.to_string().len() == 13 {
                        println!("\n✓ Conversion is correct");
                        println!("   Input '{}' is in nanoseconds", time_unix_nano);
                        println!(
                            "   Output '{}' is in milliseconds (13 digits)",
                            ms_from_nano
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn path_generation_sanitizes_service() {
        let path =
            generate_parquet_path(SignalType::Logs, None, "svc /name", 1_736_938_800_000_000)
                .unwrap();
        assert!(path.starts_with("logs/svc__name/year="));
        assert!(path.contains("/month="));
        assert!(path.ends_with(".parquet"));
        // UUID suffix should provide uniqueness; ensure it's present.
        assert!(path.split('-').next_back().unwrap().ends_with(".parquet"));
    }
}
