//! Batch configuration and alarm management.

use worker::{Env, Result, State};

/// Default batch configuration values.
pub const DEFAULT_MAX_ROWS: i64 = 50_000;
pub const DEFAULT_MAX_BYTES: i64 = 10_485_760; // 10MB
pub const DEFAULT_MAX_AGE_SECS: i64 = 60;

/// 512KB - Maximum size for a single SQLite blob.
pub const MAX_CHUNK_BYTES: usize = 512 * 1024;

/// 48MB - Maximum Arrow memory bytes to flush in a single write operation.
pub const WASM_MAX_FLUSH_BYTES: usize = 48 * 1024 * 1024;

/// 16MB - Maximum IPC bytes to accept per batch at ingest.
pub const MAX_INGEST_IPC_BYTES: usize = 16 * 1024 * 1024;

/// 50MB - Backpressure threshold before rejecting requests.
pub const BACKPRESSURE_THRESHOLD_BYTES: usize = 50_000_000;

/// 5 retries with exponential backoff before DLQ.
pub const MAX_WRITE_RETRIES: u32 = 5;

/// Get batch config from environment variables.
pub fn get_batch_config(env: &Env) -> (i64, i64, i64) {
    let max_rows = env
        .var("OTLP2PARQUET_BATCH_MAX_ROWS")
        .ok()
        .and_then(|v| v.to_string().parse().ok())
        .unwrap_or(DEFAULT_MAX_ROWS);

    let max_bytes = env
        .var("OTLP2PARQUET_BATCH_MAX_BYTES")
        .ok()
        .and_then(|v| v.to_string().parse().ok())
        .unwrap_or(DEFAULT_MAX_BYTES);

    let max_age_secs = env
        .var("OTLP2PARQUET_BATCH_MAX_AGE_SECS")
        .ok()
        .and_then(|v| v.to_string().parse().ok())
        .unwrap_or(DEFAULT_MAX_AGE_SECS);

    (max_rows, max_bytes, max_age_secs)
}

/// Ensure alarm is set for time-based flush.
pub async fn ensure_alarm(state: &State, max_age_secs: i64) -> Result<()> {
    let storage = state.storage();

    let existing: Option<i64> = storage.get_alarm().await?;

    let needs_reset = match existing {
        None => {
            worker::console_log!("[DO] No alarm set, will create one");
            true
        }
        Some(_alarm_time) => {
            worker::console_log!("[DO] Alarm already set, keeping existing");
            false
        }
    };

    if needs_reset {
        let offset_ms = max_age_secs * 1000;
        storage.set_alarm(offset_ms).await?;
        worker::console_log!(
            "[DO] Set alarm: offset_ms={}, fires_in={}s",
            offset_ms,
            max_age_secs
        );
        tracing::debug!(
            max_age_secs,
            offset_ms,
            "Set alarm for flush (offset-based)"
        );
    }

    Ok(())
}
