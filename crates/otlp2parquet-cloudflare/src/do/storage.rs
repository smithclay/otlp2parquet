//! SQLite storage operations for Durable Object persistence.
//!
//! These functions provide persistence for batch data and DO state,
//! surviving Cloudflare's 10-second hibernation window.

use crate::r#do::config::MAX_CHUNK_BYTES;
use crate::r#do::types::{BatchGroup, DoState, PendingReceiptOwned};
use bytes::Bytes;
use serde::Deserialize;
use worker::{Result, SqlStorageValue, State};

/// Initialize SQLite schema (idempotent).
///
/// Creates tables for batch storage and DO state if they don't exist.
/// Runs on every DO wake to ensure schema is present.
pub fn init_schema(state: &State) -> Result<()> {
    let sql = state.storage().sql();

    // Check if batches table has new schema (chunk_group_id column)
    // If old schema exists, drop it (one-time migration, data loss acceptable per design)
    let has_new_schema = sql
        .exec("SELECT chunk_group_id FROM batches LIMIT 1", None)
        .is_ok();
    if !has_new_schema {
        let _ = sql.exec("DROP TABLE IF EXISTS batches", None);
    }

    // Create batches table with chunking support
    let _ = sql.exec(
        "CREATE TABLE IF NOT EXISTS batches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chunk_group_id TEXT NOT NULL,
            chunk_index INTEGER NOT NULL,
            blob BLOB NOT NULL,
            bytes INTEGER NOT NULL,
            rows INTEGER NOT NULL
        )",
        None,
    );

    // Index for efficient chunk group queries (reassembly and FIFO ordering)
    let _ = sql.exec(
        "CREATE INDEX IF NOT EXISTS idx_batches_chunk_group ON batches(chunk_group_id)",
        None,
    );

    let _ = sql.exec(
        "CREATE TABLE IF NOT EXISTS state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            signal_type TEXT,
            service_name TEXT,
            first_event_timestamp INTEGER,
            pending_receipt TEXT
        )",
        None,
    );

    let _ = sql.exec("INSERT OR IGNORE INTO state (id) VALUES (1)", None);

    Ok(())
}

/// Store a batch blob in SQLite, splitting into chunks if necessary.
///
/// Large blobs are split into MAX_CHUNK_BYTES (512KB) chunks to avoid
/// SQLite's blob size limit (~2MB in Cloudflare DO). All chunks share
/// the same `chunk_group_id` and are ordered by `chunk_index`.
pub fn store_batch(state: &State, blob: &[u8], rows: i64) -> Result<()> {
    let chunk_group_id = uuid::Uuid::new_v4().to_string();
    let sql = state.storage().sql();

    for (chunk_index, chunk) in blob.chunks(MAX_CHUNK_BYTES).enumerate() {
        sql.exec(
            "INSERT INTO batches (chunk_group_id, chunk_index, blob, bytes, rows) VALUES (?, ?, ?, ?, ?)",
            Some(vec![
                SqlStorageValue::String(chunk_group_id.clone()),
                SqlStorageValue::Integer(chunk_index as i64),
                SqlStorageValue::Blob(chunk.to_vec()),
                SqlStorageValue::Integer(chunk.len() as i64),
                SqlStorageValue::Integer(rows),
            ]),
        )?;
    }
    Ok(())
}

/// Get total bytes and rows of pending batches.
///
/// Bytes are summed across all chunks. Rows are counted once per chunk group
/// (since the same row count is stored on each chunk within a group).
pub fn get_batch_totals(state: &State) -> Result<(i64, i64)> {
    let sql = state.storage().sql();
    // Sum all chunk bytes, but only count rows once per group (chunk_index = 0)
    let cursor = sql.exec(
        "SELECT COALESCE(SUM(bytes), 0) as total_bytes,
                COALESCE(SUM(CASE WHEN chunk_index = 0 THEN rows ELSE 0 END), 0) as total_rows
         FROM batches",
        None,
    )?;
    #[derive(Deserialize)]
    struct Totals {
        total_bytes: i64,
        total_rows: i64,
    }
    let row: Totals = cursor
        .one()
        .map_err(|e| worker::Error::RustError(format!("Failed to get batch totals: {}", e)))?;
    Ok((row.total_bytes, row.total_rows))
}

/// Get count of pending batch groups (not individual chunks).
pub fn get_batch_count(state: &State) -> Result<i64> {
    let sql = state.storage().sql();
    let cursor = sql.exec(
        "SELECT COUNT(DISTINCT chunk_group_id) as cnt FROM batches",
        None,
    )?;
    #[derive(Deserialize)]
    struct Count {
        cnt: i64,
    }
    let row: Count = cursor
        .one()
        .map_err(|e| worker::Error::RustError(format!("Failed to get batch count: {}", e)))?;
    Ok(row.cnt)
}

/// Load batch groups for flush, up to the specified byte limit (FIFO order).
///
/// Returns the oldest batch groups that fit within `max_bytes`. Each group's
/// chunks are reassembled into the original IPC blob. Groups are NOT deleted
/// from SQLite - call `delete_batch_groups()` after successful write.
pub fn load_batches_for_flush(state: &State, max_bytes: usize) -> Result<Vec<BatchGroup>> {
    let sql = state.storage().sql();

    // Step 1: Get group metadata ordered by FIFO (earliest id = oldest)
    #[derive(Deserialize)]
    struct GroupMeta {
        chunk_group_id: String,
        #[allow(dead_code)]
        first_id: i64,
        total_bytes: i64,
        rows: i64,
    }

    let groups_cursor = sql.exec(
        "SELECT chunk_group_id, MIN(id) as first_id, SUM(bytes) as total_bytes, MAX(rows) as rows
         FROM batches
         GROUP BY chunk_group_id
         ORDER BY first_id",
        None,
    )?;

    let groups: Vec<GroupMeta> = groups_cursor.to_array()?.into_iter().collect();

    // Step 2: Select groups until we hit max_bytes (FIFO)
    let mut selected_groups: Vec<GroupMeta> = Vec::new();
    let mut accumulated_bytes: usize = 0;

    for group in groups {
        let group_bytes = group.total_bytes as usize;
        // Always include at least one group (even if it exceeds max_bytes)
        // to avoid stalling if a single batch is larger than the limit
        if accumulated_bytes + group_bytes > max_bytes && !selected_groups.is_empty() {
            break;
        }
        accumulated_bytes += group_bytes;
        selected_groups.push(group);
    }

    if selected_groups.is_empty() {
        return Ok(vec![]);
    }

    // Step 3: Load and reassemble chunks for selected groups
    let mut batch_groups = Vec::with_capacity(selected_groups.len());

    for group_meta in selected_groups {
        let chunks_cursor = sql.exec(
            "SELECT blob, bytes FROM batches WHERE chunk_group_id = ? ORDER BY chunk_index",
            Some(vec![SqlStorageValue::String(
                group_meta.chunk_group_id.clone(),
            )]),
        )?;

        // Reassemble chunks using raw() iterator for proper blob handling
        let mut reassembled = Vec::new();
        let mut total_bytes: usize = 0;

        for row_result in chunks_cursor.raw() {
            let row = row_result.map_err(|e| {
                worker::Error::RustError(format!("Failed to read chunk row: {}", e))
            })?;

            let mut iter = row.into_iter();
            if let Some(SqlStorageValue::Blob(chunk_data)) = iter.next() {
                total_bytes += chunk_data.len();
                reassembled.extend_from_slice(&chunk_data);
            }
        }

        batch_groups.push(BatchGroup {
            chunk_group_id: group_meta.chunk_group_id,
            reassembled_blob: Bytes::from(reassembled),
            total_bytes,
            rows: group_meta.rows,
        });
    }

    Ok(batch_groups)
}

/// Delete specific batch groups from SQLite after successful flush.
pub fn delete_batch_groups(state: &State, groups: &[BatchGroup]) -> Result<()> {
    let sql = state.storage().sql();
    for group in groups {
        sql.exec(
            "DELETE FROM batches WHERE chunk_group_id = ?",
            Some(vec![SqlStorageValue::String(group.chunk_group_id.clone())]),
        )?;
    }
    Ok(())
}

/// Get persistent DO state from SQLite.
pub fn get_do_state(state: &State) -> Result<DoState> {
    let sql = state.storage().sql();
    let cursor = sql.exec(
        "SELECT signal_type, service_name, first_event_timestamp, pending_receipt FROM state WHERE id = 1",
        None,
    )?;
    #[derive(Deserialize, Default)]
    struct StateRow {
        signal_type: Option<String>,
        service_name: Option<String>,
        first_event_timestamp: Option<i64>,
        pending_receipt: Option<String>,
    }
    let row: StateRow = cursor.one().unwrap_or_default();
    Ok(DoState {
        signal_type: row.signal_type,
        service_name: row.service_name,
        first_event_timestamp: row.first_event_timestamp,
        pending_receipt: row.pending_receipt,
    })
}

/// Set signal type and service name (identity) in SQLite.
pub fn set_identity(state: &State, signal_type: &str, service_name: &str) -> Result<()> {
    let sql = state.storage().sql();
    sql.exec(
        "UPDATE state SET signal_type = ?, service_name = ? WHERE id = 1",
        Some(vec![
            SqlStorageValue::String(signal_type.to_string()),
            SqlStorageValue::String(service_name.to_string()),
        ]),
    )?;
    Ok(())
}

/// Update first event timestamp (only if earlier than existing).
pub fn update_first_event_timestamp(state: &State, timestamp_micros: i64) -> Result<()> {
    let sql = state.storage().sql();
    sql.exec(
        "UPDATE state SET first_event_timestamp = ?
         WHERE id = 1 AND (first_event_timestamp IS NULL OR first_event_timestamp > ?)",
        Some(vec![
            SqlStorageValue::Integer(timestamp_micros),
            SqlStorageValue::Integer(timestamp_micros),
        ]),
    )?;
    Ok(())
}

/// Clear first event timestamp after successful flush.
pub fn clear_first_event_timestamp(state: &State) -> Result<()> {
    let sql = state.storage().sql();
    sql.exec(
        "UPDATE state SET first_event_timestamp = NULL WHERE id = 1",
        None,
    )?;
    Ok(())
}

/// Set pending receipt (JSON serialized).
pub fn set_pending_receipt(state: &State, receipt: &PendingReceiptOwned) -> Result<()> {
    let json = serde_json::to_string(receipt)
        .map_err(|e| worker::Error::RustError(format!("Failed to serialize receipt: {}", e)))?;
    let sql = state.storage().sql();
    sql.exec(
        "UPDATE state SET pending_receipt = ? WHERE id = 1",
        Some(vec![SqlStorageValue::String(json)]),
    )?;
    Ok(())
}

/// Clear pending receipt after successful forwarding.
pub fn clear_pending_receipt(state: &State) -> Result<()> {
    let sql = state.storage().sql();
    sql.exec("UPDATE state SET pending_receipt = NULL WHERE id = 1", None)?;
    Ok(())
}

/// Get and clear pending receipt (for retry).
pub fn take_pending_receipt(state: &State) -> Result<Option<PendingReceiptOwned>> {
    let state_data = get_do_state(state)?;
    if let Some(json) = state_data.pending_receipt {
        let receipt: PendingReceiptOwned = serde_json::from_str(&json).map_err(|e| {
            worker::Error::RustError(format!("Failed to deserialize receipt: {}", e))
        })?;
        clear_pending_receipt(state)?;
        Ok(Some(receipt))
    } else {
        Ok(None)
    }
}
