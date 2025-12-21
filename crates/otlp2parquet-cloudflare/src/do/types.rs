//! Pure data structures for the batcher Durable Object.

use bytes::Bytes;
use indexmap::IndexSet;
use serde::{Deserialize, Serialize};

/// 1024 recent batch keys for idempotency deduplication.
pub const MAX_RECENT_BATCH_KEYS: usize = 1024;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct BatchKey {
    pub request_id: String,
    pub batch_index: u32,
}

/// Tracks recently seen batch keys for idempotency with bounded memory.
pub struct RecentBatches {
    keys: IndexSet<BatchKey>,
}

impl RecentBatches {
    pub fn new() -> Self {
        Self {
            keys: IndexSet::new(),
        }
    }

    /// Record a batch key. Returns true if the key was already seen.
    pub fn record(&mut self, key: BatchKey) -> bool {
        if self.keys.contains(&key) {
            return true;
        }

        if self.keys.len() >= MAX_RECENT_BATCH_KEYS {
            self.keys.shift_remove_index(0);
        }

        self.keys.insert(key);
        false
    }
}

impl Default for RecentBatches {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct PendingReceiptOwned {
    pub path: String,
    pub table: String,
    pub rows: usize,
    pub timestamp_ms: i64,
}

/// Response from DO back to Worker.
#[derive(Serialize)]
pub struct IngestResponse {
    pub status: String,
    pub buffered_records: i64,
    pub buffered_bytes: i64,
}

/// Pending receipt delivered back to the main Worker for KV tracking.
#[derive(Serialize)]
pub struct PendingReceipt<'a> {
    pub path: &'a str,
    pub table: &'a str,
    pub rows: usize,
    pub timestamp_ms: i64,
}

/// Persistent DO state stored in SQLite (survives hibernation).
#[derive(Default)]
pub struct DoState {
    pub signal_type: Option<String>,
    pub service_name: Option<String>,
    pub first_event_timestamp: Option<i64>,
    pub pending_receipt: Option<String>,
}

/// A group of chunks that together form one Arrow IPC batch.
pub struct BatchGroup {
    pub chunk_group_id: String,
    pub reassembled_blob: Bytes,
    pub total_bytes: usize,
    pub rows: i64,
}

/// Context for pending buffer data during flush operations.
pub struct PendingBufferContext {
    pub blobs: Vec<Bytes>,
    pub bytes: usize,
    pub rows: i64,
    pub signal_type_str: String,
    pub service_name: String,
}
