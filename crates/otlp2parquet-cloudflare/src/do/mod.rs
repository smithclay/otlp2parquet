//! Durable Object for OTLP batching.
//!
//! SQLite-backed buffering with time/size-based flush.
//! State persists across DO hibernation (10s inactivity threshold).

pub mod config;
pub mod dlq;
pub mod flush;
pub mod receipts;
pub mod storage;
pub mod types;

use crate::do_config::ensure_storage_initialized;
use crate::parse_do_id;
use crate::r#do::config::{
    ensure_alarm, get_batch_config, BACKPRESSURE_THRESHOLD_BYTES, MAX_INGEST_IPC_BYTES,
};
use crate::r#do::types::{BatchKey, IngestResponse, RecentBatches};
use bytes::Bytes;
use otlp2parquet_core::config::RuntimeConfig;
use std::cell::RefCell;
use worker::{durable_object, DurableObject, Env, Request, Response, Result, State};

/// Durable Object that accumulates Arrow RecordBatches via SQLite storage,
/// flushing to R2 when size or time thresholds are exceeded.
///
/// # SQLite-backed Persistence
///
/// All batch data and DO identity state are persisted to SQLite, which survives
/// Cloudflare's 10-second hibernation window. This ensures data is not lost
/// when the alarm fires at 60 seconds (after hibernation).
///
/// ## Tables
/// - `batches`: Stores pending Arrow IPC blobs with byte/row counts
/// - `state`: Stores signal_type, service_name, first_event_timestamp, pending_receipt
///
/// # Thread Safety
///
/// This struct uses `RefCell` for ephemeral state, which is safe because:
/// 1. Cloudflare Durable Objects run in single-threaded WASM isolates
/// 2. The `#[durable_object]` macro enforces single-threaded execution
/// 3. Each DO instance handles requests sequentially (no concurrent fetch calls)
#[durable_object]
pub struct OtlpBatcherV2 {
    state: State,
    env: Env,

    // Ephemeral state (OK to lose on hibernation)
    /// Recent batch keys for idempotent ingest across retries (best-effort)
    recent_batches: RefCell<RecentBatches>,
    /// Consecutive write failure count for retry limiting
    write_retry_count: RefCell<u32>,
    /// Cached runtime config (lazy-initialized on first request)
    cached_config: RefCell<Option<RuntimeConfig>>,
}

impl OtlpBatcherV2 {
    /// Get or initialize cached config. Parses env vars once per DO lifetime.
    /// Returns a clone to avoid holding RefCell borrow across await points.
    fn get_config(&self) -> Result<RuntimeConfig> {
        if self.cached_config.borrow().is_none() {
            let config = ensure_storage_initialized(&self.env)?;
            self.cached_config.replace(Some(config));
        }

        Ok(self.cached_config.borrow().as_ref().unwrap().clone())
    }

    /// Handle ingest request - accumulate in SQLite, flush on threshold.
    #[tracing::instrument(
        name = "do.ingest",
        skip(self, req),
        fields(
            request_id = tracing::field::Empty,
            record_count = tracing::field::Empty,
            buffered_bytes = tracing::field::Empty,
            error = tracing::field::Empty,
        )
    )]
    async fn handle_ingest(&self, mut req: Request) -> Result<Response> {
        let state = crate::r#do::storage::get_do_state(&self.state)?;
        worker::console_log!(
            "[DO] handle_ingest() called: signal={:?}, service={:?}",
            state.signal_type.as_deref().unwrap_or("unknown"),
            state.service_name.as_deref().unwrap_or("unknown")
        );
        // Idempotency: require X-Request-Id and X-Batch-Index headers from internal callers.
        // These are always set by route_to_batcher() in lib.rs.
        let request_id = req.headers().get("X-Request-Id")?;
        let batch_index = req
            .headers()
            .get("X-Batch-Index")?
            .and_then(|v| v.parse::<u32>().ok());

        let (req_id, idx) = match (request_id.as_ref(), batch_index) {
            (Some(id), Some(idx)) => {
                // Record request_id for correlation
                tracing::Span::current().record("request_id", id.as_str());
                (id.clone(), idx)
            }
            _ => {
                // Missing headers indicates a bug in the caller or direct external access.
                // Reject with 400 to surface the issue rather than silently skip idempotency.
                tracing::error!(
                    has_request_id = request_id.is_some(),
                    has_batch_index = batch_index.is_some(),
                    "Missing required idempotency headers (X-Request-Id, X-Batch-Index)"
                );
                let err = worker::Error::RustError(
                    "Missing required headers: X-Request-Id and X-Batch-Index".to_string(),
                );
                tracing::Span::current().record("error", tracing::field::display(&err));
                return Response::error(
                    "Missing required headers: X-Request-Id and X-Batch-Index",
                    400,
                );
            }
        };

        let key = BatchKey {
            request_id: req_id,
            batch_index: idx,
        };

        let is_duplicate = {
            let mut tracker = self.recent_batches.borrow_mut();
            tracker.record(key)
        };

        if is_duplicate {
            let (total_bytes, total_rows) = crate::r#do::storage::get_batch_totals(&self.state)?;
            return Response::from_json(&IngestResponse {
                status: "accepted".to_string(),
                buffered_records: total_rows,
                buffered_bytes: total_bytes,
            });
        }

        // Track earliest event timestamp to preserve partitioning semantics
        if let Some(header_ts) = req
            .headers()
            .get("X-First-Timestamp-Micros")?
            .and_then(|v| v.parse::<i64>().ok())
            .filter(|v| *v > 0)
        {
            crate::r#do::storage::update_first_event_timestamp(&self.state, header_ts)?;
        }

        // Get raw bytes as Bytes (zero-copy)
        let ipc_bytes: Bytes = req.bytes().await?.into();

        // Reject oversized batches - IPC can expand 2-3x to Arrow memory
        if ipc_bytes.len() > MAX_INGEST_IPC_BYTES {
            tracing::warn!(
                size = ipc_bytes.len(),
                limit = MAX_INGEST_IPC_BYTES,
                "Rejecting oversized batch - configure upstream collector to use smaller batches"
            );
            return Response::error(
                format!(
                    "Batch too large: {} bytes exceeds {} byte limit. Configure upstream OTel Collector batch size.",
                    ipc_bytes.len(),
                    MAX_INGEST_IPC_BYTES
                ),
                413,
            );
        }

        // Backpressure check - reject when accepting batch would exceed threshold.
        let (current_bytes, _) = crate::r#do::storage::get_batch_totals(&self.state)?;
        let projected_bytes = (current_bytes as usize).saturating_add(ipc_bytes.len());
        if projected_bytes > BACKPRESSURE_THRESHOLD_BYTES {
            tracing::warn!(
                current_bytes,
                incoming_size = ipc_bytes.len(),
                projected_bytes,
                threshold = BACKPRESSURE_THRESHOLD_BYTES,
                "Backpressure: accepting batch would exceed threshold"
            );
            return Response::error("Service unavailable - buffer full", 503);
        }
        let record_count: i64 = req
            .headers()
            .get("X-Record-Count")?
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
            .max(0);

        // Record the record count for this batch
        tracing::Span::current().record("record_count", record_count);

        // Store batch in SQLite (persists across hibernation, chunked if large)
        crate::r#do::storage::store_batch(&self.state, &ipc_bytes, record_count)?;

        let (max_rows, max_bytes, max_age_secs) = get_batch_config(&self.env);
        let (total_bytes, total_rows) = crate::r#do::storage::get_batch_totals(&self.state)?;
        let row_threshold_hit = max_rows > 0 && total_rows >= max_rows;

        worker::console_log!(
            "[DO] After accumulate: bytes={}, rows={}, max_age_secs={}, will_set_alarm={}",
            total_bytes,
            total_rows,
            max_age_secs,
            !(total_bytes >= max_bytes || row_threshold_hit)
        );

        // Check if threshold exceeded
        if total_bytes >= max_bytes || row_threshold_hit {
            tracing::info!(
                bytes = total_bytes,
                max_bytes,
                rows = total_rows,
                max_rows,
                "Threshold exceeded, flushing"
            );
            let config = self.get_config()?;
            if let Err(e) =
                crate::r#do::flush::flush(&self.state, &self.env, &config, &self.write_retry_count)
                    .await
            {
                // Ensure a retry alarm if data or receipts are pending so we don't stall.
                let batch_count = crate::r#do::storage::get_batch_count(&self.state)?;
                let do_state = crate::r#do::storage::get_do_state(&self.state)?;
                if batch_count > 0 || do_state.pending_receipt.is_some() {
                    if let Err(alarm_err) = ensure_alarm(&self.state, max_age_secs).await {
                        tracing::warn!(
                            error = ?alarm_err,
                            "Failed to schedule alarm after flush error"
                        );
                    }
                }
                tracing::Span::current().record("error", tracing::field::display(&e));
                return Err(e);
            }
            self.state.storage().delete_alarm().await?;
            let batch_count = crate::r#do::storage::get_batch_count(&self.state)?;
            if batch_count > 0 {
                ensure_alarm(&self.state, max_age_secs).await?;
            }
        } else {
            // Ensure alarm is set for time-based flush
            ensure_alarm(&self.state, max_age_secs).await?;
        }

        let (final_bytes, final_rows) = crate::r#do::storage::get_batch_totals(&self.state)?;
        tracing::Span::current().record("buffered_bytes", final_bytes);
        Response::from_json(&IngestResponse {
            status: "accepted".to_string(),
            buffered_records: final_rows,
            buffered_bytes: final_bytes,
        })
    }
}

impl DurableObject for OtlpBatcherV2 {
    fn new(state: State, env: Env) -> Self {
        // Initialize SQLite schema (idempotent, runs on every wake)
        let _ = crate::r#do::storage::init_schema(&state);

        Self {
            state,
            env,
            recent_batches: RefCell::new(RecentBatches::new()),
            write_retry_count: RefCell::new(0),
            cached_config: RefCell::new(None),
        }
    }

    async fn fetch(&self, req: Request) -> Result<Response> {
        // Load identity from SQLite (persists across hibernation)
        let state = crate::r#do::storage::get_do_state(&self.state)?;

        // If identity not set, parse from URL query param and persist
        if state.signal_type.is_none() || state.service_name.is_none() {
            let url = req.url()?;
            match url.query_pairs().find(|(k, _)| k == "name").map(|(_, v)| v) {
                Some(name) => {
                    if let Some((sig, svc)) = parse_do_id(&name) {
                        // Persist identity to SQLite (survives hibernation)
                        crate::r#do::storage::set_identity(&self.state, sig, svc)?;
                    } else {
                        // Name param present but malformed
                        tracing::error!(
                            name = %name,
                            "Durable Object name param malformed: expected 'signal_key|service' format"
                        );
                        return Response::error(
                            format!(
                                "Invalid 'name' query param format: '{}'. Expected 'signal_key|service' (e.g., 'logs|my-service' or 'metrics:gauge|my-service').",
                                name
                            ),
                            400,
                        );
                    }
                }
                None => {
                    // Name param missing entirely
                    tracing::error!("Durable Object missing required 'name' query param");
                    return Response::error(
                        "Missing required 'name' query param. Request URL must include '?name=signal_key|service'.",
                        400,
                    );
                }
            }
        }

        // Double-check initialization from SQLite
        let state = crate::r#do::storage::get_do_state(&self.state)?;
        if state.signal_type.is_none() || state.service_name.is_none() {
            tracing::error!("Durable Object initialization incomplete after parsing");
            return Response::error("Internal error: DO initialization failed", 500);
        }

        self.handle_ingest(req).await
    }

    async fn alarm(&self) -> Result<Response> {
        // Load state from SQLite (survives hibernation!)
        let state = crate::r#do::storage::get_do_state(&self.state)?;
        let (total_bytes, total_rows) = crate::r#do::storage::get_batch_totals(&self.state)?;

        worker::console_log!(
            "[DO] ALARM TRIGGERED! signal={:?}, service={:?}, pending_bytes={}, pending_rows={}",
            state.signal_type.as_deref().unwrap_or("unknown"),
            state.service_name.as_deref().unwrap_or("unknown"),
            total_bytes,
            total_rows
        );
        tracing::debug!(
            signal_type = ?state.signal_type,
            service_name = ?state.service_name,
            total_bytes,
            total_rows,
            "Alarm triggered, flushing"
        );

        let config = self.get_config()?;
        match crate::r#do::flush::flush(&self.state, &self.env, &config, &self.write_retry_count)
            .await
        {
            Ok(_) => Response::ok("flushed"),
            Err(e) => {
                tracing::error!(error = ?e, "Alarm flush failed");
                // Data is preserved in SQLite for next alarm/request retry

                // Re-schedule alarm for retry only if data or receipts are still pending
                let batch_count = crate::r#do::storage::get_batch_count(&self.state)?;
                let do_state = crate::r#do::storage::get_do_state(&self.state)?;
                if batch_count > 0 || do_state.pending_receipt.is_some() {
                    let (_, _, max_age_secs) = get_batch_config(&self.env);
                    if let Err(alarm_err) = ensure_alarm(&self.state, max_age_secs).await {
                        tracing::warn!(
                            error = ?alarm_err,
                            "Failed to reschedule alarm after flush failure"
                        );
                    }
                }

                Response::error(format!("Flush failed: {:?}", e), 500)
            }
        }
    }
}

// Legacy stub class - needed for migration from old KV-backed DO to new SQLite-backed DO.
// This class is renamed from OtlpBatcher and will be deleted once migration is complete.
#[durable_object]
pub struct OtlpBatcherLegacy {
    state: State,
    #[allow(dead_code)]
    env: Env,
}

impl DurableObject for OtlpBatcherLegacy {
    fn new(state: State, env: Env) -> Self {
        Self { state, env }
    }

    async fn fetch(&self, _req: Request) -> Result<Response> {
        // Legacy DO - return error directing to new endpoint
        Response::error(
            "This Durable Object has been migrated. Please retry your request.",
            410,
        )
    }

    async fn alarm(&self) -> Result<Response> {
        // Clear any pending alarms from legacy DO
        let _ = self.state.storage().delete_alarm().await;
        Response::ok("legacy alarm cleared")
    }
}
