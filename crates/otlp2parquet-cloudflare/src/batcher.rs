//! Durable Object for batching OTLP data before writing to R2.
//!
//! SQLite-backed buffering with time/size-based flush.
//! State persists across DO hibernation (10s inactivity threshold).

use crate::do_config::ensure_storage_initialized;
use crate::parse_do_id;
use crate::r#do::config::{
    ensure_alarm, get_batch_config, BACKPRESSURE_THRESHOLD_BYTES, MAX_INGEST_IPC_BYTES,
    MAX_WRITE_RETRIES, WASM_MAX_FLUSH_BYTES,
};
use crate::r#do::types::{
    BatchGroup, BatchKey, IngestResponse, PendingBufferContext, PendingReceipt,
    PendingReceiptOwned, RecentBatches,
};
use bytes::Bytes;
use otlp2parquet_core::batch::ipc::{deserialize_batches, validate_ipc_header};
use otlp2parquet_core::config::{CatalogMode, RuntimeConfig};
use otlp2parquet_core::SignalKey;
use otlp2parquet_writer::{write_multi_batch, WriteMultiBatchRequest};
use std::cell::RefCell;
use std::str::FromStr;
use worker::{
    durable_object, Date, DurableObject, Env, Headers, Method, Request, RequestInit, Response,
    Result, State,
};

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

    /// Write failed batches to dead-letter queue (R2 failed/ prefix) for manual recovery.
    /// This is called after MAX_WRITE_RETRIES have been exhausted.
    ///
    /// # Format
    /// ```text
    /// [8 bytes] Magic + version: "OTLPIPC1"
    /// [4 bytes] Number of blobs (u32 LE)
    /// For each blob:
    ///   [4 bytes] Blob length (u32 LE)
    ///   [N bytes] Blob data (Arrow IPC)
    /// ```
    ///
    /// To recover, read magic, verify version, then iterate blobs and deserialize
    /// each as Arrow IPC using `otlp2parquet_core::batch::ipc::deserialize_batches`.
    async fn write_dlq_batch(&self, path: &str, blobs: &[Bytes]) -> Result<()> {
        use otlp2parquet_writer::get_operator_clone;

        let operator = get_operator_clone().ok_or_else(|| {
            worker::Error::RustError("Storage operator not initialized for DLQ write".to_string())
        })?;

        // Format: [magic][version][num_blobs][blob1_len][blob1_data]...
        let mut dlq_data = Vec::new();
        dlq_data.extend_from_slice(b"OTLPIPC1"); // Magic + version (8 bytes)
        dlq_data.extend_from_slice(&(blobs.len() as u32).to_le_bytes());

        for blob in blobs {
            if blob.len() > u32::MAX as usize {
                return Err(worker::Error::RustError(format!(
                    "Blob size {} exceeds u32::MAX",
                    blob.len()
                )));
            }
            let len = blob.len() as u32;
            dlq_data.extend_from_slice(&len.to_le_bytes());
            dlq_data.extend_from_slice(blob);
        }

        operator
            .write(path, dlq_data)
            .await
            .map(|_| ())
            .map_err(|e| worker::Error::RustError(format!("DLQ write failed: {}", e)))
    }

    /// Send a receipt to the main Worker so it can persist to KV (DOs cannot write KV directly).
    /// Uses a self service binding to avoid needing to know the Worker's public URL.
    #[tracing::instrument(
        name = "do.send_receipt",
        skip(self, receipt),
        fields(
            path = %receipt.path,
            table = %receipt.table,
            rows = receipt.rows,
            error = tracing::field::Empty,
        )
    )]
    async fn send_receipt_to_worker(&self, receipt: &PendingReceipt<'_>) -> Result<()> {
        tracing::debug!("send_receipt_to_worker: getting SELF service binding");
        // Use service binding to call back to main Worker (avoids URL chicken-and-egg problem)
        let service = self.env.service("SELF").map_err(|e| {
            tracing::error!(error = %e, "SELF service binding not found");
            tracing::Span::current().record("error", tracing::field::display(&e));
            worker::Error::RustError(format!(
                "SELF service binding not configured: {}. Add [[services]] binding in wrangler.toml",
                e
            ))
        })?;
        tracing::debug!("Got SELF service binding");

        let headers = Headers::new();
        headers.set("Content-Type", "application/json")?;

        let mut init = RequestInit::new();
        init.with_method(Method::Post);
        init.with_headers(headers);
        init.with_body(Some(
            serde_json::to_string(receipt)
                .map_err(|e| worker::Error::RustError(format!("receipt serialize failed: {}", e)))?
                .into(),
        ));

        // Use relative URL with service binding - no need to know public hostname
        let req = Request::new_with_init("https://self/__internal/receipt", &init)?;
        tracing::debug!("Calling service.fetch_request");
        let resp = service.fetch_request(req).await?;
        tracing::debug!(status = resp.status_code(), "Service response received");

        if !(200..300).contains(&resp.status_code()) {
            let err = worker::Error::RustError(format!(
                "Receipt callback failed with status {}",
                resp.status_code()
            ));
            tracing::Span::current().record("error", tracing::field::display(&err));
            return Err(err);
        }

        Ok(())
    }

    /// Retry any previously failed receipt before attempting a new write.
    async fn retry_pending_receipt(&self) -> Result<()> {
        if let Some(pending) = crate::r#do::storage::take_pending_receipt(&self.state)? {
            let path = pending.path.clone();
            let table = pending.table.clone();
            let receipt = PendingReceipt {
                path: &path,
                table: &table,
                rows: pending.rows,
                timestamp_ms: pending.timestamp_ms,
            };

            if let Err(e) = self.send_receipt_to_worker(&receipt).await {
                // Put it back for the next attempt (re-store in SQLite)
                crate::r#do::storage::set_pending_receipt(&self.state, &pending)?;
                return Err(worker::Error::RustError(format!(
                    "Receipt forwarding failed: {} (path={})",
                    e, path
                )));
            }
        }
        Ok(())
    }

    /// Validate IPC headers and deserialize Arrow batches.
    ///
    /// Note: On failure, batches remain in SQLite (not deleted) since we use
    /// the load-then-delete-on-success pattern.
    fn validate_and_deserialize(
        &self,
        blobs: &[Bytes],
    ) -> Result<Vec<arrow::record_batch::RecordBatch>> {
        // Validate Arrow IPC headers before deserialization
        for (idx, blob) in blobs.iter().enumerate() {
            if let Err(err) = validate_ipc_header(blob) {
                return Err(worker::Error::RustError(format!(
                    "Invalid Arrow IPC format in batch {}: {}. \
                     This may indicate data corruption or incompatible client version.",
                    idx, err
                )));
            }
        }

        // Deserialize Arrow IPC batches
        deserialize_batches(blobs)
            .enumerate()
            .map(|(idx, res)| {
                res.map_err(|e| {
                    worker::Error::RustError(format!("Failed to deserialize batch {}: {}", idx, e))
                })
            })
            .collect()
    }

    /// Handle write failure with retry logic and DLQ fallback.
    /// Returns Ok(path) on success, or Err on failure.
    ///
    /// Note: On failure, batches are NOT restored to SQLite - they remain there
    /// since we use load-then-delete-on-success pattern. The `group_ids` are
    /// passed to DLQ handler for cleanup if batches are moved to dead-letter queue.
    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(
        name = "do.write_parquet",
        skip(self, record_batches, ctx, group_ids),
        fields(
            signal_key = %signal_key,
            namespace,
            rows = record_batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            error = tracing::field::Empty,
        )
    )]
    async fn write_with_retries(
        &self,
        record_batches: &[arrow::record_batch::RecordBatch],
        signal_key: &SignalKey,
        namespace: &str,
        event_timestamp_micros: i64,
        ctx: PendingBufferContext,
        group_ids: &[String],
    ) -> Result<String> {
        tracing::Span::current().record("namespace", namespace);
        let req = WriteMultiBatchRequest {
            catalog: None,
            namespace,
            batches: record_batches,
            signal_type: signal_key.signal_type(),
            metric_type: signal_key.metric_type().map(|mt| mt.as_str()),
            service_name: &ctx.service_name,
            timestamp_micros: event_timestamp_micros,
            snapshot_timestamp_ms: Some(Date::now().as_millis() as i64),
            retry_policy: otlp2parquet_writer::RetryPolicy::default(),
        };

        match write_multi_batch(req).await {
            Ok(path) => {
                *self.write_retry_count.borrow_mut() = 0;
                Ok(path)
            }
            Err(e) => {
                let current_retries = {
                    let mut retry_count = self.write_retry_count.borrow_mut();
                    *retry_count += 1;
                    *retry_count
                };

                if current_retries >= MAX_WRITE_RETRIES {
                    self.handle_dlq_fallback(&e, current_retries, &ctx, group_ids)
                        .await
                } else {
                    tracing::warn!(
                        error = %e,
                        retry_count = current_retries,
                        max_retries = MAX_WRITE_RETRIES,
                        "Write failed, will retry (attempt {}/{})",
                        current_retries,
                        MAX_WRITE_RETRIES
                    );
                    // Batches remain in SQLite - no restore needed with new pattern
                    let err = worker::Error::RustError(format!("Write failed: {}", e));
                    tracing::Span::current().record("error", tracing::field::display(&err));
                    Err(err)
                }
            }
        }
    }

    /// Handle DLQ fallback after exhausting retries.
    ///
    /// Only deletes batch groups from SQLite if DLQ write succeeds.
    /// If DLQ write fails, batches remain in SQLite for manual recovery.
    async fn handle_dlq_fallback(
        &self,
        original_error: &otlp2parquet_writer::WriterError,
        retry_count: u32,
        ctx: &PendingBufferContext,
        group_ids: &[String],
    ) -> Result<String> {
        tracing::error!(
            error = %original_error,
            retry_count,
            batches = ctx.blobs.len(),
            bytes = ctx.bytes,
            rows = ctx.rows,
            "Write failed after {} retries, moving to dead-letter queue",
            MAX_WRITE_RETRIES
        );

        let dlq_path = format!(
            "failed/{}/{}/{}-{}.ipc",
            ctx.signal_type_str,
            ctx.service_name,
            Date::now().as_millis(),
            uuid::Uuid::new_v4()
        );

        let dlq_success = match self.write_dlq_batch(&dlq_path, &ctx.blobs).await {
            Ok(()) => {
                tracing::warn!(
                    dlq_path = %dlq_path,
                    bytes = ctx.bytes,
                    "Batch moved to dead-letter queue for manual recovery"
                );
                true
            }
            Err(dlq_err) => {
                tracing::error!(
                    error = %dlq_err,
                    dlq_path = %dlq_path,
                    "DLQ write also failed - keeping batches in SQLite for retry"
                );

                // Emit metric for alerting on DLQ failure
                if let Ok(metrics) = self.env.analytics_engine("METRICS") {
                    use worker::AnalyticsEngineDataPointBuilder;

                    let _ = AnalyticsEngineDataPointBuilder::new()
                        .indexes(["dlq_failure"])
                        .add_blob(ctx.signal_type_str.as_str())
                        .add_double(ctx.bytes as f64)
                        .add_double(ctx.rows as f64)
                        .add_double(1.0)
                        .write_to(&metrics);
                }
                false
            }
        };

        // Only delete batch groups if DLQ write succeeded
        // If DLQ failed, keep batches in SQLite for future retry
        if dlq_success {
            // Delete groups by constructing temporary BatchGroup structs with just the IDs
            let groups_to_delete: Vec<BatchGroup> = group_ids
                .iter()
                .map(|id| BatchGroup {
                    chunk_group_id: id.clone(),
                    reassembled_blob: Bytes::new(),
                    total_bytes: 0,
                    rows: 0,
                })
                .collect();
            let _ = crate::r#do::storage::delete_batch_groups(&self.state, &groups_to_delete);
        }

        *self.write_retry_count.borrow_mut() = 0;
        let _ = crate::r#do::storage::clear_first_event_timestamp(&self.state);

        Err(worker::Error::RustError(format!(
            "Write failed after {} retries, {}: {}",
            MAX_WRITE_RETRIES,
            if dlq_success {
                "moved to DLQ"
            } else {
                "DLQ also failed"
            },
            original_error
        )))
    }

    /// Forward receipt to main worker if catalog mode is Iceberg.
    async fn forward_receipt_if_needed(
        &self,
        config: &otlp2parquet_core::config::RuntimeConfig,
        path: &str,
        signal_key: &SignalKey,
        total_rows: usize,
    ) -> Result<()> {
        tracing::debug!(catalog_mode = ?config.catalog_mode, "Checking catalog mode for receipt");
        if config.catalog_mode != CatalogMode::Iceberg {
            tracing::debug!("Skipping receipt (catalog_mode != Iceberg)");
            return Ok(());
        }

        let table_name = signal_key.table_name();
        let receipt = PendingReceipt {
            path,
            table: &table_name,
            rows: total_rows,
            timestamp_ms: Date::now().as_millis() as i64,
        };

        tracing::debug!(path = %path, table = %table_name, "Sending receipt to Worker");
        if let Err(e) = self.send_receipt_to_worker(&receipt).await {
            tracing::error!(error = %e, path = %path, "Receipt forwarding failed");
            let pending = PendingReceiptOwned {
                path: path.to_string(),
                table: table_name,
                rows: total_rows,
                timestamp_ms: Date::now().as_millis() as i64,
            };
            crate::r#do::storage::set_pending_receipt(&self.state, &pending)?;
            let _ = crate::r#do::storage::clear_first_event_timestamp(&self.state);
            return Err(worker::Error::RustError(format!(
                "Receipt forwarding failed: {} (path={})",
                e, path
            )));
        }
        tracing::debug!("Receipt sent successfully");
        Ok(())
    }

    /// Emit usage metrics to Analytics Engine (fire-and-forget).
    fn emit_usage_metrics(&self, signal_key: &SignalKey, bytes: usize, rows: usize) {
        if let Ok(metrics) = self.env.analytics_engine("METRICS") {
            use worker::AnalyticsEngineDataPointBuilder;

            let signal_label = signal_key.analytics_label();

            let _ = AnalyticsEngineDataPointBuilder::new()
                .indexes([signal_label])
                .add_blob(signal_label)
                .add_double(bytes as f64)
                .add_double(rows as f64)
                .add_double(1.0)
                .write_to(&metrics);
        }
    }

    /// Flush batches to R2 storage in chunks up to WASM_MAX_FLUSH_BYTES.
    ///
    /// Uses partial flush pattern: loads oldest batch groups up to memory limit,
    /// writes them, deletes them, and loops until all batches are flushed.
    #[tracing::instrument(
        name = "do.flush",
        skip(self),
        fields(
            signal_key = tracing::field::Empty,
            service_name = tracing::field::Empty,
            total_bytes = tracing::field::Empty,
            total_rows = tracing::field::Empty,
            iterations = tracing::field::Empty,
            error = tracing::field::Empty,
        )
    )]
    async fn flush(&self) -> Result<()> {
        let (initial_bytes, initial_rows) = crate::r#do::storage::get_batch_totals(&self.state)?;
        worker::console_log!(
            "[DO] flush() called: pending_bytes={}, pending_rows={}",
            initial_bytes,
            initial_rows
        );

        // Retry any previously failed receipt first
        self.retry_pending_receipt().await?;

        // Get DO state for signal/service info
        let do_state = crate::r#do::storage::get_do_state(&self.state)?;
        let signal_type_str = match do_state.signal_type {
            Some(s) => s,
            None => {
                tracing::debug!("No signal_type set, skipping flush");
                return Ok(());
            }
        };
        let service_name = match do_state.service_name {
            Some(s) => s,
            None => {
                tracing::debug!("No service_name set, skipping flush");
                return Ok(());
            }
        };

        let config = self.get_config()?;
        let namespace = config.catalog_namespace();
        let signal_key = SignalKey::from_str(&signal_type_str).map_err(|e| {
            worker::Error::RustError(format!("Invalid signal key '{}': {}", signal_type_str, e))
        })?;

        // Record span fields
        tracing::Span::current().record("signal_key", signal_key.to_string().as_str());
        tracing::Span::current().record("service_name", service_name.as_str());
        tracing::Span::current().record("total_bytes", initial_bytes);
        tracing::Span::current().record("total_rows", initial_rows);

        // Partial flush loop: process batches in chunks up to WASM_MAX_FLUSH_BYTES
        let mut iteration = 0;
        loop {
            iteration += 1;

            // Load batch groups up to memory limit (FIFO order)
            let batch_groups =
                crate::r#do::storage::load_batches_for_flush(&self.state, WASM_MAX_FLUSH_BYTES)?;
            if batch_groups.is_empty() {
                tracing::debug!(
                    iterations = iteration - 1,
                    "Flush complete, no more batches"
                );
                tracing::Span::current().record("iterations", iteration - 1);
                break;
            }

            let blobs: Vec<Bytes> = batch_groups
                .iter()
                .map(|g| g.reassembled_blob.clone())
                .collect();
            let total_bytes: usize = batch_groups.iter().map(|g| g.total_bytes).sum();
            let total_rows: usize = batch_groups.iter().map(|g| g.rows as usize).sum();

            tracing::info!(
                signal_key = %signal_key,
                service = %service_name,
                iteration,
                batch_groups = batch_groups.len(),
                total_bytes,
                total_rows,
                "Flushing batch groups to R2"
            );

            // Validate and deserialize Arrow IPC batches
            let record_batches = self.validate_and_deserialize(&blobs)?;

            // Check actual Arrow memory size (can be larger than IPC blob size)
            let arrow_memory_size: usize = record_batches
                .iter()
                .map(|b| b.get_array_memory_size())
                .sum();

            // If Arrow memory exceeds limit, reduce batch count or handle oversized single batch
            let (record_batches, batch_groups, blobs, total_bytes, total_rows) =
                if arrow_memory_size > WASM_MAX_FLUSH_BYTES {
                    if batch_groups.len() > 1 {
                        tracing::warn!(
                            arrow_memory_size,
                            ipc_size = total_bytes,
                            batch_count = batch_groups.len(),
                            limit = WASM_MAX_FLUSH_BYTES,
                            "Arrow memory exceeds limit, reducing batch count"
                        );

                        // Process only the first batch group this iteration
                        let first_group = batch_groups.into_iter().next().unwrap();
                        let first_blob = blobs.into_iter().next().unwrap();
                        let first_batch = record_batches.into_iter().next().unwrap();

                        // Re-check if single batch still exceeds limit
                        let single_arrow_size = first_batch.get_array_memory_size();
                        if single_arrow_size > WASM_MAX_FLUSH_BYTES {
                            tracing::error!(
                                arrow_memory_size = single_arrow_size,
                                limit = WASM_MAX_FLUSH_BYTES,
                                group_id = %first_group.chunk_group_id,
                                "Single batch exceeds WASM memory limit, moving to DLQ"
                            );
                            // Move to DLQ and continue with next batch
                            let dlq_path = format!(
                                "failed/{}/{}/{}-{}.ipc",
                                signal_type_str,
                                service_name,
                                Date::now().as_millis(),
                                uuid::Uuid::new_v4()
                            );
                            let _ = self.write_dlq_batch(&dlq_path, &[first_blob]).await;
                            crate::r#do::storage::delete_batch_groups(&self.state, &[first_group])?;
                            continue; // Try next batch in loop
                        }

                        let new_total_bytes = first_group.total_bytes;
                        let new_total_rows = first_group.rows as usize;
                        (
                            vec![first_batch],
                            vec![first_group],
                            vec![first_blob],
                            new_total_bytes,
                            new_total_rows,
                        )
                    } else {
                        // Single batch exceeds limit - move to DLQ
                        tracing::error!(
                            arrow_memory_size,
                            ipc_size = total_bytes,
                            limit = WASM_MAX_FLUSH_BYTES,
                            "Single batch exceeds WASM memory limit, moving to DLQ"
                        );
                        let first_group = batch_groups.into_iter().next().unwrap();
                        let first_blob = blobs.into_iter().next().unwrap();
                        let dlq_path = format!(
                            "failed/{}/{}/{}-{}.ipc",
                            signal_type_str,
                            service_name,
                            Date::now().as_millis(),
                            uuid::Uuid::new_v4()
                        );
                        let _ = self.write_dlq_batch(&dlq_path, &[first_blob]).await;
                        crate::r#do::storage::delete_batch_groups(&self.state, &[first_group])?;
                        continue; // Try next batch in loop
                    }
                } else {
                    (record_batches, batch_groups, blobs, total_bytes, total_rows)
                };

            // Get first event timestamp
            let timestamp_micros = Date::now().as_millis() as i64 * 1000;
            let event_timestamp_micros = do_state
                .first_event_timestamp
                .filter(|ts| *ts > 0)
                .unwrap_or(timestamp_micros);

            // Build context for write (needed for DLQ fallback)
            let ctx = PendingBufferContext {
                blobs,
                bytes: total_bytes,
                rows: total_rows as i64,
                signal_type_str: signal_type_str.clone(),
                service_name: service_name.clone(),
            };

            // Collect group IDs for deletion/DLQ
            let group_ids: Vec<String> = batch_groups
                .iter()
                .map(|g| g.chunk_group_id.clone())
                .collect();

            // Write with retry logic and DLQ fallback
            let write_result = self
                .write_with_retries(
                    &record_batches,
                    &signal_key,
                    &namespace,
                    event_timestamp_micros,
                    ctx,
                    &group_ids,
                )
                .await;

            match write_result {
                Ok(path) => {
                    tracing::debug!(path = %path, rows = total_rows, "Wrote Parquet file");

                    // Forward receipt if catalog mode is Iceberg
                    self.forward_receipt_if_needed(&config, &path, &signal_key, total_rows)
                        .await?;

                    // Delete successfully flushed batch groups
                    crate::r#do::storage::delete_batch_groups(&self.state, &batch_groups)?;

                    // Emit usage metrics
                    self.emit_usage_metrics(&signal_key, total_bytes, total_rows);
                }
                Err(e) => {
                    // Batches remain in SQLite for retry (or were moved to DLQ)
                    // Break the loop and let the alarm retry later
                    tracing::error!(error = %e, iteration, "Flush iteration failed");
                    tracing::Span::current().record("error", tracing::field::display(&e));
                    tracing::Span::current().record("iterations", iteration);
                    return Err(e);
                }
            }
        }

        // Clear state after all batches successfully flushed
        let _ = crate::r#do::storage::clear_first_event_timestamp(&self.state);
        let _ = crate::r#do::storage::clear_pending_receipt(&self.state);

        Ok(())
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
            if let Err(e) = self.flush().await {
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

        match self.flush().await {
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
