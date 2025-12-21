//! Flush orchestration for batched OTLP data.
//!
//! Handles the entire flush lifecycle:
//! 1. Retry pending receipts
//! 2. Load batches from SQLite in memory-limited chunks
//! 3. Validate and deserialize Arrow IPC
//! 4. Write Parquet to R2 with retry logic
//! 5. Handle DLQ fallback for persistent failures
//! 6. Forward receipts for catalog tracking
//! 7. Emit usage metrics

use crate::r#do::config::{MAX_WRITE_RETRIES, WASM_MAX_FLUSH_BYTES};
use crate::r#do::types::PendingBufferContext;
use bytes::Bytes;
use otlp2parquet_core::batch::ipc::{deserialize_batches, validate_ipc_header};
use otlp2parquet_core::config::{CatalogMode, RuntimeConfig};
use otlp2parquet_core::SignalKey;
use otlp2parquet_writer::{write_multi_batch, WriteMultiBatchRequest};
use std::cell::RefCell;
use std::str::FromStr;
use worker::{Date, Env, Result, State};

/// Flush batches to R2 storage in chunks up to WASM_MAX_FLUSH_BYTES.
///
/// Uses partial flush pattern: loads oldest batch groups up to memory limit,
/// writes them, deletes them, and loops until all batches are flushed.
#[tracing::instrument(
    name = "do.flush",
    skip(state, env),
    fields(
        signal_key = tracing::field::Empty,
        service_name = tracing::field::Empty,
        total_bytes = tracing::field::Empty,
        total_rows = tracing::field::Empty,
        iterations = tracing::field::Empty,
        error = tracing::field::Empty,
    )
)]
pub async fn flush(
    state: &State,
    env: &Env,
    config: &RuntimeConfig,
    write_retry_count: &RefCell<u32>,
) -> Result<()> {
    let (initial_bytes, initial_rows) = crate::r#do::storage::get_batch_totals(state)?;
    worker::console_log!(
        "[DO] flush() called: pending_bytes={}, pending_rows={}",
        initial_bytes,
        initial_rows
    );

    // Retry any previously failed receipt first
    crate::r#do::receipts::retry_pending_receipt(state, env).await?;

    // Get DO state for signal/service info
    let do_state = crate::r#do::storage::get_do_state(state)?;
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
            crate::r#do::storage::load_batches_for_flush(state, WASM_MAX_FLUSH_BYTES)?;
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
        let record_batches = validate_and_deserialize(&blobs)?;

        // Check actual Arrow memory size (can be larger than IPC blob size)
        let arrow_memory_size: usize = record_batches
            .iter()
            .map(|b| b.get_array_memory_size())
            .sum();

        // If Arrow memory exceeds limit, reduce batch count or handle oversized single batch
        let (record_batches, batch_groups, blobs, total_bytes, total_rows) = if arrow_memory_size
            > WASM_MAX_FLUSH_BYTES
        {
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
                    let dlq_path =
                        crate::r#do::dlq::build_dlq_path(&signal_type_str, &service_name);
                    let _ = crate::r#do::dlq::write_dlq_batch(&dlq_path, &[first_blob]).await;
                    crate::r#do::storage::delete_batch_groups(state, &[first_group])?;
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
                let dlq_path = crate::r#do::dlq::build_dlq_path(&signal_type_str, &service_name);
                let _ = crate::r#do::dlq::write_dlq_batch(&dlq_path, &[first_blob]).await;
                crate::r#do::storage::delete_batch_groups(state, &[first_group])?;
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
        let write_result = write_with_retries(
            state,
            env,
            &record_batches,
            &signal_key,
            &namespace,
            event_timestamp_micros,
            ctx,
            &group_ids,
            write_retry_count,
        )
        .await;

        match write_result {
            Ok(path) => {
                tracing::debug!(path = %path, rows = total_rows, "Wrote Parquet file");

                // Forward receipt if catalog mode is Iceberg
                forward_receipt_if_needed(state, env, config, &path, &signal_key, total_rows)
                    .await?;

                // Delete successfully flushed batch groups
                crate::r#do::storage::delete_batch_groups(state, &batch_groups)?;

                // Emit usage metrics
                emit_usage_metrics(env, &signal_key, total_bytes, total_rows);
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
    let _ = crate::r#do::storage::clear_first_event_timestamp(state);
    let _ = crate::r#do::storage::clear_pending_receipt(state);

    Ok(())
}

/// Validate IPC headers and deserialize Arrow batches.
///
/// Note: On failure, batches remain in SQLite (not deleted) since we use
/// the load-then-delete-on-success pattern.
fn validate_and_deserialize(blobs: &[Bytes]) -> Result<Vec<arrow::record_batch::RecordBatch>> {
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
    skip(state, record_batches, ctx, group_ids, write_retry_count),
    fields(
        signal_key = %signal_key,
        namespace,
        rows = record_batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        error = tracing::field::Empty,
    )
)]
async fn write_with_retries(
    state: &State,
    env: &Env,
    record_batches: &[arrow::record_batch::RecordBatch],
    signal_key: &SignalKey,
    namespace: &str,
    event_timestamp_micros: i64,
    ctx: PendingBufferContext,
    group_ids: &[String],
    write_retry_count: &RefCell<u32>,
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
            *write_retry_count.borrow_mut() = 0;
            Ok(path)
        }
        Err(e) => {
            let current_retries = {
                let mut retry_count = write_retry_count.borrow_mut();
                *retry_count += 1;
                *retry_count
            };

            if current_retries >= MAX_WRITE_RETRIES {
                handle_dlq_fallback(
                    state,
                    env,
                    &e,
                    current_retries,
                    &ctx,
                    group_ids,
                    write_retry_count,
                )
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
    state: &State,
    env: &Env,
    original_error: &otlp2parquet_writer::WriterError,
    retry_count: u32,
    ctx: &PendingBufferContext,
    group_ids: &[String],
    write_retry_count: &RefCell<u32>,
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

    let dlq_path = crate::r#do::dlq::build_dlq_path(&ctx.signal_type_str, &ctx.service_name);

    let dlq_success = match crate::r#do::dlq::write_dlq_batch(&dlq_path, &ctx.blobs).await {
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
            crate::r#do::dlq::emit_dlq_failure_metric(
                env,
                &ctx.signal_type_str,
                ctx.bytes,
                ctx.rows,
            );
            false
        }
    };

    // Only delete batch groups if DLQ write succeeded
    // If DLQ failed, keep batches in SQLite for future retry
    if dlq_success {
        // Delete groups by constructing temporary BatchGroup structs with just the IDs
        let groups_to_delete: Vec<crate::r#do::types::BatchGroup> = group_ids
            .iter()
            .map(|id| crate::r#do::types::BatchGroup {
                chunk_group_id: id.clone(),
                reassembled_blob: Bytes::new(),
                total_bytes: 0,
                rows: 0,
            })
            .collect();
        let _ = crate::r#do::storage::delete_batch_groups(state, &groups_to_delete);
    }

    *write_retry_count.borrow_mut() = 0;
    let _ = crate::r#do::storage::clear_first_event_timestamp(state);

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
    state: &State,
    env: &Env,
    config: &RuntimeConfig,
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
    let receipt = crate::r#do::types::PendingReceipt {
        path,
        table: &table_name,
        rows: total_rows,
        timestamp_ms: Date::now().as_millis() as i64,
    };

    tracing::debug!(path = %path, table = %table_name, "Sending receipt to Worker");
    if let Err(e) = crate::r#do::receipts::send_receipt_to_worker(env, &receipt).await {
        tracing::error!(error = %e, path = %path, "Receipt forwarding failed");
        crate::r#do::receipts::store_pending_receipt(state, path, table_name, total_rows)?;
        let _ = crate::r#do::storage::clear_first_event_timestamp(state);
        return Err(worker::Error::RustError(format!(
            "Receipt forwarding failed: {} (path={})",
            e, path
        )));
    }
    tracing::debug!("Receipt sent successfully");
    Ok(())
}

/// Emit usage metrics to Analytics Engine (fire-and-forget).
pub fn emit_usage_metrics(env: &Env, signal_key: &SignalKey, bytes: usize, rows: usize) {
    if let Ok(metrics) = env.analytics_engine("METRICS") {
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
