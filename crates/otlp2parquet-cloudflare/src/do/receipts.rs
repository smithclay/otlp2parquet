//! Receipt forwarding to main Worker for KV tracking.

use crate::r#do::storage;
use crate::r#do::types::{PendingReceipt, PendingReceiptOwned};
use worker::{Env, Headers, Method, Request, RequestInit, Result, State};

/// Send a receipt to the main Worker for KV persistence.
#[tracing::instrument(
    name = "do.send_receipt",
    skip(env, receipt),
    fields(
        path = %receipt.path,
        table = %receipt.table,
        rows = receipt.rows,
        error = tracing::field::Empty,
    )
)]
pub async fn send_receipt_to_worker(env: &Env, receipt: &PendingReceipt<'_>) -> Result<()> {
    tracing::debug!("send_receipt_to_worker: getting SELF service binding");

    let service = env.service("SELF").map_err(|e| {
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
pub async fn retry_pending_receipt(state: &State, env: &Env) -> Result<()> {
    if let Some(pending) = storage::take_pending_receipt(state)? {
        let path = pending.path.clone();
        let table = pending.table.clone();
        let receipt = PendingReceipt {
            path: &path,
            table: &table,
            rows: pending.rows,
            timestamp_ms: pending.timestamp_ms,
        };

        if let Err(e) = send_receipt_to_worker(env, &receipt).await {
            storage::set_pending_receipt(state, &pending)?;
            return Err(worker::Error::RustError(format!(
                "Receipt forwarding failed: {} (path={})",
                e, path
            )));
        }
    }
    Ok(())
}

/// Store a pending receipt for retry on next alarm.
pub fn store_pending_receipt(state: &State, path: &str, table: String, rows: usize) -> Result<()> {
    let pending = PendingReceiptOwned {
        path: path.to_string(),
        table,
        rows,
        timestamp_ms: worker::Date::now().as_millis() as i64,
    };
    storage::set_pending_receipt(state, &pending)
}
