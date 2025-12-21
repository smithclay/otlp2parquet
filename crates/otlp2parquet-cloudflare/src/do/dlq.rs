//! Dead letter queue for failed batch writes.

use bytes::Bytes;
use worker::{Date, Env, Result};

/// Write failed batches to dead-letter queue for manual recovery.
pub async fn write_dlq_batch(path: &str, blobs: &[Bytes]) -> Result<()> {
    use otlp2parquet_writer::get_operator_clone;

    let operator = get_operator_clone().ok_or_else(|| {
        worker::Error::RustError("Storage operator not initialized for DLQ write".to_string())
    })?;

    let mut dlq_data = Vec::new();
    dlq_data.extend_from_slice(b"OTLPIPC1");
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

/// Build the DLQ path for failed batches.
pub fn build_dlq_path(signal_type: &str, service_name: &str) -> String {
    format!(
        "failed/{}/{}/{}-{}.ipc",
        signal_type,
        service_name,
        Date::now().as_millis(),
        uuid::Uuid::new_v4()
    )
}

/// Emit DLQ failure metric for alerting.
pub fn emit_dlq_failure_metric(env: &Env, signal_type: &str, bytes: usize, rows: i64) {
    if let Ok(metrics) = env.analytics_engine("METRICS") {
        use worker::AnalyticsEngineDataPointBuilder;

        let _ = AnalyticsEngineDataPointBuilder::new()
            .indexes(["dlq_failure"])
            .add_blob(signal_type)
            .add_double(bytes as f64)
            .add_double(rows as f64)
            .add_double(1.0)
            .write_to(&metrics);
    }
}
