//! Table name mapping for OTLP signals

use crate::error::{Result, WriterError};
use once_cell::sync::OnceCell;
use otlp2parquet_core::SignalType;
use std::collections::HashMap;

static TABLE_NAME_OVERRIDES: OnceCell<HashMap<String, String>> = OnceCell::new();

/// Configure table name overrides from Iceberg configuration.
///
/// Keys use the same format as `IcebergConfig::to_tables_map()`:
/// - "logs", "traces"
/// - "metrics:gauge", "metrics:sum", "metrics:histogram",
///   "metrics:exponential_histogram", "metrics:summary"
pub fn set_table_name_overrides(map: HashMap<String, String>) {
    if map.is_empty() {
        return;
    }

    let _ = TABLE_NAME_OVERRIDES.set(map);
}

/// Get the table name for a given signal type and optional metric type
///
/// Returns the canonical table name used in Iceberg catalog
pub fn table_name_for_signal(signal: SignalType, metric_type: Option<&str>) -> Result<String> {
    let overrides = TABLE_NAME_OVERRIDES.get();

    match signal {
        SignalType::Logs => Ok(overrides
            .and_then(|m| m.get("logs"))
            .cloned()
            .unwrap_or_else(|| "otel_logs".to_string())),
        SignalType::Traces => Ok(overrides
            .and_then(|m| m.get("traces"))
            .cloned()
            .unwrap_or_else(|| "otel_traces".to_string())),
        SignalType::Metrics => match metric_type {
            Some("gauge") => Ok(overrides
                .and_then(|m| m.get("metrics:gauge"))
                .cloned()
                .unwrap_or_else(|| "otel_metrics_gauge".to_string())),
            Some("sum") => Ok(overrides
                .and_then(|m| m.get("metrics:sum"))
                .cloned()
                .unwrap_or_else(|| "otel_metrics_sum".to_string())),
            Some("histogram") => Ok(overrides
                .and_then(|m| m.get("metrics:histogram"))
                .cloned()
                .unwrap_or_else(|| "otel_metrics_histogram".to_string())),
            Some("exponential_histogram") => Ok(overrides
                .and_then(|m| m.get("metrics:exponential_histogram"))
                .cloned()
                .unwrap_or_else(|| "otel_metrics_exponential_histogram".to_string())),
            Some("summary") => Ok(overrides
                .and_then(|m| m.get("metrics:summary"))
                .cloned()
                .unwrap_or_else(|| "otel_metrics_summary".to_string())),
            _ => Err(WriterError::InvalidTableName {
                signal,
                metric_type: metric_type.map(String::from),
            }),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_name_for_signal() {
        assert_eq!(
            table_name_for_signal(SignalType::Logs, None).unwrap(),
            "otel_logs"
        );
        assert_eq!(
            table_name_for_signal(SignalType::Traces, None).unwrap(),
            "otel_traces"
        );
        assert_eq!(
            table_name_for_signal(SignalType::Metrics, Some("gauge")).unwrap(),
            "otel_metrics_gauge"
        );
        assert_eq!(
            table_name_for_signal(SignalType::Metrics, Some("sum")).unwrap(),
            "otel_metrics_sum"
        );
        assert_eq!(
            table_name_for_signal(SignalType::Metrics, Some("histogram")).unwrap(),
            "otel_metrics_histogram"
        );
        assert_eq!(
            table_name_for_signal(SignalType::Metrics, Some("exponential_histogram")).unwrap(),
            "otel_metrics_exponential_histogram"
        );
        assert_eq!(
            table_name_for_signal(SignalType::Metrics, Some("summary")).unwrap(),
            "otel_metrics_summary"
        );
    }

    #[test]
    fn test_table_name_for_signal_error() {
        // Test that unknown metric type returns error instead of panicking
        assert!(table_name_for_signal(SignalType::Metrics, None).is_err());
        assert!(table_name_for_signal(SignalType::Metrics, Some("unknown")).is_err());
    }
}
